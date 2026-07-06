# Enterprise RBAC, Role Ownership, and FoundationDB Roadmap

> Status: planning checkpoint before implementation.  
> Scope: nova-core role-based access control, object ownership, privileges, auth/session integration, FDB persistence, and Dynamic Table security.  
> Goal: Snowflake-inspired security model with Nova-specific implementation constraints.

---

## 0. Executive Summary

Nova Core should move from the current prototype RBAC model to a FoundationDB-backed enterprise authorization system.

Core decision:

```text
Objects are owned by roles, not users.
Privileges are granted to roles.
Users receive one or more roles.
A session executes using one primary role plus optional secondary roles.
The owner role is the superuser for the object it owns.
FoundationDB is the source of truth for users, roles, grants, ownership, and security metadata.
```

This matches the important Snowflake patterns while staying practical for Nova Core:

- RBAC: roles receive privileges; users receive roles.
- DAC/ownership: every securable object has exactly one owner role.
- Parent visibility: object access requires `USAGE` on parent containers.
- Dynamic Tables: background refresh executes as the dynamic table owner role.
- FDB: all security state is persistent, atomic, and shared by all coordinators.

---

## 1. Current State and Gaps

Current code has a basic in-memory RBAC prototype:

```text
crates/nova-coordinator/src/rbac.rs
crates/nova-coordinator/src/auth.rs
crates/nova-coordinator/src/executor.rs
crates/nova-coordinator/src/mysql_protocol/server.rs
crates/nova-storage/src/metadata/fdb_store.rs
```

Current gaps:

| Area | Current State | Enterprise Requirement |
|---|---|---|
| User store | In-memory `AuthManager.users` | FDB-backed `UserMeta` |
| Role store | In-memory `RbacManager.roles` | FDB-backed `RoleMeta` |
| Grants | In-memory table-name string map | FDB object-type + object-id grants |
| Object owner | Hardcoded / partial user owner | Role owner for every object |
| Session auth | Username only | `SecurityContext` with user + role context |
| Executor user | Global `current_user` field | Per-query/session security context |
| SELECT checks | Missing | Required for table and Dynamic Table reads |
| DB/schema checks | Missing/partial | Parent `USAGE` and create privileges |
| Dynamic Table checks | Missing | Owner-role refresh + dependency privileges |
| Multi-coordinator | Not shared | FDB source of truth |
| Cache invalidation | Not security-aware | Security epoch |

Do not treat current Phase 6 RBAC as enterprise-ready; it is a prototype and must be replaced/wired through FDB.

---

## 2. Design Principles

### 2.1 Deny by default

If no active role proves access, deny.

```text
No grant + not owner + not admin = access denied
```

### 2.2 Role owns objects

When a user creates an object, ownership is assigned to the session primary role.

```sql
USE ROLE data_engineer;
CREATE TABLE sales.public.orders (...);
```

Result:

```text
object_owner(TABLE, orders_id) = data_engineer_role_id
created_by_user_id = alice_user_id
```

### 2.3 Owner is object superuser

The owner role can perform all operations on its object:

```text
SELECT / INSERT / UPDATE / DELETE / ALTER / DROP / GRANT / REVOKE / TRANSFER OWNERSHIP
```

For safety, parent container visibility should still be required for name resolution unless the role has account-level admin privileges.

### 2.4 Privileges are role grants

Users do not get object privileges directly in V1.

```text
User -> Roles -> Privileges -> Objects
```

This keeps the model simpler and closer to the user's requested role-first design.

### 2.5 Parent `USAGE` is required

To operate on `database.schema.object`, a role must see the containers:

```text
USAGE on DATABASE
USAGE on SCHEMA
specific privilege or ownership on object
```

### 2.6 CREATE uses primary role only

Object creation and new ownership use the primary role only. Secondary roles can help with non-CREATE reads/writes later, but the created object owner is always the current primary role.

### 2.7 Dynamic Table refresh uses owner role

A Dynamic Table is a background object. Refresh must never run as admin by accident.

```text
refresh context = SYSTEM user + dynamic_table.owner_role_id + no secondary roles
```

If the owner role loses `SELECT`/`USAGE` on source dependencies, refresh fails.

### 2.8 FDB is the security source of truth

No in-memory-only security state. Caches are allowed only as derived state keyed by security epoch.

---

## 3. Securable Objects

V1 object types:

```rust
pub enum ObjectType {
    Account,
    Database,
    Schema,
    Table,
    DynamicTable,
    Stream,
    Role,
    User,
}
```

Future object types:

```text
Warehouse
Stage
Function
View
MaterializedView
Task
Policy
```

Object identity must be stable and numeric:

```text
ObjectRef = (object_type, object_id)
```

Never use table name strings as privilege keys.

---

## 4. Privilege Model

### 4.1 Account privileges

| Privilege | Purpose |
|---|---|
| `CREATE_DATABASE` | Create databases |
| `CREATE_ROLE` | Create roles |
| `CREATE_USER` | Create users |
| `MANAGE_GRANTS` | Grant/revoke on any object as owner |
| `OWNERSHIP` | Account-level administration |

### 4.2 Database privileges

| Privilege | Purpose |
|---|---|
| `USAGE` | See/resolve database |
| `CREATE_SCHEMA` | Create schema in database |
| `MONITOR` | View database metadata/usage |
| `MODIFY` | Change database properties |
| `OWNERSHIP` | Full database control |

### 4.3 Schema privileges

| Privilege | Purpose |
|---|---|
| `USAGE` | See/resolve schema |
| `CREATE_TABLE` | Create tables in schema |
| `CREATE_DYNAMIC_TABLE` | Create Dynamic Tables in schema |
| `CREATE_STREAM` | Create streams in schema |
| `MONITOR` | View schema metadata |
| `MODIFY` | Change schema properties |
| `OWNERSHIP` | Full schema control |

### 4.4 Table privileges

| Privilege | Purpose |
|---|---|
| `SELECT` | Read table data |
| `INSERT` | Insert rows / MPs |
| `UPDATE` | Update rows via COW |
| `DELETE` | Delete rows via COW |
| `REFERENCES` | Use as dependency / FK / metadata reference |
| `MONITOR` | View table stats/metadata |
| `OWNERSHIP` | Full table control including drop/alter/grants |

`DROP TABLE` should require `OWNERSHIP`, not a standalone `DROP` privilege.

### 4.5 Dynamic Table privileges

| Privilege | Purpose |
|---|---|
| `SELECT` | Query dynamic table output |
| `OPERATE` | Refresh, suspend, resume, target lag changes |
| `MONITOR` | View refresh status/history |
| `OWNERSHIP` | Drop, alter definition, transfer ownership |

### 4.6 Role privileges

| Privilege | Purpose |
|---|---|
| `OWNERSHIP` | Modify role, grant role, transfer role ownership |

---

## 5. Authorization Matrix

| Operation | Required privileges |
|---|---|
| `CREATE DATABASE db` | `CREATE_DATABASE` on account |
| `DROP DATABASE db` | `OWNERSHIP` on database |
| `CREATE SCHEMA db.s` | `USAGE` + `CREATE_SCHEMA` on database |
| `DROP SCHEMA db.s` | `OWNERSHIP` on schema |
| `CREATE TABLE db.s.t` | `USAGE` on db + `USAGE` on schema + `CREATE_TABLE` on schema |
| `SELECT FROM db.s.t` | `USAGE` on db + `USAGE` on schema + `SELECT` on table or table owner |
| `INSERT INTO db.s.t` | parent `USAGE` + `INSERT` on table or owner |
| `UPDATE db.s.t` | parent `USAGE` + `UPDATE` on table or owner |
| `DELETE FROM db.s.t` | parent `USAGE` + `DELETE` on table or owner |
| `ALTER TABLE db.s.t` | parent `USAGE` + `OWNERSHIP` on table |
| `DROP TABLE db.s.t` | parent `USAGE` + `OWNERSHIP` on table |
| `CREATE DYNAMIC TABLE db.s.dt AS ...` | parent `USAGE` + `CREATE_DYNAMIC_TABLE` on schema + `SELECT` on all dependencies |
| `SELECT FROM db.s.dt` | parent `USAGE` + `SELECT` on Dynamic Table or owner |
| `ALTER DYNAMIC TABLE dt REFRESH/SUSPEND/RESUME` | parent `USAGE` + `OPERATE` or `OWNERSHIP` on Dynamic Table |
| `SHOW DYNAMIC TABLES` | Show only objects with `SELECT`, `MONITOR`, `OPERATE`, or `OWNERSHIP` |
| `DROP DYNAMIC TABLE dt` | parent `USAGE` + `OWNERSHIP` on Dynamic Table |
| `GRANT privilege ON object TO ROLE r` | object owner or `MANAGE_GRANTS` |
| `REVOKE privilege ON object FROM ROLE r` | object owner or `MANAGE_GRANTS` |
| `GRANT OWNERSHIP ON object TO ROLE r` | current owner or `MANAGE_GRANTS`; transfer rules apply |
| `GRANT ROLE r TO USER u` | owner of role or `MANAGE_GRANTS` / security admin |
| `USE ROLE r` | role is granted to user |

---

## 6. Session and Security Context

### 6.1 Session state

Extend MySQL session state:

```rust
pub struct Session {
    pub user_id: UserId,
    pub username: String,
    pub current_db: String,
    pub primary_role_id: RoleId,
    pub secondary_roles: SecondaryRoles,
    // existing fields...
}
```

```rust
pub enum SecondaryRoles {
    None,
    All,
    Explicit(Vec<RoleId>),
}
```

### 6.2 Query security context

Every query receives a context:

```rust
pub struct SecurityContext {
    pub user_id: UserId,
    pub username: String,
    pub primary_role_id: RoleId,
    pub secondary_roles: SecondaryRoles,
}
```

`Executor` must not store global current user state. Global `current_user` is unsafe across concurrent MySQL sessions.

### 6.3 QueryEngine trait change

Current shape:

```rust
async fn execute_sql(&self, sql: &str, current_db: &str) -> Result<QueryResult>;
```

Required shape:

```rust
async fn execute_sql(
    &self,
    sql: &str,
    current_db: &str,
    security: SecurityContext,
) -> Result<QueryResult>;
```

---

## 7. FoundationDB Role

FDB is the metadata and security authority. It stores security state, not table rows.

```text
S3 / MinIO = immutable Parquet micro-partition data
FDB        = metadata, transactions, user/role/grant/owner security state
```

FDB answers:

```text
Who is this user?
Which roles does the user have?
Which role is active?
Who owns this object?
Which role has which privilege on this object?
Which objects can be listed?
Can a Dynamic Table refresh using its owner role?
```

Why FDB is required:

- Persistent across restart.
- Shared by all coordinators.
- ACID transactions across multiple keys.
- Atomic grant/revoke/ownership transfer.
- Range scans for metadata listing and `SHOW GRANTS`.
- Strong consistency for security decisions.

---

## 8. FDB Key Schema

All keys remain under the Nova subspace.

### 8.1 Users

```text
("user", user_id)
  -> UserMeta {
       id,
       name,
       password_hash,
       mysql_native_hash,
       default_role_id,
       disabled,
       created_at,
       created_by_user_id,
       created_by_role_id,
       comment
     }

("user_by_name", normalized_username)
  -> user_id
```

### 8.2 Roles

```text
("role", role_id)
  -> RoleMeta {
       id,
       name,
       owner_role_id,
       system,
       created_at,
       created_by_user_id,
       comment
     }

("role_by_name", normalized_role_name)
  -> role_id
```

Bootstrap roles:

```text
ACCOUNTADMIN
SECURITYADMIN
SYSADMIN
USERADMIN
PUBLIC
```

V1 may enforce only `ACCOUNTADMIN` and `PUBLIC`, but reserve all names.

### 8.3 User-role grants

```text
("user_role", user_id, role_id)
  -> RoleGrantMeta {
       granted_by_role_id,
       created_at
     }

("role_user", role_id, user_id)
  -> ()
```

### 8.4 Role hierarchy grants

Phase 2:

```text
("role_inherits", parent_role_id, child_role_id)
  -> RoleGrantMeta

("role_parent", child_role_id, parent_role_id)
  -> ()
```

Meaning:

```text
parent role inherits child role privileges
```

Cycle detection is mandatory before writing.

### 8.5 Object ownership

Store ownership in separate keys first to avoid bincode metadata migration risk.

```text
("object_owner", object_type, object_id)
  -> ObjectOwnerMeta {
       object_type,
       object_id,
       owner_role_id,
       created_by_user_id,
       created_at,
       transferred_at
     }
```

Existing objects missing this key are treated as owned by `ACCOUNTADMIN` until migrated.

### 8.6 Privilege grants

```text
("grant", role_id, object_type, object_id)
  -> GrantSet {
       privilege_bits,
       grant_option_bits,
       granted_by_role_id,
       updated_at
     }

("grant_by_object", object_type, object_id, role_id)
  -> privilege_bits
```

`privilege_bits` should be a compact `u64` bitmask.

### 8.7 Dynamic Table dependencies

```text
("dynamic_table_dep", dt_id, object_type, object_id)
  -> DtDependencyMeta {
       object_type,
       object_id,
       required_privilege: SELECT
     }

("dynamic_table_dep_by_object", object_type, object_id, dt_id)
  -> ()
```

### 8.8 Security epoch

```text
("security_epoch") -> u64
```

Increment on:

```text
CREATE/DROP USER
CREATE/DROP ROLE
GRANT/REVOKE ROLE
GRANT/REVOKE privilege
GRANT OWNERSHIP
object owner changes
```

Used to invalidate authorization caches.

### 8.9 Audit log

Phase 2/3:

```text
("security_audit", timestamp, event_id)
  -> SecurityAuditEvent {
       actor_user_id,
       actor_primary_role_id,
       action,
       object_type,
       object_id,
       target_role_id,
       success,
       error
     }
```

---

## 9. Required MetadataStore API

Add FDB-backed methods to `MetadataStore` or a dedicated `SecurityStore` trait.

Preferred: keep `MetadataStore` from becoming too large by adding `SecurityStore` implemented by `FdbMetadataStore`.

```rust
#[async_trait]
pub trait SecurityStore: Send + Sync {
    async fn bootstrap_security(&self) -> Result<()>;

    async fn create_user(&self, user: UserMeta) -> Result<UserId>;
    async fn get_user(&self, user_id: UserId) -> Result<Option<UserMeta>>;
    async fn get_user_by_name(&self, name: &str) -> Result<Option<UserMeta>>;

    async fn create_role(&self, role: RoleMeta) -> Result<RoleId>;
    async fn get_role(&self, role_id: RoleId) -> Result<Option<RoleMeta>>;
    async fn get_role_by_name(&self, name: &str) -> Result<Option<RoleMeta>>;

    async fn grant_role_to_user(&self, user_id: UserId, role_id: RoleId, granted_by: RoleId) -> Result<()>;
    async fn revoke_role_from_user(&self, user_id: UserId, role_id: RoleId) -> Result<()>;
    async fn list_user_roles(&self, user_id: UserId) -> Result<Vec<RoleId>>;

    async fn set_object_owner(&self, owner: ObjectOwnerMeta) -> Result<()>;
    async fn get_object_owner(&self, object: ObjectRef) -> Result<Option<ObjectOwnerMeta>>;
    async fn transfer_object_owner(&self, object: ObjectRef, new_owner: RoleId, mode: TransferMode) -> Result<()>;

    async fn grant_privileges(&self, grant: GrantSetMeta) -> Result<()>;
    async fn revoke_privileges(&self, role_id: RoleId, object: ObjectRef, privileges: PrivilegeSet) -> Result<()>;
    async fn get_grant(&self, role_id: RoleId, object: ObjectRef) -> Result<Option<GrantSet>>;
    async fn list_grants_on_object(&self, object: ObjectRef) -> Result<Vec<GrantSetMeta>>;
    async fn list_grants_to_role(&self, role_id: RoleId) -> Result<Vec<GrantSetMeta>>;

    async fn security_epoch(&self) -> Result<u64>;
    async fn bump_security_epoch(&self) -> Result<u64>;
}
```

---

## 10. Authorization Engine

Introduce a dedicated authorizer:

```rust
pub struct Authorizer<S: SecurityStore> {
    store: Arc<S>,
    cache: AuthzCache,
}
```

Main methods:

```rust
check_account(ctx, privilege)
check_database(ctx, db_id, privilege)
check_schema(ctx, db_id, schema_id, privilege)
check_table(ctx, db_id, schema_id, table_id, privilege)
check_dynamic_table(ctx, db_id, schema_id, dt_id, privilege)
can_grant(ctx, object, privileges)
visible_databases(ctx)
visible_tables(ctx, db_id, schema_id)
```

Privilege algorithm:

```text
1. Resolve active roles from session context.
2. Include inherited roles if role hierarchy is enabled.
3. If active role owns object: allow object privilege.
4. If active role has account admin / MANAGE_GRANTS for grant operation: allow.
5. Check explicit grants for active roles.
6. Otherwise deny.
```

Table operation algorithm:

```text
1. Check USAGE on parent database.
2. Check USAGE on parent schema.
3. Check privilege on table or ownership.
```

---

## 11. SQL Surface Roadmap

### 11.1 Phase 1 SQL

```sql
CREATE ROLE analyst;
DROP ROLE analyst;

CREATE USER alice PASSWORD = 'secret' DEFAULT_ROLE = analyst;
DROP USER alice;

GRANT ROLE analyst TO USER alice;
REVOKE ROLE analyst FROM USER alice;

GRANT USAGE ON DATABASE sales TO ROLE analyst;
GRANT USAGE ON SCHEMA sales.public TO ROLE analyst;
GRANT SELECT ON TABLE sales.public.orders TO ROLE analyst;
GRANT INSERT, UPDATE ON TABLE sales.public.orders TO ROLE engineer;

REVOKE SELECT ON TABLE sales.public.orders FROM ROLE analyst;

USE ROLE analyst;
USE SECONDARY ROLES ALL;
USE SECONDARY ROLES NONE;

SHOW ROLES;
SHOW USERS;
SHOW GRANTS TO ROLE analyst;
SHOW GRANTS ON TABLE sales.public.orders;
```

### 11.2 Phase 2 SQL

```sql
GRANT ROLE reader TO ROLE analyst;
REVOKE ROLE reader FROM ROLE analyst;

GRANT OWNERSHIP ON TABLE sales.public.orders TO ROLE data_owner COPY CURRENT GRANTS;
GRANT OWNERSHIP ON TABLE sales.public.orders TO ROLE data_owner REVOKE CURRENT GRANTS;

GRANT SELECT ON ALL TABLES IN SCHEMA sales.public TO ROLE analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA sales.public TO ROLE analyst;
```

### 11.3 Dynamic Table SQL

```sql
GRANT CREATE DYNAMIC TABLE ON SCHEMA sales.public TO ROLE transform;
GRANT SELECT ON DYNAMIC TABLE sales.public.dt_orders TO ROLE analyst;
GRANT OPERATE ON DYNAMIC TABLE sales.public.dt_orders TO ROLE pipeline_admin;
GRANT MONITOR ON DYNAMIC TABLE sales.public.dt_orders TO ROLE ops;
GRANT OWNERSHIP ON DYNAMIC TABLE sales.public.dt_orders TO ROLE transform_owner;
```

---

## 12. Executor Integration

Every `ResolvedStatement` execution should receive `SecurityContext`.

Required checks:

| ResolvedStatement | Check |
|---|---|
| `CreateDatabase` | account `CREATE_DATABASE` |
| `CreateTable` | db/schema `USAGE`, schema `CREATE_TABLE` |
| `Select` | db/schema `USAGE`, object `SELECT`; all join tables too |
| `Insert` | db/schema `USAGE`, table `INSERT` |
| `Update` | db/schema `USAGE`, table `UPDATE` |
| `Delete` | db/schema `USAGE`, table `DELETE` |
| `AlterTable` | table `OWNERSHIP` |
| `DropTable` | table `OWNERSHIP` |
| `DropDatabase` | database `OWNERSHIP` |
| `DropSchema` | schema `OWNERSHIP` |
| `CreateClone` | source `SELECT`, target schema `CREATE_TABLE` |
| `CreateStream` | table `SELECT`/`REFERENCES`, schema `CREATE_STREAM` |
| `CreateDynamicTable` | schema `CREATE_DYNAMIC_TABLE`, base object `SELECT` |
| `RefreshDynamicTable` | Dynamic Table `OPERATE` or owner |
| `Suspend/ResumeDynamicTable` | Dynamic Table `OPERATE` or owner |
| `DropDynamicTable` | Dynamic Table `OWNERSHIP` |
| `ShowDynamicTables` | filter by visibility |
| `Gc` | account admin / maintenance privilege |
| `Backup/Restore` | account admin |

DataFusion path must register only tables authorized for the query. JOIN dependency extraction must not rely only on string scan long term; use sqlparser AST walking when practical.

---

## 13. Dynamic Table Security Design

### 13.1 Create flow

```text
1. Resolve target database and schema.
2. Check USAGE on database.
3. Check USAGE on schema.
4. Check CREATE_DYNAMIC_TABLE on schema.
5. Parse SELECT query and resolve dependencies.
6. Check SELECT on each base table/Dynamic Table dependency.
7. Create hidden output table.
8. Store DynamicTableMeta.
9. Store object_owner for Dynamic Table and output table.
10. Store dependency keys.
11. If INITIALIZE = ON_CREATE, refresh using owner role context.
```

### 13.2 Refresh flow

```text
1. Load DynamicTableMeta.
2. Load owner role from object_owner.
3. Build SecurityContext: SYSTEM user + owner role + secondary NONE.
4. Check dependencies from dynamic_table_dep keys.
5. Execute query using only authorized dependencies.
6. Write new output MPs.
7. Update refresh status.
```

If a dependency grant is revoked after creation:

```text
refresh_status = Failed { error: "owner role lacks SELECT on ..." }
```

### 13.3 Query flow

Querying a Dynamic Table requires:

```text
USAGE on database
USAGE on schema
SELECT on Dynamic Table or owner role
```

`GRANT SELECT ON TABLE ...` must not grant access to a Dynamic Table. It is a separate object type.

---

## 14. FDB Transaction Patterns

### 14.1 Create table with owner

Single FDB transaction should write:

```text
("table", db_id, schema_id, table_id) -> TableMeta
("table_by_name", db_id, schema_id, normalized_name) -> table_id
("object_owner", TABLE, table_id) -> owner_role_id
("table_version", table_id) -> 0
```

### 14.2 Grant privilege

Single FDB transaction should:

```text
1. Verify object exists.
2. Verify target role exists.
3. Verify grantor owns object or has MANAGE_GRANTS.
4. Upsert grant bitset.
5. Upsert grant_by_object index.
6. Increment security_epoch.
7. Write audit event.
```

### 14.3 Revoke privilege

Single FDB transaction should:

```text
1. Verify revoke authority.
2. Remove bits from grant bitset.
3. Delete key if no privileges remain.
4. Update grant_by_object index.
5. Increment security_epoch.
6. Write audit event.
```

### 14.4 Transfer ownership

Single FDB transaction should:

```text
1. Verify current role owns object or has MANAGE_GRANTS.
2. Verify target role exists.
3. If COPY CURRENT GRANTS: keep outbound grants and set grantor to new owner.
4. If REVOKE CURRENT GRANTS: delete outbound grant keys.
5. Update object_owner.
6. Increment security_epoch.
7. Write audit event.
```

V1 can require explicit `COPY CURRENT GRANTS` or `REVOKE CURRENT GRANTS` when outbound grants exist.

---

## 15. Bootstrap and Admin Model

On empty FDB:

```text
1. Create ACCOUNTADMIN role.
2. Create PUBLIC role.
3. Create root user.
4. Grant ACCOUNTADMIN to root.
5. Grant PUBLIC to root.
6. Set root default role = ACCOUNTADMIN.
7. Grant PUBLIC automatically to every new user.
```

Optional system role hierarchy:

```text
ACCOUNTADMIN inherits SECURITYADMIN
ACCOUNTADMIN inherits SYSADMIN
SECURITYADMIN inherits USERADMIN
```

V1 can skip hierarchy but reserve names.

---

## 16. Caching Strategy

V1 can start without authorization cache.

When needed:

```text
AuthzCache key = (security_epoch, active_roles_hash, object_type, object_id, privilege)
AuthzCache value = allow/deny
```

Rules:

```text
Any security mutation increments security_epoch.
Cache entry is valid only for current epoch.
Deny cache should be short-lived if explicit epoch fetch is expensive.
```

Never cache across epoch changes.

---

## 17. Migration Strategy

### 17.1 Fix object ID allocation first

FDB `create_database`, `create_schema`, `create_table`, `create_dynamic_table`, and related methods must assign stable IDs when input ID is `0`.

Current ID `0` behavior must be fixed before object-id grants are reliable.

### 17.2 Add owner side table

Do not immediately mutate all existing bincode metadata structs. Add separate `object_owner` keys.

Missing owner fallback:

```text
if object_owner missing: owner = ACCOUNTADMIN
```

### 17.3 Replace in-memory Auth/RBAC

Move auth and RBAC state to FDB-backed stores. Keep in-memory maps only for short-lived caches.

### 17.4 Filter metadata lists

`SHOW DATABASES`, `SHOW TABLES`, and `SHOW DYNAMIC TABLES` must filter by current role visibility.

---

## 18. Enterprise Implementation Phases

### Phase 0 — FDB object ID correctness

Goal: every securable object has stable unique ID.

Checklist:

- [x] Add atomic ID allocation for database/schema/table/dynamic_table/user/role.
- [x] Add name indexes where missing.
- [x] Prevent duplicate names in same scope.
- [x] Reopen tests prove IDs persist.
- [x] Existing ID `0` behavior no longer creates collisions.

Acceptance tests:

- [x] Create two databases; IDs differ and survive reopen.
- [x] Create two tables in same schema; IDs differ and survive reopen.
- [x] Duplicate table name fails.

### Phase 1 — FDB-backed security metadata

Goal: persistent users, roles, grants, and object owners.

Status: ✅ Complete — implemented in `nova-common` types, `SecurityStore`, and `FdbMetadataStore` security keys. Verified by `cargo test -p nova-storage phase1_security_metadata_persists_when_fdb_configured` against real FDB.

Checklist:

- [x] Add `UserMeta`, `RoleMeta`, `ObjectRef`, `ObjectOwnerMeta`, `GrantSetMeta` types.
- [x] Add `SecurityStore` trait or extend metadata store deliberately.
- [x] Implement FDB keys for users, roles, role grants, object owner, privilege grants.
- [x] Bootstrap `ACCOUNTADMIN` and `PUBLIC`.
- [x] Add `security_epoch`.
- [x] Add persistence tests.
- [x] Commit multi-key security mutations in one FDB transaction.
- [x] Guard `create_user` / `create_role` with checked write preconditions for duplicate key races.
- [x] Merge `GRANT` / partial `REVOKE` privilege bitsets inside one FDB transaction.
- [x] Bump `security_epoch` in the same transaction as security metadata mutations.
- [x] Keep bootstrap idempotent so repeated startup does not churn `security_epoch`.
- [x] Bootstrap built-in roles with atomic metadata + name-index writes.
- [x] Validate role-grant user/role/grantor existence inside the grant transaction.
- [x] Make idempotent role grant/revoke and privilege grant/revoke no-op without epoch churn.
- [x] Add tests for idempotent grant epoch behavior and single-epoch multi-key creates.

Acceptance tests:

- [x] Create role, reopen FDB, role exists.
- [x] Create user, reopen FDB, user exists.
- [x] Grant role to user, reopen FDB, grant exists.
- [x] Grant SELECT on table to role, reopen FDB, grant exists.
- [x] Object owner persists.

### Phase 2 — Session SecurityContext plumbing

Goal: no global current user; every query carries session role context.

Checklist:

- [x] Add `user_id`, `primary_role_id`, `secondary_roles` to MySQL `Session` via `SecurityContext`.
- [x] Auth lookup reads FDB user.
- [x] Default role validation at login.
- [x] Implement `USE ROLE`.
- [x] Implement `USE SECONDARY ROLES ALL/NONE`.
- [x] Change `QueryEngine::execute_sql` signature to include `SecurityContext`.
- [x] Remove/disable `Executor.current_user` global path.
- [x] Use FDB `UserMeta.mysql_native_hash` as the MySQL auth source of truth.
- [x] Reject disabled users before OK packet.
- [x] Scope result-cache keys by `SecurityContext` and `security_epoch`.
- [x] Route parameterless prepared statement execution through `SecurityContext`; reject parameterized prepared execution until binding is implemented.
- [x] Add Phase 2 regression check for security-scoped cache keys.
- [x] Add session isolation regression test.
- [x] Harden `USE ROLE` / `USE SECONDARY ROLES` parsing with unit coverage.
- [x] Add active role calculation helper for Phase 3 enforcement.
- [x] Rename root execution helper to `execute_as_root_for_internal` and audit direct callers.

Acceptance tests:

- [x] Two concurrent sessions with different roles cannot leak privileges.
- [x] `USE ROLE` affects only current session.
- [x] Login fails for disabled user.
- [x] Login fails if default role is not granted.

### Phase 3 — Core authorization enforcement

Goal: database/schema/table DDL+DML are protected.

Checklist:

- [x] Add FDB-backed authorization helper in executor path.
- [x] Enforce `CREATE_DATABASE` on account object.
- [x] Enforce database/schema `USAGE` for table operations.
- [x] Enforce schema `CREATE_TABLE` when schema exists.
- [x] Enforce table `SELECT/INSERT/UPDATE/DELETE`.
- [x] Enforce table `OWNERSHIP` for drop table.
- [x] Extract JOIN / subquery table dependencies and authorize every dependency for `SELECT`.
- [x] Filter `SHOW DATABASES` and `SHOW TABLES`.

Acceptance tests:

- [x] User without `SELECT` cannot read table.
- [x] User with `SELECT` but without parent `USAGE` cannot read table.
- [x] Owner can operate on owned table.
- [x] Non-owner cannot drop table without ownership.
- [x] JOIN fails if any referenced table lacks `SELECT`.
- [x] SHOW only returns visible objects.

### Phase 4 — SQL GRANT/REVOKE/SHOW

Goal: users can manage roles and privileges via SQL.

Checklist:

- [ ] Parse/analyze/execute `CREATE ROLE`.
- [ ] Parse/analyze/execute `CREATE USER`.
- [ ] Parse/analyze/execute `GRANT ROLE TO USER`.
- [ ] Parse/analyze/execute `REVOKE ROLE FROM USER`.
- [ ] Parse/analyze/execute `GRANT privilege ON object TO ROLE`.
- [ ] Parse/analyze/execute `REVOKE privilege ON object FROM ROLE`.
- [ ] Parse/analyze/execute `SHOW GRANTS TO ROLE`.
- [ ] Parse/analyze/execute `SHOW GRANTS ON object`.
- [ ] Increment security epoch on every mutation.

Acceptance tests:

- [ ] Grant makes access allowed immediately.
- [ ] Revoke makes access denied immediately.
- [ ] Only owner or `MANAGE_GRANTS` can grant.
- [ ] `SHOW GRANTS` returns correct role/object rows.

### Phase 5 — Dynamic Table RBAC

Goal: no DT privilege bypass.

Checklist:

- [ ] Resolve DT base dependencies using SQL AST.
- [ ] Store DT dependency keys.
- [ ] Enforce schema `CREATE_DYNAMIC_TABLE`.
- [ ] Enforce base table `SELECT` on create.
- [ ] Store DT owner role.
- [ ] Run scheduler refresh as owner role.
- [ ] Enforce `SELECT`, `OPERATE`, `MONITOR`, `OWNERSHIP` on DT.
- [ ] Filter `SHOW DYNAMIC TABLES`.

Acceptance tests:

- [ ] Cannot create DT without base table `SELECT`.
- [ ] DT refresh fails after owner role loses base table `SELECT`.
- [ ] `OPERATE` can refresh/suspend/resume but cannot drop.
- [ ] `MONITOR` can show status but cannot refresh.
- [ ] `SELECT` can query DT but cannot manage it.

### Phase 6 — Ownership transfer

Goal: Snowflake-like `GRANT OWNERSHIP`.

Checklist:

- [ ] Parse/analyze/execute `GRANT OWNERSHIP`.
- [ ] Support `COPY CURRENT GRANTS`.
- [ ] Support `REVOKE CURRENT GRANTS`.
- [ ] Block transfer with outbound grants unless copy/revoke mode specified.
- [ ] Revalidate DT owner dependencies on transfer.

Acceptance tests:

- [ ] Old owner loses owner powers after transfer.
- [ ] New owner gains full control.
- [ ] Copy grants preserves access.
- [ ] Revoke grants removes outbound access.
- [ ] DT transfer warns/fails refresh if new owner lacks dependencies.

### Phase 7 — Role hierarchy

Goal: parent roles inherit child role privileges.

Checklist:

- [ ] Implement `GRANT ROLE child TO ROLE parent`.
- [ ] Implement `REVOKE ROLE child FROM ROLE parent`.
- [ ] Add cycle detection.
- [ ] Resolve transitive active roles.
- [ ] Add cache with security epoch.

Acceptance tests:

- [ ] Parent inherits child privileges.
- [ ] Child does not inherit parent privileges.
- [ ] Cycle creation fails.
- [ ] Revoking child role removes inherited privileges.

### Phase 8 — Future grants and bulk grants

Goal: scalable grant management.

Checklist:

- [ ] `GRANT SELECT ON ALL TABLES IN SCHEMA ...`.
- [ ] `GRANT SELECT ON FUTURE TABLES IN SCHEMA ...`.
- [ ] Future grants on database and schema scopes.
- [ ] Schema-level future grants override database-level future grants.
- [ ] Apply future grants atomically during object creation.

Acceptance tests:

- [ ] Existing table bulk grant works.
- [ ] Future grant applies to new tables.
- [ ] Revoke future grant stops applying to new objects.
- [ ] Schema future grant takes precedence.

### Phase 9 — Enterprise hardening

Goal: operational robustness.

Checklist:

- [ ] Security audit log.
- [ ] Admin views / information schema for grants.
- [ ] Authz cache with epoch invalidation.
- [ ] Security metrics.
- [ ] Backup/restore includes security metadata.
- [ ] FDB transaction conflict tests.
- [ ] Fuzz-ish tests for GRANT/REVOKE sequences.

Acceptance tests:

- [ ] Backup/restore preserves users, roles, grants, owners.
- [ ] Concurrent grants do not corrupt bitsets.
- [ ] Security epoch invalidates cache.
- [ ] Audit rows written for success and failure.

---

## 19. Minimum V1 Cut

The minimum safe implementation cut is:

```text
Phase 0: FDB object ID correctness
Phase 1: FDB security metadata
Phase 2: SecurityContext plumbing
Phase 3: core table/db/schema authorization
Phase 5 partial: Dynamic Table create/refresh owner checks
```

Do not ship enterprise RBAC without:

```text
SELECT enforcement
parent USAGE enforcement
per-session security context
FDB-backed grants
owner role storage
DT owner-role refresh
```

---

## 20. Non-Goals for Initial Enterprise RBAC

Skip until the base is stable:

```text
Row access policies
Column masking
OAuth/OIDC/SAML/SCIM
Database roles separate from account roles
Managed access schemas
WITH GRANT OPTION
Future grants
```

These are important, but not needed to establish the correct foundation.

---

## 21. Checkpoint Rules Before Claiming Complete

Before marking any RBAC phase complete:

1. Tests must cover deny and allow paths.
2. Tests must verify persistence after reopening FDB.
3. Tests must include at least one non-admin user.
4. Tests must include parent `USAGE` denial.
5. Tests must include owner-role behavior.
6. Dynamic Table tests must verify refresh under owner role.
7. Full query path must use per-session `SecurityContext`; no global executor user.
8. README status must not claim enterprise RBAC until all minimum V1 cut items pass.

---

## 22. Implementation Order Reminder

Shortest safe path:

```text
1. Fix FDB object IDs.
2. Add FDB security keys and bootstrap roles.
3. Add SecurityContext to sessions and query engine.
4. Add Authorizer and core checks.
5. Add SQL GRANT/REVOKE/SHOW.
6. Secure Dynamic Tables.
7. Add hierarchy/future grants later.
```

Do not start with UI or syntax sugar before FDB security state and enforcement are correct.
