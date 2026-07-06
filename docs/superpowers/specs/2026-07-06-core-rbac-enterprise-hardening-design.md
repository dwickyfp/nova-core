# Core RBAC Enterprise Hardening Design

## Scope

This increment makes Core RBAC mature enough to be the foundation for Phase 16 governance. It covers users, account roles, role-to-user grants, role-to-role inheritance, object privilege grants, ownership checks, session active roles, and a minimal SQL grant/revoke/show surface. It intentionally excludes row access policies, masking policies, tags, audit history, database roles, future grants, managed access schemas, and enterprise identity providers; those depend on this foundation.

## Architecture invariants

- FoundationDB is the source of truth for all security metadata.
- Multi-key security mutations must be atomic FDB transactions.
- Authorization is deny-by-default on missing grants, missing roles, corrupt metadata, stale context, or malformed security state.
- CREATE authorization and new object ownership use the session primary role only.
- Non-CREATE authorization uses primary role, active secondary roles, and inherited child roles.
- `ACCOUNTADMIN` grants administrative bypass if it appears anywhere in the effective active role closure.
- Security epoch changes only after successful security mutations and must protect cache/security decisions from stale reuse.

## Metadata design

Security metadata remains in `nova-storage::metadata::SecurityStore` and FDB-backed `FdbMetadataStore`.

Required stable indexes:

- `user_role(user_id, role_id) -> RoleGrantMeta`
- `role_user(role_id, user_id) -> empty`
- `role_child(parent_role_id, child_role_id) -> RoleGrantMeta`
- `role_parent(child_role_id, parent_role_id) -> empty`
- object grant indexes already used by `GrantSetMeta`
- object ownership metadata via `ObjectOwnerMeta`

All grant/revoke writes that touch multiple keys must be single FDB transactions. Role hierarchy grants must read the relevant role records and child closure in the same transaction that writes the new edge, so concurrent `A -> B` and `B -> A` cannot both commit.

Duplicate grants are idempotent. Revokes are idempotent but must clear both forward and reverse indexes atomically. Corrupt role/grant metadata returns typed Nova errors; production authz paths must not panic.

## Authorization design

The coordinator computes effective roles from `SecurityContext`:

1. Start with `SecurityContext::active_role_ids()`.
2. Traverse `role_child(parent, child)` breadth-first.
3. Reject missing roles with `PermissionDenied`.
4. Enforce a max inheritance depth of 64.
5. De-duplicate visited roles.

Privilege checks use the effective role closure for ownership and grant lookup. CREATE and object owner assignment continue to use only `primary_role_id`. This preserves Snowflake-style primary role ownership while allowing inherited privileges for reads and non-CREATE DML/DDL checks where already supported.

## SQL grant management design

Add a minimal enterprise-safe SQL surface:

- `GRANT ROLE <role> TO USER <user>`
- `REVOKE ROLE <role> FROM USER <user>`
- `GRANT ROLE <child> TO ROLE <parent>`
- `REVOKE ROLE <child> FROM ROLE <parent>`
- `GRANT <privilege>[, ...] ON <object> TO ROLE <role>`
- `REVOKE <privilege>[, ...] ON <object> FROM ROLE <role>`
- `SHOW GRANTS TO ROLE <role>`
- `SHOW GRANTS ON <object>`

Initial authorization for grant management requires `ACCOUNTADMIN` in the effective role closure. `MANAGE_GRANTS`, grant option semantics, ownership transfer, future grants, all-object grants, and managed access schemas are separate follow-up increments.

Object resolution must use existing database/schema/table metadata and return `PermissionDenied` for unauthorized grant operations, `TableNotFound`/specific metadata errors for genuinely missing objects where existing executor behavior already does that.

## Testing and QA

Required tests before completion:

- Storage/FDB tests:
  - role-to-role grant persists both indexes
  - cycle/self-cycle rejection
  - duplicate grant idempotency
  - revoke clears both indexes
  - missing role grant fails
  - security epoch bumps on successful mutations only
- Coordinator/executor tests:
  - inherited table privileges allow access
  - direct role without inherited child grant denies
  - revoke removes inherited access
  - secondary role inheritance works
  - missing inherited role denies
  - inherited `ACCOUNTADMIN` grants admin bypass
- SQL E2E tests:
  - role-to-user grant/revoke
  - role-to-role grant/revoke
  - object privilege grant/revoke
  - show grants visibility for supported forms
  - non-admin grant management denied
- Regression checks:
  - dynamic table auth tests
  - RBAC phase2 security context tests
  - coordinator E2E tests
  - storage tests against Docker FDB
  - fmt, clippy, diff check

## Delivery order

1. Finish and harden the current role hierarchy implementation.
2. Add missing negative and revoke tests.
3. Add SQL parser/analyzer/executor grant management support.
4. Add `SHOW GRANTS` output for supported forms.
5. Run focused tests, broad tests, review agents, and QA.
6. Update `ROADMAP.md` only for completed items.
7. Commit the verified increment.

## Out of scope for this increment

- Row access policies
- Column masking policies
- Tags/classification
- Audit/access history
- Database roles
- Managed access schemas
- Future/all grants
- Ownership transfer semantics beyond current owner checks
- External identity providers or MFA
- Full distributed worker-side authorization matrix
