# Core RBAC Enterprise Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Core RBAC enterprise-grade enough to support Phase 16 governance work.

**Architecture:** FoundationDB remains the authoritative security metadata store. Coordinator authorization computes effective roles from session active roles plus inherited child roles; CREATE ownership remains primary-role-only. SQL grant management is added only for core account roles and object grants.

**Tech Stack:** Rust, async_trait, FoundationDB tuple keys, sqlparser-rs/parser layer, nova-storage metadata traits, nova-coordinator analyzer/executor, tokio tests.

## Global Constraints

- No new third-party dependencies.
- FoundationDB is the source of truth for all security metadata.
- Multi-key security mutations must be atomic FDB transactions.
- Authorization is deny-by-default on missing grants, missing roles, corrupt metadata, stale context, or malformed security state.
- CREATE authorization and new object ownership use the session primary role only.
- Non-CREATE authorization uses primary role, active secondary roles, and inherited child roles.
- `ACCOUNTADMIN` grants administrative bypass if it appears anywhere in the effective active role closure.
- Security epoch changes only after successful security mutations.
- Production fallible paths must not use `unwrap()` or `expect()`.
- FDB integration tests use `NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500'` when local Docker FDB is available.

---

## File Structure

- Modify `crates/nova-storage/src/metadata/mod.rs`: public `SecurityStore` trait methods for role hierarchy and grant inspection.
- Modify `crates/nova-storage/src/metadata/security_impl.rs`: atomic FDB role grants/revokes, closure validation, metadata tests.
- Modify `crates/nova-coordinator/src/executor.rs`: effective role closure authorization, grant/revoke/show execution.
- Modify `crates/nova-coordinator/src/parser.rs`: parse supported `GRANT`, `REVOKE`, and `SHOW GRANTS` statements if current parser does not expose them.
- Modify `crates/nova-coordinator/src/analyzer.rs`: add resolved statement variants for grant management.
- Modify `crates/nova-coordinator/tests/e2e_tests.rs`: SQL and executor RBAC E2E tests.
- Modify `crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs`: session role inheritance tests.
- Modify `ROADMAP.md`: mark only completed Core RBAC items.

---

### Task 1: Finish atomic role hierarchy metadata

**Files:**
- Modify: `crates/nova-storage/src/metadata/mod.rs`
- Modify: `crates/nova-storage/src/metadata/security_impl.rs`

**Interfaces:**
- Consumes: existing `SecurityStore`, `RoleGrantMeta`, `RoleId`, `FdbMetadataStore`.
- Produces:
  - `SecurityStore::grant_role_to_role(parent_role_id: RoleId, child_role_id: RoleId, granted_by: RoleId) -> Result<()>`
  - `SecurityStore::revoke_role_from_role(parent_role_id: RoleId, child_role_id: RoleId) -> Result<()>`
  - `SecurityStore::list_role_children(parent_role_id: RoleId) -> Result<Vec<RoleId>>`

- [ ] **Step 1: Write storage tests for robust hierarchy behavior**

Add/extend tests in `crates/nova-storage/src/metadata/security_impl.rs`:

```rust
#[tokio::test]
async fn role_inheritance_persists_and_rejects_cycles() -> Result<()> {
    let Ok(cluster_file) = std::env::var("NOVA_FDB_CLUSTER_FILE") else {
        return Ok(());
    };
    let subspace = format!("nova_test_role_inheritance_{}", now_micros()).into_bytes();
    let store = FdbMetadataStore::open_test(&cluster_file, subspace.clone())?;
    store.bootstrap_security().await?;

    let parent = store.create_role(role("parent_role")).await?;
    let child = store.create_role(role("child_role")).await?;
    let epoch_before = store.security_epoch().await?;

    store.grant_role_to_role(parent, child, ACCOUNTADMIN_ROLE_ID).await?;
    assert_eq!(store.list_role_children(parent).await?, vec![child]);
    assert!(store.security_epoch().await? > epoch_before);

    assert!(store.grant_role_to_role(child, parent, ACCOUNTADMIN_ROLE_ID).await.is_err());
    assert!(store.grant_role_to_role(parent, parent, ACCOUNTADMIN_ROLE_ID).await.is_err());

    let reopened = FdbMetadataStore::open_test(&cluster_file, subspace)?;
    assert_eq!(reopened.list_role_children(parent).await?, vec![child]);
    reopened.revoke_role_from_role(parent, child).await?;
    assert!(reopened.list_role_children(parent).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn role_inheritance_rejects_missing_roles() -> Result<()> {
    let Ok(cluster_file) = std::env::var("NOVA_FDB_CLUSTER_FILE") else {
        return Ok(());
    };
    let subspace = format!("nova_test_role_missing_{}", now_micros()).into_bytes();
    let store = FdbMetadataStore::open_test(&cluster_file, subspace)?;
    store.bootstrap_security().await?;
    let parent = store.create_role(role("parent_role")).await?;

    assert!(store.grant_role_to_role(parent, parent + 10_000, ACCOUNTADMIN_ROLE_ID).await.is_err());
    assert!(store.grant_role_to_role(parent + 20_000, parent, ACCOUNTADMIN_ROLE_ID).await.is_err());
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify current failures or coverage**

Run:

```bash
NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage role_inheritance
```

Expected before implementation is FAIL if methods are missing or non-robust; PASS after the current partial implementation is completed.

- [ ] **Step 3: Implement atomic metadata operations**

Implement role hierarchy methods in `SecurityStore for FdbMetadataStore`. The grant transaction must check role existence, scan child closure, reject cycles, set both indexes, and bump `security_epoch` in one FDB transaction. The revoke transaction must clear both indexes and bump epoch in one transaction.

Use this shape in `security_impl.rs`:

```rust
async fn grant_role_to_role(
    &self,
    parent_role_id: RoleId,
    child_role_id: RoleId,
    granted_by: RoleId,
) -> Result<()> {
    if parent_role_id == child_role_id {
        return Err(NovaError::Internal { message: "role cannot inherit itself".to_string() });
    }
    // In one FDB transaction:
    // 1. read parent, child, grantor role records with conflict-checked reads
    // 2. breadth-first scan role_child(child, *) up to depth 64
    // 3. reject if parent appears in child closure
    // 4. set role_child(parent, child) and role_parent(child, parent)
    // 5. increment security_epoch
    // Return NovaError::Internal for corrupt metadata and missing role preconditions.
    self.atomic_grant_role_to_role(parent_role_id, child_role_id, granted_by).await
}
```

If helper methods are introduced, keep them private to `security_impl.rs` and avoid new public APIs unless tests need them.

- [ ] **Step 4: Run storage tests**

Run:

```bash
NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage role_inheritance
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add crates/nova-storage/src/metadata/mod.rs crates/nova-storage/src/metadata/security_impl.rs
git commit -m "feat(storage): add atomic role hierarchy grants"
```

---

### Task 2: Harden executor effective-role authorization

**Files:**
- Modify: `crates/nova-coordinator/src/executor.rs`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`
- Modify: `crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs`

**Interfaces:**
- Consumes: `SecurityStore::list_role_children`, `SecurityContext::active_role_ids()`.
- Produces: executor privilege checks that use inherited role closure.

- [ ] **Step 1: Add executor E2E tests**

Add tests to `crates/nova-coordinator/tests/e2e_tests.rs`:

```rust
#[tokio::test]
async fn phase3_inherited_role_select_can_read_table() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE securedb", "securedb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE sensitive (id INT)", "securedb").await.unwrap();
    exec_sql(&executor, "INSERT INTO sensitive VALUES (1)", "securedb").await.unwrap();

    let parent_role_id = create_role(&executor, "reader_parent_role").await;
    let child_role_id = create_role(&executor, "reader_child_role").await;
    let (db_meta, schema_meta, table_meta) = securedb_objects(&executor, "sensitive").await;
    grant_parent_usage(&executor, child_role_id, &db_meta, &schema_meta).await;
    grant(
        &executor,
        child_role_id,
        nova_common::ObjectRef::new(nova_common::ObjectType::Table, table_meta.id),
        nova_common::SecurityPrivilege::Select,
    ).await;
    executor.meta().grant_role_to_role(parent_role_id, child_role_id, nova_common::ACCOUNTADMIN_ROLE_ID).await.unwrap();

    let inherited_reader = context_for(parent_role_id, "inherited_reader_user");
    let result = exec_sql_as(&executor, "SELECT * FROM sensitive", "securedb", &inherited_reader).await.unwrap();
    assert!(matches!(result, QueryResult::Rows { .. }));
}

#[tokio::test]
async fn phase3_revoked_inherited_role_select_is_denied() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE securedb", "securedb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE sensitive (id INT)", "securedb").await.unwrap();

    let parent_role_id = create_role(&executor, "revoked_parent_role").await;
    let child_role_id = create_role(&executor, "revoked_child_role").await;
    let (db_meta, schema_meta, table_meta) = securedb_objects(&executor, "sensitive").await;
    grant_parent_usage(&executor, child_role_id, &db_meta, &schema_meta).await;
    grant(&executor, child_role_id, nova_common::ObjectRef::new(nova_common::ObjectType::Table, table_meta.id), nova_common::SecurityPrivilege::Select).await;
    executor.meta().grant_role_to_role(parent_role_id, child_role_id, nova_common::ACCOUNTADMIN_ROLE_ID).await.unwrap();
    executor.meta().revoke_role_from_role(parent_role_id, child_role_id).await.unwrap();

    let inherited_reader = context_for(parent_role_id, "revoked_inherited_reader_user");
    let err = exec_sql_as(&executor, "SELECT * FROM sensitive", "securedb", &inherited_reader).await.expect_err("revoked inherited grant must deny");
    assert!(matches!(err, nova_common::NovaError::PermissionDenied { .. }), "expected PermissionDenied, got {err:?}");
}
```

- [ ] **Step 2: Run executor test filter**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests phase3_inherited_role_select_can_read_table
cargo test -p nova-coordinator --test e2e_tests phase3_revoked_inherited_role_select_is_denied
```

Expected before implementation: inherited test may fail; revoke test should fail if stale role closure is used.

- [ ] **Step 3: Implement effective role closure in executor**

Add private helper in `Executor`:

```rust
async fn active_role_closure(&self, security: &SecurityContext) -> Result<HashSet<RoleId>> {
    let mut roles = HashSet::new();
    let mut queue = VecDeque::new();
    for role_id in security.active_role_ids() {
        queue.push_back((role_id, 0usize));
    }
    while let Some((role_id, depth)) = queue.pop_front() {
        if depth > 64 {
            return Err(NovaError::PermissionDenied {
                user: security.username.clone(),
                action: "role inheritance depth limit exceeded".to_string(),
            });
        }
        if !roles.insert(role_id) {
            continue;
        }
        if self.meta.get_role(role_id).await?.is_none() {
            return Err(NovaError::PermissionDenied {
                user: security.username.clone(),
                action: format!("use missing role {}", role_id),
            });
        }
        for child_id in self.meta.list_role_children(role_id).await? {
            queue.push_back((child_id, depth + 1));
        }
    }
    Ok(roles)
}
```

Then update `has_privilege` so it calls `active_role_closure`, grants admin bypass if the closure contains `ACCOUNTADMIN_ROLE_ID`, and checks ownership/grants against that closure.

- [ ] **Step 4: Run executor tests**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests phase3_inherited_role_select_can_read_table
cargo test -p nova-coordinator --test e2e_tests phase3_revoked_inherited_role_select_is_denied
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
git add crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs
git commit -m "feat(coordinator): enforce inherited RBAC privileges"
```

---

### Task 3: Add SQL grant/revoke resolved statements

**Files:**
- Modify: `crates/nova-coordinator/src/parser.rs`
- Modify: `crates/nova-coordinator/src/analyzer.rs`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`

**Interfaces:**
- Consumes: parser output from existing SQL parser.
- Produces resolved statements:
  - `ResolvedStatement::GrantRoleToUser { role: String, user: String }`
  - `ResolvedStatement::RevokeRoleFromUser { role: String, user: String }`
  - `ResolvedStatement::GrantRoleToRole { child_role: String, parent_role: String }`
  - `ResolvedStatement::RevokeRoleFromRole { child_role: String, parent_role: String }`
  - `ResolvedStatement::GrantPrivileges { privileges: Vec<SecurityPrivilege>, object_type: ObjectType, object_name: String, role: String }`
  - `ResolvedStatement::RevokePrivileges { privileges: Vec<SecurityPrivilege>, object_type: ObjectType, object_name: String, role: String }`

- [ ] **Step 1: Add analyzer/parser tests for supported forms**

Add tests near existing analyzer tests:

```rust
#[test]
fn analyzer_resolves_grant_role_to_user() {
    let parser = SqlParser::new();
    let analyzer = Analyzer::new("securedb".to_string(), "public".to_string());
    let stmts = parser.parse("GRANT ROLE analyst TO USER alice").unwrap();
    let resolved = analyzer.resolve(&stmts[0]).unwrap();
    assert!(matches!(resolved, ResolvedStatement::GrantRoleToUser { .. }));
}

#[test]
fn analyzer_resolves_grant_select_on_table() {
    let parser = SqlParser::new();
    let analyzer = Analyzer::new("securedb".to_string(), "public".to_string());
    let stmts = parser.parse("GRANT SELECT ON TABLE sensitive TO ROLE analyst").unwrap();
    let resolved = analyzer.resolve(&stmts[0]).unwrap();
    assert!(matches!(resolved, ResolvedStatement::GrantPrivileges { .. }));
}
```

- [ ] **Step 2: Run analyzer/parser tests**

Run:

```bash
cargo test -p nova-coordinator analyzer_resolves_grant
```

Expected before implementation: FAIL with unsupported statement or missing variant.

- [ ] **Step 3: Add resolved statement variants**

In `crates/nova-coordinator/src/analyzer.rs`, add the variants listed in this task's interfaces. Map SQL privileges to existing `SecurityPrivilege` values only. Reject unsupported privileges with `NovaError::SqlAnalysisError`.

- [ ] **Step 4: Implement parsing/analyzer mapping**

Use sqlparser-rs statement variants if available. If the existing parser wrapper hides them, add minimal pattern support in `parser.rs` for the exact supported forms. Do not implement future grants, all grants, database roles, managed access, or grant options in this task.

- [ ] **Step 5: Run parser/analyzer tests**

Run:

```bash
cargo test -p nova-coordinator analyzer_resolves_grant
```

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
git add crates/nova-coordinator/src/parser.rs crates/nova-coordinator/src/analyzer.rs crates/nova-coordinator/tests/e2e_tests.rs
git commit -m "feat(coordinator): resolve core RBAC grant statements"
```

---

### Task 4: Execute SQL grant/revoke operations

**Files:**
- Modify: `crates/nova-coordinator/src/executor.rs`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`

**Interfaces:**
- Consumes: resolved statements from Task 3.
- Produces executor handlers for core grant/revoke SQL.

- [ ] **Step 1: Add SQL E2E tests**

Add tests:

```rust
#[tokio::test]
async fn sql_grant_role_to_role_enables_inherited_select() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE securedb", "securedb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE sensitive (id INT)", "securedb").await.unwrap();
    create_role(&executor, "sql_parent_role").await;
    create_role(&executor, "sql_child_role").await;

    exec_sql(&executor, "GRANT USAGE ON DATABASE securedb TO ROLE sql_child_role", "securedb").await.unwrap();
    exec_sql(&executor, "GRANT USAGE ON SCHEMA public TO ROLE sql_child_role", "securedb").await.unwrap();
    exec_sql(&executor, "GRANT SELECT ON TABLE sensitive TO ROLE sql_child_role", "securedb").await.unwrap();
    exec_sql(&executor, "GRANT ROLE sql_child_role TO ROLE sql_parent_role", "securedb").await.unwrap();

    let parent = executor.meta().get_role_by_name("sql_parent_role").await.unwrap().unwrap();
    let ctx = context_for(parent.id, "sql_parent_user");
    let result = exec_sql_as(&executor, "SELECT * FROM sensitive", "securedb", &ctx).await.unwrap();
    assert!(matches!(result, QueryResult::Rows { .. }));
}

#[tokio::test]
async fn sql_grant_requires_accountadmin() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE securedb", "securedb").await.unwrap();
    let analyst = create_role(&executor, "grant_denied_role").await;
    let ctx = context_for(analyst, "grant_denied_user");
    let err = exec_sql_as(&executor, "GRANT ROLE grant_denied_role TO USER nobody", "securedb", &ctx).await.expect_err("non-admin grant must be denied");
    assert!(matches!(err, nova_common::NovaError::PermissionDenied { .. }), "expected PermissionDenied, got {err:?}");
}
```

- [ ] **Step 2: Run SQL E2E tests**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests sql_grant
```

Expected before implementation: FAIL with unsupported execution.

- [ ] **Step 3: Implement executor grant handlers**

In `execute_with_context`, add match arms for grant/revoke variants. Each handler must first require `ACCOUNTADMIN_ROLE_ID` in `active_role_closure(security)`. Then resolve role/user/object names via metadata and call `SecurityStore` methods.

For object grants, resolve:

- `DATABASE <name>` to `ObjectRef::new(ObjectType::Database, db.id)`
- `SCHEMA <name>` to current database schema
- `TABLE <name>` to current database/current schema table

Use `GrantSetMeta` and `PrivilegeSet::from_privileges(&privileges)` for grants. Use `revoke_privileges` for revokes.

- [ ] **Step 4: Run SQL E2E tests**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests sql_grant
```

Expected: PASS.

- [ ] **Step 5: Commit Task 4**

Run:

```bash
git add crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs
git commit -m "feat(coordinator): execute core RBAC grants"
```

---

### Task 5: Add supported SHOW GRANTS forms

**Files:**
- Modify: `crates/nova-coordinator/src/parser.rs`
- Modify: `crates/nova-coordinator/src/analyzer.rs`
- Modify: `crates/nova-coordinator/src/executor.rs`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`

**Interfaces:**
- Consumes: `SecurityStore::list_grants_to_role`, `SecurityStore::list_grants_on_object`.
- Produces:
  - `ResolvedStatement::ShowGrantsToRole { role: String }`
  - `ResolvedStatement::ShowGrantsOnObject { object_type: ObjectType, object_name: String }`

- [ ] **Step 1: Add SHOW GRANTS E2E tests**

Add tests:

```rust
#[tokio::test]
async fn sql_show_grants_to_role_returns_rows() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE securedb", "securedb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE sensitive (id INT)", "securedb").await.unwrap();
    create_role(&executor, "show_grants_role").await;
    exec_sql(&executor, "GRANT SELECT ON TABLE sensitive TO ROLE show_grants_role", "securedb").await.unwrap();

    let result = exec_sql(&executor, "SHOW GRANTS TO ROLE show_grants_role", "securedb").await.unwrap();
    match result {
        QueryResult::Rows { batches, .. } => assert!(!batches.is_empty()),
        other => panic!("expected rows, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run SHOW GRANTS test**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests sql_show_grants
```

Expected before implementation: FAIL with unsupported statement.

- [ ] **Step 3: Implement parser/analyzer/executor support**

Implement only `SHOW GRANTS TO ROLE <role>` and `SHOW GRANTS ON <object>`. Return a `QueryResult::Rows` with stable columns such as `created_on`, `privilege`, `granted_on`, `name`, `granted_to`, `grantee_name`, `grant_option`, and `granted_by`. If existing `QueryResult::Rows` requires Arrow `RecordBatch`, build string arrays and keep values deterministic.

- [ ] **Step 4: Run SHOW GRANTS test**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests sql_show_grants
```

Expected: PASS.

- [ ] **Step 5: Commit Task 5**

Run:

```bash
git add crates/nova-coordinator/src/parser.rs crates/nova-coordinator/src/analyzer.rs crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs
git commit -m "feat(coordinator): show core RBAC grants"
```

---

### Task 6: Final QA, roadmap update, and review

**Files:**
- Modify: `ROADMAP.md`

**Interfaces:**
- Consumes: completed Tasks 1-5.
- Produces: verified Core RBAC increment and accurate roadmap status.

- [ ] **Step 1: Update roadmap only for completed items**

Mark these if implemented and verified:

```markdown
- [x] Implement role-to-user grants and role-to-role hierarchy grants
- [x] Implement role inheritance traversal with cycle detection and recursion/depth guards
- [x] Enforce non-CREATE authorization using primary role plus active secondary roles and inherited roles
```

Mark SQL Grant Management items only if Task 4 and Task 5 are complete.

- [ ] **Step 2: Run focused tests**

Run:

```bash
cargo test -p nova-coordinator --test rbac_phase2_security_context_test
cargo test -p nova-coordinator --test e2e_tests sql_grant
cargo test -p nova-coordinator --test e2e_tests sql_show_grants
cargo test -p nova-coordinator --test e2e_tests phase3_inherited_role_select_can_read_table
NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage role_inheritance
```

Expected: all PASS.

- [ ] **Step 3: Run broader regression checks**

Run:

```bash
cargo test -p nova-coordinator --test dynamic_table_test
cargo test -p nova-coordinator --test e2e_tests
NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage
cargo fmt --all -- --check
cargo clippy -p nova-storage -p nova-coordinator --all-targets -- -D warnings
git diff --check
```

Expected: all PASS.

- [ ] **Step 4: Request specialized reviews**

Dispatch review agents:

```text
rust-reviewer: review current RBAC implementation for correctness, crate boundaries, async/error handling, and tests.
fdb-rbac-auditor: audit FDB transaction correctness, metadata consistency, RBAC deny-by-default semantics, and cache/security epoch behavior.
query-engine-debugger: review executor/query auth paths for bypass or result mismatch risks.
```

Expected: no blocking issues. Fix any blocking issue before committing.

- [ ] **Step 5: Final commit**

Run:

```bash
git add ROADMAP.md
git commit -m "docs(rbac): update core RBAC hardening status"
```

If previous task commits were skipped because execution happened in one session, make one coherent implementation commit instead:

```bash
git add ROADMAP.md crates/nova-storage/src/metadata/mod.rs crates/nova-storage/src/metadata/security_impl.rs crates/nova-coordinator/src/parser.rs crates/nova-coordinator/src/analyzer.rs crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs
git commit -m "feat(rbac): harden core enterprise authorization"
```
