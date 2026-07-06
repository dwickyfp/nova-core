# Task 1 Report: Finish atomic role hierarchy metadata

STATUS: DONE_WITH_CONCERNS

## Scope
- Modified `crates/nova-storage/src/metadata/mod.rs` to expose role hierarchy methods on `SecurityStore`.
- Modified `crates/nova-storage/src/metadata/security_impl.rs` to add atomic FDB role-to-role grant/revoke helpers, child listing, and storage tests.
- Did not edit other changed files (`ROADMAP.md`, coordinator executor/tests); those were pre-existing workspace changes.

## Implementation
- `grant_role_to_role(parent_role_id, child_role_id, granted_by)` rejects self-inheritance before opening the transaction.
- `atomic_grant_role_to_role` performs role existence checks for parent, child, and grantor; scans the child closure breadth-first to depth 64; rejects cycles; writes `role_child(parent, child)` and `role_parent(child, parent)`; bumps `security_epoch` in the same FDB transaction.
- `atomic_revoke_role_from_role` clears both hierarchy indexes and bumps `security_epoch` in the same FDB transaction.
- `list_role_children(parent_role_id)` scans `role_child(parent, *)`.

## Tests
- Added/updated:
  - `role_inheritance_persists_and_rejects_cycles`
  - `role_inheritance_rejects_missing_roles`
- Verification run:
  - `NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage role_inheritance` — exit 0
  - `cargo test -p nova-storage` — exit 0
  - `cargo clippy -p nova-storage -- -D warnings` — exit 0
  - `cargo fmt --all -- --check` — exit 0

## TDD note
- The requested tests were added before implementation changes. The focused test command compiled/passed because the workspace already contained partial role hierarchy implementation from prior controller work; I then refactored that implementation into the brief's requested private atomic helper shape and re-ran verification.

## Architecture/RBAC checks
- FDB remains authoritative for role hierarchy metadata.
- Grant/revoke metadata changes and security epoch bumps are kept inside one FDB transaction.
- Missing parent/child/grantor roles fail closed.
- Cycle/self-inheritance attempts fail closed.
- No new dependencies added.

## Concerns
- Full `cargo test --all` and full `cargo clippy --all -- -D warnings` were not run due to task focus/time; crate-level storage checks passed.
- Other uncommitted files outside Task 1 remain in the workspace and were not staged by this task.

---

# Task 1 Review Fix Report: Core RBAC enterprise hardening

STATUS: DONE

## Fixes
- Replaced role hierarchy grant/revoke `security_epoch` fallback decoding with fail-closed malformed metadata errors.
- Changed role-to-role revoke to validate parent and child roles inside the FDB transaction.
- Changed role-to-role revoke to conflict-read the forward edge and return `Ok(())` without clearing or bumping the security epoch when the edge is absent.
- Added storage regression coverage for missing-edge revoke no-op behavior, missing-role revoke failures, and malformed epoch fail-closed grant/revoke paths.

## Verification
- `NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage role_inheritance` — exit 0
- `cargo test -p nova-storage` — exit 0
- `cargo clippy -p nova-storage -- -D warnings` — exit 0
- `cargo fmt --all -- --check` — exit 0

## Concerns
- Command output in this harness was abbreviated for successful cargo runs, but all required commands returned exit 0.

---

# Task 1 Review Fix Report: Fail-closed RBAC metadata reads

STATUS: DONE

## Fixes
- `atomic_grant_role_to_role` and `atomic_revoke_role_from_role` now deserialize role records inside the FDB transaction, so corrupt role metadata fails closed instead of passing existence checks.
- Role hierarchy closure scans and `list_role_children` now deserialize `RoleGrantMeta` values for `role_child` edges and fail closed on malformed grant metadata.
- Replaced user-role grant/revoke `security_epoch` `unwrap_or([0; 8])` decoding with explicit malformed-metadata errors.
- Added focused FDB corruption tests for corrupt role records and corrupt `role_child` grant values.

## Verification
- `NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage role_inheritance` — exit 0
- `cargo test -p nova-storage` — exit 0
- `cargo clippy -p nova-storage -- -D warnings` — exit 0
- `cargo fmt --all -- --check` — exit 0

## Concerns
- Other uncommitted files outside this review fix remain in the workspace and were not edited or staged by this task.

---

# Task 1 Review Fix Report: Dangling role hierarchy edges

STATUS: DONE

## Fixes
- `atomic_grant_role_to_role` now conflict-reads and deserializes each child `RoleMeta` found while scanning `role_child` closure edges, failing closed on missing or corrupt child roles.
- `list_role_children` now validates each child role record after validating `RoleGrantMeta`, failing closed on dangling or corrupt child roles.
- Added focused dangling `role_child` edge regression tests for closure scans and child listing.

## Verification
- `NOVA_FDB_CLUSTER_FILE='docker:docker@127.0.0.1:4500' cargo test -p nova-storage role_inheritance` — exit 0
- `cargo test -p nova-storage` — exit 0
- `cargo clippy -p nova-storage -- -D warnings` — exit 0
- `cargo fmt --all -- --check` — exit 0

## Concerns
- Cargo output was abbreviated by the harness for successful commands, but all requested commands returned exit 0.
