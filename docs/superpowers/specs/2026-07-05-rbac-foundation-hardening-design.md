# RBAC Foundation Hardening Design

Date: 2026-07-05

## Context

Nova Core is in Phase 16 enterprise security/governance work. Phase 1 security metadata, Phase 2 session security context plumbing, and much of Phase 3 authorization enforcement already exist. Before implementing user-facing SQL GRANT/REVOKE/SHOW, the RBAC foundation needs to be deterministic, durable, and covered by acceptance tests.

This design covers Option A: finish Phase 0 FDB object ID correctness and complete Phase 2/3 acceptance coverage. It intentionally does not implement Phase 4 SQL GRANT/REVOKE/SHOW or later policy/tag/audit features.

## Goals

1. Every securable metadata object created with `id == 0` receives a stable unique ID before persistence.
2. Object name uniqueness is enforced with FDB-backed name indexes.
3. Reopening the same FDB test subspace preserves IDs and name indexes.
4. Phase 2 session auth acceptance cases are tested and enforced.
5. Phase 3 core authorization acceptance cases are tested and enforced through the executor/session security context path.
6. All affected unit and integration tests pass, with clippy and formatting clean.

## Non-Goals

- SQL GRANT/REVOKE/SHOW support.
- Role-to-role hierarchy and cycle detection.
- Ownership transfer via `GRANT OWNERSHIP`.
- Managed access schemas, future grants, database roles, policies, tags, audit, or access history.
- New third-party dependencies.

## Metadata ID and Name Index Design

`FdbMetadataStore` is the source of truth for object IDs and name uniqueness. Create operations for database, schema, table, dynamic table, stream, user, and role follow one pattern:

1. If caller passes `id == 0`, allocate a new ID using the relevant FDB `next_id` counter.
2. Compute the canonical name-index key for the object scope.
3. In one FDB transaction, assert the name-index key is absent, then write metadata and name index.
4. Store the allocated ID in metadata before serialization.
5. Return a deterministic error if the name already exists.

Name-index scopes:

- Database: global database name.
- Schema: `(db_id, schema_name)`.
- Table: `(db_id, schema_id, table_name)`.
- Dynamic table: `(db_id, schema_id, dynamic_table_name)`.
- Stream: `(db_id, schema_id, stream_name)` when stream lookup/listing requires stable name identity.
- User: global user name.
- Role: global role name.

Existing `id == 0` must never become a persisted object ID. It means “allocate one”. Reserved security IDs such as root user, ACCOUNTADMIN, and PUBLIC remain explicit non-zero IDs and must not be remapped.

## Duplicate Name Behavior

Duplicate creates in the same scope must fail before overwriting existing metadata. The first implementation can use the existing `NovaError::Internal { message }` if no better domain error exists, but the message must identify the duplicate object kind and name. Tests should assert failure, not exact message text unless the message becomes a stable API.

## Reopen Tests

Storage tests should verify persistence by opening the same FDB subspace twice:

1. Create objects through `store1`.
2. Open `store2` with the same subspace.
3. Assert object IDs differ where expected.
4. Assert names resolve/list correctly after reopen.
5. Assert duplicate-name attempts fail after reopen.

At minimum:

- Create two databases; IDs differ and survive reopen.
- Create two tables in the same schema; IDs differ and survive reopen.
- Duplicate table name fails.

Additional tests should cover schema, dynamic table, user, and role ID/name behavior if their create paths are changed.

## Phase 2 Acceptance Design

The MySQL/session security path must reject unsafe login/session state.

Acceptance cases:

1. Disabled user login fails before the OK packet.
2. User default role must be granted to the user. If the default role is missing or not granted, session security context creation fails.

The roadmap explicitly expects invalid default role login to fail, so there is no silent fallback to PUBLIC for this hardening pass.

Tests should target the narrowest reliable path. If full MySQL handshake tests are already practical, use them. Otherwise, test the `security_context_for_user` and `user_for_auth` paths that the handshake uses, plus parser/session unit tests where appropriate.

## Phase 3 Acceptance Design

Core authorization acceptance must exercise `execute_with_context`, not root/internal helpers.

Required cases:

1. A user without table `SELECT` cannot read the table.
2. A user with table `SELECT` but without parent database/schema `USAGE` cannot read the table.
3. An object owner can operate on the owned object.
4. A non-owner cannot drop the object without `OWNERSHIP`.

Test pattern:

1. Root/internal setup creates database, schema, and table.
2. Security metadata creates role and user context.
3. Grants are applied directly via `SecurityStore` metadata helpers.
4. SQL or resolved statements execute through `execute_with_context` using the target `SecurityContext`.
5. Tests assert `PermissionDenied` or success.

Authorization semantics remain:

- CREATE authorization and new object ownership use the session primary role.
- Non-CREATE authorization uses the active role set (`primary + active secondary roles`) as currently implemented.
- Object ownership metadata grants owner powers for operations requiring `OWNERSHIP`.

## Error Handling

- Duplicate-name and missing-object errors must not silently overwrite metadata.
- Authorization failures return `NovaError::PermissionDenied`.
- Invalid login/default-role state returns an auth/security error appropriate to the current path.
- Tests should prefer matching error variants over brittle exact messages.

## Test and Verification Plan

Required verification before completion:

```bash
cargo test -p nova-storage
cargo test -p nova-coordinator --test rbac_phase2_security_context_test
cargo test -p nova-coordinator --test e2e_tests
cargo test -p nova-coordinator --test dynamic_table_test
cargo clippy -p nova-storage -p nova-coordinator --all-targets -- -D warnings
cargo fmt --all -- --check
```

If changes touch worker/distributed/shared execution types, also run:

```bash
cargo clippy -p nova-worker --all-targets -- -D warnings
```

## Documentation Updates

After implementation:

- Update `docs/design/enterprise-rbac-roadmap.md` Phase 0 checkboxes that are actually complete.
- Update Phase 2 acceptance checkboxes when disabled-user and invalid-default-role tests pass.
- Update Phase 3 acceptance checkboxes when missing SELECT, missing parent USAGE, owner operation, and non-owner drop tests pass.
- Do not mark Phase 4 or later work complete.

## Definition of Done

- Phase 0 checklist is implemented and verified for the intended object types.
- Phase 2 disabled-user and invalid-default-role acceptance tests pass.
- Phase 3 missing SELECT, missing parent USAGE, owner operation, and non-owner drop acceptance tests pass.
- Required verification commands pass.
- No secrets or local-only credentials are committed.
- Documentation reflects only verified completion.
