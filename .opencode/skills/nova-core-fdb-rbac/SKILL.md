---
name: nova-core-fdb-rbac
description: Use when changing FoundationDB metadata, transactions, auth, RBAC, security context, roles, privileges, or governance features in nova-core.
---

# Nova Core FDB and RBAC

Use this skill for changes involving FoundationDB metadata, transactional semantics, authentication, RBAC, roles, privileges, grants, session security context, or governance features.

## First Checks

1. Load `nova-core-orientation` and `nova-core-safe-rust-change`.
2. Inspect current code with CodeGraph before editing:
   - `codegraph explore "RBAC security context privileges grants"`
   - `codegraph explore "FoundationDB metadata transaction tuple key"`
3. Read relevant docs when present:
   - `docs/design/enterprise-rbac-roadmap.md`
   - `docs/design/architecture.md`
   - tests under `crates/nova-coordinator/tests/`
   - metadata code under `crates/nova-storage/src/metadata/`

## FoundationDB Rules

- Treat FoundationDB as an ordered key-value store, not SQL.
- Prefer tuple-encoded or otherwise well-delimited keys; avoid ambiguous string prefixes.
- Keep multi-key metadata updates inside one FDB transaction whenever correctness requires atomicity.
- Be explicit about snapshot reads versus conflict-checked reads.
- Preserve idempotency for retries where FoundationDB transactions can be retried.
- Tests should cover key ordering, prefix scans, empty ranges, duplicate objects, and rollback/failure behavior.

## RBAC and Security Rules

- Resolve the session user, active role, and privileges before executing protected operations.
- Default role behavior must be deterministic and tested.
- Deny by default when role/user lookup is missing or ambiguous.
- Do not bypass security context in helper paths, MySQL protocol paths, or direct coordinator execution paths.
- Add negative tests for forbidden access, not only happy-path grants.
- Keep audit/security metadata free of secrets; never log raw passwords, tokens, or API keys.

## Local Test Context

The repo may provide a default FoundationDB cluster file through cargo env config. If tests fail due to FDB availability, report that clearly and avoid weakening tests to pass locally.

Useful checks:

```bash
cargo test -p nova-storage metadata
cargo test -p nova-coordinator rbac
cargo test --all rbac
cargo test --all security
```

## Review Questions

Before completion, answer:

- Which keys or metadata records are read/written?
- Which operations must be atomic?
- What is the deny-by-default behavior?
- Which negative test proves unauthorized access is blocked?
