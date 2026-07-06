# Nova Core Engineering Rules

> Canonical engineering rules for `nova-core`. These rules define how to build, review, test, and audit Nova Core as a Rust-native, Snowflake-style OLAP database engine.

---

## 1. North Star

Nova Core is a Rust-native analytical query engine built around Apache Arrow, DataFusion, FoundationDB metadata, and immutable Parquet micro-partitions in object storage.

Nova Core must remain:

- **OLAP-first**: optimized for analytical scans, filters, joins, aggregations, and columnar execution.
- **Snowflake-style**: separated compute/storage, stateless workers, durable metadata, immutable micro-partitions, MVCC, and metadata-driven pruning.
- **Rust-native**: no JVM or C++ engine components; use Rust safety and explicit error handling.
- **Test-backed**: correctness claims require unit, integration, or documented verification evidence.

Nova Core must not drift into:

- an OLTP row-store,
- a UI/backend application,
- a generic service framework,
- an ad-hoc SQL toy,
- a worker-local durable storage system,
- a cache-first system that risks stale or unauthorized results.

---

## 2. Non-Negotiable Architecture Invariants

### 2.1 Immutable Micro-Partitions

Parquet micro-partitions are immutable. Code must never modify existing Parquet files in place.

Allowed write pattern:

1. Read affected micro-partitions.
2. Produce new Arrow batches.
3. Write new Parquet micro-partitions.
4. Commit new metadata.
5. Mark old metadata as superseded when applicable.

### 2.2 Copy-on-Write Updates and Deletes

`UPDATE` and `DELETE` must use copy-on-write. Old micro-partitions remain available for MVCC, Time Travel, rollback, or retention-based cleanup.

### 2.3 FoundationDB Metadata Authority

FoundationDB is the authoritative metadata store. Code must not treat FDB like SQL storage and must not infer durable table state from local worker files, process memory, or cache entries.

Every metadata change must have:

- an explicit key schema,
- a clear transaction boundary,
- defined conflict behavior,
- serialization compatibility expectations,
- tests for success and failure paths when feasible.

### 2.4 Stateless Workers

Workers may cache data, hold transient execution state, and stream batches, but they must not own durable data. A worker restart must not lose committed table state.

### 2.5 MVCC and Snapshot Correctness

Queries must see a consistent snapshot. Micro-partition metadata that participates in visibility must carry enough version information to decide whether it is visible for a given query timestamp or transaction snapshot.

### 2.6 Metadata-Driven Pruning Before Expensive Reads

Where query predicates and metadata statistics allow pruning, code should prune micro-partitions before expensive object-store reads. Pruning must be correctness-preserving: skipping a micro-partition is allowed only when metadata proves it cannot match.

### 2.7 Cache Safety

Caches are performance optimizations, not sources of truth. A cache must not return:

- stale data after relevant table/version/security changes,
- results visible to the wrong user or role,
- data that bypasses authorization checks.

Authorization-sensitive cache keys must include the relevant security context and invalidation epoch/version.

---

## 3. Clean Rust Rules

### 3.1 Public API Documentation

Public structs, enums, traits, functions, and modules require rustdoc explaining domain meaning, usage, and relevant errors.

### 3.2 Typed Errors Over Stringly Errors

Use typed domain errors for library code. Prefer `nova_common::NovaError` or crate-specific typed errors over unstructured strings. Application entrypoints may use `anyhow` only at the boundary where errors are reported to users.

### 3.3 No Production `unwrap()` or `expect()` on Fallible Paths

Production code must not call `.unwrap()` or `.expect()` on fallible results unless all of these are true:

1. the failure is impossible because of a local invariant,
2. the invariant is documented near the call,
3. the panic cannot be triggered by user input, storage state, network state, metadata state, or concurrency.

Tests may use `.unwrap()` when it keeps assertions readable.

### 3.4 Explicit Async Boundaries

I/O and network operations should be async. Blocking work in async contexts must be isolated and justified. Shared state must be `Send + Sync` safe for Tokio execution.

### 3.5 Bounded Modules

A module should have one clear responsibility. If a file mixes parsing, planning, storage, security, and transport concerns, split only when the split reduces risk and can be verified with tests.

### 3.6 Domain Naming

Names should use Nova domain terms consistently: database, schema, table, micro-partition, transaction, snapshot, commit timestamp, worker, coordinator, role, privilege, security context, warehouse, cache epoch.

Avoid names that hide domain behavior, such as `data`, `thing`, `manager2`, `tmp_result`, or `handle_all`.

---

## 4. Testing Rules

### 4.1 Unit Tests Are Primary Evidence

Unit test results are the first proof that a clean-code or correctness change is safe. A refactor is incomplete until relevant tests are run and results are reported.

### 4.2 Coverage Expectations

Every public or non-trivial function should have tests, or a documented reason why testing must happen at a higher integration layer.

Required negative tests where feasible:

- unauthorized RBAC access,
- stale cache prevention,
- FDB missing-key or conflict behavior,
- invalid SQL analysis/planning,
- metadata serialization failure,
- object-store read/write failure,
- MVCC visibility edge cases,
- copy-on-write update/delete behavior.

### 4.3 Regression Tests for Bugs

For a verified bug, write or identify a failing test before fixing when feasible. If infrastructure prevents a direct test, add the closest deterministic unit test and document the blocker.

### 4.4 Test Isolation

Tests must not depend on global mutable state, fixed external service state, or execution order. FDB/docker-dependent tests must declare their environment needs and clean up their keyspace or use isolated prefixes.

---

## 5. Domain-Specific Rules

### 5.1 FoundationDB and Metadata

- Define key prefixes and value encoding clearly.
- Keep transaction boundaries explicit.
- Do not hide FDB conflicts by converting them to generic success.
- Do not perform read-modify-write metadata changes outside the intended transaction.
- Keep security metadata, roles, privileges, and ownership changes auditable.

### 5.2 Storage and Micro-Partitions

- Parquet files are immutable.
- Object paths must be deterministic enough for debugging and safe enough to avoid collision.
- Metadata must include row count, byte size, column statistics where available, commit/version information, and active/superseded state where relevant.
- Readers should support projection and predicate pushdown where available.

### 5.3 Query Engine

- Parser, analyzer, planner, scheduler, and executor boundaries must stay explicit.
- DataFusion should remain the execution engine unless a documented custom operator is required.
- Analyzer/planner errors should be actionable and typed.
- Query result correctness beats optimization.

### 5.4 Security and RBAC

- Security context must flow from protocol/auth layer to execution and metadata access.
- Result cache keys and invalidation logic must account for user, role, secondary-role behavior, and security epoch/version when results can differ.
- RBAC tests must include allowed and denied cases.

### 5.5 Distributed and Raft

- Coordinator HA state must not assume single-process durability in production paths.
- Local or in-memory stores are allowed only for tests/dev paths and must be named as such.
- Network/RPC errors must be propagated or translated into typed failures with context.

### 5.6 CLI and Configuration

- User errors should produce clear diagnostics instead of panics.
- Credentials and secrets must come from config or environment, not hardcoded production paths.
- Defaults must be safe for local development and explicit for production.

---

## 6. Audit Severity Model

- **P0 Architecture breach**: violates OLAP/database invariants, such as in-place micro-partition mutation, durable worker-owned data, incorrect MVCC visibility, FDB misuse, or stale/unauthorized cache behavior.
- **P1 Correctness/security**: can produce wrong query results, security/RBAC leakage, swallowed errors, unsafe transaction behavior, production panic on fallible paths, or async/concurrency bugs.
- **P2 Testability/maintainability**: public or non-trivial code without tests, unclear module boundaries, duplicate logic, stringly typed domain behavior, or overly large responsibilities.
- **P3 Style/readability**: naming, formatting, stale comments, missing local documentation, or small readability issues.

P0/P1 issues are fixed first. P2 issues are fixed when local and safe. P3 issues are fixed opportunistically when touching the same area.

---

## 7. Review Checklist

Before merging or claiming completion, verify:

- [ ] The change preserves immutable micro-partitions.
- [ ] Metadata authority remains in FoundationDB.
- [ ] Workers remain stateless for durable data.
- [ ] MVCC/snapshot visibility remains correct.
- [ ] Cache behavior cannot return stale or unauthorized results.
- [ ] Public APIs have rustdoc.
- [ ] Production fallible paths avoid undocumented `unwrap()`/`expect()`.
- [ ] Errors are typed or translated at the correct boundary.
- [ ] Relevant unit tests were added or run.
- [ ] Security/RBAC changes include denied-case tests.
- [ ] FDB changes include transaction/keyspace reasoning.
- [ ] `cargo fmt --all -- --check` passes or a blocker is documented.
- [ ] `cargo test --all` passes or a blocker is documented.
- [ ] `cargo clippy --all -- -D warnings` passes or a blocker is documented.

---

## 8. Documentation Precedence

When documentation, roadmap, and source disagree:

1. Current source code and tests are the immediate truth for behavior.
2. `ROADMAP.md` is the truth for current project status and phase claims.
3. `docs/design/architecture.md` is the truth for target architecture.
4. `AGENTS.md` is the truth for agent workflow.
5. This file is the truth for engineering rules.

If a conflict is found, update the stale document as part of the audit.
