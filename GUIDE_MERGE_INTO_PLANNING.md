# GUIDE_MERGE_INTO_PLANNING.md — Snowflake MERGE INTO Planning

> Status: Planning reference for ROADMAP Phase 18
> Scope: Snowflake-inspired `MERGE INTO` for nova-core, including semantics, implementation steps, RBAC, and TASK integration
> Last updated: July 2026

---

## 1. Purpose

This guide defines how nova-core should implement a Snowflake-compatible Phase 18 subset of `MERGE INTO` without violating nova-core architecture invariants:

- Micro-partitions are immutable Parquet files.
- UPDATE, DELETE, and MERGE effects use copy-on-write, never in-place mutation.
- Metadata commits are atomic in FoundationDB.
- Workers remain stateless.
- MVCC timestamps and table versions drive visibility, Time Travel, Streams, and result-cache invalidation.
- RBAC is deny-by-default for both interactive SQL and background TASK execution.

`MERGE INTO` is Phase 18 in `ROADMAP.md`. It depends on the existing COW DML, DataFusion query path, stream metadata, result-cache invalidation, Phase 16 enterprise RBAC, and Phase 17 task execution context.

---

## 2. Current Repository References

### Roadmap references

- `ROADMAP.md` Phase 3 includes MVCC, UPDATE/DELETE COW, Time Travel, Clone, Streams, and GC. MERGE must reuse these capabilities rather than creating a separate mutation path.
- `ROADMAP.md` Phase 9 includes COW visibility and result-cache invalidation. MERGE must update table versions and invalidate caches the same way committed INSERT/UPDATE/DELETE does.
- `ROADMAP.md` Phase 16 includes row access policies, masking policies, audit/access history, cache security epochs, and background job bypass prevention. MERGE must be governed by the same security model.
- `ROADMAP.md` Phase 17 includes TASK orchestration, stream-triggered tasks, transactional stream consumption, task owner-role execution, and task audit/history. MERGE must integrate with scheduled and stream-triggered task execution.

### Architecture references

- `docs/design/architecture.md` defines immutable micro-partitions and copy-on-write: UPDATE/DELETE create new MPs and retain old MPs for Time Travel.
- `docs/design/architecture.md` defines FoundationDB as metadata source of truth for catalogs, versioning, transactions, RBAC, streams, and clone mappings.
- `docs/design/architecture.md` defines stateless workers and coordinator-owned planning/scheduling.

### Current source-code touchpoints

These are the current code areas that Phase 18 must inspect and extend before implementation. Line numbers are current anchors for this planning pass and should be rechecked before coding:

- SQL parsing: `crates/nova-coordinator/src/parser.rs:18` parses SQL with `sqlparser-rs` and pre-parses custom Nova syntax when sqlparser does not support a Snowflake feature.
- Scheduling: `crates/nova-coordinator/src/scheduler.rs:24` executes resolved statements through the coordinator executor with `SecurityContext`.
- MySQL query path: `crates/nova-coordinator/src/mysql_server.rs:120` performs parse -> analyze -> execute for MySQL text queries.
- Worker DataFusion execution: `crates/nova-worker/src/executor.rs:33` executes SQL through DataFusion `SessionContext` after registering Nova table providers.
- Security privileges: `crates/nova-common/src/types.rs:138` exposes privilege set membership and mutation helpers. Phase 18 must use the enterprise RBAC layer planned in Phase 16, not ad-hoc root bypasses.

### TASK planning reference status

`ROADMAP.md` references `GUIDE_TASK_PLANNING.md`, and that guide is present in the current checkout. Phase 18 TASK integration should be cross-checked against both `ROADMAP.md` Phase 17 and `GUIDE_TASK_PLANNING.md` before coding, especially for immutable task version snapshots, owner-role execution, `EXECUTE AS USER`, stream offset advancement, HA leases, and background-job RBAC bypass prevention.

---

## 3. Snowflake MERGE INTO Research Summary

Primary references:

- Snowflake `MERGE`: https://docs.snowflake.com/en/sql-reference/sql/merge
- Snowflake `ERROR_ON_NONDETERMINISTIC_MERGE`: https://docs.snowflake.com/en/sql-reference/parameters#label-error-on-nondeterministic-merge
- Snowflake transactions and locks: https://docs.snowflake.com/en/sql-reference/transactions
- Snowflake `ALTER SESSION`: https://docs.snowflake.com/en/sql-reference/sql/alter-session
- Snowflake `UPDATE`: https://docs.snowflake.com/en/sql-reference/sql/update
- Snowflake `DELETE`: https://docs.snowflake.com/en/sql-reference/sql/delete
- Snowflake `INSERT`: https://docs.snowflake.com/en/sql-reference/sql/insert
- Snowflake access-control privileges: https://docs.snowflake.com/en/user-guide/security-access-control-privileges
- Snowflake hybrid table limitations: https://docs.snowflake.com/en/user-guide/tables-hybrid-limitations

### Supported Snowflake syntax

```sql
MERGE INTO <target_table>
  USING <source>
  ON <join_expr>
  { matchedClause | notMatchedClause } [ ... ]
```

Matched clause:

```sql
WHEN MATCHED
  [ AND <case_predicate> ]
  THEN {
    UPDATE { ALL BY NAME | SET <col_name> = <expr> [ , <col_name> = <expr> ... ] }
    | DELETE
  }
```

Not-matched clause:

```sql
WHEN NOT MATCHED
  [ AND <case_predicate> ]
  THEN INSERT {
    ALL BY NAME
    | [ ( <col_name> [ , ... ] ) ] VALUES ( <expr> [ , ... ] )
  }
```

### Semantics to preserve

1. `USING` can be a table or subquery.
2. `ON` defines target/source matching.
3. `WHEN MATCHED` handles updates and deletes for target rows that match source rows.
4. `WHEN NOT MATCHED` handles inserts for source rows that do not match target rows.
5. Multiple matched and not-matched clauses are allowed.
6. Clauses are ordered. The first matching clause for its matched/not-matched category wins.
7. A catch-all clause without `AND <case_predicate>` must be last for that clause type.
8. `UPDATE ALL BY NAME` and `INSERT ALL BY NAME` require target and source to have the same column count and names; column order may differ.
9. Duplicate source rows that do not match any target row are all inserted.
10. Duplicate source rows that match one target row can be deterministic or nondeterministic depending on the actions they trigger.

### Nondeterministic duplicate handling

Snowflake default:

```sql
ERROR_ON_NONDETERMINISTIC_MERGE = TRUE
```

If multiple source rows match one target row, MERGE is nondeterministic when:

- more than one source row attempts to update the same target row with potentially different values;
- one source row attempts to update the target row while another attempts to delete it.

With `ERROR_ON_NONDETERMINISTIC_MERGE = TRUE`, the statement errors. With `FALSE`, Snowflake allows the statement but the selected source row/action is undefined.

Deterministic cases include:

- one or more source rows trigger only DELETE for the target row and no source row triggers UPDATE;
- exactly one source row triggers UPDATE and all other matched source rows trigger no matched action.

### Transaction and locking behavior

Snowflake treats MERGE as DML. With autocommit enabled, a MERGE outside an explicit transaction is a single-statement transaction. Inside an explicit transaction, it participates in that transaction. UPDATE, DELETE, and MERGE hold locks that generally block concurrent UPDATE/DELETE/MERGE on the same standard table until commit or rollback.

nova-core should implement the equivalent logical guarantees using MVCC, table/MP conflict detection, and atomic FDB metadata commits.

### Unsupported initial scope for Nova Phase 18

Initial Phase 18 should not implement:

- `WHEN NOT MATCHED BY SOURCE`;
- `RETURNING` or row payload output;
- multi-target MERGE;
- arbitrary nondeterministic behavior unless the compatibility parameter is explicitly disabled;
- worker-local mutation state;
- a claim of complete Snowflake compatibility beyond documented Phase 18 scope.

---

## 4. Compatibility Goals

### Required compatibility

- Syntax and behavior for matched UPDATE, matched DELETE, not-matched INSERT, multiple ordered clauses, aliases, subquery source, and `ALL BY NAME`.
- Snowflake-compatible duplicate source behavior with `ERROR_ON_NONDETERMINISTIC_MERGE` defaulting to `TRUE`.
- At least session-scoped support for setting `ERROR_ON_NONDETERMINISTIC_MERGE=FALSE` in Phase 18, so nondeterministic compatibility mode can be tested explicitly. Account/user-level parameter resolution may be added through the Phase 16 parameter model when available.
- Source duplicate inserts insert all copies when there is no matching target row.
- Row-count result reporting for inserted, updated, and deleted rows.
- Statement-level snapshot semantics: rows inserted earlier in the same MERGE do not become target matches for later source rows.

### Nova-specific constraints

- Mutations use immutable MP copy-on-write.
- Commit visibility is controlled by FDB metadata, table versions, and commit timestamps.
- Result-cache invalidation is table-version driven.
- Streams observe committed MP/version changes only.
- TASK runs execute under task security context, not interactive caller context.

---

## 5. Implementation Plan

### Step 1: Parser support

1. Check whether the pinned `sqlparser-rs` version supports `Statement::Merge` and whether it supports Snowflake `ALL BY NAME`.
2. If native support is sufficient, use sqlparser AST directly.
3. If native support is incomplete, add a narrow Nova pre-parser in `SqlParser::parse` following the existing custom syntax pattern in `crates/nova-coordinator/src/parser.rs`.
4. Preserve original SQL text for audit, task version snapshots, and diagnostics.
5. Add parser tests for:
   - simple update MERGE;
   - insert-only MERGE;
   - mixed delete/update/insert MERGE;
   - `ALL BY NAME`;
   - target/source aliases;
   - source subquery;
   - unreachable catch-all clauses;
   - invalid syntax.

### Step 2: Resolved AST and analyzer

Add a resolved representation that carries enough information for both interactive and task execution:

```text
ResolvedStatement::Merge {
  target,
  target_alias,
  source,
  source_alias,
  join_expr,
  clauses,
  original_sql,
}
```

Clause model:

```text
MergeClause {
  kind: Matched | NotMatched,
  predicate: Option<Expr>,
  action: UpdateSet | UpdateAllByName | Delete | InsertValues | InsertAllByName,
}
```

Analyzer responsibilities:

- Resolve target table and source relation/subquery.
- Resolve aliases and column references.
- Validate expressions by action type:
  - UPDATE expressions may refer to target and source columns.
  - INSERT values for not-matched rows must not depend on a target row.
- Validate duplicate target assignments in `UPDATE SET`.
- Validate insert column count and expression count.
- Validate `ALL BY NAME` by comparing target/source names and count.
- Validate catch-all clause ordering.
- Keep enough column lineage for audit and policy enforcement.

### Step 3: RBAC pre-check

Before planning/execution, collect required privileges from the statement:

| Statement component | Required privilege |
|---|---|
| Source table/view/stream read | `SELECT` or stream read equivalent |
| Target insert clause | `INSERT` on target table |
| Target update clause | `UPDATE` on target table |
| Target delete clause | `DELETE` on target table |
| Warehouse execution | existing warehouse/compute usage rules |
| Task execution | Phase 17 task execution privileges and owner-role checks |

Pre-check should require privileges statically for every action clause present after validation. Authorization must not depend on source data, target data, or whether a clause happens to match at runtime. Runtime policy checks still apply row-by-row after predicates and row access policies are evaluated.

Deny-by-default when any user, role, object, privilege, or security epoch lookup is missing or stale.

### Step 4: Logical planning

The planner should build a logical MERGE plan with these phases:

1. Capture statement snapshot timestamp/version.
2. Evaluate source relation/subquery under the active security context and policies.
3. Join target snapshot to source using the `ON` expression.
4. Classify joined rows as matched.
5. Classify source rows with no target match as not matched.
6. Evaluate ordered clauses.
7. Produce intended action records:
   - update target row to new values;
   - delete target row;
   - insert new row;
   - no action.
8. Detect duplicate target action conflicts before writing.
9. Hand action records to COW mutation executor.

The key correctness rule: source duplicate insert rows are evaluated against the statement's original target snapshot, not against rows inserted earlier in the same MERGE.

### Step 5: Duplicate target action detection

For each target row identity, group all source matches that produced an action.

Rules:

| Action group for one target row | Result |
|---|---|
| zero actions | no-op |
| one UPDATE | deterministic update |
| one DELETE | deterministic delete |
| multiple DELETE actions only | deterministic delete |
| exactly one UPDATE and all other matched rows no-op | deterministic update |
| multiple UPDATE actions | nondeterministic |
| UPDATE and DELETE actions | nondeterministic |

When nondeterministic:

- if `ERROR_ON_NONDETERMINISTIC_MERGE=TRUE`, abort before any write becomes visible;
- if `FALSE`, allow execution but mark query history/audit/tracing as nondeterministic compatibility mode.

### Step 6: DataFusion execution integration

Use DataFusion for set computation but keep Nova metadata semantics outside DataFusion:

- Use DataFusion for source subquery execution.
- Register target/source table providers with snapshot-aware active MPs.
- Use DataFusion expressions for join and clause predicate evaluation where possible.
- Materialize action batches with target row identifiers and source values.
- Avoid relying on DataFusion to mutate storage directly.

Target row identity must be stable enough to map rows back to original micro-partitions for COW rewriting. If current storage lacks row identity metadata, Phase 18 must add an internal row locator such as `(table_id, mp_id, row_offset)` in the mutation pipeline without exposing it as a user column.

### Step 7: Copy-on-write mutation execution

For updates/deletes:

1. Group actions by affected MP.
2. Read original MP batches.
3. Apply updates/deletes to affected rows.
4. Write replacement MPs for changed row sets.
5. Mark old MPs as superseded in FDB metadata.
6. Keep unchanged MPs active.

For inserts:

1. Build insert RecordBatches from not-matched action records.
2. Apply target schema defaults/nullability/type coercion rules.
3. Write new MPs.
4. Add new active MP metadata.

For mixed MERGE, commit all metadata changes atomically. A partial MERGE must never become visible.

### Step 8: FDB metadata and transaction atomicity

The FDB transaction for a successful MERGE should atomically include:

- new MP metadata for inserts and rewritten MPs;
- superseded metadata updates for old MPs;
- table version increment;
- transaction/commit timestamp record;
- stream change metadata for inserted/updated/deleted rows or MP versions;
- result-cache invalidation metadata if persisted;
- query/audit success reference;
- task run update if executing inside a TASK transaction boundary.

On error:

- no new MP metadata becomes active;
- old MPs remain active;
- table version does not increment;
- target stream CDC metadata does not record a successful target change;
- source stream offsets consumed by interactive or task MERGE do not advance;
- task run records do not claim stream-offset advancement;
- audit records record denial/error without claiming success.

Object-store files written before an FDB abort are orphan candidates. They should be written under temporary/staging paths or recorded for cleanup so they cannot be observed as active data without FDB metadata.

### Step 9: Result output

Return Snowflake-style row counts:

```text
number of rows inserted | number of rows updated | number of rows deleted
```

The executor result type may need to represent DML summaries consistently across INSERT/UPDATE/DELETE/MERGE. MySQL protocol should send this as a result set or OK packet consistent with existing DML behavior.

### Step 10: Session/account/user parameter

Implement or plan parameter resolution for:

```sql
ERROR_ON_NONDETERMINISTIC_MERGE = TRUE | FALSE
```

Phase 18 must expose at least a session-scoped way to set this parameter so `FALSE` mode can be tested. Resolution order should follow Nova's Phase 16 parameter model when it exists; until then, use session state with default `TRUE` and no account/user override.

---

## 6. RBAC, Governance, and Audit Design

### Authorization timing

MERGE needs two layers of authorization:

1. Compile-time/pre-execution checks for object-level privileges.
2. Runtime policy checks for row/column governance and security context changes.

If a privilege is revoked after a task is created but before it runs, the task run must fail. Task creation does not grant future execution authority.

### Privilege matrix

| MERGE feature | Required target privilege | Required source privilege |
|---|---|---|
| `WHEN MATCHED THEN UPDATE` | `UPDATE` | `SELECT` |
| `WHEN MATCHED THEN DELETE` | `DELETE` | `SELECT` |
| `WHEN NOT MATCHED THEN INSERT` | `INSERT` | `SELECT` |
| `UPDATE ALL BY NAME` | `UPDATE` | `SELECT` |
| `INSERT ALL BY NAME` | `INSERT` | `SELECT` |
| source subquery with joins | action-specific target privilege | `SELECT` on all referenced objects |
| source stream | action-specific target privilege | stream/table access required by Phase 16/17 |

### Row access and masking policies

- Source row access policies must filter source rows before they participate in the MERGE.
- Target row access policies must control which target rows can be matched/modified.
- Before implementation, choose and document the policy-hidden target row rule: either deny MERGE when a source row would match a policy-hidden target row, or treat policy-hidden target rows as invisible and therefore not matched. The chosen behavior needs negative tests to avoid accidental duplicate inserts or unauthorized modifications.
- Masking policies must apply to expressions consistently. A user must not bypass masking by using a masked column in an `ON`, clause predicate, UPDATE expression, or INSERT expression.
- MP pruning must remain conservative when policy columns are involved.

### Audit and access history

Record:

- query id and transaction id;
- user, active role, secondary-role policy, and security epoch;
- source objects read;
- target table modified;
- columns read, updated, and inserted;
- row counts inserted/updated/deleted;
- table version before and after commit;
- stream offsets consumed if applicable;
- policies referenced;
- authorization failures and denied object references;
- nondeterministic mode usage if `ERROR_ON_NONDETERMINISTIC_MERGE=FALSE`.

Do not log raw secrets, passwords, tokens, sensitive expression values, or unredacted policy internals.

---

## 7. TASK Integration Design

MERGE is a common operation for scheduled ELT and stream-triggered CDC upsert tasks. Phase 18 must integrate with the Phase 17 task model rather than adding a separate scheduler path.

### Task SQL body support

TASK SQL bodies may contain MERGE statements:

```sql
CREATE TASK upsert_orders
  SCHEDULE = '5 MINUTES'
AS
  MERGE INTO fact_orders t
  USING stage_orders s
  ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET t.status = s.status
  WHEN NOT MATCHED THEN INSERT (order_id, status) VALUES (s.order_id, s.status);
```

Task definitions must store the original MERGE SQL in immutable task version snapshots. A running task uses the version active at run creation time, even if later DDL changes the task.

### Task execution identity

By default, a task run executes as:

- system service user;
- task owner role;
- task security epoch/version captured and re-checked at run start.

If Phase 17 supports `EXECUTE AS USER`, MERGE must use that validated execution identity. The owner role must have impersonation rights and the execute-as user must be granted the owner role according to Phase 17 rules.

### Privilege re-check before each run

Before executing a MERGE task, re-check:

- task `USAGE`/operation authority according to Phase 17;
- account-level task execution privilege;
- warehouse/serverless execution privilege;
- source object SELECT/stream access;
- target table INSERT/UPDATE/DELETE privileges required by the MERGE body;
- row/masking policy access;
- current security epoch.

Revoked privileges must fail the task run rather than using stale authorization from task creation or resume time.

### Stream-triggered MERGE

Typical pattern:

```sql
CREATE TASK merge_customer_changes
  WHEN SYSTEM$STREAM_HAS_DATA('customer_changes')
AS
  MERGE INTO dim_customer t
  USING customer_changes s
  ON t.customer_id = s.customer_id
  WHEN MATCHED AND s.metadata_action = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE SET t.name = s.name
  WHEN NOT MATCHED THEN INSERT (customer_id, name) VALUES (s.customer_id, s.name);
```

Rules:

- `SYSTEM$STREAM_HAS_DATA` should remain metadata-only.
- MERGE consumes source stream rows under the same transaction as target modifications for both interactive SQL and TASK execution.
- Source stream offsets advance only after the MERGE commits.
- Failed MERGE runs leave source stream offsets unchanged for retry.
- Target stream CDC metadata is produced only by committed target table changes and is separate from source stream offset advancement.
- Task history records source stream offsets consumed and target table version produced.

### Task run history

Record in task run metadata:

- MERGE row counts;
- source table/stream names;
- target table name;
- table version before/after;
- stream offsets before/after;
- duplicate conflict errors;
- authorization failures;
- nondeterministic compatibility mode usage;
- retry source and attempt number.

### Failure behavior

| Failure | Expected behavior |
|---|---|
| source SELECT revoked | task run fails, no offset advancement |
| target UPDATE revoked | task run fails, no target changes |
| nondeterministic duplicate conflict | task run fails by default |
| FDB conflict | retry only if idempotency and transaction retry rules are satisfied |
| worker crash before commit | run can retry, no visible partial target changes |
| coordinator failover before scheduling | FDB task leases prevent duplicate graph runs |
| coordinator failover after commit | committed table version and task run record are source of truth |

---

## 8. Test Strategy

### Parser and analyzer tests

- Valid simple update MERGE.
- Valid insert-only MERGE.
- Valid mixed delete/update/insert MERGE.
- Valid aliases for target and source.
- Valid source subquery.
- Valid `UPDATE ALL BY NAME` with different column order.
- Valid `INSERT ALL BY NAME` with different column order.
- Error for catch-all matched clause before predicate matched clause.
- Error for catch-all not-matched clause before predicate not-matched clause.
- Error for duplicate target column assignments.
- Error for missing or extra `ALL BY NAME` columns.
- Error for target-only references in not-matched insert values.

### Execution correctness tests

- Basic update changes one row.
- Basic insert inserts one row.
- Mixed clauses delete, update, and insert in one statement.
- Unmatched target rows are unchanged.
- Duplicate unmatched source rows are all inserted.
- Multiple source rows updating one target row error by default.
- Update/delete conflict errors by default.
- Multiple delete actions for one target row succeed deterministically.
- Exactly one update action with other no-op matches succeeds deterministically.
- `ERROR_ON_NONDETERMINISTIC_MERGE=FALSE` permits ambiguous duplicates and records nondeterministic mode.

### Storage/MVCC tests

- MERGE rewrites affected MPs and leaves old MPs retained for Time Travel.
- Time Travel before MERGE returns old target state.
- Time Travel after MERGE returns new target state.
- Clone source remains isolated when clone target is merged.
- Streams observe MERGE insert/update/delete changes.
- Result cache invalidates after committed MERGE.
- Failed MERGE leaves active MPs and table version unchanged.
- Concurrent MERGE/UPDATE/DELETE conflicts on the same target table or affected MPs are detected.
- FDB conflict retry does not duplicate inserted rows or supersede wrong MPs.

### RBAC/governance tests

- User with source SELECT and target INSERT can run insert-only MERGE.
- User missing source SELECT is denied.
- User missing target UPDATE is denied for update MERGE.
- User missing target DELETE is denied for delete MERGE.
- User missing target INSERT is denied for insert MERGE.
- User is denied for an action clause they are not privileged to run even when data would not match that clause at runtime.
- Revoked privilege between task creation and task run causes task failure.
- Row access policy prevents unauthorized target rows from being modified.
- Policy-hidden target row behavior follows the documented deny-or-invisible rule and cannot cause unauthorized duplicate inserts.
- Masking policy cannot be bypassed through ON predicate or update expression.
- MySQL protocol path uses session user security context, not root.
- Background task execution cannot bypass RBAC.

### TASK integration tests

- Scheduled MERGE task executes and records row counts.
- Stream-triggered MERGE task runs when stream has data.
- Failed MERGE task does not advance source stream offset.
- Interactive failed `MERGE ... USING <stream>` does not advance source stream offset.
- Retry after failure consumes the same stream rows once and commits once.
- Revoked task owner role privilege fails the run.
- Execute-as impersonation denial fails the run.
- Coordinator failover does not duplicate a queued graph run.
- Task history redacts sensitive SQL/error details according to security policy.

### Benchmark tests

- Small upsert batch.
- Large CDC batch.
- High duplicate source batch.
- `ALL BY NAME` merge.
- Selective join with MP pruning.
- Stream-triggered task MERGE latency.
- MP rewrite amplification per updated row distribution.

---

## 9. Acceptance Criteria

Phase 18 is complete when:

1. `MERGE INTO` works through parser, analyzer, executor, scheduler, MySQL protocol, and TASK execution paths.
2. Matched UPDATE/DELETE, not-matched INSERT, ordered clauses, `ALL BY NAME`, source subqueries, aliases, and row-count output are implemented and tested.
3. Duplicate source behavior follows Snowflake-compatible deterministic rules and defaults to error for nondeterministic update/delete conflicts.
4. `ERROR_ON_NONDETERMINISTIC_MERGE=FALSE` can be set at least at session scope and is covered by explicit tests and audit/tracing markers.
5. MERGE uses immutable MP copy-on-write and preserves MVCC, Time Travel, Clone, Streams, and result-cache invalidation.
6. FDB commits make all target metadata changes atomically visible or not visible at all.
7. Interactive and task `MERGE ... USING <stream>` advance source stream offsets only after the same transaction commits successfully.
8. Concurrent MERGE/UPDATE/DELETE conflict, retry, and idempotency behavior is tested for same-table or affected-MP conflicts.
9. RBAC and governance deny unauthorized interactive and background MERGE execution by default.
10. Scheduled and stream-triggered TASK workflows can run MERGE without advancing source stream offsets on failed runs.
11. Audit, access history, tracing, metrics, and task run history expose row counts, security context, modified objects, and failures without leaking secrets.
12. Documentation clearly states Nova-specific compatibility boundaries.

---

## 10. Implementation Order

Recommended order:

1. Parser/analyzer support with no execution.
2. Resolved MERGE AST and validation tests.
3. Logical action classification using in-memory test data.
4. Duplicate detection and session parameter default.
5. COW mutation execution for simple matched update/delete and insert.
6. Mixed action execution and row-count output.
7. FDB atomic commit, cache invalidation, stream metadata, and Time Travel tests.
8. RBAC enforcement and negative tests.
9. TASK execution support and stream-triggered MERGE tests.
10. Observability, benchmarks, and hardening.

This order keeps correctness testable before distributed/task complexity is added.

---

## 11. Engineering Constraints

- Do not add new dependencies without approval.
- Prefer existing `sqlparser-rs`, DataFusion, Arrow, Parquet, object_store, FoundationDB, and tracing infrastructure.
- Do not use `unwrap()` or `expect()` in production paths.
- Keep shared types in `nova-common` only when they are needed across crates.
- Keep storage mutation primitives in `nova-storage` and query coordination in `nova-coordinator`.
- Do not make `nova-worker` depend on `nova-coordinator`.
- Do not store execution state on workers.
- Do not expose internal row locators as user-visible columns.

---

## 12. Verification Commands

Use narrow checks first, then broaden:

```bash
cargo test -p nova-coordinator merge
cargo test -p nova-storage merge
cargo test --all merge
cargo test --all rbac
cargo test --all security
cargo test --all task
cargo clippy --all -- -D warnings
cargo fmt --all -- --check
```

If FoundationDB, MinIO, or Docker-backed services are unavailable, report that clearly and do not weaken tests to pass locally.

---

## 13. Operational Guidance for Users

Users should de-duplicate source data when multiple source rows could update the same target row:

```sql
MERGE INTO target t
USING (
  SELECT k, MAX(v) AS v
  FROM src
  GROUP BY k
) s
ON t.k = s.k
WHEN MATCHED THEN UPDATE SET t.v = s.v
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v);
```

Task authors should design MERGE statements to be idempotent where possible, especially when consuming streams. Failed task runs may retry with the same stream offset, so deterministic source keys and duplicate handling are essential for correctness.
