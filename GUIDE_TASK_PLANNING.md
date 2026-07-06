# GUIDE_TASK_PLANNING.md — Enterprise Task Orchestration for nova-core

> Planning guide for Phase 17: Enterprise Task Orchestration. This document defines the target design for Snowflake-inspired `TASK` support in nova-core: scheduled SQL automation, stream-triggered ELT, task graphs, HA scheduling, RBAC, audit, observability, and robust failure handling.

---

## 1. Purpose

Nova TASK is the database-native orchestration layer for recurring and event-driven data workflows. It lets users define SQL work that runs automatically inside nova-core without an external cron service, Airflow deployment, or worker-local scheduler.

The feature must support:

- Scheduled SQL automation using interval schedules and Snowflake-style cron expressions.
- Stream-triggered ELT using `WHEN SYSTEM$STREAM_HAS_DATA('<stream>')`.
- Task graphs/DAGs using `AFTER` dependencies and optional finalizers.
- Transactionally correct stream consumption: stream offsets advance only after successful committed DML.
- Enterprise RBAC: ownership, task privileges, execute privileges, warehouse privileges, stream/table privileges, and optional execute-as-user semantics.
- HA-safe scheduling through FoundationDB leases and coordinator leadership.
- Full observability: task history, graph history, metrics, tracing, audit, and cost attribution.

This is a planning document, not a claim of full Snowflake compatibility. Nova should implement Snowflake-inspired semantics where they align with nova-core architecture.

---

## 2. Architectural Invariants

TASK implementation must preserve nova-core's non-negotiable architecture rules:

1. **Immutable micro-partitions**
   - Tasks execute SQL that may create new micro-partitions, but never modify Parquet files in place.
   - UPDATE/DELETE/MERGE-style operations must remain copy-on-write.

2. **MVCC everywhere**
   - Task runs must see a consistent snapshot.
   - Stream offsets are version/commit timestamp bookmarks.
   - Stream consumption advances offsets only on committed transactions.

3. **FoundationDB is source of truth**
   - Task definitions, graph edges, versions, leases, run records, stream trigger indexes, RBAC references, and audit pointers live in FDB.
   - Workers must not store durable scheduler state.

4. **Stateless workers**
   - Workers execute query fragments or task SQL but do not own task schedule state.
   - Worker crashes must not corrupt task state or stream offsets.

5. **Coordinator-controlled orchestration**
   - The coordinator schedules, validates, leases, and records task runs.
   - In HA mode, only the current leader or a valid FDB lease holder may create a run for a task/graph.

6. **Deny-by-default security**
   - Missing user, role, grant, owner, warehouse, stream, table, or task metadata means deny.
   - No background job may bypass RBAC just because it runs outside an interactive session.

7. **No new dependencies without approval**
   - Use the existing tech stack where possible.
   - If cron/timezone support requires a crate, it must be justified and approved before implementation.

---

## 3. Snowflake Parity Target

Nova should support the Snowflake concepts that matter for enterprise data pipelines:

| Snowflake Concept | Nova Target |
|---|---|
| `CREATE TASK` | Supported for SQL statements that nova-core can execute |
| `ALTER TASK ... RESUME/SUSPEND` | Supported with RBAC checks and scheduler state changes |
| Interval schedule | Supported: seconds, minutes, hours |
| Cron schedule | Supported with five-field expression and explicit timezone |
| Triggered task | Supported using `WHEN SYSTEM$STREAM_HAS_DATA(...)` |
| Scheduled task with `WHEN` | Supported; skip when `WHEN` is false |
| Task graph | Supported with `AFTER` dependencies |
| Finalizer task | Supported with `FINALIZE = <root_task>` |
| Versioned task runs | Supported through immutable task version snapshots |
| Task history | Supported through information schema/account usage views |
| Serverless task | Planned as Nova-managed warehouse/compute abstraction, not cloud-provider serverless initially |
| Notification integrations | Not in initial scope |
| External functions/procedures | Limited to capabilities already available in nova-core |

---

## 4. Non-Goals for Initial Implementation

The initial enterprise design intentionally excludes:

- Arbitrary OS-level cron or shell command execution.
- Worker-local durable scheduling state.
- External notification integrations such as SNS, Event Grid, Pub/Sub, email, or webhooks.
- Full Snowflake Scripting compatibility beyond SQL constructs nova-core supports.
- Stored procedures in languages not already supported by nova-core.
- Exactly-once claims under all possible external side effects. Nova can provide transactional correctness for nova-core metadata/data writes, but task SQL must still be designed idempotently where it interacts with non-transactional outputs.
- Complete Snowflake compatibility language in documentation or marketing.

---

## 5. Technology Summary

| Layer | Technology / Component | Usage |
|---|---|---|
| SQL parser | `sqlparser-rs` | Parse TASK DDL/DCL and task SQL bodies where supported |
| Coordinator async runtime | `tokio` | Scheduler loop, run dispatch, timeouts, background health checks |
| Metadata | FoundationDB | Task catalog, versions, graph edges, leases, history, RBAC links, trigger indexes |
| Query execution | DataFusion + nova coordinator/executor | Execute task SQL through the same secured execution path as user queries |
| Storage | Parquet micro-partitions in S3/MinIO via `object_store` | Task DML writes immutable MPs and metadata |
| Streams | MVCC table versions + stream offsets | Trigger and consume CDC changes |
| HA | `openraft` coordinator leadership + FDB leases | Avoid duplicate scheduling during failover |
| RPC | `tonic` gRPC | Dispatch execution to workers in distributed mode |
| Cache | Foyer/Nova cache layers | Must be invalidated by task-owned DML and security epoch changes |
| Observability | `tracing`, Prometheus metrics, history views | Scheduler decisions, execution spans, audit, cost metrics |
| Serialization | `bincode`/serde | Persist task metadata records |
| Error handling | `thiserror`/NovaError | Typed user and internal errors |

---

## 6. User-Facing SQL Surface

### 6.1 Create scheduled task

```sql
CREATE TASK refresh_sales_hourly
  WAREHOUSE = analytics_wh
  SCHEDULE = '1 HOUR'
AS
  INSERT INTO sales_hourly
  SELECT date_trunc('hour', order_time), sum(order_total)
  FROM orders
  GROUP BY 1;
```

### 6.2 Create cron task

```sql
CREATE TASK refresh_sales_daily
  WAREHOUSE = analytics_wh
  SCHEDULE = 'USING CRON 0 3 * * * UTC'
  USER_TASK_TIMEOUT_MS = 7200000
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
AS
  INSERT INTO sales_daily
  SELECT current_date, sum(order_total)
  FROM orders;
```

### 6.3 Create stream-triggered task

```sql
CREATE TASK consume_orders_stream
  WAREHOUSE = etl_wh
  WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
  INSERT INTO fact_orders
  SELECT order_id, customer_id, order_total, order_time
  FROM orders_stream
  WHERE METADATA$ACTION = 'INSERT';
```

### 6.4 Create scheduled task with stream guard

```sql
CREATE TASK consume_orders_batch_5m
  WAREHOUSE = etl_wh
  SCHEDULE = '5 MINUTES'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
  INSERT INTO fact_orders
  SELECT order_id, customer_id, order_total, order_time
  FROM orders_stream;
```

### 6.5 Create task graph

```sql
CREATE TASK root_load
  WAREHOUSE = etl_wh
  SCHEDULE = '15 MINUTES'
AS
  SELECT 1;

CREATE TASK load_customers
  WAREHOUSE = etl_wh
  AFTER root_load
AS
  INSERT INTO dim_customer SELECT * FROM customer_stream;

CREATE TASK load_orders
  WAREHOUSE = etl_wh
  AFTER root_load
AS
  INSERT INTO fact_order SELECT * FROM order_stream;

CREATE TASK aggregate_sales
  WAREHOUSE = etl_wh
  AFTER load_customers, load_orders
AS
  INSERT INTO sales_summary SELECT * FROM fact_order;
```

### 6.6 Create finalizer task

```sql
CREATE TASK cleanup_load
  WAREHOUSE = etl_wh
  FINALIZE = root_load
AS
  DELETE FROM task_temp_state WHERE run_id = SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');
```

### 6.7 Manage task lifecycle

```sql
ALTER TASK root_load RESUME;
ALTER TASK root_load SUSPEND;
EXECUTE TASK root_load;
DROP TASK cleanup_load;
SHOW TASKS;
DESC TASK root_load;
```

---

## 7. Scheduler Model

Nova TASK scheduler is a database service, not an OS cron daemon.

### 7.1 Scheduler responsibilities

The coordinator-side scheduler must:

1. Discover due tasks.
2. Acquire an HA-safe lease.
3. Create immutable task/graph run records.
4. Evaluate `WHEN` conditions through metadata-only paths when possible.
5. Dispatch task SQL through the normal secured executor.
6. Record state transitions.
7. Advance stream offsets only through committed transactions.
8. Schedule child tasks after predecessor success.
9. Run finalizer tasks after graph completion/failure.
10. Release or expire leases safely.

### 7.2 Interval schedule semantics

Supported interval forms:

```sql
SCHEDULE = '10 SECONDS'
SCHEDULE = '5 MINUTES'
SCHEDULE = '1 HOUR'
```

Rules:

- Interval base time is set when the task is resumed.
- A task must not run before its interval is reached.
- If a standalone scheduled task is still running when the next schedule fires, the scheduled run is skipped unless a future overlap policy explicitly permits it.
- Skipped runs must be recorded with enough detail for history and debugging.

### 7.3 Cron schedule semantics

Supported cron form:

```sql
SCHEDULE = 'USING CRON <minute> <hour> <day_of_month> <month> <day_of_week> <timezone>'
```

Cron fields:

```text
# ┌──────── minute: 0-59
# │ ┌────── hour: 0-23
# │ │ ┌──── day of month: 1-31, L
# │ │ │ ┌── month: 1-12, JAN-DEC
# │ │ │ │ ┌ day of week: 0-6, SUN-SAT, L
# │ │ │ │ │
  * * * * *
```

Rules:

- Timezone is required.
- `UTC` is recommended for predictable enterprise operation.
- Daylight-saving transitions must be deterministic and tested.
- Cron parsing should start with a minimal supported subset and reject unsupported syntax with clear errors.
- If a new cron/timezone dependency is proposed, it requires explicit dependency approval.

### 7.4 HA leases

Task scheduling must be protected by FDB leases:

```text
task_scheduler_loop
  -> read due task
  -> attempt lease compare-and-set in FDB
  -> if lease acquired, create run record
  -> dispatch run
  -> update run state
  -> release/expire lease
```

Lease requirements:

- Lease keys include task id or graph root id.
- Lease value includes coordinator id, epoch/term, expiration timestamp, and run id.
- FDB transaction conflicts must be retried safely.
- Expired leases can be reclaimed by a new leader.
- A run id must be deterministic enough to detect duplicate creation attempts.

---

## 8. Stream Integration

### 8.1 Stream offset model

A stream is a bookmark over table version history:

```text
stream_offset_version -> current_table_version
```

Querying a stream alone does not advance its offset. The offset advances only when a DML transaction consumes the stream and commits.

### 8.2 `SYSTEM$STREAM_HAS_DATA`

`SYSTEM$STREAM_HAS_DATA('<stream_name>')` must be metadata-only:

```text
resolve stream
  -> read stream offset version/commit_ts
  -> read current source object version/commit_ts
  -> compare version metadata
  -> return bool
```

Correctness rules:

- Avoid false negatives: if CDC records exist, return true.
- False positives are allowed and documented.
- Do not scan Parquet micro-partitions to evaluate the function.
- View-stream support may produce more false positives because underlying tables can change without changing view output.
- If the function returns true, task SQL should consume the stream, even if the stream result is empty, so the offset can advance.

### 8.3 Triggered task flow

```text
DML commit on source table
  -> table version increments
  -> stream metadata observes new version
  -> trigger index finds tasks referencing the stream
  -> scheduler marks task due
  -> scheduler acquires lease
  -> evaluates WHEN
  -> dispatches task SQL
  -> task consumes stream in DML
  -> transaction commits
  -> stream offset advances
  -> run marked SUCCEEDED
```

If task SQL fails:

```text
task SQL fails
  -> transaction rolls back
  -> stream offset remains unchanged
  -> run marked FAILED
  -> retry/failure policy applies
```

### 8.4 Multiple consumers

One stream should be consumed by one logical consumer. If two tasks need the same CDC feed, create two streams on the same table:

```sql
CREATE STREAM orders_stream_task_a ON TABLE orders;
CREATE STREAM orders_stream_task_b ON TABLE orders;
```

Sharing one stream between tasks intentionally shares one offset and can cause one task to consume records before the other sees them.

---

## 9. FoundationDB Metadata Design

The key layout must use tuple-encoded, well-delimited FDB keys. Example categories:

```text
("task", task_id) -> TaskMeta
("task_by_name", db_id, schema_id, normalized_name) -> task_id
("task_version", task_id, version) -> TaskVersionMeta
("task_current_version", task_id) -> version
("task_dependency", child_task_id, parent_task_id) -> TaskDependencyMeta
("task_children", parent_task_id, child_task_id) -> child_task_id
("task_finalizer", root_task_id) -> finalizer_task_id
("task_run", task_id, scheduled_time, run_id) -> TaskRunMeta
("task_run_by_state", state, scheduled_time, run_id) -> task_id
("task_graph_run", root_task_id, graph_run_group_id) -> TaskGraphRunMeta
("task_lease", root_or_task_id) -> TaskLeaseMeta
("task_stream_trigger", stream_id, task_id) -> TaskStreamTriggerMeta
("task_due", due_time, task_id) -> TaskDueMeta
("task_audit_ref", task_id, event_time, event_id) -> AuditRef
```

### 9.1 `TaskMeta`

Required fields:

```text
id
account_id
db_id
schema_id
name
owner_role_id
created_at
updated_at
state: Suspended | Resumed
task_kind: Standalone | Root | Child | Finalizer
current_version
comment
```

### 9.2 `TaskVersionMeta`

Required fields:

```text
task_id
version
sql_text
when_expr_text
schedule: None | Interval | Cron
warehouse_ref
serverless_config
session_parameters
predecessor_task_ids
finalize_root_task_id
overlap_policy
user_task_timeout_ms
suspend_task_after_num_failures
task_auto_retry_attempts
minimum_trigger_interval_seconds
execute_as_user_id
created_by_user_id
created_by_role_id
created_at
security_epoch_at_definition
```

Task version records are immutable. Altering task definition creates a new version.

### 9.3 `TaskRunMeta`

Required fields:

```text
run_id
task_id
task_version
graph_run_group_id
scheduled_from: Schedule | Trigger | ExecuteTask | ManualRetry | AutomaticRetry
state: Scheduled | Queued | Executing | Succeeded | Failed | FailedAndAutoSuspended | Cancelled | Skipped
scheduled_time
query_start_time
completed_time
next_scheduled_time
query_id
condition_text
query_text_hash
return_value
attempt_number
error_code
error_message_redacted
owner_role_id
execute_as_user_id
warehouse_ref
security_epoch
```

### 9.4 Atomicity requirements

The following operations must be atomic:

- Create task + name index + owner metadata + initial version.
- Replace task + new version + graph/index updates.
- Drop task + graph edge removal + trigger index removal + lease cleanup.
- Resume/suspend state change + scheduler due index update.
- Run creation + lease acquisition + history state transition.
- Successful stream-consuming DML + stream offset advancement + run success record.
- Grant/revoke/ownership change + security epoch bump.

---

## 10. RBAC and Security Model

### 10.1 Securable object type

TASK becomes a first-class securable object.

Task object identity must include:

```text
ObjectType::Task
task_id
object_generation
owner_role_id
```

### 10.2 Privileges

| Privilege | Scope | Purpose |
|---|---|---|
| `CREATE TASK` | Schema | Allows creating tasks in schema |
| `OWNERSHIP` | Task | Full control, alter properties, transfer/drop |
| `OPERATE` | Task | Resume, suspend, execute, retry without ownership |
| `MONITOR` | Task | View task state/history where allowed |
| `USAGE ON TASK` | Task | Reference task where needed |
| `EXECUTE TASK` | Account | Required for owned tasks to run |
| `EXECUTE MANAGED TASK` | Account | Required for Nova-managed/serverless execution |
| `USAGE ON WAREHOUSE` | Warehouse | Required for user-managed task warehouse |
| `SELECT` | Stream/source table | Required when task reads stream/source |
| DML privileges | Target table | Required for INSERT/UPDATE/DELETE/MERGE target |
| `IMPERSONATE` | User | Required for `EXECUTE AS USER` |

### 10.3 Execution identity

Default execution:

```text
SYSTEM service user
  + task owner role
  + no accidental admin bypass
```

`EXECUTE AS USER` execution:

```text
specified user
  + task owner role as primary role
  + user's allowed secondary role policy
```

Validation rules:

- Task owner role must still exist.
- Owner role must have account-level `EXECUTE TASK`.
- Serverless/Nova-managed tasks require `EXECUTE MANAGED TASK`.
- User-managed tasks require warehouse `USAGE`.
- `EXECUTE AS USER` requires owner role to have `IMPERSONATE` on user.
- The specified user must be granted the owner role.
- SQL body permissions are checked at run time.

### 10.4 Deny-by-default cases

Deny task creation/execution when any of these are true:

- Current session lacks `CREATE TASK` on schema.
- Owner role is missing or dropped.
- Task metadata is ambiguous or missing.
- Warehouse is missing or `USAGE` revoked.
- Account-level `EXECUTE TASK` revoked.
- Serverless task lacks `EXECUTE MANAGED TASK`.
- Stream/table/source object access revoked.
- Target DML privileges revoked.
- Execute-as user is missing, disabled, or not impersonable.
- Security epoch is stale and authorization cache cannot be refreshed.

### 10.5 Audit requirements

Audit records must capture:

- Create/alter/drop/resume/suspend/execute/retry.
- Auto-suspend after failures.
- Authorization failures.
- Execute-as user usage.
- SQL body object access.
- Stream offset advancement.
- Owner role and security epoch.

Audit must never store raw passwords, tokens, secrets, or unredacted sensitive session parameters.

---

## 11. Task Graph Semantics

### 11.1 Graph structure

A task graph is a DAG:

```text
root task
  -> child A
  -> child B
child A + child B
  -> child C
root task
  -> finalizer task after graph completion/failure
```

Rules:

- Root task defines schedule or trigger.
- Child task uses `AFTER` and cannot define schedule.
- Finalizer uses `FINALIZE = <root_task>` and cannot define schedule or children.
- All graph tasks must be in the same schema.
- All graph tasks must have the same owner role.
- Graph cycles are rejected.
- Root task must be suspended before graph mutation.

### 11.2 Overlap policies

| Policy | Behavior |
|---|---|
| `NO_OVERLAP` | Default. One graph run at a time. Later schedule is skipped if graph is still running. |
| `ALLOW_CHILD_OVERLAP` | Root does not overlap, but child tasks from previous graph run may still be running when the next graph run starts. |
| `ALLOW_ALL_OVERLAP` | Multiple full graph runs can overlap. Use only when task SQL is idempotent and safe. |

### 11.3 Finalizer behavior

Finalizer runs after all other graph tasks are terminal:

- Run after success.
- Run after failure, unless root task itself was skipped before graph start.
- Finalizer failure is recorded separately.
- Finalizer cannot schedule descendants.

---

## 12. Run State Machine

Task run states:

```text
Scheduled
  -> Queued
  -> Executing
  -> Succeeded
```

Failure path:

```text
Executing
  -> Failed
  -> AutomaticRetry Scheduled
  -> FailedAndAutoSuspended
```

Skip path:

```text
Scheduled
  -> WHEN false
  -> Skipped
```

Cancel path:

```text
Queued | Executing
  -> Cancelled
```

Run state updates must be monotonic and persisted in FDB. A terminal state must not transition back to a non-terminal state.

---

## 13. Failure Handling and Retry

### 13.1 Timeout

`USER_TASK_TIMEOUT_MS` limits a task run. The planned default is `3,600,000` ms (1 hour), with a maximum of `604,800,000` ms (7 days) unless Nova configuration intentionally narrows the limit.

Rules:

- Standalone task timeout applies to that task run.
- Root graph timeout applies to total graph duration.
- Child timeout overrides root timeout for that child.
- Timeout produces a failed run state.

### 13.2 Failure counter

`SUSPEND_TASK_AFTER_NUM_FAILURES` tracks consecutive user failures/timeouts.

Do not increment the counter for:

- Skipped runs.
- Cancelled runs.
- Indeterminate system failures where retry is safe and user SQL did not fail.

### 13.3 Retry

Manual retry:

```sql
EXECUTE TASK root_task RETRY LAST;
```

Automatic retry:

```sql
TASK_AUTO_RETRY_ATTEMPTS = 2
```

Rules:

- Retry starts from the failed task where graph semantics allow it.
- Attempt number increments.
- Run group id remains tied to the graph retry group.
- Stream offsets advance only on committed retry success.

---

## 14. Observability and History

### 14.1 Information schema views/functions

Planned surfaces:

```sql
INFORMATION_SCHEMA.TASK_HISTORY(...)
INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS(...)
INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS(...)
INFORMATION_SCHEMA.TASK_DEPENDENTS(...)
```

Account usage surfaces:

```sql
ACCOUNT_USAGE.TASK_HISTORY
ACCOUNT_USAGE.TASK_VERSIONS
ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
ACCOUNT_USAGE.COMPLETE_TASK_GRAPHS
```

### 14.2 Task history columns

Core columns:

```text
QUERY_ID
NAME
DATABASE_NAME
SCHEMA_NAME
QUERY_TEXT
CONDITION_TEXT
STATE
ERROR_CODE
ERROR_MESSAGE
SCHEDULED_TIME
QUERY_START_TIME
NEXT_SCHEDULED_TIME
COMPLETED_TIME
ROOT_TASK_ID
GRAPH_VERSION
RUN_ID
RETURN_VALUE
SCHEDULED_FROM
ATTEMPT_NUMBER
CONFIG
QUERY_HASH
GRAPH_RUN_GROUP_ID
```

### 14.3 Metrics

Prometheus metrics should include:

```text
nova_task_runs_total{state, scheduled_from}
nova_task_run_duration_seconds{task, warehouse}
nova_task_queue_duration_seconds{task}
nova_task_when_evaluations_total{result}
nova_task_scheduler_lease_conflicts_total
nova_task_trigger_lag_seconds{stream, task}
nova_task_graph_runs_total{state}
nova_task_retries_total{reason}
nova_task_auto_suspensions_total
```

### 14.4 Tracing

Each run should emit spans for:

- Scheduler scan.
- Lease acquisition.
- `WHEN` evaluation.
- RBAC authorization.
- SQL dispatch.
- Worker execution.
- Stream offset advancement.
- Child scheduling.
- Finalizer scheduling.
- Run completion.

---

## 15. Cost and Resource Controls

Task execution must avoid unbounded cost and load.

Controls:

- Max resumed tasks per account/config.
- Max concurrent runs per account.
- Max concurrent runs per warehouse.
- Max queued runs per task.
- Minimum trigger interval for stream-triggered tasks.
- Scheduler scan budget per tick.
- Run history retention bounds.
- Auto-suspend after repeated failures.
- Timeout enforcement.

Cost attribution:

- User-managed task cost is attributed to warehouse execution duration and queued time.
- Nova-managed/serverless cost should record estimated compute size and duration once the compute abstraction exists.
- Skipped `WHEN` evaluations should be counted separately from executed task runs.

---

## 16. Cache and Consistency

Task-owned SQL can modify tables and security metadata. Therefore:

- Result cache must invalidate on table version changes produced by tasks.
- Authorization cache must include security epoch and invalidate on grant/revoke/ownership changes.
- Stream consumption must invalidate stream-related metadata cache.
- Dynamic table refresh triggered by tasks must use the dynamic table owner role, not admin bypass.
- Task version changes must not affect already-running task runs.

---

## 17. Backup and Restore

Backup must include:

- Task definitions.
- Task versions.
- Task graph edges.
- Finalizer links.
- Schedule state.
- Stream trigger indexes.
- Run history within retention policy.
- RBAC ownership/grants.
- Audit references.

Restore rules:

- Leases are not restored as active leases.
- Scheduler recomputes due tasks after restore.
- Restored task ownership must map to restored roles.
- Missing owner roles cause tasks to restore suspended and inoperable until repaired by an administrator.
- Stream-triggered tasks must validate restored stream ids and source table versions before resuming.

---

## 18. Implementation Roadmap

### Stage 1: Metadata foundation

Deliver:

- Task types in `nova-common`.
- FDB metadata operations.
- Versioned definitions.
- Name lookup.
- Graph edge metadata.
- Run history metadata.
- Unit tests for key layout and atomicity.

### Stage 2: SQL DDL and introspection

Deliver:

- Parser/analyzer/executor support for task DDL.
- `SHOW TASKS` and `DESC TASK`.
- `EXECUTE TASK` manual run.
- MySQL protocol integration.
- Negative tests for invalid syntax and graph restrictions.

### Stage 3: Scheduler MVP

Deliver:

- Interval scheduler.
- FDB leases.
- Task run records.
- No-overlap standalone behavior.
- Basic timeout and failure recording.

### Stage 4: Cron support

Deliver:

- Five-field cron parser.
- Explicit timezone validation.
- Next-run calculation.
- DST and invalid-expression tests.

### Stage 5: Stream-triggered tasks

Deliver:

- `SYSTEM$STREAM_HAS_DATA`.
- Stream trigger indexes.
- Triggered scheduler path.
- Transactional offset advancement tests.

### Stage 6: Task graphs

Deliver:

- `AFTER` dependencies.
- Graph run groups.
- Parallel children.
- Multi-parent joins.
- Finalizers.
- Graph retries.

### Stage 7: Enterprise RBAC and audit

Deliver:

- Task securable type.
- Privilege enforcement.
- Execute-as-user semantics.
- Audit events.
- Negative authorization tests.

### Stage 8: Observability and hardening

Deliver:

- Task history views.
- Metrics.
- Tracing.
- Cost attribution.
- Chaos/failover tests.
- Backup/restore integration.

---

## 19. Testing Strategy

### 19.1 Unit tests

Cover:

- Cron parsing and next-run calculation.
- Interval base time after resume.
- Task state transitions.
- Graph cycle detection.
- FDB key ordering and prefix scans.
- Lease acquisition conflicts.
- `SYSTEM$STREAM_HAS_DATA` metadata diff.
- RBAC privilege matrix.

### 19.2 Integration tests

Cover:

- Create/resume/suspend/drop task.
- Manual `EXECUTE TASK`.
- Scheduled task execution.
- Triggered task execution after stream changes.
- Failed task does not advance stream offset.
- Task graph ordering.
- Finalizer execution.
- Auto-suspend after failures.
- MySQL protocol end-to-end behavior.

### 19.3 Security tests

Cover denial for:

- Missing `CREATE TASK`.
- Missing `OPERATE`.
- Missing account `EXECUTE TASK`.
- Revoked warehouse `USAGE`.
- Revoked stream `SELECT`.
- Revoked target DML privilege.
- Invalid `EXECUTE AS USER` impersonation.
- Dropped owner role.
- Stale authorization cache.

### 19.4 HA and chaos tests

Cover:

- Coordinator failover during scheduler scan.
- Coordinator failover after lease acquisition but before dispatch.
- Worker crash during task SQL.
- FDB transaction conflict during run creation.
- Duplicate stream trigger event.
- Clock skew around schedule calculation.
- Large graph with parallel branches.
- High-frequency stream changes batched by minimum trigger interval.

### 19.5 Verification ladder

Use narrow tests first, then broaden:

```bash
cargo test -p nova-common task
cargo test -p nova-storage task
cargo test -p nova-coordinator task
cargo test -p nova-coordinator stream
cargo test --all -- task
cargo test --all -- rbac
cargo clippy --all -- -D warnings
cargo fmt --all -- --check
```

If FoundationDB, MinIO, or Docker services are unavailable, report that clearly and do not weaken tests to pass locally.

---

## 20. Enterprise Operational Guidance

### 20.1 Recommended task authoring practices

- Prefer idempotent SQL bodies.
- Use one stream per logical consumer.
- Use scheduled `WHEN` tasks for batch windows.
- Use triggered tasks for low-latency ELT.
- Keep task SQL small; move complex logic into tested views or procedures when those are supported.
- Use UTC cron schedules for predictable operations.
- Set timeouts and failure auto-suspend values explicitly for production tasks.
- Monitor task history and failed/skipped runs.

### 20.2 Admin practices

- Create dedicated owner roles for production task graphs.
- Grant least privilege to task owner roles.
- Avoid using `ACCOUNTADMIN` as task owner for routine pipelines.
- Use service users for `EXECUTE AS USER` only when user-based governance policies require it.
- Review task audit logs after ownership transfers and privilege changes.
- Keep task graph mutations behind suspend/resume change control.

---

## 21. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Duplicate task run during coordinator failover | FDB leases, deterministic run ids, graph run uniqueness checks |
| Stream offset advances despite failed task | Advance offset only inside committed transaction that consumes stream |
| Background job bypasses RBAC | Require `SecurityContext` for task execution and deny missing context |
| Cron behavior surprises around DST | Require timezone, recommend UTC, add DST tests |
| False positive stream trigger causes wasted runs | Document behavior, allow empty DML consumption to advance offset |
| Task graph explosion overloads cluster | Enforce graph size, concurrency, queue, and scheduler scan limits |
| Long-running tasks block schedules | Timeout, no-overlap skipping, history visibility, serverless/warehouse sizing guidance |
| Secrets leak through task metadata | Redact SQL/config/session parameters in logs, audit, and unauthorized views |
| Stale authorization cache allows access | Security epoch in cache keys and run-time privilege re-checks |
| Restored tasks run under missing roles | Restore suspended/inoperable until administrator repairs ownership |

---

## 22. Done Criteria

Phase 17 is complete when:

- Task DDL and lifecycle operations work through the MySQL protocol.
- Interval and cron schedules produce correct due times.
- Scheduler leases prevent duplicate runs across HA coordinators.
- Stream-triggered tasks work through `SYSTEM$STREAM_HAS_DATA` without scanning data.
- Stream offsets advance only on committed task DML.
- Task graphs support dependencies, parallel branches, finalizers, retries, and failure suspension.
- RBAC denies unauthorized operations by default.
- Task history, graph history, audit, metrics, tracing, and cost attribution are available.
- Backup/restore preserves task definitions and security continuity.
- Tests cover unit, integration, security, HA, and chaos scenarios.
- Documentation states Nova-specific compatibility boundaries clearly.
