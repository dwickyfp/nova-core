# Enterprise Kafka-like Streams Design

Date: 2026-07-05
Status: Approved design, pending implementation plan
Owner: nova-core

## 1. Context

nova-core currently has partial stream support: `CREATE STREAM ... ON TABLE ...` creates a metadata object and RBAC owner record, but there is no consumable stream table, no offset commit on read, no full INSERT/UPDATE/DELETE CDC output, and no `SYSTEM$STREAM_HAS_DATA` equivalent.

This design defines an enterprise-grade stream feature for nova-core with Snowflake-style stream rows and Kafka-like consumption semantics:

- A stream is a schema object that tracks changes on a source table.
- `SELECT * FROM stream_name` consumes backlog and commits the stream offset after successful result delivery.
- `SELECT * FROM stream_name WITH (COMMIT = FALSE)` previews/debugs without committing the stream offset.
- `SYSTEM$STREAM_HAS_DATA('<stream_name>')` checks whether a stream has unconsumed changes.
- Standard streams only; no append-only mode.
- Full DML CDC: INSERT, DELETE, UPDATE.

This intentionally differs from Snowflake in one important way. In Snowflake, plain `SELECT` from a stream does not advance the offset; DML statements that use the stream advance the offset. In nova-core, by product decision, plain `SELECT` is Kafka-like and advances the offset by default. The no-commit preview mode is provided explicitly for debugging.

## 2. Goals

1. Provide a robust stream-table feature suitable for production applications.
2. Preserve nova-core architecture invariants:
   - immutable micro-partitions;
   - FoundationDB metadata and transactional state;
   - object storage for bulk payloads;
   - workers remain stateless;
   - MVCC correctness.
3. Support full DML CDC:
   - INSERT emits inserted rows;
   - DELETE emits deleted rows;
   - UPDATE emits Snowflake-style DELETE old row + INSERT new row.
4. Make stream consumption transactional and safe under concurrency.
5. Enforce RBAC consistently for create, read/consume, has-data, show, describe, and drop operations.
6. Provide comprehensive tests, including negative RBAC tests and concurrency/failure cases.
7. Avoid storing large row payloads directly in FoundationDB.

## 3. Non-goals

1. Append-only streams.
2. Streams on views, dynamic tables, external tables, stages, or event tables.
3. Arbitrary historical stream creation with `AT` or `BEFORE` clauses in the first implementation wave.
4. Snowflake-compatible DML-only offset advancement. nova-core intentionally uses SELECT-as-consume by default.
5. Per-row offset commit for filtered or limited SQL queries.
6. Cross-consumer fan-out inside one stream object. Separate consumers should use separate stream objects.

## 4. User-facing SQL

### 4.1 Create stream

```sql
CREATE STREAM order_stream ON TABLE orders;
```

Behavior:

- Creates stream object `order_stream` in the current schema.
- Source table must exist.
- Stream name is unique per schema.
- Initial offset is the current committed change sequence of the source table.
- Existing rows in the source table do not appear in the first stream read.
- No `APPEND ONLY` option is accepted.

Future-compatible optional clauses can be added later, but the enterprise implementation should reject unsupported clauses with explicit errors.

### 4.2 Consume stream

```sql
SELECT * FROM order_stream;
```

Behavior:

- Resolves `order_stream` as a stream object if no table with the same name is selected. Name resolution should reject ambiguity rather than silently choosing the wrong object.
- Reads all unconsumed CDC records from stream offset to a fixed read-end sequence captured at query start.
- Returns source table columns plus stream metadata columns.
- After the result is successfully produced and delivered, commits stream offset to the read-end sequence using an atomic compare-and-set.
- If another consumer advanced the same stream offset concurrently, return a stream concurrent-consume error rather than overwriting the offset.

### 4.3 Preview without committing

```sql
SELECT * FROM order_stream WITH (COMMIT = FALSE);
```

Behavior:

- Returns the same rows that a consuming SELECT would see.
- Does not update the stream offset.
- Can be repeated for debugging.
- Does not acquire an exclusive consume lock.

### 4.4 Has-data function

```sql
SELECT SYSTEM$STREAM_HAS_DATA('order_stream');
SELECT SYSTEM$STREAM_HAS_DATA('db.public.order_stream');
```

Behavior:

- Returns `TRUE` if there is at least one committed table change sequence greater than the stream offset.
- Returns `FALSE` if no potential backlog exists.
- Must avoid false negatives for committed DML.
- May allow false positives in future optimization cases, but the initial change-log design can provide exact answers.
- Requires stream read authorization.

### 4.5 Lifecycle commands

The enterprise feature includes:

```sql
SHOW STREAMS;
DESCRIBE STREAM order_stream;
DROP STREAM order_stream;
```

Behavior:

- `SHOW STREAMS` lists stream objects visible to the active role.
- `DESCRIBE STREAM` shows source table, offset, stale status, owner, and timestamps.
- `DROP STREAM` deletes only stream metadata and offsets; it does not delete source table data or table-level CDC logs still needed by other streams.

## 5. Stream output schema

A stream query returns source table columns in source table order plus metadata columns:

```text
<source columns...>
METADATA$ACTION      Utf8      -- INSERT | DELETE
METADATA$ISUPDATE    Boolean
METADATA$ROW_ID      Utf8      -- stable per logical row
METADATA$TXN_ID      UInt64
METADATA$COMMIT_TS   Int64     -- nova timestamp micros
METADATA$SEQUENCE    UInt64    -- monotonically increasing per table
```

DML mapping:

| Source DML | Stream rows |
|---|---|
| INSERT | new row, `METADATA$ACTION='INSERT'`, `METADATA$ISUPDATE=false` |
| DELETE | old row, `METADATA$ACTION='DELETE'`, `METADATA$ISUPDATE=false` |
| UPDATE | old row, `METADATA$ACTION='DELETE'`, `METADATA$ISUPDATE=true`; new row, `METADATA$ACTION='INSERT'`, `METADATA$ISUPDATE=true` |

The metadata column names intentionally match Snowflake names where practical.

## 6. Stable row identity

Full UPDATE/DELETE streams require a stable logical row identifier. nova-core should add hidden row identity metadata for rows written after stream/change-tracking support is enabled.

Recommended row id format:

```text
<table_id>:<origin_mp_id>:<origin_row_ordinal>:<row_generation>
```

Rules:

- INSERT creates a new stable row id.
- UPDATE preserves logical row id across old and new versions, or emits a related row id that can be tracked as the same logical row. For Snowflake-style streams, old and new update records should share the same `METADATA$ROW_ID` when possible.
- DELETE emits the row id from the deleted logical row.
- Row id is hidden table metadata, not a user-visible source column.

If current table files lack hidden row ids, the implementation must either:

1. backfill row ids when the table is first updated after streams are enabled, or
2. reject full UPDATE/DELETE stream tracking on legacy MPs until they are rewritten with row ids.

For enterprise robustness, the implementation should include a compatibility path and tests for existing tables.

## 7. Storage architecture

### 7.1 Principle

FoundationDB stores ordered metadata and offsets. Object storage stores bulk CDC row payloads as immutable files. Do not store large row payloads in FDB.

### 7.2 Metadata keys

Proposed FDB key layout:

```text
/stream/{stream_id}/meta                         -> StreamMeta
/stream/{stream_id}/offset                       -> StreamOffset
/stream_by_name/{db_id}/{schema_id}/{name}       -> stream_id
/streams_by_table/{table_id}/{stream_id}         -> empty

/table_change_seq/{table_id}                     -> u64
/table_change_log/{table_id}/{sequence}          -> ChangeRecordMeta
/table_change_log_by_txn/{txn_id}/{table_id}/{sequence} -> empty
/table_change_retention/{table_id}               -> ChangeRetentionMeta
```

### 7.3 StreamMeta

`StreamMeta` should evolve from the current minimal struct into:

```rust
pub struct StreamMeta {
    pub stream_id: StreamId,
    pub db_id: DatabaseId,
    pub schema_id: SchemaId,
    pub source_table_id: TableId,
    pub name: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub owner_role_id: RoleId,
    pub comment: Option<String>,
    pub stale_after: Option<Timestamp>,
    pub dropped: bool,
}
```

`append_only` should be removed from new semantics. If backward compatibility is needed for existing serialized data, use a versioned metadata representation or tolerate old records during deserialization/migration.

### 7.4 StreamOffset

```rust
pub struct StreamOffset {
    pub table_id: TableId,
    pub committed_sequence: u64,
    pub committed_ts: Timestamp,
    pub last_consumed_at: Option<Timestamp>,
    pub last_consumed_txn_id: Option<TxnId>,
}
```

### 7.5 ChangeRecordMeta

```rust
pub struct ChangeRecordMeta {
    pub table_id: TableId,
    pub sequence: u64,
    pub txn_id: TxnId,
    pub commit_ts: Timestamp,
    pub payload_path: String,
    pub payload_row_start: u64,
    pub payload_row_count: u64,
    pub action_counts: ChangeActionCounts,
    pub min_row_id: Option<String>,
    pub max_row_id: Option<String>,
}
```

The metadata points to immutable CDC payload files in object storage.

### 7.6 CDC payload files

Recommended path format:

```text
cdc/tables/{table_id}/txn-{txn_id}/seq-{start_sequence}-{end_sequence}.parquet
```

Payload schema:

- source table columns;
- hidden row id;
- metadata columns listed in section 5.

The payload should be Arrow/Parquet to preserve vectorized reads.

## 8. DML capture flow

### 8.1 INSERT

1. Build RecordBatch for inserted rows.
2. Assign row ids.
3. Write table MP Parquet.
4. Build CDC payload rows with action `INSERT`, `is_update=false`.
5. Write CDC Parquet.
6. In one FDB transaction:
   - insert MP metadata;
   - allocate contiguous table change sequences;
   - insert change log metadata;
   - update table version;
   - commit transaction metadata.

### 8.2 DELETE

1. Identify affected active MPs.
2. Read affected rows.
3. Write replacement MPs for survivors, preserving row ids.
4. Mark old MPs superseded.
5. Write CDC payload rows for deleted rows with action `DELETE`, `is_update=false`.
6. Commit MP metadata and CDC metadata atomically.

### 8.3 UPDATE

1. Identify affected active MPs.
2. Read old rows and compute new rows.
3. Preserve row ids for updated logical rows.
4. Write replacement MPs.
5. Mark old MPs superseded.
6. Write two CDC rows per changed logical row:
   - old row: `DELETE`, `is_update=true`;
   - new row: `INSERT`, `is_update=true`.
7. Commit MP metadata and CDC metadata atomically.

### 8.4 Failure handling

- If object storage write succeeds but FDB commit fails, the file is orphaned and must be cleaned by GC.
- If FDB commit succeeds, the CDC log is visible and payload file must exist.
- Missing CDC payload at read time is a query error and must not advance stream offset.

## 9. Stream read and offset commit flow

### 9.1 Query start

1. Resolve stream by name.
2. Authorize read/consume.
3. Read current stream offset.
4. Capture `read_start_sequence = offset.committed_sequence`.
5. Capture `read_end_sequence = current table_change_seq(table_id)`.
6. Read change log metadata where:

```text
read_start_sequence < sequence <= read_end_sequence
```

### 9.2 Result production

1. Read CDC payload files in sequence order.
2. Apply SQL projection/filter/limit to the output rows.
3. Keep `read_end_sequence` independent of filter/limit.
4. Return rows to client.

### 9.3 Commit mode

For default `SELECT * FROM stream`:

1. After successful result production, run an FDB compare-and-set:

```text
if offset.committed_sequence == read_start_sequence:
    offset.committed_sequence = read_end_sequence
    offset.committed_ts = now
    offset.last_consumed_at = now
else:
    error StreamConcurrentConsume
```

2. If the offset commit fails, return an explicit error to the client.
3. Enterprise-safe behavior is to materialize the stream result first, commit the offset, then send the result to the client. This avoids the case where the client receives rows but offset commit fails. For very large streams, add chunking/checkpointing in a later design.

### 9.4 Preview mode

For `WITH (COMMIT = FALSE)`:

- no compare-and-set;
- no offset update;
- no exclusive consume conflict;
- repeated preview queries return the same backlog until a consuming query commits it.

## 10. SQL filtering and commit scope

If a consuming stream query uses `WHERE`, `LIMIT`, aggregation, or projection, the stream offset still commits the entire backlog up to `read_end_sequence`.

Example:

```sql
SELECT * FROM order_stream WHERE customer_id = 10;
```

If the query succeeds, changes for other customer IDs in the same backlog are also consumed. This is intentional and must be documented. Users who want to inspect subsets without consuming should use:

```sql
SELECT * FROM order_stream WITH (COMMIT = FALSE) WHERE customer_id = 10;
```

If parser limitations make option placement easier, implementation may initially require the stream option immediately after the stream name:

```sql
SELECT * FROM order_stream WITH (COMMIT = FALSE) WHERE customer_id = 10;
```

## 11. RBAC

### 11.1 CREATE STREAM

Required privileges:

- `USAGE` on database;
- `USAGE` on schema;
- `CREATE STREAM` on schema;
- `SELECT` on source table.

### 11.2 SELECT/consume stream

Required privileges:

- `USAGE` on database;
- `USAGE` on schema;
- `SELECT` on stream object;
- `SELECT` on source table.

Source-table `SELECT` is required because the stream exposes source table data.

### 11.3 Preview stream

Same as consuming select:

- `SELECT` on stream object;
- `SELECT` on source table;
- parent `USAGE` privileges.

### 11.4 SYSTEM$STREAM_HAS_DATA

Required privileges:

- `SELECT` on stream object;
- `SELECT` on source table;
- parent `USAGE` privileges.

This avoids leaking table activity to roles that cannot read stream/source data.

### 11.5 DROP STREAM

Required privilege:

- `OWNERSHIP` on stream object, or account-admin/root bypass.

### 11.6 SHOW/DESCRIBE STREAM

- Show only streams visible through privileges.
- Describe requires at least `SELECT` or `OWNERSHIP` on the stream and parent `USAGE`.

## 12. Concurrency and isolation

1. DML can continue while stream is read.
2. Stream read uses a stable `read_end_sequence` captured at query start.
3. Preview reads can run concurrently.
4. Consuming reads can run concurrently optimistically, but only one wins the offset CAS.
5. Failed consumers must not advance offset.
6. Explicit transactions should preserve repeatable stream snapshots if/when nova-core supports multi-statement stream transactions.

## 13. Retention and staleness

A stream is stale if its committed offset points before the earliest retained change sequence for the source table.

Behavior:

- `SELECT * FROM stale_stream` returns a clear stale stream error.
- `SYSTEM$STREAM_HAS_DATA` on stale stream returns an error or a stale status, not a false empty result.
- Dropping a source table should mark dependent streams stale or delete them consistently. The current repo already clears stream metadata on table drop; enterprise behavior should be explicit and tested.
- CDC payload GC must keep records needed by the oldest non-stale stream on a table.

## 14. Observability

Add structured tracing/metrics for:

- stream created;
- stream consumed;
- preview read;
- offset commit success/failure;
- concurrent consume conflict;
- has-data checks;
- stale stream errors;
- CDC payload missing;
- CDC records written per DML.

Metrics should include table_id, stream_id, sequence range, row counts, and duration. Do not log row data.

## 15. Error model

Add explicit error variants where needed:

- `StreamNotFound { stream_name }`
- `StreamAlreadyExists { stream_name }`
- `StreamConcurrentConsume { stream_id }`
- `StreamStale { stream_id, earliest_sequence, offset_sequence }`
- `StreamPayloadMissing { stream_id, payload_path }`
- `UnsupportedStreamSyntax { message }`
- `AmbiguousRelationName { name }`

Avoid `Internal` for expected user or concurrency errors.

## 16. Implementation phases

Although the target is enterprise-complete, implementation should be split into safe checkpoints.

### Phase 1: Metadata and RBAC foundation

- Evolve stream metadata and offset types.
- Add stream lookup by schema/name.
- Add list/drop/describe metadata operations.
- Add offset compare-and-set.
- Enforce RBAC for create/show/describe/drop/read/has-data.
- Tests: metadata, duplicate names, RBAC negatives, offset CAS.

### Phase 2: CDC log write path

- Add row id support.
- Add CDC payload writer.
- Extend INSERT/UPDATE/DELETE to emit CDC payload + change log metadata.
- Ensure MP metadata and CDC metadata commit atomically.
- Tests: DML emits correct CDC records, update/delete correctness, orphan handling.

### Phase 3: Stream table read path

- Resolve stream in SELECT.
- Implement CDC payload reader.
- Add metadata columns.
- Implement default consume offset commit.
- Implement `WITH (COMMIT = FALSE)` preview.
- Tests: consume once, preview repeatability, filtered consume commits full backlog, concurrent consume conflict.

### Phase 4: Has-data and lifecycle SQL

- Implement `SYSTEM$STREAM_HAS_DATA`.
- Implement `SHOW STREAMS`, `DESCRIBE STREAM`, `DROP STREAM`.
- Tests: has-data true/false, lifecycle commands, RBAC visibility.

### Phase 5: Hardening

- Staleness and retention.
- GC for CDC payloads and orphan files.
- Client failure behavior.
- Performance tests for large streams.
- End-to-end tests through MySQL protocol.

## 17. Test plan

### 17.1 Core behavior

1. Create stream after existing rows; first read is empty.
2. Insert 3 rows after stream creation; first read returns 3 rows; second read returns empty.
3. Preview read returns rows repeatedly and does not advance offset.
4. Has-data returns true before consume and false after consume.
5. DELETE emits deleted row with `METADATA$ACTION='DELETE'` and `METADATA$ISUPDATE=false`.
6. UPDATE emits old DELETE and new INSERT with `METADATA$ISUPDATE=true`.
7. Filtered consuming SELECT commits full backlog.
8. Projection preserves metadata columns when requested and supports source-column projection.

### 17.2 RBAC

1. Role without source table SELECT cannot create stream.
2. Role without schema CREATE STREAM cannot create stream.
3. Role without stream SELECT cannot read stream.
4. Role with stream SELECT but without source table SELECT cannot read stream.
5. Role with both stream SELECT and source table SELECT can read stream.
6. Unauthorized role cannot call `SYSTEM$STREAM_HAS_DATA`.
7. Unauthorized role cannot drop stream.

### 17.3 Transaction/failure/concurrency

1. Two consuming readers from same offset: one succeeds, one gets conflict.
2. Preview and consuming read concurrently: preview does not interfere.
3. Query failure before success does not advance offset.
4. Missing CDC payload fails query and does not advance offset.
5. Stale stream returns explicit stale error.
6. FDB transaction conflict on offset commit is surfaced.

### 17.4 Storage/retention

1. CDC payload files are written and referenced by metadata.
2. Orphan payload cleanup does not delete committed payloads.
3. GC keeps CDC payload needed by oldest active stream.
4. Dropping stream allows later GC of no-longer-needed CDC payloads.

## 18. Migration notes

Current code has existing `StreamMeta { stream_id, table_id, name, append_only, created_at }`. The enterprise implementation should include one of:

1. a versioned metadata enum, or
2. a tolerant migration path that reads old stream records and rewrites them to v2 on access, or
3. a development-only migration that clears old test stream metadata.

Because this project is still pre-production, a simple v1-to-v2 migration may be acceptable, but tests must cover reopen/read of existing metadata if backwards compatibility is kept.

## 19. Source references inspected

- Current stream parser: `crates/nova-coordinator/src/parser.rs:111`
- Current stream resolved statement: `crates/nova-coordinator/src/analyzer.rs:54`
- Current stream executor path: `crates/nova-coordinator/src/executor.rs:381`
- Current create stream executor: `crates/nova-coordinator/src/executor.rs:1323`
- Current metadata trait stream methods: `crates/nova-storage/src/metadata/mod.rs:131`
- Current FDB stream metadata methods: `crates/nova-storage/src/metadata/fdb_store.rs:989`
- Current stream/common types: `crates/nova-common/src/types.rs:397`
- Current create stream tests: `crates/nova-coordinator/src/executor.rs:2677`
- Current stream RBAC e2e tests: `crates/nova-coordinator/tests/e2e_tests.rs:342`

## 20. External references

- Snowflake Streams introduction: https://docs.snowflake.com/en/user-guide/streams-intro
- Snowflake CREATE STREAM: https://docs.snowflake.com/en/sql-reference/sql/create-stream
- Snowflake SYSTEM$STREAM_HAS_DATA: https://docs.snowflake.com/en/sql-reference/functions/system_stream_has_data

## 21. Open implementation risks

1. Current UPDATE/DELETE copy-on-write metadata path may need correctness fixes before CDC can be reliable.
2. Stable row identity is required for robust UPDATE/DELETE semantics.
3. Sending large stream results only after offset commit may require result materialization; large streams need memory-safe chunking/checkpoint design.
4. Parser support for `WITH (COMMIT = FALSE)` and `SYSTEM$STREAM_HAS_DATA` may require custom pre-parse logic because sqlparser support may be limited.
5. FDB transactions must remain small; change payload must stay in object storage.

## 22. Approval state

The user approved the enterprise-grade direction and requested a fully complete, robust feature rather than an MVP. This spec is ready for implementation planning after review.
