# Enterprise Kafka-like Streams Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build enterprise-grade, Snowflake-style CDC stream tables with Kafka-like SELECT consumption semantics for nova-core.

**Architecture:** Persist ordered table CDC metadata in FoundationDB and store bulk CDC row payloads as immutable Parquet objects. Stream reads resolve schema-level stream objects, read CDC payloads after the stream offset, and atomically advance offsets with compare-and-set unless `WITH (COMMIT = FALSE)` is used. DML writes table MPs and CDC metadata as one logical commit path, preserving immutable storage and MVCC semantics.

**Tech Stack:** Rust, Tokio, FoundationDB tuple keys, Apache Arrow, Parquet, object_store, sqlparser-rs, DataFusion-adjacent coordinator execution, nova RBAC metadata.

## Global Constraints

- Do not add new dependencies without explicit user approval.
- Keep micro-partitions immutable; UPDATE/DELETE must use copy-on-write and never modify existing Parquet objects.
- Store durable stream offsets and CDC metadata in FoundationDB; do not store large row payloads in FoundationDB.
- Store CDC payload batches in object storage as immutable Parquet files.
- Workers remain stateless; durable state belongs in FDB/object storage.
- Enforce RBAC on every SQL path; deny by default.
- Avoid `unwrap()` and `expect()` in production code.
- Use explicit `NovaError` variants for user-visible stream, concurrency, and staleness errors.
- Keep existing crate boundaries: `nova-common` has shared types/errors; `nova-storage` has metadata + object payload I/O; `nova-coordinator` has parser/analyzer/executor/RBAC.
- Do not commit changes unless the user explicitly asks for commits.

---

## File Structure

### Shared types and errors

- Modify `crates/nova-common/src/types.rs`
  - Replace current minimal stream structs with enterprise stream metadata, offset, CDC log metadata, stream read mode, stream metadata output types, and stable row-id helpers.
  - Keep compatibility with current aliases and RBAC object types.
- Modify `crates/nova-common/src/error.rs`
  - Add explicit stream errors.

### Storage payload I/O

- Create `crates/nova-storage/src/cdc.rs`
  - Owns `CdcPayloadWriter`, `CdcPayloadReader`, and helpers for Parquet CDC payload paths.
- Modify `crates/nova-storage/src/lib.rs`
  - Export the CDC module and payload reader/writer.
- Modify `crates/nova-storage/Cargo.toml`
  - No dependency additions expected.

### Metadata layer

- Modify `crates/nova-storage/src/metadata/mod.rs`
  - Extend `MetadataStore` with stream lookup/list/drop, offset CAS, table change sequence, and change-log methods.
- Modify `crates/nova-storage/src/metadata/fdb_store.rs`
  - Implement the new metadata trait methods with tuple keys and FDB transactions.
  - Update existing stream create/drop-table cleanup tests.

### Coordinator parser/analyzer/executor

- Modify `crates/nova-coordinator/src/parser.rs`
  - Parse supported stream custom SQL: `CREATE STREAM`, `DROP STREAM`, `SHOW STREAMS`, `DESCRIBE STREAM`, stream SELECT with `WITH (COMMIT = FALSE)`, and `SYSTEM$STREAM_HAS_DATA`.
- Modify `crates/nova-coordinator/src/analyzer.rs`
  - Add resolved statement variants and stream read mode.
- Modify `crates/nova-coordinator/src/executor.rs`
  - Enforce stream RBAC.
  - Implement create/drop/show/describe stream.
  - Emit CDC records from INSERT/UPDATE/DELETE.
  - Implement stream SELECT consume/preview.
  - Implement `SYSTEM$STREAM_HAS_DATA`.
- Optional if `executor.rs` becomes unmanageable: create `crates/nova-coordinator/src/stream_exec.rs` and re-export it from `crates/nova-coordinator/src/lib.rs`. Keep this split focused on stream helpers only.

### Tests

- Modify `crates/nova-storage/src/metadata/fdb_store.rs`
  - Add metadata unit/integration tests that skip when `NOVA_FDB_CLUSTER_FILE` is unavailable, following existing FDB test style.
- Modify `crates/nova-storage/src/cdc.rs`
  - Add local object-store payload writer/reader tests.
- Modify `crates/nova-coordinator/tests/e2e_tests.rs`
  - Add stream end-to-end tests for consume, preview, full DML, RBAC, has-data, and lifecycle SQL.
- Modify `crates/nova-coordinator/src/executor.rs` test module if narrow executor tests are easier for `ResolvedStatement` paths.

---

## Task 1: Shared Stream Types and Errors

**Files:**
- Modify: `crates/nova-common/src/types.rs:393-428`
- Modify: `crates/nova-common/src/error.rs:7-71`

**Interfaces:**
- Produces:
  - `StreamMeta`
  - `StreamOffset`
  - `StreamReadMode`
  - `ChangeRecordMeta`
  - `ChangePayloadRef`
  - `ChangeActionCounts`
  - `CdcRowRef`
  - `stream_row_id(table_id: TableId, mp_id: MpId, row_ordinal: u64, generation: u64) -> String`
  - `NovaError` variants for stream behavior.
- Consumes: existing ID aliases, `ObjectType::Stream`, `SecurityPrivilege::CreateStream`.

- [ ] **Step 1: Add failing type tests**

Add a `#[cfg(test)]` module near the bottom of `crates/nova-common/src/types.rs`:

```rust
#[cfg(test)]
mod stream_tests {
    use super::*;

    #[test]
    fn stream_row_id_is_stable_and_parseable_text() {
        let row_id = stream_row_id(10, 20, 3, 1);
        assert_eq!(row_id, "10:20:3:1");
    }

    #[test]
    fn stream_read_mode_defaults_to_commit() {
        assert_eq!(StreamReadMode::default(), StreamReadMode::Commit);
    }
}
```

- [ ] **Step 2: Run the narrow test and verify it fails**

Run:

```bash
cargo test -p nova-common stream_tests -- --nocapture
```

Expected: compile failure mentioning missing `stream_row_id` and `StreamReadMode`.

- [ ] **Step 3: Replace stream type section with enterprise structs**

In `crates/nova-common/src/types.rs`, replace the current `// ── Stream ──` section with:

```rust
// ── Stream / CDC ──

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum StreamReadMode {
    #[default]
    Commit,
    Preview,
}

/// Stream metadata stored in FoundationDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Stream offset cursor for Kafka-like consumption.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOffset {
    pub table_id: TableId,
    pub committed_sequence: u64,
    pub committed_ts: Timestamp,
    pub last_consumed_at: Option<Timestamp>,
    pub last_consumed_txn_id: Option<TxnId>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ChangeActionCounts {
    pub inserts: u64,
    pub deletes: u64,
    pub update_pairs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePayloadRef {
    pub path: String,
    pub row_start: u64,
    pub row_count: u64,
}

/// Ordered metadata for a CDC payload range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecordMeta {
    pub table_id: TableId,
    pub sequence: u64,
    pub txn_id: TxnId,
    pub commit_ts: Timestamp,
    pub payload: ChangePayloadRef,
    pub action_counts: ChangeActionCounts,
    pub min_row_id: Option<String>,
    pub max_row_id: Option<String>,
}

/// Row-level CDC action type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChangeAction {
    Insert,
    Delete,
}

impl std::fmt::Display for ChangeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeAction::Insert => write!(f, "INSERT"),
            ChangeAction::Delete => write!(f, "DELETE"),
        }
    }
}

/// Lightweight row pointer used by coordinator CDC construction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcRowRef {
    pub action: ChangeAction,
    pub is_update: bool,
    pub row_id: String,
    pub sequence: u64,
    pub txn_id: TxnId,
    pub commit_ts: Timestamp,
}

pub fn stream_row_id(table_id: TableId, mp_id: MpId, row_ordinal: u64, generation: u64) -> String {
    format!("{table_id}:{mp_id}:{row_ordinal}:{generation}")
}
```

- [ ] **Step 4: Add stream error variants**

In `crates/nova-common/src/error.rs`, add these variants before `Internal`:

```rust
    // ── Streams ──
    #[error("stream not found: {stream_name}")]
    StreamNotFound { stream_name: String },

    #[error("stream already exists: {stream_name}")]
    StreamAlreadyExists { stream_name: String },

    #[error("stream concurrent consume conflict: stream_id={stream_id}")]
    StreamConcurrentConsume { stream_id: u64 },

    #[error("stream is stale: stream_id={stream_id}, earliest_sequence={earliest_sequence}, offset_sequence={offset_sequence}")]
    StreamStale {
        stream_id: u64,
        earliest_sequence: u64,
        offset_sequence: u64,
    },

    #[error("stream payload missing: stream_id={stream_id}, payload_path={payload_path}")]
    StreamPayloadMissing { stream_id: u64, payload_path: String },

    #[error("unsupported stream syntax: {message}")]
    UnsupportedStreamSyntax { message: String },

    #[error("ambiguous relation name: {name}")]
    AmbiguousRelationName { name: String },
```

- [ ] **Step 5: Update call sites that instantiate old `StreamMeta`**

Update existing helpers/tests in:

- `crates/nova-storage/src/metadata/fdb_store.rs`
- `crates/nova-coordinator/src/executor.rs`

Use this field mapping when old data only has table id:

```rust
StreamMeta {
    stream_id,
    db_id,
    schema_id,
    source_table_id: table_id,
    name: name.to_string(),
    created_at: now_micros(),
    updated_at: now_micros(),
    owner_role_id: ACCOUNTADMIN_ROLE_ID,
    comment: None,
    stale_after: None,
    dropped: false,
}
```

- [ ] **Step 6: Run common tests**

Run:

```bash
cargo test -p nova-common stream_tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Run broad compile check for affected crates**

Run:

```bash
cargo test -p nova-storage stream --no-run
cargo test -p nova-coordinator stream --no-run
```

Expected: both compile. If compile errors reference old fields `table_id` or `append_only`, update the code to use `source_table_id` and remove append-only behavior.

- [ ] **Step 8: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-common/src/types.rs crates/nova-common/src/error.rs crates/nova-storage/src/metadata/fdb_store.rs crates/nova-coordinator/src/executor.rs
```

Expected: only stream type/error migrations and call-site updates. Do not commit unless the user explicitly requests it.

---

## Task 2: CDC Payload Parquet I/O

**Files:**
- Create: `crates/nova-storage/src/cdc.rs`
- Modify: `crates/nova-storage/src/lib.rs:3-17`

**Interfaces:**
- Consumes: `ChangeAction`, `ChangeActionCounts`, `ChangePayloadRef`, `ChangeRecordMeta`, `Result`, `TableId`, `TxnId`.
- Produces:
  - `CdcPayloadWriter::new(store: Arc<dyn ObjectStore>, bucket: String) -> Self`
  - `CdcPayloadWriter::write_payload(&self, table_id: TableId, txn_id: TxnId, start_sequence: u64, batch: &RecordBatch) -> Result<ChangePayloadRef>`
  - `CdcPayloadReader::new(store: Arc<dyn ObjectStore>) -> Self`
  - `CdcPayloadReader::read_payload(&self, payload: &ChangePayloadRef) -> Result<Vec<RecordBatch>>`
  - `cdc_payload_path(bucket: &str, table_id: TableId, txn_id: TxnId, start_sequence: u64, end_sequence: u64) -> String`

- [ ] **Step 1: Create failing CDC payload tests**

Create `crates/nova-storage/src/cdc.rs` with this initial test module and empty type declarations that intentionally do not implement methods yet:

```rust
use arrow::record_batch::RecordBatch;
use nova_common::{ChangePayloadRef, Result, TableId, TxnId};
use object_store::ObjectStore;
use std::sync::Arc;

pub struct CdcPayloadWriter {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

pub struct CdcPayloadReader {
    store: Arc<dyn ObjectStore>,
}

pub fn cdc_payload_path(
    bucket: &str,
    table_id: TableId,
    txn_id: TxnId,
    start_sequence: u64,
    end_sequence: u64,
) -> String {
    format!("{bucket}/cdc/tables/{table_id}/txn-{txn_id}/seq-{start_sequence}-{end_sequence}.parquet")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn cdc_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("METADATA$ACTION", DataType::Utf8, false),
            Field::new("METADATA$ISUPDATE", DataType::Boolean, false),
            Field::new("METADATA$ROW_ID", DataType::Utf8, false),
            Field::new("METADATA$TXN_ID", DataType::UInt64, false),
            Field::new("METADATA$COMMIT_TS", DataType::UInt64, false),
            Field::new("METADATA$SEQUENCE", DataType::UInt64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["INSERT", "INSERT", "INSERT"])),
                Arc::new(BooleanArray::from(vec![false, false, false])),
                Arc::new(StringArray::from(vec!["1:10:0:0", "1:10:1:0", "1:10:2:0"])),
                Arc::new(UInt64Array::from(vec![7, 7, 7])),
                Arc::new(UInt64Array::from(vec![100, 100, 100])),
                Arc::new(UInt64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn payload_round_trip_preserves_rows_and_metadata() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
        let writer = CdcPayloadWriter::new(store.clone(), "nova".to_string());
        let reader = CdcPayloadReader::new(store);
        let payload = writer.write_payload(42, 7, 1, &cdc_batch()).await.unwrap();
        assert_eq!(payload.path, "nova/cdc/tables/42/txn-7/seq-1-3.parquet");
        assert_eq!(payload.row_start, 0);
        assert_eq!(payload.row_count, 3);

        let batches = reader.read_payload(&payload).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].schema().field(1).name(), "METADATA$ACTION");
    }
}
```

- [ ] **Step 2: Export module and run failing test**

Add to `crates/nova-storage/src/lib.rs`:

```rust
pub mod cdc;
pub use cdc::{cdc_payload_path, CdcPayloadReader, CdcPayloadWriter};
```

Run:

```bash
cargo test -p nova-storage payload_round_trip_preserves_rows_and_metadata -- --nocapture
```

Expected: compile failure for missing `new`, `write_payload`, and `read_payload` methods.

- [ ] **Step 3: Implement CDC payload writer/reader**

Replace the top implementation in `crates/nova-storage/src/cdc.rs` with:

```rust
use arrow::record_batch::RecordBatch;
use nova_common::{ChangePayloadRef, NovaError, Result, TableId, TxnId};
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

pub struct CdcPayloadWriter {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

impl CdcPayloadWriter {
    pub fn new(store: Arc<dyn ObjectStore>, bucket: String) -> Self {
        Self { store, bucket }
    }

    pub async fn write_payload(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        start_sequence: u64,
        batch: &RecordBatch,
    ) -> Result<ChangePayloadRef> {
        if batch.num_rows() == 0 {
            return Err(NovaError::Internal {
                message: "cannot write empty CDC payload".to_string(),
            });
        }
        let end_sequence = start_sequence + batch.num_rows() as u64 - 1;
        let path = cdc_payload_path(&self.bucket, table_id, txn_id, start_sequence, end_sequence);
        let bytes = write_parquet_bytes(batch)?;
        self.store
            .put(&object_store::path::Path::from(path.clone()), PutPayload::from(bytes))
            .await
            .map_err(|e| NovaError::ObjectStoreError { source: Box::new(e) })?;
        Ok(ChangePayloadRef {
            path,
            row_start: 0,
            row_count: batch.num_rows() as u64,
        })
    }
}

pub struct CdcPayloadReader {
    store: Arc<dyn ObjectStore>,
}

impl CdcPayloadReader {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn read_payload(&self, payload: &ChangePayloadRef) -> Result<Vec<RecordBatch>> {
        let data = self
            .store
            .get(&object_store::path::Path::from(payload.path.clone()))
            .await
            .map_err(|e| NovaError::ObjectStoreError { source: Box::new(e) })?
            .bytes()
            .await
            .map_err(|e| NovaError::ObjectStoreError { source: Box::new(e) })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)
            .map_err(|e| NovaError::ParquetError { source: Box::new(e) })?;
        let reader = builder
            .build()
            .map_err(|e| NovaError::ParquetError { source: Box::new(e) })?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|e| NovaError::ArrowError { source: Box::new(e) })?);
        }
        Ok(batches)
    }
}

pub fn cdc_payload_path(
    bucket: &str,
    table_id: TableId,
    txn_id: TxnId,
    start_sequence: u64,
    end_sequence: u64,
) -> String {
    format!("{bucket}/cdc/tables/{table_id}/txn-{txn_id}/seq-{start_sequence}-{end_sequence}.parquet")
}

fn write_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
    let props = WriterProperties::builder()
        .set_compression(ParquetCompression::SNAPPY)
        .set_max_row_group_size(128 * 1024)
        .build();
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))
            .map_err(|e| NovaError::ParquetError { source: Box::new(e) })?;
        writer
            .write(batch)
            .map_err(|e| NovaError::ParquetError { source: Box::new(e) })?;
        writer
            .close()
            .map_err(|e| NovaError::ParquetError { source: Box::new(e) })?;
    }
    Ok(buffer)
}
```

Keep the test module from Step 1 below this implementation.

- [ ] **Step 4: Run CDC payload tests**

Run:

```bash
cargo test -p nova-storage payload_round_trip_preserves_rows_and_metadata -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run storage compile check**

Run:

```bash
cargo test -p nova-storage cdc -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-storage/src/cdc.rs crates/nova-storage/src/lib.rs
```

Expected: new CDC module only, no dependency changes. Do not commit unless requested.

---

## Task 3: Metadata Store Stream and Change-Log APIs

**Files:**
- Modify: `crates/nova-storage/src/metadata/mod.rs:127-143`
- Modify: `crates/nova-storage/src/metadata/fdb_store.rs:60-1150`

**Interfaces:**
- Consumes: types from Task 1.
- Produces MetadataStore methods:
  - `get_stream_by_name(db_id, schema_id, name)`
  - `list_streams(db_id, schema_id)`
  - `drop_stream(stream_id)`
  - `get_table_change_sequence(table_id)`
  - `allocate_table_change_sequences(table_id, count)`
  - `insert_change_records(records)`
  - `get_change_records(table_id, after_sequence, through_sequence)`
  - `compare_and_set_stream_offset(stream_id, expected_sequence, new_offset)`
  - `stream_has_data(stream_id)`

- [ ] **Step 1: Extend the trait with exact signatures**

In `crates/nova-storage/src/metadata/mod.rs`, replace the stream operation block with:

```rust
    // ══════════════════════════════════════════════════════════════
    //  STREAM / CDC OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn create_stream(&self, stream: StreamMeta) -> Result<()>;
    async fn get_stream(&self, stream_id: StreamId) -> Result<Option<StreamMeta>>;
    async fn get_stream_by_name(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        name: &str,
    ) -> Result<Option<StreamMeta>>;
    async fn list_streams(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<Vec<StreamMeta>>;
    async fn drop_stream(&self, stream_id: StreamId) -> Result<()>;
    async fn get_stream_offset(&self, stream_id: StreamId) -> Result<Option<StreamOffset>>;
    async fn set_stream_offset(&self, stream_id: StreamId, offset: StreamOffset) -> Result<()>;
    async fn compare_and_set_stream_offset(
        &self,
        stream_id: StreamId,
        expected_sequence: u64,
        new_offset: StreamOffset,
    ) -> Result<()>;
    async fn get_table_change_sequence(&self, table_id: TableId) -> Result<u64>;
    async fn allocate_table_change_sequences(&self, table_id: TableId, count: u64) -> Result<u64>;
    async fn insert_change_records(&self, records: Vec<ChangeRecordMeta>) -> Result<()>;
    async fn get_change_records(
        &self,
        table_id: TableId,
        after_sequence: u64,
        through_sequence: u64,
    ) -> Result<Vec<ChangeRecordMeta>>;
    async fn stream_has_data(&self, stream_id: StreamId) -> Result<bool>;
```

- [ ] **Step 2: Add failing metadata tests**

In `crates/nova-storage/src/metadata/fdb_store.rs` test module, add:

```rust
#[tokio::test]
async fn stream_offset_cas_and_has_data_are_transactional() -> Result<()> {
    let Some(cluster_file) = fdb_cluster_file() else {
        return Ok(());
    };
    let subspace = test_subspace("stream_offset_cas");
    let store = FdbMetadataStore::open_test(&cluster_file, subspace)?;

    store.create_database(database("streamdb")).await?;
    let db_id = store.list_databases().await?.into_iter().next().unwrap().id;
    store.create_schema(schema(db_id, "public")).await?;
    let schema_id = store.list_schemas(db_id).await?.into_iter().next().unwrap().id;
    let mut source = table(db_id, schema_id, "orders");
    source.id = 88_001;
    store.create_table(source).await?;

    let stream = StreamMeta {
        stream_id: 99_001,
        db_id,
        schema_id,
        source_table_id: 88_001,
        name: "orders_stream".to_string(),
        created_at: now_micros(),
        updated_at: now_micros(),
        owner_role_id: ACCOUNTADMIN_ROLE_ID,
        comment: None,
        stale_after: None,
        dropped: false,
    };
    store.create_stream(stream).await?;
    store
        .set_stream_offset(
            99_001,
            StreamOffset {
                table_id: 88_001,
                committed_sequence: 0,
                committed_ts: now_micros(),
                last_consumed_at: None,
                last_consumed_txn_id: None,
            },
        )
        .await?;

    let first_sequence = store.allocate_table_change_sequences(88_001, 2).await?;
    assert_eq!(first_sequence, 1);
    assert!(store.stream_has_data(99_001).await?);

    store
        .compare_and_set_stream_offset(
            99_001,
            0,
            StreamOffset {
                table_id: 88_001,
                committed_sequence: 2,
                committed_ts: now_micros(),
                last_consumed_at: Some(now_micros()),
                last_consumed_txn_id: Some(7),
            },
        )
        .await?;
    assert!(!store.stream_has_data(99_001).await?);

    let err = store
        .compare_and_set_stream_offset(
            99_001,
            0,
            StreamOffset {
                table_id: 88_001,
                committed_sequence: 3,
                committed_ts: now_micros(),
                last_consumed_at: Some(now_micros()),
                last_consumed_txn_id: Some(8),
            },
        )
        .await
        .expect_err("stale expected sequence must conflict");
    assert!(matches!(err, NovaError::StreamConcurrentConsume { .. }));
    Ok(())
}
```

- [ ] **Step 3: Run the failing metadata test**

Run:

```bash
cargo test -p nova-storage stream_offset_cas_and_has_data_are_transactional -- --nocapture
```

Expected: compile failure for missing trait methods or method implementations.

- [ ] **Step 4: Implement stream name lookup and listing**

In `FdbMetadataStore`, update stream helper code to use keys:

```rust
("stream", stream_id)
("stream_offset", stream_id)
("stream_by_name", db_id, schema_id, normalize_ident(name))
("streams_by_table", source_table_id, stream_id)
```

Implementation shape for `get_stream_by_name`:

```rust
async fn get_stream_by_name(
    &self,
    db_id: DatabaseId,
    schema_id: SchemaId,
    name: &str,
) -> Result<Option<StreamMeta>> {
    let key = self.pack(&("stream_by_name", db_id, schema_id, normalize_ident(name)));
    match self.fdb_get(key).await? {
        Some(bytes) => {
            let stream_id = Self::read_u64(&bytes)?;
            self.get_stream(stream_id).await
        }
        None => Ok(None),
    }
}
```

Add helper if it does not exist:

```rust
fn read_u64(bytes: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = bytes.try_into().map_err(|_| NovaError::Internal {
        message: "invalid u64 metadata value".to_string(),
    })?;
    Ok(u64::from_be_bytes(arr))
}
```

- [ ] **Step 5: Implement sequence allocation**

Use one FDB transaction for `allocate_table_change_sequences`. It must return the first allocated sequence:

```rust
async fn allocate_table_change_sequences(&self, table_id: TableId, count: u64) -> Result<u64> {
    if count == 0 {
        return Ok(self.get_table_change_sequence(table_id).await? + 1);
    }
    let key = self.pack(&("table_change_seq", table_id));
    self.db
        .run(|trx, _| {
            let key = key.clone();
            async move {
                let current = trx
                    .get(&key, false)
                    .await
                    .map_err(foundationdb::FdbBindingError::from)?
                    .map(|v| {
                        let arr: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                        u64::from_be_bytes(arr)
                    })
                    .unwrap_or(0);
                let first = current + 1;
                let next = current + count;
                trx.set(&key, &next.to_be_bytes()[..]);
                Ok::<u64, foundationdb::FdbBindingError>(first)
            }
        })
        .await
        .map_err(|e| NovaError::Internal {
            message: format!("allocate table change sequence failed: {e}"),
        })
}
```

- [ ] **Step 6: Implement offset compare-and-set**

`compare_and_set_stream_offset` must read current offset inside the same FDB transaction and fail if the committed sequence differs:

```rust
async fn compare_and_set_stream_offset(
    &self,
    stream_id: StreamId,
    expected_sequence: u64,
    new_offset: StreamOffset,
) -> Result<()> {
    let key = self.pack(&("stream_offset", stream_id));
    let value = Self::serialize(&new_offset)?;
    self.db
        .run(|trx, _| {
            let key = key.clone();
            let value = value.clone();
            async move {
                let current = trx
                    .get(&key, false)
                    .await
                    .map_err(foundationdb::FdbBindingError::from)?;
                let Some(bytes) = current else {
                    return Err(foundationdb::FdbBindingError::CustomError(Box::new(
                        std::io::Error::new(std::io::ErrorKind::NotFound, "stream offset missing"),
                    )));
                };
                let offset: StreamOffset = bincode::deserialize(bytes.as_ref()).map_err(|e| {
                    foundationdb::FdbBindingError::CustomError(Box::new(e))
                })?;
                if offset.committed_sequence != expected_sequence {
                    return Err(foundationdb::FdbBindingError::CustomError(Box::new(
                        std::io::Error::new(std::io::ErrorKind::WouldBlock, "stream offset conflict"),
                    )));
                }
                trx.set(&key, &value);
                Ok::<(), foundationdb::FdbBindingError>(())
            }
        })
        .await
        .map_err(|_| NovaError::StreamConcurrentConsume { stream_id })
}
```

If the exact FoundationDB error boxing does not compile because `bincode::Error` type does not meet trait bounds, wrap it in `std::io::Error` with `InvalidData`.

- [ ] **Step 7: Implement change record insert/range read**

Keys:

```rust
("table_change_log", table_id, sequence) -> ChangeRecordMeta
("table_change_log_by_txn", txn_id, table_id, sequence) -> empty
```

Range reader:

```rust
async fn get_change_records(
    &self,
    table_id: TableId,
    after_sequence: u64,
    through_sequence: u64,
) -> Result<Vec<ChangeRecordMeta>> {
    if through_sequence <= after_sequence {
        return Ok(vec![]);
    }
    let start = self.pack(&("table_change_log", table_id, after_sequence + 1));
    let end = self.pack(&("table_change_log", table_id, through_sequence + 1));
    let mut records = Vec::new();
    for (_, value) in self.fdb_get_range(start, end).await? {
        records.push(Self::deserialize(&value)?);
    }
    records.sort_by_key(|record: &ChangeRecordMeta| record.sequence);
    Ok(records)
}
```

- [ ] **Step 8: Implement has-data**

```rust
async fn stream_has_data(&self, stream_id: StreamId) -> Result<bool> {
    let stream = self
        .get_stream(stream_id)
        .await?
        .ok_or_else(|| NovaError::StreamNotFound {
            stream_name: stream_id.to_string(),
        })?;
    let offset = self
        .get_stream_offset(stream_id)
        .await?
        .ok_or_else(|| NovaError::StreamNotFound {
            stream_name: stream.name.clone(),
        })?;
    let current = self.get_table_change_sequence(stream.source_table_id).await?;
    Ok(current > offset.committed_sequence)
}
```

- [ ] **Step 9: Run metadata tests**

Run:

```bash
cargo test -p nova-storage stream_offset_cas_and_has_data_are_transactional -- --nocapture
```

Expected: PASS or skip with `Ok(())` when `NOVA_FDB_CLUSTER_FILE` is unset.

- [ ] **Step 10: Run stream metadata group**

Run:

```bash
cargo test -p nova-storage stream -- --nocapture
```

Expected: PASS or FDB-gated tests skip cleanly when no cluster file is configured.

- [ ] **Step 11: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-storage/src/metadata/mod.rs crates/nova-storage/src/metadata/fdb_store.rs
```

Expected: metadata trait and FDB stream/change-log implementation only. Do not commit unless requested.

---

## Task 4: Parser and Analyzer Stream SQL

**Files:**
- Modify: `crates/nova-coordinator/src/parser.rs:16-374`
- Modify: `crates/nova-coordinator/src/analyzer.rs:8-140,383-432,501-620`

**Interfaces:**
- Consumes: `StreamReadMode` from Task 1.
- Produces `ResolvedStatement` variants:
  - `ReadStream { db, schema, stream_name, projection, read_mode, raw_sql }`
  - `DropStream { db, schema, name }`
  - `ShowStreams { db, schema, pattern }`
  - `DescribeStream { db, schema, name }`
  - `SystemStreamHasData { db, schema, stream_name }`

- [ ] **Step 1: Add parser/analyzer tests**

Add to `crates/nova-coordinator/src/parser.rs` test module, or create one if absent:

```rust
#[cfg(test)]
mod stream_parser_tests {
    use super::*;
    use crate::analyzer::{Analyzer, ResolvedStatement};
    use nova_common::StreamReadMode;

    #[test]
    fn parse_stream_select_commit_false() {
        let parser = SqlParser::new();
        let analyzer = Analyzer::new("db".to_string(), "public".to_string());
        let stmt = parser
            .parse("SELECT * FROM orders_stream WITH (COMMIT = FALSE)")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let resolved = analyzer.resolve(&stmt).unwrap();
        match resolved {
            ResolvedStatement::ReadStream { stream_name, read_mode, .. } => {
                assert_eq!(stream_name, "orders_stream");
                assert_eq!(read_mode, StreamReadMode::Preview);
            }
            other => panic!("expected ReadStream, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_stream_has_data() {
        let parser = SqlParser::new();
        let analyzer = Analyzer::new("db".to_string(), "public".to_string());
        let stmt = parser
            .parse("SELECT SYSTEM$STREAM_HAS_DATA('orders_stream')")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let resolved = analyzer.resolve(&stmt).unwrap();
        match resolved {
            ResolvedStatement::SystemStreamHasData { stream_name, .. } => {
                assert_eq!(stream_name, "orders_stream");
            }
            other => panic!("expected SystemStreamHasData, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run failing parser tests**

Run:

```bash
cargo test -p nova-coordinator stream_parser_tests -- --nocapture
```

Expected: compile failure for missing `ResolvedStatement` variants.

- [ ] **Step 3: Add resolved statement variants**

In `crates/nova-coordinator/src/analyzer.rs`, add variants after `CreateStream`:

```rust
    ReadStream {
        db: String,
        schema: String,
        stream_name: String,
        projection: Vec<String>,
        read_mode: nova_common::StreamReadMode,
        raw_sql: Option<String>,
    },
    DropStream {
        db: String,
        schema: String,
        name: String,
    },
    ShowStreams {
        db: String,
        schema: String,
        pattern: Option<String>,
    },
    DescribeStream {
        db: String,
        schema: String,
        name: String,
    },
    SystemStreamHasData {
        db: String,
        schema: String,
        stream_name: String,
    },
```

- [ ] **Step 4: Add custom parser branches**

In `SqlParser::parse`, add before generic parser:

```rust
        if upper.starts_with("DROP STREAM ") {
            return self.parse_drop_stream(sql);
        }
        if upper.starts_with("SHOW STREAMS") {
            return self.parse_show_streams(sql);
        }
        if upper.starts_with("DESCRIBE STREAM ") || upper.starts_with("DESC STREAM ") {
            return self.parse_describe_stream(sql);
        }
        if upper.starts_with("SELECT SYSTEM$STREAM_HAS_DATA") {
            return self.parse_system_stream_has_data(sql);
        }
        if upper.starts_with("SELECT ") && upper.contains(" WITH (COMMIT = FALSE)") {
            return self.parse_stream_preview_select(sql);
        }
```

- [ ] **Step 5: Encode custom stream statements as existing AST shapes**

Add parser helper methods:

```rust
fn parse_drop_stream(&self, sql: &str) -> Result<Vec<Statement>> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let name = parts.get(2).ok_or_else(|| NovaError::SqlParseError {
        message: "DROP STREAM syntax: DROP STREAM <name>".to_string(),
    })?.trim_end_matches(';');
    let fake_sql = format!("DROP TABLE __drop_stream__{}", name);
    Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError { message: e.to_string() })
}

fn parse_show_streams(&self, sql: &str) -> Result<Vec<Statement>> {
    let upper = sql.to_uppercase();
    let pattern = if let Some(like_pos) = upper.find(" LIKE ") {
        sql[like_pos + 6..].trim().trim_matches(';').trim().trim_matches('\'').to_string()
    } else {
        String::new()
    };
    let fake_sql = format!("DROP TABLE __show_streams__{}", pattern);
    Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError { message: e.to_string() })
}

fn parse_describe_stream(&self, sql: &str) -> Result<Vec<Statement>> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let name = parts.get(2).ok_or_else(|| NovaError::SqlParseError {
        message: "DESCRIBE STREAM syntax: DESCRIBE STREAM <name>".to_string(),
    })?.trim_end_matches(';');
    let fake_sql = format!("DROP TABLE __describe_stream__{}", name);
    Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError { message: e.to_string() })
}

fn parse_system_stream_has_data(&self, sql: &str) -> Result<Vec<Statement>> {
    let start = sql.find('(').ok_or_else(|| NovaError::SqlParseError {
        message: "SYSTEM$STREAM_HAS_DATA requires a stream name".to_string(),
    })?;
    let end = sql.rfind(')').ok_or_else(|| NovaError::SqlParseError {
        message: "SYSTEM$STREAM_HAS_DATA requires closing ')'".to_string(),
    })?;
    let name = sql[start + 1..end].trim().trim_matches('\'').trim_matches('"');
    let fake_sql = format!("DROP TABLE __stream_has_data__{}", name);
    Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError { message: e.to_string() })
}

fn parse_stream_preview_select(&self, sql: &str) -> Result<Vec<Statement>> {
    let clean_sql = sql.replace(" WITH (COMMIT = FALSE)", "").replace(" with (commit = false)", "");
    let encoded = clean_sql.replace(' ', "_");
    let fake_sql = format!("DROP TABLE __stream_preview__{}", encoded);
    Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError { message: e.to_string() })
}
```

If case-insensitive replacement is needed, implement a helper that finds the uppercase marker position and removes the original substring by byte indices.

- [ ] **Step 6: Analyzer detects encoded stream statements**

In the `Statement::Drop` branch, add detections before normal drop handling:

```rust
if table_name.starts_with("__drop_stream__") {
    return Ok(ResolvedStatement::DropStream {
        db: self.default_db.clone(),
        schema: self.default_schema.clone(),
        name: table_name.trim_start_matches("__drop_stream__").to_string(),
    });
}
if table_name.starts_with("__show_streams__") {
    let pattern = table_name.trim_start_matches("__show_streams__");
    return Ok(ResolvedStatement::ShowStreams {
        db: self.default_db.clone(),
        schema: self.default_schema.clone(),
        pattern: if pattern.is_empty() { None } else { Some(pattern.to_string()) },
    });
}
if table_name.starts_with("__describe_stream__") {
    return Ok(ResolvedStatement::DescribeStream {
        db: self.default_db.clone(),
        schema: self.default_schema.clone(),
        name: table_name.trim_start_matches("__describe_stream__").to_string(),
    });
}
if table_name.starts_with("__stream_has_data__") {
    return Ok(ResolvedStatement::SystemStreamHasData {
        db: self.default_db.clone(),
        schema: self.default_schema.clone(),
        stream_name: table_name.trim_start_matches("__stream_has_data__").to_string(),
    });
}
if table_name.starts_with("__stream_preview__") {
    let clean_sql = table_name.trim_start_matches("__stream_preview__").replace('_', " ");
    let stream_name = clean_sql
        .split_whitespace()
        .skip_while(|token| !token.eq_ignore_ascii_case("FROM"))
        .nth(1)
        .unwrap_or("")
        .to_string();
    return Ok(ResolvedStatement::ReadStream {
        db: self.default_db.clone(),
        schema: self.default_schema.clone(),
        stream_name,
        projection: vec!["*".to_string()],
        read_mode: nova_common::StreamReadMode::Preview,
        raw_sql: Some(clean_sql),
    });
}
```

- [ ] **Step 7: Detect normal `SELECT * FROM stream` at execution time rather than analyzer time**

Keep normal `Statement::Query` resolving to `ResolvedStatement::Select`. The executor will decide whether the relation is a table or stream by metadata lookup. This avoids parser guessing for every SELECT.

- [ ] **Step 8: Run parser tests**

Run:

```bash
cargo test -p nova-coordinator stream_parser_tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Run analyzer compile group**

Run:

```bash
cargo test -p nova-coordinator analyzer -- --nocapture
```

Expected: PASS or no analyzer-specific tests found with successful compile.

- [ ] **Step 10: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-coordinator/src/parser.rs crates/nova-coordinator/src/analyzer.rs
```

Expected: custom stream SQL parsing and resolved statement variants only. Do not commit unless requested.

---

## Task 5: Stream Lifecycle Executor and RBAC

**Files:**
- Modify: `crates/nova-coordinator/src/executor.rs:254-400,1323-1368,1942-1965`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs:330-630`

**Interfaces:**
- Consumes metadata APIs from Task 3 and resolved statements from Task 4.
- Produces executor methods:
  - `find_stream(db, schema, stream_name) -> Result<StreamMeta>`
  - `authorize_stream_read(security, db, schema, stream) -> Result<TableMeta>`
  - `exec_create_stream(security, db, schema, stream_name, table) -> Result<QueryResult>`
  - `exec_drop_stream(security, db, schema, name) -> Result<QueryResult>`
  - `exec_show_streams(security, db, schema, pattern) -> Result<QueryResult>`
  - `exec_describe_stream(security, db, schema, name) -> Result<QueryResult>`

- [ ] **Step 1: Update CREATE STREAM tests for no append-only**

Change existing create stream tests so expected success messages do not mention append-only. Add this e2e test:

```rust
#[tokio::test]
async fn stream_lifecycle_requires_rbac_and_shows_metadata() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE streamdb", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE orders (id INT)", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE STREAM orders_stream ON TABLE orders", "streamdb").await.unwrap();

    let show = exec_sql(&executor, "SHOW STREAMS", "streamdb").await.unwrap();
    match show {
        nova_coordinator::executor::QueryResult::Rows { columns, rows } => {
            assert!(columns.contains(&"name".to_string()));
            assert!(rows.iter().any(|row| row.iter().any(|cell| cell == "orders_stream")));
        }
        other => panic!("expected rows, got {other:?}"),
    }

    let desc = exec_sql(&executor, "DESCRIBE STREAM orders_stream", "streamdb").await.unwrap();
    match desc {
        nova_coordinator::executor::QueryResult::Rows { columns, rows } => {
            assert!(columns.contains(&"source_table_id".to_string()));
            assert_eq!(rows.len(), 1);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    exec_sql(&executor, "DROP STREAM orders_stream", "streamdb").await.unwrap();
    let err = exec_sql(&executor, "DESCRIBE STREAM orders_stream", "streamdb")
        .await
        .expect_err("dropped stream should not resolve");
    assert!(matches!(err, nova_common::NovaError::StreamNotFound { .. }));
}
```

- [ ] **Step 2: Run failing lifecycle test**

Run:

```bash
cargo test -p nova-coordinator stream_lifecycle_requires_rbac_and_shows_metadata -- --nocapture
```

Expected: compile failure for missing executor branches or runtime failure for unsupported SQL.

- [ ] **Step 3: Update `execute_with_context` match arms**

Add branches for the new resolved statements:

```rust
ResolvedStatement::DropStream { db, schema, name } => {
    self.exec_drop_stream(security, &db, &schema, &name).await
}
ResolvedStatement::ShowStreams { db, schema, pattern } => {
    self.exec_show_streams(security, &db, &schema, pattern).await
}
ResolvedStatement::DescribeStream { db, schema, name } => {
    self.exec_describe_stream(security, &db, &schema, &name).await
}
ResolvedStatement::SystemStreamHasData { db, schema, stream_name } => {
    self.exec_system_stream_has_data(security, &db, &schema, &stream_name).await
}
ResolvedStatement::ReadStream { db, schema, stream_name, projection, read_mode, raw_sql } => {
    self.exec_read_stream(security, &db, &schema, &stream_name, projection, read_mode, raw_sql).await
}
```

For `ResolvedStatement::CreateStream`, remove `append_only` from the variant and call signature after Task 4 updates. If Task 4 kept the field temporarily, ignore it and reject append-only in parser.

- [ ] **Step 4: Implement stream lookup and authorization helpers**

Add methods inside `impl Executor`:

```rust
async fn find_stream(&self, db: &str, schema: &str, stream_name: &str) -> Result<StreamMeta> {
    let db_meta = self.find_database(db).await?;
    let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
    self.meta
        .get_stream_by_name(db_meta.id, schema_meta.id, stream_name)
        .await?
        .filter(|stream| !stream.dropped)
        .ok_or_else(|| NovaError::StreamNotFound {
            stream_name: stream_name.to_string(),
        })
}

async fn authorize_stream_read(
    &self,
    security: &SecurityContext,
    db: &str,
    schema: &str,
    stream: &StreamMeta,
) -> Result<TableMeta> {
    let db_meta = self.find_database(db).await?;
    let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
    self.require_privilege(security, ObjectRef::new(ObjectType::Database, db_meta.id), SecurityPrivilege::Usage).await?;
    self.require_privilege(security, ObjectRef::new(ObjectType::Schema, schema_meta.id), SecurityPrivilege::Usage).await?;
    self.require_privilege(security, ObjectRef::new(ObjectType::Stream, stream.stream_id), SecurityPrivilege::Select).await?;
    let table = self
        .meta
        .get_table(stream.db_id, stream.schema_id, stream.source_table_id)
        .await?
        .ok_or_else(|| NovaError::TableNotFound { table_name: stream.source_table_id.to_string() })?;
    self.require_privilege(security, ObjectRef::new(ObjectType::Table, table.id), SecurityPrivilege::Select).await?;
    Ok(table)
}
```

- [ ] **Step 5: Update create stream execution**

Change `exec_create_stream` to initialize stream offset and remove append-only:

```rust
async fn exec_create_stream(
    &self,
    security: &SecurityContext,
    db: &str,
    schema: &str,
    stream_name: &str,
    table: &str,
) -> Result<QueryResult> {
    let db_meta = self.find_database(db).await?;
    let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
    let table_meta = self.find_table(db, schema, table).await?;
    let stream_id = generate_id();
    let now = now_micros();
    let current_sequence = self.meta.get_table_change_sequence(table_meta.id).await?;
    let stream = StreamMeta {
        stream_id,
        db_id: db_meta.id,
        schema_id: schema_meta.id,
        source_table_id: table_meta.id,
        name: stream_name.to_string(),
        created_at: now,
        updated_at: now,
        owner_role_id: security.primary_role_id,
        comment: None,
        stale_after: None,
        dropped: false,
    };
    self.meta.create_stream(stream).await?;
    self.meta
        .set_stream_offset(
            stream_id,
            StreamOffset {
                table_id: table_meta.id,
                committed_sequence: current_sequence,
                committed_ts: now,
                last_consumed_at: None,
                last_consumed_txn_id: None,
            },
        )
        .await?;
    if self.meta.get_role(security.primary_role_id).await?.is_some() {
        self.meta
            .set_object_owner(ObjectOwnerMeta {
                object: ObjectRef::new(ObjectType::Stream, stream_id),
                owner_role_id: security.primary_role_id,
                created_by_user_id: security.user_id,
                created_at: now,
                transferred_at: None,
            })
            .await?;
    }
    Ok(QueryResult::Success {
        message: format!("Stream '{}' created on table '{}' (id={})", stream_name, table, stream_id),
    })
}
```

- [ ] **Step 6: Implement show/describe/drop stream**

Implement results with stable column names:

```rust
async fn exec_show_streams(
    &self,
    security: &SecurityContext,
    db: &str,
    schema: &str,
    pattern: Option<String>,
) -> Result<QueryResult> {
    let db_meta = self.find_database(db).await?;
    let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
    self.require_privilege(security, ObjectRef::new(ObjectType::Database, db_meta.id), SecurityPrivilege::Usage).await?;
    self.require_privilege(security, ObjectRef::new(ObjectType::Schema, schema_meta.id), SecurityPrivilege::Usage).await?;
    let mut rows = Vec::new();
    for stream in self.meta.list_streams(db_meta.id, schema_meta.id).await? {
        if stream.dropped {
            continue;
        }
        if let Some(ref pat) = pattern
            && !stream.name.contains(pat)
        {
            continue;
        }
        if self.has_privilege(security, ObjectRef::new(ObjectType::Stream, stream.stream_id), SecurityPrivilege::Select).await?
            || self.has_privilege(security, ObjectRef::new(ObjectType::Stream, stream.stream_id), SecurityPrivilege::Ownership).await?
        {
            rows.push(vec![
                stream.name,
                stream.stream_id.to_string(),
                stream.source_table_id.to_string(),
                stream.created_at.to_string(),
                stream.owner_role_id.to_string(),
            ]);
        }
    }
    Ok(QueryResult::Rows {
        columns: vec!["name".to_string(), "stream_id".to_string(), "source_table_id".to_string(), "created_at".to_string(), "owner_role_id".to_string()],
        rows,
    })
}
```

Implement `exec_describe_stream` similarly with columns `name`, `stream_id`, `source_table_id`, `offset_sequence`, `created_at`, `updated_at`, `stale_after`.

Implement `exec_drop_stream` with `OWNERSHIP` required on `ObjectType::Stream` and call `meta.drop_stream(stream.stream_id)`.

- [ ] **Step 7: Run lifecycle tests**

Run:

```bash
cargo test -p nova-coordinator stream_lifecycle_requires_rbac_and_shows_metadata -- --nocapture
```

Expected: PASS with local FDB running at the repository test address.

- [ ] **Step 8: Run existing stream RBAC tests**

Run:

```bash
cargo test -p nova-coordinator create_stream -- --nocapture
cargo test -p nova-coordinator non_admin_cannot_create_stream -- --nocapture
```

Expected: PASS after updating expected success message strings.

- [ ] **Step 9: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs
```

Expected: lifecycle/RBAC executor changes and tests. Do not commit unless requested.

---

## Task 6: DML CDC Capture for INSERT, UPDATE, DELETE

**Files:**
- Modify: `crates/nova-coordinator/src/executor.rs:760-1014,1016-1243`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`

**Interfaces:**
- Consumes `CdcPayloadWriter`, `ChangeRecordMeta`, `ChangeActionCounts`, `stream_row_id`.
- Produces helper methods:
  - `build_insert_cdc_batch(table, source_batch, start_sequence, txn_id, commit_ts, mp_id) -> Result<RecordBatch>`
  - `build_delete_cdc_batch(table, old_batch, mask, start_sequence, txn_id, commit_ts, mp_id, is_update) -> Result<RecordBatch>`
  - `build_update_cdc_batch(table, old_batch, new_batch, mask, start_sequence, txn_id, commit_ts, mp_id) -> Result<RecordBatch>`
  - `write_cdc_payload_and_metadata(table_id, txn_id, start_sequence, batch, action_counts) -> Result<ChangeRecordMeta>`

- [ ] **Step 1: Add failing DML CDC e2e test**

Add to `crates/nova-coordinator/tests/e2e_tests.rs`:

```rust
#[tokio::test]
async fn stream_captures_insert_update_delete_with_snowflake_metadata() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE streamdb", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE orders (id INT, status TEXT)", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE STREAM orders_stream ON TABLE orders", "streamdb").await.unwrap();

    exec_sql(&executor, "INSERT INTO orders VALUES (1, 'new'), (2, 'new'), (3, 'new')", "streamdb").await.unwrap();
    exec_sql(&executor, "UPDATE orders SET status = 'paid' WHERE id = 2", "streamdb").await.unwrap();
    exec_sql(&executor, "DELETE FROM orders WHERE id = 3", "streamdb").await.unwrap();

    let preview = exec_sql(
        &executor,
        "SELECT * FROM orders_stream WITH (COMMIT = FALSE)",
        "streamdb",
    )
    .await
    .unwrap();
    match preview {
        nova_coordinator::executor::QueryResult::Rows { columns, rows } => {
            assert!(columns.contains(&"METADATA$ACTION".to_string()));
            assert!(columns.contains(&"METADATA$ISUPDATE".to_string()));
            assert!(rows.iter().any(|row| row.iter().any(|cell| cell == "INSERT")));
            assert!(rows.iter().any(|row| row.iter().any(|cell| cell == "DELETE")));
            assert!(rows.iter().any(|row| row.iter().any(|cell| cell == "true")));
        }
        other => panic!("expected rows, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run failing DML CDC test**

Run:

```bash
cargo test -p nova-coordinator stream_captures_insert_update_delete_with_snowflake_metadata -- --nocapture
```

Expected: failure because stream reads are not implemented yet or CDC records are absent. Keep the failing test for Task 7 if stream read path is not complete. This task focuses on writing CDC metadata and payloads.

- [ ] **Step 3: Add CDC writer to Executor**

Modify `Executor` struct:

```rust
pub struct Executor {
    meta: Arc<dyn MetadataStore>,
    writer: MpWriter,
    reader: MpReader,
    cdc_writer: CdcPayloadWriter,
    cdc_reader: CdcPayloadReader,
    optimizer: NovaOptimizer,
    current_txn: Arc<std::sync::Mutex<Option<TxnId>>>,
}
```

Update `Executor::new`:

```rust
pub fn new(meta: Arc<dyn MetadataStore>, writer: MpWriter, reader: MpReader) -> Self {
    let cdc_writer = CdcPayloadWriter::new(writer.store_arc(), writer.bucket_name().to_string());
    let cdc_reader = CdcPayloadReader::new(reader.store_arc());
    Self {
        meta,
        writer,
        reader,
        cdc_writer,
        cdc_reader,
        optimizer: NovaOptimizer::new(),
        current_txn: Arc::new(std::sync::Mutex::new(None)),
    }
}
```

To support this, add safe accessors to `MpWriter` and `MpReader`:

```rust
pub fn store_arc(&self) -> Arc<dyn ObjectStore> {
    self.store.clone()
}

pub fn bucket_name(&self) -> &str {
    &self.bucket
}
```

and:

```rust
pub fn store_arc(&self) -> Arc<dyn ObjectStore> {
    self.store.clone()
}
```

- [ ] **Step 4: Build CDC batches with source columns plus metadata columns**

Add helper that appends metadata arrays to a source `RecordBatch`:

```rust
fn append_cdc_metadata_columns(
    source: &RecordBatch,
    actions: Vec<String>,
    is_updates: Vec<bool>,
    row_ids: Vec<String>,
    txn_ids: Vec<u64>,
    commit_ts_values: Vec<u64>,
    sequences: Vec<u64>,
) -> Result<RecordBatch> {
    use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    let mut fields = source.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new("METADATA$ACTION", DataType::Utf8, false)));
    fields.push(Arc::new(Field::new("METADATA$ISUPDATE", DataType::Boolean, false)));
    fields.push(Arc::new(Field::new("METADATA$ROW_ID", DataType::Utf8, false)));
    fields.push(Arc::new(Field::new("METADATA$TXN_ID", DataType::UInt64, false)));
    fields.push(Arc::new(Field::new("METADATA$COMMIT_TS", DataType::UInt64, false)));
    fields.push(Arc::new(Field::new("METADATA$SEQUENCE", DataType::UInt64, false)));
    let mut columns: Vec<ArrayRef> = source.columns().to_vec();
    columns.push(Arc::new(StringArray::from(actions)) as ArrayRef);
    columns.push(Arc::new(BooleanArray::from(is_updates)) as ArrayRef);
    columns.push(Arc::new(StringArray::from(row_ids)) as ArrayRef);
    columns.push(Arc::new(UInt64Array::from(txn_ids)) as ArrayRef);
    columns.push(Arc::new(UInt64Array::from(commit_ts_values)) as ArrayRef);
    columns.push(Arc::new(UInt64Array::from(sequences)) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| NovaError::ArrowError { source: Box::new(e) })
}
```

- [ ] **Step 5: Emit CDC for INSERT**

In `exec_insert`, after building the source batch and before metadata commit:

1. Allocate `row_count` table change sequences.
2. Build metadata arrays where action is `INSERT`, `is_update=false`.
3. Write CDC payload.
4. Insert `ChangeRecordMeta` in FDB.
5. Insert MP metadata and commit transaction.

Implementation shape:

```rust
let commit_ts = now_micros();
let start_sequence = self.meta.allocate_table_change_sequences(table_meta.id, row_count as u64).await?;
let cdc_batch = self.build_insert_cdc_batch(&table_meta, &batch, start_sequence, txn_id, commit_ts, mp_id)?;
let payload = self.cdc_writer.write_payload(table_meta.id, txn_id, start_sequence, &cdc_batch).await?;
let change_meta = ChangeRecordMeta {
    table_id: table_meta.id,
    sequence: start_sequence,
    txn_id,
    commit_ts,
    payload,
    action_counts: ChangeActionCounts { inserts: row_count as u64, deletes: 0, update_pairs: 0 },
    min_row_id: Some(stream_row_id(table_meta.id, mp_id, 0, 0)),
    max_row_id: Some(stream_row_id(table_meta.id, mp_id, row_count as u64 - 1, 0)),
};
self.meta.insert_change_records(vec![change_meta]).await?;
```

- [ ] **Step 6: Fix UPDATE/DELETE MP metadata correctness before CDC assertions**

Current UPDATE/DELETE writes replacement MPs but does not insert committed new MP metadata. Update both paths so replacement MPs are committed like INSERT:

```rust
let mut committed_new_mp = new_mp;
committed_new_mp.s3_path = committed_new_mp.s3_temp_path.take().unwrap_or(committed_new_mp.s3_path);
committed_new_mp.commit_ts = commit_ts;
committed_new_mp.active = true;
committed_new_mp.supersedes = Some(mp.mp_id);
self.meta.insert_mp(committed_new_mp).await?;
self.meta.mark_superseded(mp.mp_id, committed_new_mp.mp_id).await?;
```

Use unique `mp_id = generate_id()` instead of `mp.mp_id + 1000`.

- [ ] **Step 7: Emit CDC for DELETE**

Before applying delete, compute the matched rows using `eval_filter_on_batch`. Build a batch of deleted rows with `arrow::compute::take`, append metadata action `DELETE`, `is_update=false`, write payload, and insert change metadata.

The sequence count equals number of deleted rows. If zero rows match, do not allocate sequences and do not write CDC payload.

- [ ] **Step 8: Emit CDC for UPDATE**

For each affected batch:

1. Compute match mask.
2. Build old-row batch from matching rows.
3. Build modified full batch.
4. Build new-row batch from matching rows.
5. Allocate `matched_count * 2` sequences.
6. Create one CDC payload batch by concatenating old rows and new rows:
   - old rows action `DELETE`, `is_update=true`;
   - new rows action `INSERT`, `is_update=true`.
7. Write payload and change metadata with `update_pairs=matched_count`.

If Arrow concat helpers are unavailable in the current version, build separate old/new payloads with contiguous sequence ranges and insert two `ChangeRecordMeta` entries.

- [ ] **Step 9: Run targeted DML compile/tests**

Run:

```bash
cargo test -p nova-coordinator stream_captures_insert_update_delete_with_snowflake_metadata -- --nocapture
```

Expected at this task boundary: test may still fail because `SELECT FROM stream` is not complete. It must not fail due to DML panic or missing CDC payload metadata. If it reaches stream read unsupported, proceed to Task 7.

- [ ] **Step 10: Run existing UPDATE/DELETE tests**

Run:

```bash
cargo test -p nova-coordinator update -- --nocapture
cargo test -p nova-coordinator delete -- --nocapture
```

Expected: existing UPDATE/DELETE behavior remains passing, and replacement MPs are visible after DML.

- [ ] **Step 11: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-coordinator/src/executor.rs crates/nova-storage/src/mp_writer.rs crates/nova-storage/src/mp_reader.rs crates/nova-coordinator/tests/e2e_tests.rs
```

Expected: DML CDC capture and safe storage accessor changes. Do not commit unless requested.

---

## Task 7: Stream SELECT Consume and Preview

**Files:**
- Modify: `crates/nova-coordinator/src/executor.rs:305-331,805-927`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`

**Interfaces:**
- Consumes CDC metadata APIs, `CdcPayloadReader`, parser/analyzer ReadStream variant.
- Produces:
  - `exec_read_stream(...) -> Result<QueryResult>`
  - stream-vs-table resolution in normal SELECT path.

- [ ] **Step 1: Add consume and preview e2e tests**

Add:

```rust
#[tokio::test]
async fn select_stream_consumes_once_and_preview_does_not_commit() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE streamdb", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE events (id INT)", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE STREAM events_stream ON TABLE events", "streamdb").await.unwrap();
    exec_sql(&executor, "INSERT INTO events VALUES (1), (2), (3)", "streamdb").await.unwrap();

    let preview_1 = exec_sql(&executor, "SELECT * FROM events_stream WITH (COMMIT = FALSE)", "streamdb").await.unwrap();
    let preview_2 = exec_sql(&executor, "SELECT * FROM events_stream WITH (COMMIT = FALSE)", "streamdb").await.unwrap();
    let rows_1 = match preview_1 { nova_coordinator::executor::QueryResult::Rows { rows, .. } => rows, _ => panic!("expected rows") };
    let rows_2 = match preview_2 { nova_coordinator::executor::QueryResult::Rows { rows, .. } => rows, _ => panic!("expected rows") };
    assert_eq!(rows_1.len(), 3);
    assert_eq!(rows_2.len(), 3);

    let consumed = exec_sql(&executor, "SELECT * FROM events_stream", "streamdb").await.unwrap();
    let consumed_rows = match consumed { nova_coordinator::executor::QueryResult::Rows { rows, .. } => rows, _ => panic!("expected rows") };
    assert_eq!(consumed_rows.len(), 3);

    let empty = exec_sql(&executor, "SELECT * FROM events_stream", "streamdb").await.unwrap();
    let empty_rows = match empty { nova_coordinator::executor::QueryResult::Rows { rows, .. } => rows, _ => panic!("expected rows") };
    assert!(empty_rows.is_empty());
}
```

- [ ] **Step 2: Run failing consume test**

Run:

```bash
cargo test -p nova-coordinator select_stream_consumes_once_and_preview_does_not_commit -- --nocapture
```

Expected: failure because stream SELECT is not implemented.

- [ ] **Step 3: Route normal SELECT to stream if relation is a stream**

In `execute_with_context`, for `ResolvedStatement::Select`, before authorizing table dependencies, check stream existence by db/schema/table name:

```rust
if let Ok(stream) = self.find_stream(&db, &schema, &table).await {
    let table_meta = self.authorize_stream_read(security, &db, &schema, &stream).await?;
    drop(table_meta);
    return self
        .exec_read_stream(
            security,
            &db,
            &schema,
            &table,
            projection,
            StreamReadMode::Commit,
            raw_sql,
        )
        .await;
}
```

If both table and stream with same name exist, return `NovaError::AmbiguousRelationName { name: table }`.

- [ ] **Step 4: Implement `exec_read_stream`**

Add:

```rust
async fn exec_read_stream(
    &self,
    security: &SecurityContext,
    db: &str,
    schema: &str,
    stream_name: &str,
    _projection: Vec<String>,
    read_mode: StreamReadMode,
    _raw_sql: Option<String>,
) -> Result<QueryResult> {
    let stream = self.find_stream(db, schema, stream_name).await?;
    let _source_table = self.authorize_stream_read(security, db, schema, &stream).await?;
    let offset = self
        .meta
        .get_stream_offset(stream.stream_id)
        .await?
        .ok_or_else(|| NovaError::StreamNotFound { stream_name: stream_name.to_string() })?;
    let read_start = offset.committed_sequence;
    let read_end = self.meta.get_table_change_sequence(stream.source_table_id).await?;
    let records = self
        .meta
        .get_change_records(stream.source_table_id, read_start, read_end)
        .await?;
    let mut columns: Option<Vec<String>> = None;
    let mut rows = Vec::new();
    for record in &records {
        let batches = self.cdc_reader.read_payload(&record.payload).await.map_err(|err| match err {
            NovaError::ObjectStoreError { .. } => NovaError::StreamPayloadMissing {
                stream_id: stream.stream_id,
                payload_path: record.payload.path.clone(),
            },
            other => other,
        })?;
        for batch in batches {
            if columns.is_none() {
                columns = Some(batch.schema().fields().iter().map(|field| field.name().clone()).collect());
            }
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for col_idx in 0..batch.num_columns() {
                    row.push(array_value_to_string(batch.column(col_idx), row_idx));
                }
                rows.push(row);
            }
        }
    }
    if read_mode == StreamReadMode::Commit {
        self.meta
            .compare_and_set_stream_offset(
                stream.stream_id,
                read_start,
                StreamOffset {
                    table_id: stream.source_table_id,
                    committed_sequence: read_end,
                    committed_ts: now_micros(),
                    last_consumed_at: Some(now_micros()),
                    last_consumed_txn_id: None,
                },
            )
            .await?;
    }
    Ok(QueryResult::Rows {
        columns: columns.unwrap_or_else(|| vec![
            "METADATA$ACTION".to_string(),
            "METADATA$ISUPDATE".to_string(),
            "METADATA$ROW_ID".to_string(),
            "METADATA$TXN_ID".to_string(),
            "METADATA$COMMIT_TS".to_string(),
            "METADATA$SEQUENCE".to_string(),
        ]),
        rows,
    })
}
```

- [ ] **Step 5: Ensure preview variant calls stream read**

The `ResolvedStatement::ReadStream` branch must call `exec_read_stream` with `StreamReadMode::Preview`.

- [ ] **Step 6: Run consume/preview test**

Run:

```bash
cargo test -p nova-coordinator select_stream_consumes_once_and_preview_does_not_commit -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Run full stream DML test from Task 6**

Run:

```bash
cargo test -p nova-coordinator stream_captures_insert_update_delete_with_snowflake_metadata -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Add and run filtered commit scope test**

Add:

```rust
#[tokio::test]
async fn filtered_stream_select_commits_full_backlog() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE streamdb", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE events (id INT)", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE STREAM events_stream ON TABLE events", "streamdb").await.unwrap();
    exec_sql(&executor, "INSERT INTO events VALUES (1), (2), (3)", "streamdb").await.unwrap();

    let _ = exec_sql(&executor, "SELECT * FROM events_stream WHERE id = 1", "streamdb").await.unwrap();
    let second = exec_sql(&executor, "SELECT * FROM events_stream", "streamdb").await.unwrap();
    let rows = match second { nova_coordinator::executor::QueryResult::Rows { rows, .. } => rows, _ => panic!("expected rows") };
    assert!(rows.is_empty());
}
```

Run:

```bash
cargo test -p nova-coordinator filtered_stream_select_commits_full_backlog -- --nocapture
```

Expected: PASS. If DataFusion raw SQL routing bypasses stream read for `WHERE`, update the normal SELECT routing to detect stream relation before DataFusion execution.

- [ ] **Step 9: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs
```

Expected: stream read/consume/preview logic and tests. Do not commit unless requested.

---

## Task 8: SYSTEM$STREAM_HAS_DATA and Lifecycle SQL Completion

**Files:**
- Modify: `crates/nova-coordinator/src/executor.rs`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`

**Interfaces:**
- Consumes `stream_has_data` metadata method.
- Produces `exec_system_stream_has_data(...) -> Result<QueryResult>`.

- [ ] **Step 1: Add has-data e2e test**

Add:

```rust
#[tokio::test]
async fn system_stream_has_data_tracks_backlog() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE streamdb", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE events (id INT)", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE STREAM events_stream ON TABLE events", "streamdb").await.unwrap();

    let empty = exec_sql(&executor, "SELECT SYSTEM$STREAM_HAS_DATA('events_stream')", "streamdb").await.unwrap();
    assert_eq!(single_bool_cell(empty), "false");

    exec_sql(&executor, "INSERT INTO events VALUES (1)", "streamdb").await.unwrap();
    let has_data = exec_sql(&executor, "SELECT SYSTEM$STREAM_HAS_DATA('events_stream')", "streamdb").await.unwrap();
    assert_eq!(single_bool_cell(has_data), "true");

    exec_sql(&executor, "SELECT * FROM events_stream", "streamdb").await.unwrap();
    let consumed = exec_sql(&executor, "SELECT SYSTEM$STREAM_HAS_DATA('events_stream')", "streamdb").await.unwrap();
    assert_eq!(single_bool_cell(consumed), "false");
}

fn single_bool_cell(result: nova_coordinator::executor::QueryResult) -> String {
    match result {
        nova_coordinator::executor::QueryResult::Rows { rows, .. } => rows[0][0].clone(),
        other => panic!("expected rows, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run failing has-data test**

Run:

```bash
cargo test -p nova-coordinator system_stream_has_data_tracks_backlog -- --nocapture
```

Expected: failure until executor function is implemented.

- [ ] **Step 3: Implement has-data executor**

Add:

```rust
async fn exec_system_stream_has_data(
    &self,
    security: &SecurityContext,
    db: &str,
    schema: &str,
    stream_name: &str,
) -> Result<QueryResult> {
    let stream = self.find_stream(db, schema, stream_name).await?;
    let _ = self.authorize_stream_read(security, db, schema, &stream).await?;
    let has_data = self.meta.stream_has_data(stream.stream_id).await?;
    Ok(QueryResult::Rows {
        columns: vec![format!("SYSTEM$STREAM_HAS_DATA('{}')", stream_name)],
        rows: vec![vec![has_data.to_string()]],
    })
}
```

- [ ] **Step 4: Add RBAC has-data test**

Add:

```rust
#[tokio::test]
async fn system_stream_has_data_requires_stream_and_source_select() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE streamdb", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE events (id INT)", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE STREAM events_stream ON TABLE events", "streamdb").await.unwrap();

    let role_id = create_role(&executor, "stream_has_data_reader").await;
    let analyst = context_for(role_id, "stream_has_data_user");
    let err = exec_sql_as(&executor, "SELECT SYSTEM$STREAM_HAS_DATA('events_stream')", "streamdb", &analyst)
        .await
        .expect_err("missing stream/source select should deny has-data");
    assert!(matches!(err, nova_common::NovaError::PermissionDenied { .. }));
}
```

- [ ] **Step 5: Run has-data tests**

Run:

```bash
cargo test -p nova-coordinator stream_has_data -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs
```

Expected: has-data implementation and tests. Do not commit unless requested.

---

## Task 9: Enterprise RBAC, Concurrency, and Staleness Hardening

**Files:**
- Modify: `crates/nova-storage/src/metadata/fdb_store.rs`
- Modify: `crates/nova-coordinator/src/executor.rs`
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs`

**Interfaces:**
- Consumes stream read/offset methods.
- Produces tests proving unauthorized access is blocked and offset CAS conflicts are surfaced.

- [ ] **Step 1: Add unauthorized stream read tests**

Add:

```rust
#[tokio::test]
async fn stream_select_requires_stream_select_and_source_select() {
    let (executor, _dir) = setup();
    exec_sql(&executor, "CREATE DATABASE streamdb", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE TABLE events (id INT)", "streamdb").await.unwrap();
    exec_sql(&executor, "CREATE STREAM events_stream ON TABLE events", "streamdb").await.unwrap();
    exec_sql(&executor, "INSERT INTO events VALUES (1)", "streamdb").await.unwrap();

    let role_id = create_role(&executor, "stream_reader_limited").await;
    let analyst = context_for(role_id, "stream_reader_limited_user");
    let err = exec_sql_as(&executor, "SELECT * FROM events_stream", "streamdb", &analyst)
        .await
        .expect_err("role with no grants cannot read stream");
    assert!(matches!(err, nova_common::NovaError::PermissionDenied { .. }));
}
```

- [ ] **Step 2: Add authorized stream read test**

Use existing helper `grant` and object lookup helpers. Grant `USAGE` on db/schema, `SELECT` on source table, and `SELECT` on stream object. Then assert the read succeeds.

Code shape:

```rust
// After creating table and stream, find ids through metadata.
grant(&executor, role_id, nova_common::ObjectRef::new(nova_common::ObjectType::Database, db_meta.id), nova_common::SecurityPrivilege::Usage).await;
grant(&executor, role_id, nova_common::ObjectRef::new(nova_common::ObjectType::Schema, schema_meta.id), nova_common::SecurityPrivilege::Usage).await;
grant(&executor, role_id, nova_common::ObjectRef::new(nova_common::ObjectType::Table, table_meta.id), nova_common::SecurityPrivilege::Select).await;
grant(&executor, role_id, nova_common::ObjectRef::new(nova_common::ObjectType::Stream, stream_id), nova_common::SecurityPrivilege::Select).await;
let ok = exec_sql_as(&executor, "SELECT * FROM events_stream", "streamdb", &analyst).await.unwrap();
```

- [ ] **Step 3: Add offset CAS conflict metadata test**

The storage test from Task 3 covers CAS. Add an executor-level test only if there is a public way to create two concurrent consumers. If not, rely on metadata CAS and ensure `exec_read_stream` propagates `StreamConcurrentConsume`.

- [ ] **Step 4: Add stale stream metadata method**

If retention metadata is implemented now, add:

```rust
async fn earliest_change_sequence(&self, table_id: TableId) -> Result<u64>;
```

If retention metadata is not implemented in this pass, make stream reads compare against `0` and document that no stream can become stale until CDC retention GC is enabled. Do not return false empty results for missing payloads.

- [ ] **Step 5: Run RBAC stream tests**

Run:

```bash
cargo test -p nova-coordinator stream_select_requires_stream_select_and_source_select -- --nocapture
cargo test -p nova-coordinator authorized_stream -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Run all coordinator stream tests**

Run:

```bash
cargo test -p nova-coordinator stream -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Review diff checkpoint**

Run:

```bash
git diff -- crates/nova-storage/src/metadata/fdb_store.rs crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs
```

Expected: RBAC, concurrency, and staleness guardrails only. Do not commit unless requested.

---

## Task 10: Documentation Alignment and Final Verification

**Files:**
- Modify: `README.md:353-364`
- Modify: `ROADMAP.md:246-275` only if you are updating completed status after tests pass
- Modify: `docs/design/architecture.md:1380-1400` only if aligning Kafka-like SELECT semantics

**Interfaces:**
- Consumes all implementation tasks.
- Produces user-facing docs that state nova-core stream semantics clearly.

- [ ] **Step 1: Update README stream example**

Replace the existing stream section with:

```markdown
### Streams (CDC)

Nova streams are Kafka-like CDC cursors over tables.

```sql
CREATE STREAM orders_stream ON TABLE orders;

INSERT INTO orders VALUES (2000, 1, 999.99, 'new');

-- Consumes and commits the stream offset by default.
SELECT * FROM orders_stream;

-- Debug/preview without committing the offset.
SELECT * FROM orders_stream WITH (COMMIT = FALSE);

-- Check whether a stream has unconsumed records.
SELECT SYSTEM$STREAM_HAS_DATA('orders_stream');
```

Stream output includes source table columns plus `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`, `METADATA$TXN_ID`, `METADATA$COMMIT_TS`, and `METADATA$SEQUENCE`.
```

Ensure nested fenced code blocks render correctly in the actual file.

- [ ] **Step 2: Run narrow stream test suite**

Run:

```bash
cargo test -p nova-common stream -- --nocapture
cargo test -p nova-storage stream -- --nocapture
cargo test -p nova-storage cdc -- --nocapture
cargo test -p nova-coordinator stream -- --nocapture
```

Expected: all pass. If FoundationDB is unavailable, report exactly which tests could not run and do not weaken tests.

- [ ] **Step 3: Run broader checks**

Run:

```bash
cargo test --all
cargo clippy --all -- -D warnings
cargo fmt --all -- --check
```

Expected: all pass. If external services are missing, report the specific service and command output.

- [ ] **Step 4: Inspect final diff**

Run:

```bash
git diff --stat
git diff -- crates/nova-common/src/types.rs crates/nova-common/src/error.rs crates/nova-storage/src/cdc.rs crates/nova-storage/src/lib.rs crates/nova-storage/src/metadata/mod.rs crates/nova-storage/src/metadata/fdb_store.rs crates/nova-coordinator/src/parser.rs crates/nova-coordinator/src/analyzer.rs crates/nova-coordinator/src/executor.rs crates/nova-coordinator/tests/e2e_tests.rs README.md
```

Expected: only stream feature changes and documentation alignment. Do not commit unless requested.

---

## Plan Self-Review

### Spec coverage

- Kafka-like default SELECT commit: Task 7.
- `WITH (COMMIT = FALSE)`: Tasks 4 and 7.
- Full DML CDC INSERT/UPDATE/DELETE: Task 6.
- Snowflake-style metadata columns: Tasks 6 and 7.
- `SYSTEM$STREAM_HAS_DATA`: Task 8.
- Persisted CDC log with FDB metadata and object payloads: Tasks 2 and 3.
- Offset CAS and concurrency conflict: Tasks 3, 7, and 9.
- RBAC for create/read/has-data/drop/show/describe: Tasks 5, 8, and 9.
- Lifecycle SQL: Tasks 4 and 5.
- Retention/staleness guardrails: Task 9.
- Tests and verification: Tasks 1 through 10.

### Type consistency

- `StreamMeta.source_table_id` is used everywhere instead of old `table_id`.
- `StreamOffset.committed_sequence` is the sole durable consumption cursor.
- `ChangeRecordMeta.sequence` stores the first sequence for the payload range; payload rows carry exact `METADATA$SEQUENCE` values.
- `StreamReadMode::{Commit, Preview}` maps to default SELECT and `WITH (COMMIT = FALSE)`.

### Known implementation sequencing notes

- Task 6 may leave the full DML stream e2e test failing until Task 7 adds stream read. The task is still useful because it forces CDC write-path code to compile and sets up Task 7.
- Existing FDB-dependent tests require local FoundationDB configuration. Do not skip or weaken tests to hide missing services.
- The plan avoids new dependencies and uses existing Arrow/Parquet/object_store crates.
