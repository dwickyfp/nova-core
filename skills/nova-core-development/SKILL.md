# nova-core-development Skill

> **Trigger:** Working on nova-core (Rust analytical query engine).
> Load this skill before implementing any nova-core component.

---

## Skill Overview

This skill provides implementation patterns, architectural rules, and verified workflows for developing nova-core. It is the operational companion to `AGENTS.md` and `docs/design/architecture.md`.

**Before starting:** Read `AGENTS.md` and `ROADMAP.md` in the nova-core repo.

---

## 1. Project Layout

```
nova-core/
├── crates/
│   ├── nova-common/         # Shared types, errors, protobuf
│   ├── nova-storage/        # MP writer/reader, FDB metadata ops
│   ├── nova-coordinator/    # SQL parsing, optimization, scheduling
│   ├── nova-worker/         # DataFusion execution, cache, storage I/O
│   └── nova-cli/            # Binary entry points
├── docs/design/architecture.md
├── AGENTS.md
├── CLAUDE.md
└── ROADMAP.md
```

### Crate Dependency Rules

```
nova-cli       → depends on: coordinator, worker, common
nova-coordinator → depends on: storage, common (NOT worker)
nova-worker      → depends on: storage, common (NOT coordinator)
nova-storage     → depends on: common
nova-common      → depends on: nothing internal
```

**Never create circular dependencies.** If both coordinator and worker need a type, put it in `nova-common`.

---

## 2. Implementation Patterns

### 2.1 Micro-Partition Writer

```rust
// crates/nova-storage/src/mp_writer.rs

use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;

pub struct MpWriter {
    store: Arc<ObjectStore>,
    bucket: String,
    target_size_bytes: usize,  // default: 50MB compressed
    compression: Compression,  // default: Snappy
}

impl MpWriter {
    /// Writes Arrow RecordBatches as an immutable Parquet micro-partition to S3.
    ///
    /// Steps:
    /// 1. Write to temp S3 path (s3://bucket/tmp/{txn_id}/mp-{id}.parquet)
    /// 2. Compute column statistics
    /// 3. On commit: rename to permanent path
    pub async fn write(
        &self,
        table_id: u64,
        mp_id: u64,
        version: u64,
        batches: Vec<RecordBatch>,
        txn_id: u64,
    ) -> Result<MicroPartitionMeta> {
        let temp_path = format!("tmp/{}/mp-{}.parquet", txn_id, mp_id);
        let perm_path = format!("tables/{}/mp-{}-v{}.parquet", table_id, mp_id, version);

        // 1. Write Parquet to temp path
        let stats = self.write_parquet(&temp_path, &batches).await?;

        // 2. Return metadata (caller commits to FDB + renames S3 file)
        Ok(MicroPartitionMeta {
            mp_id,
            table_id,
            version,
            s3_path: perm_path,
            s3_temp_path: Some(temp_path),
            row_count: stats.row_count,
            byte_size: stats.byte_size,
            column_stats: stats.column_stats,
            commit_ts: 0, // set by transaction manager on commit
            txn_id,
            supersedes: None,
            superseded_by: None,
            active: false, // set to true on commit
        })
    }

    async fn write_parquet(
        &self,
        path: &str,
        batches: &[RecordBatch],
    ) -> Result<ParquetWriteStats> {
        // Use parquet crate's ArrowWriter
        // Collect column-level min/max/null_count/distinct_count
        // Return stats for FDB metadata
        todo!()
    }
}
```

### 2.2 Micro-Partition Reader

```rust
// crates/nova-storage/src/mp_reader.rs

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;

pub struct MpReader {
    store: Arc<ObjectStore>,
    cache: Arc<NovaCache>,
    batch_size: usize,  // default: 8192
}

impl MpReader {
    /// Reads a micro-partition as a stream of Arrow RecordBatches.
    ///
    /// Checks Foyer cache (L3) first. On miss, reads from S3.
    /// Applies column pruning and predicate pushdown.
    pub async fn read(
        &self,
        mp: &MicroPartitionMeta,
        projection: Option<&[usize]>,
        predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream> {
        let cache_key = format!("{}:{}", mp.mp_id, mp.version);

        // 1. Check cache
        if let Some(cached) = self.cache.mp_cache.get(&cache_key).await {
            return Ok(stream_from_cached(cached));
        }

        // 2. Read from S3 with pruning
        let reader = ParquetReader::builder(&mp.s3_path)
            .with_columns(projection)      // column pruning
            .with_predicate(predicate)     // row group pruning
            .with_batch_size(self.batch_size)
            .build();

        // 3. Stream + cache on the fly
        let stream = reader.stream().map_ok(|batch| {
            // Cache hot data (best-effort, non-blocking)
            let cache = self.cache.clone();
            let key = cache_key.clone();
            tokio::spawn(async move {
                cache.mp_cache.insert(key, batch.clone()).await;
            });
            batch
        });

        Ok(Box::pin(stream))
    }
}
```

### 2.3 FoundationDB Metadata Operations

```rust
// crates/nova-storage/src/metadata/fdb.rs

use foundationdb::Database;

pub struct MetadataStore {
    db: Database,
}

impl MetadataStore {
    // --- Table Operations ---

    pub async fn create_table(&self, table: TableMeta) -> Result<()> {
        let key = format!("/catalog/{}/{}/{}", table.db_id, table.schema_id, table.id);
        let val = bincode::serialize(&table)?;
        self.db.transact(|tx| {
            tx.set(key.as_bytes(), &val);
            futures::future::ok(())
        }).await?;
        Ok(())
    }

    pub async fn get_active_mps(&self, table_id: u64) -> Result<Vec<MicroPartitionMeta>> {
        let prefix = format!("/table/{}/mp/", table_id);
        self.scan_range(&prefix, |mp| mp.active).await
    }

    pub async fn get_mps_at_timestamp(
        &self,
        table_id: u64,
        ts: Timestamp,
    ) -> Result<Vec<MicroPartitionMeta>> {
        let prefix = format!("/table/{}/mp/", table_id);
        self.scan_range(&prefix, |mp| {
            mp.commit_ts <= ts && match mp.superseded_by {
                None => true,
                Some(next_id) => {
                    // Need to check next MP's commit_ts
                    // This requires a secondary lookup
                    true // simplified — see architecture.md for full logic
                }
            }
        }).await
    }

    // --- Transaction Operations ---

    pub async fn commit_transaction(&self, txn: Transaction) -> Result<()> {
        self.db.transact(|tx| {
            // 1. Verify no conflicts
            // 2. Insert new MP metadata
            // 3. Update superseded_by on old MPs
            // 4. Update active MP lists
            // 5. Increment table versions
            // 6. Record transaction
            futures::future::ok(())
        }).await?;
        Ok(())
    }

    async fn scan_range<F>(&self, prefix: &str, filter: F) -> Result<Vec<MicroPartitionMeta>>
    where
        F: Fn(&MicroPartitionMeta) -> bool,
    {
        let range = foundationdb::RangeOption {
            begin: prefix.as_bytes().into(),
            end: prefix.as_bytes().increment().into(),
            ..Default::default()
        };

        let items = self.db.snapshot().get_range(&range).await?;
        let mut results = Vec::new();
        for kv in items {
            let mp: MicroPartitionMeta = bincode::deserialize(kv.value())?;
            if filter(&mp) {
                results.push(mp);
            }
        }
        Ok(results)
    }
}
```

### 2.4 DataFusion Custom Operator

```rust
// crates/nova-worker/src/operators/mp_scan.rs

use datafusion::physical_plan::{
    ExecutionPlan, PhysicalExpr, SendableRecordBatchStream,
    DisplayFormatType, PlanProperties, Partitioning,
};
use arrow::datatypes::SchemaRef;

pub struct MicroPartitionScanExec {
    mp_list: Vec<MicroPartitionMeta>,
    schema: SchemaRef,
    projection: Vec<usize>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    cache: Arc<NovaCache>,
    properties: PlanProperties,
}

impl MicroPartitionScanExec {
    pub fn new(
        mp_list: Vec<MicroPartitionMeta>,
        schema: SchemaRef,
        projection: Vec<usize>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Arc<NovaCache>,
    ) -> Self {
        let properties = PlanProperties::new(
            // Equal partitioning: 1 MP per partition
            Partitioning::UnknownPartitioning(mp_list.len()),
            // Physical sort order: none
            datafusion::physical_plan::SortOrder::default(),
        );
        Self { mp_list, schema, projection, predicate, cache, properties }
    }
}

impl ExecutionPlan for MicroPartitionScanExec {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn schema(&self) -> SchemaRef { self.schema.clone() }

    fn properties(&self) -> &PlanProperties { &self.properties }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal("scan has no children".into()))
    }

    fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let mp = &self.mp_list[partition];
        let reader = MpReader::new(self.cache.clone());
        let stream = reader.read(mp, Some(&self.projection), /* predicate */ None);
        // Return the push-based stream
        stream.map_err(Into::into)
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MicroPartitionScan: table={}, mps={}", 
            self.mp_list.first().map(|m| m.table_id).unwrap_or(0),
            self.mp_list.len())
    }
}
```

### 2.5 Foyer Cache Integration

```rust
// crates/nova-worker/src/cache.rs

use foyer::{HybridCache, HybridCacheBuilder, FsDeviceBuilder, BlockEngineConfig};

pub struct NovaCache {
    pub mp_cache: HybridCache<String, ArrowRecordBatch>,
    pub meta_cache: HybridCache<String, Vec<MicroPartitionMeta>>,
    pub result_cache: HybridCache<u64, QueryResult>,
}

impl NovaCache {
    pub async fn new(ssd_path: &str) -> Self {
        // L3: MP data cache (4GB RAM + 100GB SSD)
        let mp_device = FsDeviceBuilder::new(format!("{ssd_path}/mps"))
            .with_capacity(100 * 1024 * 1024 * 1024)
            .build();
        let mp_cache = HybridCacheBuilder::new()
            .memory(4 * 1024 * 1024 * 1024)
            .storage()
            .with_engine_config(BlockEngineConfig::new(mp_device))
            .build()
            .await;

        // L2: Metadata cache (512MB RAM)
        let meta_device = FsDeviceBuilder::new(format!("{ssd_path}/meta"))
            .with_capacity(2 * 1024 * 1024 * 1024)
            .build();
        let meta_cache = HybridCacheBuilder::new()
            .memory(512 * 1024 * 1024)
            .storage()
            .with_engine_config(BlockEngineConfig::new(meta_device))
            .build()
            .await;

        // L1: Query result cache (2GB RAM + 50GB SSD)
        let result_device = FsDeviceBuilder::new(format!("{ssd_path}/results"))
            .with_capacity(50 * 1024 * 1024 * 1024)
            .build();
        let result_cache = HybridCacheBuilder::new()
            .memory(2 * 1024 * 1024 *  project)
            .storage()
            .with_engine_config(BlockEngineConfig::new(result_device))
            .build()
            .await;

        Self { mp_cache, meta_cache, result_cache }
    }
}
```

### 2.6 Query Result Cache (Snowflake-style)

```rust
// crates/nova-coordinator/src/cache/result_cache.rs

pub struct QueryResultCache {
    cache: HybridCache<u64, QueryResult>,
}

struct QueryCacheKey {
    sql_hash: u64,
    table_versions: Vec<(u64, u64)>,  // (table_id, version)
}

impl QueryResultCache {
    pub async fn try_get(&self, sql: &str, tables: &[u64]) -> Option<QueryResult> {
        let normalized = normalize_sql(sql);
        if !is_cacheable(&normalized) { return None; }

        let sql_hash = hash(&normalized);
        let current_versions = fdb_batch_get_table_versions(tables).await?;
        let key = hash_key(sql_hash, &current_versions);

        if let Some(cached) = self.cache.get(&key).await {
            if cached.table_versions == current_versions {
                return Some(cached);
            }
        }
        None
    }

    pub async fn put(&self, sql: &str, tables: &[(u64, u64)], result: QueryResult) {
        let normalized = normalize_sql(sql);
        let sql_hash = hash(&normalized);
        let key = hash_key(sql_hash, tables);
        self.cache.insert(key, result).await;
    }
}

fn normalize_sql(sql: &str) -> String {
    sql.to_lowercase().split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_cacheable(sql: &str) -> bool {
    let non_deterministic = ["current_timestamp", "current_date", "now()", "rand(", "uuid("];
    !non_deterministic.iter().any(|f| sql.contains(f))
}
```

---

## 3. Testing Patterns

### 3.1 Unit Test Template

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mp_writer_creates_valid_parquet() {
        // Arrange: create test data
        let batch = create_test_batch(1000);
        let writer = MpWriter::new(test_store());

        // Act: write MP
        let meta = writer.write(1, 1, 1, vec![batch], 1).await.unwrap();

        // Assert: verify metadata
        assert_eq!(meta.row_count, 1000);
        assert!(meta.byte_size > 0);
        assert_eq!(meta.column_stats.len(), 5); // 5 columns
        assert!(meta.s3_path.starts_with("s3://"));
    }

    #[tokio::test]
    async fn test_fdb_metadata_crud() {
        let store = MetadataStore::new(test_fdb()).await;

        // Create
        let table = TableMeta::new("orders", 1, 1, 1);
        store.create_table(table.clone()).await.unwrap();

        // Read
        let got = store.get_table(1, 1, 1).await.unwrap();
        assert_eq!(got.name, "orders");

        // Delete
        store.drop_table(1, 1, 1).await.unwrap();
        assert!(store.get_table(1, 1, 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_mp_pruning_with_range_predicate() {
        // Create 10 MPs, only 2 match predicate dt > '2026-06-15'
        let mps = create_test_mps(10, dt_range("2026-06-01", "2026-06-30"));
        let predicate = Expr::gt(col("dt"), lit("2026-06-15"));

        let pruned = MPPruningRule::apply(&mps, &predicate);

        assert_eq!(pruned.len(), 2); // only 2 MPs should remain
    }
}
```

### 3.2 Integration Test Template

```rust
// tests/end_to_end.rs

#[tokio::test]
async fn test_create_insert_select_cycle() {
    // 1. Start local cluster (FDB + MinIO)
    let cluster = TestCluster::start().await;

    // 2. Connect via MySQL protocol
    let mut conn = mysql_async::connect(cluster.mysql_addr()).await.unwrap();

    // 3. CREATE TABLE
    conn.exec("CREATE TABLE orders (id INT, amount DECIMAL(10,2), dt DATE)").await.unwrap();

    // 4. INSERT
    conn.exec("INSERT INTO orders VALUES (1, 500.00, '2026-06-23')").await.unwrap();

    // 5. SELECT
    let rows = conn.exec("SELECT * FROM orders WHERE id = 1").await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32>(0), 1);
}
```

### 3.3 Benchmark Template

```rust
// benches/mp_writer.rs

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_mp_write(c: &mut Criterion) {
    c.bench_function("write_10k_rows", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let batch = create_test_batch(10_000);
                let writer = MpWriter::new(test_store());
                writer.write(1, 1, 1, vec![batch], 1).await.unwrap();
            });
    });
}

criterion_group!(benches, bench_mp_write);
criterion_main!(benches);
```

---

## 4. Pitfalls and Solutions

### Pitfall 1: Modifying Parquet in-place

```rust
// ❌ WRONG
async fn update_mp(mp_id: u64, changes: Vec<Change>) -> Result<()> {
    let path = get_s3_path(mp_id);
    parquet_writer::modify_in_place(&path, changes).await?;
}

// ✅ CORRECT
async fn update_mp(old_mp: &MicroPartitionMeta, changes: Vec<Change>) -> Result<MicroPartitionMeta> {
    let old_data = read_parquet(&old_mp.s3_path).await?;
    let new_data = apply_changes(old_data, changes);
    let new_mp = write_new_mp(old_mp.table_id, new_data).await?;
    // Old MP retained for Time Travel, new MP has new version
    Ok(new_mp)
}
```

### Pitfall 2: Forgetting MVCC timestamps

```rust
// ❌ WRONG — no commit_ts, breaks Time Travel
let mp = MicroPartitionMeta {
    s3_path: path,
    row_count: 1000,
    // commit_ts missing!
};

// ✅ CORRECT — commit_ts set by transaction manager
let mp = MicroPartitionMeta {
    s3_path: path,
    row_count: 1000,
    commit_ts: txn.commit_ts,  // MUST be set
    txn_id: txn.id,
    supersedes: Some(old_mp_id),
    superseded_by: None,
    active: true,
};
```

### Pitfall 3: Treating FDB as SQL

```rust
// ❌ WRONG — FDB is a KV store
let mps = fdb_query("SELECT * FROM mps WHERE active = true").await?;

// ✅ CORRECT — use range scan with prefix + in-memory filter
let prefix = format!("/table/{}/mp/", table_id);
let all_mps = fdb_scan_range(&prefix).await?;
let active_mps: Vec<_> = all_mps.into_iter().filter(|m| m.active).collect();
```

### Pitfall 1: Modifying Parquet in-place

```rust
// ❌ WRONG
async fn update_mp(mp_id: u64, patterns: Vec<Change>) -> Result<()> {
    let path = get_s3_path(mp_id);
    parquet_writer::modify_in_place(&path, patterns).await?;
}

// ✅ CORRECT
async fn update_mp(old_mp: &MicroPartitionMeta, patterns: Vec<Change>) -> Result<MicroPartitionMeta> {
    let old_data = read_parquet(&old_mp.s3_path).await?;
    let new_data = apply_changes(old_data, patterns);
    let new_mp = write_new_mp(old_mp.table_id, new_data).await?;
    Ok(new_mp)
}
```

### Pitfall 4: Using pull-based execution

```rust
// ❌ WRONG — Volcano pull model
impl ExecutionPlan for MyOp {
    fn next(&mut self) -> Option<RecordBatch> {
        self.child.next()?; // pull
    }
}

// ✅ CORRECT — push-based stream
impl ExecutionPlan for MyOp {
    fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let input = self.children[0].execute(partition)?;
        Box::pin(input.map(|batch| transform(batch)))
    }
}
```

### Pitfall 5: Storing data on workers

```rust
// ❌ WRONG — worker stores data
struct Worker {
    local_table_data: HashMap<TableId, Vec<RecordBatch>>, // ❌
}

// ✅ CORRECT — worker is stateless, reads from S3/FDB
struct Worker {
    cache: Arc<NovaCache>,  // cache only, not source of truth
    storage: Arc<StorageAdapter>,  // reads from S3
}
```

### Pitfall 6: Manual cache invalidation

```rust
// ❌ WRONG — manual invalidation is error-prone
fn on_data_change(table_id: u64) {
    invalidate_all_queries_referencing(table_id); // ❌ complex, miss-prone
}

// ✅ CORRECT — MVCC version is the invalidation key
// Cache key includes table version. When data changes, version increments,
// new queries get new cache key, old cache entries naturally expire.
fn get_cache_key(sql: &str, tables: &[(u64, u64)]) -> u64 {
    hash(normalize_sql(sql), tables)  // tables includes versions
}
```

### Pitfall 7: Implementing compaction

```rust
// ❌ WRONG — compaction is for mutable storage (StarRocks)
async fn compact_table(table_id: u64) {
    let rowsets = get_all_rowsets(table_id).await;
    let merged = merge_rowsets(rowsets).await; // ❌ CPU-intensive
    write_merged(merged).await;
}

// ✅ CORRECT — GC is delete-only, optional MP merge for efficiency
async fn gc_expired_mps(retention: Duration) {
    let expired = get_expired_mps(now() - retention).await;
    for mp in expired {
        s3_delete(&mp.s3_path).await;  // just delete
        fdb_delete(mp.key()).await;
    }
}
```

### Pitfall 8: Skipping column stats computation

```rust
// ❌ WRONG — no stats means no pruning
async fn write_mp(batches: Vec<RecordBatch>) -> MicroPartitionMeta {
    parquet_writer::write(path, &batches).await;
    MicroPartitionMeta { s3_path: path, column_stats: HashMap::new(), .. } // ❌ empty stats
}

// ✅ CORRECT — always compute and store stats
async fn write_mp(batches: Vec<RecordBatch>) -> MicroPartitionMeta {
    let stats = compute_column_stats(&batches);  // min, max, null_count, distinct
    parquet_writer::write_with_stats(path, &batches, &stats).await;
    MicroPartitionMeta { s3_path: path, column_stats: stats, .. }
}
```

---

## 5. Common Commands

```bash
# Build
cargo build --release

# Test
cargo test --all
cargo nextest run --all

# Specific crate
cargo test -p nova-storage
cargo test -p nova-coordinator

# Lint
cargo clippy --all -- -D warnings

# Format
cargo fmt --all

# Run coordinator
./target/release/nova-cli server --config config.toml

# Run worker
./target/release/nova-cli worker --config config.toml

# Local dev cluster
docker compose -f docker/docker-compose.yml up -d
```

---

## 5. Skill Maintenance

This skill should be updated when:
- New patterns are discovered (e.g., new DataFusion operator pattern)
- Pitfalls are encountered during implementation
- The architecture document is updated
- New phases start (add phase-specific patterns)
- Dependencies change (add migration patterns)

Update path: `skills/nova-core-development/SKILL.md` in nova-core repo.
