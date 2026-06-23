# Nova Engine — Complete Architecture & Design Document

> **Version:** 0.1.0 (Design Phase)
> **Date:** June 23, 2026
> **Author:** Dwicky Feriansyah Putra
> **Status:** Research & Planning

---

## Table of Contents

1. [Vision & Goals](#1-vision--goals)
2. [Core Design Principles](#2-core-design-principles)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Core Components Detail](#4-core-components-detail)
5. [Storage Layer — Immutable Micro-Partitions](#5-storage-layer--immutable-micro-partitions)
6. [Metadata Layer — FoundationDB](#6-metadata-layer--foundationdb)
7. [Coordinator Layer — Query Planning & Optimization](#7-coordinator-layer--query-planning--optimization)
8. [Worker Layer — Execution Engine](#8-worker-layer--execution-engine)
9. [Cache Architecture — Foyer Hybrid Cache](#9-cache-architecture--foyer-hybrid-cache)
10. [Performance Engineering](#10-performance-engineering)
11. [Transaction & MVCC Model](#11-transaction--mvcc-model)
12. [Snowflake Parity Features](#12-snowflake-parity-features)
13. [Scalability Model](#13-scalability-model)
14. [High Availability](#14-high-availability)
15. [Complete Tech Stack](#15-complete-tech-stack)
16. [Development Roadmap](#16-development-roadmap)

---

## 1. Vision & Goals

### Vision

Nova Engine adalah **Rust-native, cloud-native analytical query engine** yang terinspirasi oleh arsitektur Snowflake, dibangun di atas ekosistem Apache Arrow + DataFusion, dengan tujuan:

1. **Snowflake-grade features** — Time Travel, Zero-Copy Clone, Streams (CDC)
2. **Beat StarRocks performance** — faster single-node query, no JVM overhead
3. **Rust memory safety** — no undefined behavior, no GC pauses
4. **Open source & self-hosted** — tidak lock-in ke cloud vendor

### Goals

| Goal | Metric | Target |
|---|---|---|
| Query speed (scan + filter) | vs PostgreSQL | 50-100x faster |
| Query speed (aggregation) | vs PostgreSQL | 30-150x faster |
| Query speed (JOIN) | vs StarRocks | Match atau beat |
| Repeated query (cache hit) | vs first execution | 600x faster (instant) |
| Time Travel retention | configurable | up to 90 days |
| Zero-copy clone time | any table size | < 1 second |
| Stream CDC latency | INSERT detection | < 1 second |
| Worker auto-scale | spin up new worker | < 60 seconds |
| Memory safety | undefined behavior | Zero (Rust guarantee) |

### Non-Goals

- Bukan OLTP engine (tidak replace PostgreSQL untuk transactional workloads)
- Bukan row-store (pure columnar OLAP)
- Bukan streaming engine (bukan Flink/Kafka replacement)
- Tidak rewrite StarRocks (bangun dari foundation berbeda)

---

## 2. Core Design Principles

### Principle 1: Immutable Storage (Snowflake-inspired)

> Semua data disimpan sebagai **immutable micro-partitions** (Parquet files di S3).
> UPDATE/DELETE = buat micro-partition baru, bukan modify yang lama.
> Data lama tetap ada untuk Time Travel, lalu di-GC setelah retention period.

**Konsekuensi:**
- ✅ Time Travel natural (baca version lama)
- ✅ Zero-Copy Clone natural (share S3 files via metadata)
- ✅ Streams natural (diff versions)
- ✅ No compaction overhead (hanya GC = delete)
- ❌ Write amplification (UPDATE 1 row = rewrite 1 MP)

### Principle 2: Vectorized Push-Based Execution (MonetDB/X100-inspired)

> Eksekusi query dalam **batches (vectors)** kolom, bukan row-by-row.
> Data **didorong** (push) dari source ke sink, bukan ditarik (pull).

**Konsekuensi:**
- ✅ SIMD auto-vectorization (16-32 values per instruction)
- ✅ Cache-friendly (batch fits in L1 cache)
- ✅ Low interpretation overhead (1 call per 8192 rows)
- ✅ Natural backpressure dan pipeline parallelism

### Principle 3: Separation of Compute & Storage (Snowflake-inspired)

> Workers **stateless** — tidak menyimpan data, semua di S3.
> Workers bisa spin up/down dalam detik tanpa data loss.

**Konsekuensi:**
- ✅ Elastic scaling (add/remove workers anytime)
- ✅ Auto-suspend/resume (no cost when idle)
- ✅ No data rebalancing saat scale
- ✅ Fault tolerance (worker crash = re-schedule, no data loss)

### Principle 4: Metadata-Driven Pruning (Snowflake/StarRocks-inspired)

> Sebelum baca Parquet file, cek **metadata stats** (min/max/null_count).
> Skip micro-partitions yang tidak mungkin match query predicate.

**Konsekuensi:**
- ✅ 10-100x faster untuk queries dengan range/date predicates
- ✅ S3 reads minimized (skip irrelevant MPs)
- ✅ Cost reduction (fewer S3 API calls)

### Principle 5: Multi-Layer Caching (Snowflake-inspired)

> 3 layer cache: Query Result → Metadata → Micro-Partition Data.
> Auto-invalidate berdasarkan MVCC version tracking.

**Konsekuensi:**
- ✅ Repeated queries = instant (0-10ms)
- ✅ Hot data served from RAM/SSD, cold data from S3
- ✅ Auto-invalidation (no stale results)

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                              │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ MySQL Protocol│  │ REST API     │  │ Nova UI (FastAPI)      │ │
│  │ (mysql_wire)  │  │ (axum)       │  │ (existing Nova backend)│ │
│  │ Port 4406     │  │ Port 8080    │  │ Port 8000              │ │
│  └──────┬───────┘  └──────┬───────┘  └───────────┬────────────┘ │
│         │                 │                       │               │
└─────────┼─────────────────┼───────────────────────┼──────────────┘
          │                 │                       │
┌─────────▼─────────────────▼───────────────────────▼──────────────┐
│                    COORDINATOR LAYER                              │
│                   (Nova Coordinator — Raft)                       │
│                                                                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────────┐│
│  │ SQL       │ │ Query    │ │ Metadata │ │ Transaction Manager  ││
│  │ Parser +  │ │ Optimizer│ │ Manager  │ │ (MVCC + Snapshot Isol)││
│  │ Analyzer  │ │ (CBO)    │ │          │ │                      ││
│  └──────────┘ └──────────┘ └──────────┘ └──────────────────────┘│
│                                                                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────────┐│
│  │ Auth &   │ │ Schedule │ │ Stream   │ │ Cache Manager         ││
│  │ RBAC     │ │ Manager  │ │ Manager  │ │ (Query Result Cache)  ││
│  └──────────┘ └──────────┘ └──────────┘ └──────────────────────┘│
│                                                                   │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │ Coordinator Raft Consensus (openraft)                        ││
│  │ Leader handles queries, Followers replicate state            ││
│  └──────────────────────────────────────────────────────────────┘│
└────────────────────────────┬──────────────────────────────────────┘
                             │ gRPC (tonic)
┌────────────────────────────▼──────────────────────────────────────┐
│                       WORKER LAYER                                 │
│                   (Nova Workers — Stateless, Auto-scale)           │
│                                                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐ │
│  │ Execution Engine  │  │ Cache (Foyer)    │  │ Storage Adapter │ │
│  │                   │  │                  │  │                  │ │
│  │ DataFusion core   │  │ L1: Result Cache │  │ Parquet R/W      │ │
│  │ + custom operators│  │   (2GB RAM +     │  │ (parquet crate)  │ │
│  │ + UDF (PyO3)     │  │    50GB SSD)     │  │                  │ │
│  │                   │  │ L3: MP Cache     │  │ S3/MinIO/GCS    │ │
│  │ Vectorized        │  │   (4GB RAM +     │  │ (object_store)   │ │
│  │ Push-based        │  │    100GB SSD)    │  │                  │ │
│  │ batch=8192        │  │                  │  │ Column pruning   │ │
│  │                   │  │ L2: Metadata     │  │ Predicate pushdn │ │
│  │ Operators:        │  │   (512MB RAM)    │  │ Late materializ. │ │
│  │  Scan, Filter,    │  │                  │  │                  │ │
│  │  Project, Join,   │  └──────────────────┘  └─────────────────┘ │
│  │  Aggregate, Sort, │                                               │
│  │  Window, Union    │  ┌──────────────────┐                       │
│  │                   │  │ Memory Manager   │                       │
│  │ Custom:           │  │                  │                       │
│  │  MPScanExec       │  │ Per-query limit  │                       │
│  │  ChangeDiffExec   │  │ Spill to SSD     │                       │
│  │  CloneWriteExec   │  │ OOM → fail query │                       │
│  └──────────────────┘  └──────────────────┘                       │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │ Worker Pool Manager                                           ││
│  │ Auto-scale: CPU > 70% → add worker, CPU < 20% → remove       ││
│  │ Auto-suspend: idle 5 min → terminate (no cost)               ││
│  │ Auto-resume: query arrives → provision new worker             ││
│  └──────────────────────────────────────────────────────────────┘│
└────────────────────────────┬──────────────────────────────────────┘
                             │
┌────────────────────────────▼──────────────────────────────────────┐
│                      STORAGE LAYER                                 │
│                                                                   │
│  ┌─────────────────────┐  ┌─────────────────────────────────────┐│
│  │ Metadata Store       │  │ Object Storage                      ││
│  │ (FoundationDB)       │  │ (S3 / MinIO / GCS / Azure Blob)    ││
│  │                      │  │                                     ││
│  │ Catalog: databases,  │  │ Immutable micro-partitions          ││
│  │  schemas, tables,    │  │ (Parquet files, never modified)     ││
│  │  columns             │  │                                     ││
│  │                      │  │ Version chains:                     ││
│  │ Versioning: MP meta, │  │  MP-001-v1.parquet (superseded)     ││
│  │  version chains,     │  │  MP-001-v2.parquet (active)         ││
│  │  commit timestamps   │  │                                     ││
│  │                      │  │ Copy-on-write:                      ││
│  │ Transactions: txn    │  │  UPDATE → new MP (old retained)     ││
│  │  log, commit TS      │  │  DELETE → new MP (old retained)     ││
│  │                      │  │                                     ││
│  │ RBAC: users, roles,  │  │ Format: Apache Parquet              ││
│  │  grants              │  │ Size: 16-64MB compressed per MP     ││
│  │                      │  │ Compression: Snappy/ZSTD             ││
│  │ Streams: offsets,    │  │                                     ││
│  │  stream→table mapping│  │ GC: expired MPs deleted from S3     ││
│  │                      │  │ after retention period              ││
│  │ Clones: clone→source │  │                                     ││
│  │  mapping             │  │ MP Merging: optional, merge small    ││
│  │                      │  │ MPs → large MP for scan efficiency  ││
│  │ Warehouses: virtual  │  │                                     ││
│  │  compute configs     │  │                                     ││
│  └─────────────────────┘  └─────────────────────────────────────┘│
└───────────────────────────────────────────────────────────────────┘
```

---

## 4. Core Components Detail

### Component Overview

| Component | Responsibility | Tech | Language |
|---|---|---|---|
| **SQL Parser** | Parse SQL → AST | sqlparser-rs | Rust |
| **Analyzer** | Resolve names, check types, RBAC | Custom + DataFusion | Rust |
| **Optimizer (CBO)** | Plan optimization, join reorder, pruning | DataFusion optimizer + custom rules | Rust |
| **Metadata Manager** | Catalog, table/MP metadata, versions | FoundationDB (foundationdb-rs) | Rust |
| **Transaction Manager** | MVCC, snapshot isolation, commit | Custom (on top of FDB transactions) | Rust |
| **Scheduler** | Dispatch query fragments to workers | tonic (gRPC) | Rust |
| **Execution Engine** | Vectorized push-based query execution | DataFusion + custom operators | Rust |
| **Storage Adapter** | Parquet R/W, S3 access, pruning | parquet + object_store crates | Rust |
| **Cache Manager** | Query result cache, MP cache, metadata cache | Foyer (hybrid cache) | Rust |
| **Stream Manager** | CDC stream creation, offset tracking, change diff | Custom | Rust |
| **Auth & RBAC** | Users, roles, grants, password hashing | argon2 + custom | Rust |
| **Raft Consensus** | Coordinator HA, leader election | openraft | Rust |
| **MySQL Protocol** | MySQL wire protocol server | mysql_wire crate | Rust |
| **REST API** | HTTP API for Nova UI | axum | Rust |
| **Python UDF** | In-process Python UDF execution | PyO3 | Rust + Python |
| **Background GC** | Expired MP cleanup, optional MP merge | Custom (tokio background task) | Rust |
| **Auto-Scaler** | Worker pool management, auto-suspend/resume | Custom + Docker/K8s API | Rust |

---

## 5. Storage Layer — Immutable Micro-Partitions

### 5.1 Micro-Partition Structure

```
┌────────────────────────────────────────────────────────────────┐
│  Micro-Partition (MP) = 1 Parquet file di S3                   │
│                                                                │
│  Size: 16-64MB compressed (target: ~50MB)                     │
│  Format: Apache Parquet (PAX layout, columnar)                │
│  Compression: Snappy (default) atau ZSTD (higher ratio)       │
│                                                                │
│  Structure:                                                    │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ Parquet File                                              │ │
│  │  ├── Row Group 1 (~128K rows)                            │ │
│  │  │   ├── Column Chunk: id (INT64, dictionary encoded)    │ │
│  │  │   ├── Column Chunk: customer_id (INT64)               │ │
│  │  │   ├── Column Chunk: amount (DOUBLE)                   │ │
│  │  │   ├── Column Chunk: status (STRING, dict encoded)     │ │
│  │  │   └── Column Chunk: dt (DATE)                         │ │
│  │  ├── Row Group 2                                         │ │
│  │  ├── ...                                                 │ │
│  │  └── Footer (metadata: schema, row groups, column stats) │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                │
│  S3 Path: s3://nova-bucket/tables/{table_id}/mp-{id}-v{ver}.parquet │
│  Immutable: NEVER modified after write                         │
└────────────────────────────────────────────────────────────────┘
```

### 5.2 Column Statistics (untuk Pruning)

Setiap MP menyimpan column-level statistics di Parquet footer dan di FDB metadata:

```rust
struct ColumnStats {
    min_value: ScalarValue,       // minimum value in this MP
    max_value: ScalarValue,       // maximum value in this MP
    null_count: u64,              // number of NULLs
    distinct_count: u64,          // number of distinct values
    byte_size: u64,               // uncompressed byte size
}

struct MicroPartitionMeta {
    // Identity
    mp_id: u64,                   // globally unique ID
    table_id: u64,
    partition_id: Option<u64>,    // logical partition (if partitioned table)
    version: u64,                 // version number (1, 2, 3, ...)

    // Storage
    s3_path: String,              // immutable S3 path
    row_count: u64,
    byte_size: u64,               // compressed size
    compression: Compression,     // Snappy | ZSTD

    // Column stats (untuk pruning)
    column_stats: HashMap<ColumnId, ColumnStats>,

    // Versioning & MVCC
    created_at: Timestamp,        // when MP was written
    commit_ts: Timestamp,         // when transaction committed
    txn_id: u64,                  // transaction that created this MP

    // Version chain
    supersedes: Option<u64>,      // previous version MP ID (None = new insert)
    superseded_by: Option<u64>,   // next version MP ID (None = active)
    active: bool,                 // true = visible to current queries

    // GC
    gc_eligible: bool,            // true = past retention, can be deleted
}
```

### 5.3 DML Operations (Copy-on-Write)

```
INSERT INTO orders VALUES (1, 'cust-001', 500, 'pending', '2026-06-23')
│
├── 1. Write rows to new Parquet file (temp S3 path)
├── 2. Compute column stats
├── 3. On COMMIT:
│   ├── Move temp file → permanent S3 path (mp-004-v1.parquet)
│   ├── Insert MP metadata into FDB
│   └── Add MP-004 to table's active MP list

UPDATE orders SET status = 'shipped' WHERE id = 5
│
├── 1. Find MP containing id=5 (via metadata scan)
├── 2. Read that MP (Parquet read)
├── 3. Modify matching rows → write NEW Parquet file (mp-001-v2.parquet)
│   └── 49,999 unchanged rows + 1 updated row = 50,000 rows
├── 4. On COMMIT:
│   ├── Insert MP-001-v2 metadata (supersedes MP-001-v1)
│   ├── Mark MP-001-v1 as superseded_by=MP-001-v2
│   └── Update table's active MP list: replace v1 with v2

DELETE FROM orders WHERE id = 100
│
├── 1. Find MP containing id=100
├── 2. Read that MP, remove matching row → write new Parquet (mp-001-v3.parquet)
│   └── 49,999 rows (1 deleted)
├── 3. On COMMIT:
│   ├── Insert MP-001-v3 metadata (supersedes MP-001-v2)
│   ├── Mark MP-001-v2 as superseded_by=MP-001-v3
│   └── Update table's active MP list: replace v2 with v3
```

### 5.4 Garbage Collection (Bukan Compaction)

```rust
struct BackgroundGC {
    retention_period: Duration,  // configurable: 1-90 days

    async fn run_gc_cycle(&self) {
        let cutoff = now() - self.retention_period;

        // Find MPs that are:
        // 1. Superseded (has superseded_by)
        // 2. Past retention period (commit_ts < cutoff)
        let expired = fdb_scan_expired_mps(cutoff).await;

        for mp in expired {
            // 1. Delete S3 file (async, batch for efficiency)
            s3_delete_object(&mp.s3_path).await;

            // 2. Delete FDB metadata entry
            fdb_delete(mp.key()).await;
        }
    }

    // Optional: merge small MPs for scan efficiency
    async fn merge_small_mps(&self, table_id: u64) {
        let threshold = 8 * MB;  // MPs smaller than 8MB are candidates
        let small_mps = get_mps_smaller_than(table_id, threshold).await;

        if small_mps.len() < 10 { return; }  // not worth merging

        // Read all small MPs → combine into single RecordBatch
        let combined = read_and_merge_parquet(&small_mps).await;

        // Write as single new MP
        let new_mp = write_parquet_to_s3(combined).await;

        // Atomically replace old MPs with new merged MP
        fdb_transaction(|tx| {
            tx.insert(new_mp);
            for old in &small_mps {
                tx.mark_superseded(old.mp_id, new_mp.mp_id);
            }
        }).await;

        // Old MPs will be GC'd after retention period
    }
}
```

**Kenapa ini bukan compaction seperti StarRocks:**

| Aspect | StarRocks Compaction | Nova Engine GC |
|---|---|---|
| Trigger | Continuous (rowsets accumulate) | Periodic (hourly, expired MPs only) |
| Operation | Read + Merge + Write (CPU-intensive) | Delete S3 file (API call, free) |
| Blocking | Can block queries | Never blocks queries |
| CPU cost | High (decode + merge + re-encode) | Near zero (metadata + S3 delete) |
| Data movement | Yes (rewrite data) | No (delete only) |

---

## 6. Metadata Layer — FoundationDB

### 6.1 Why FoundationDB

| Requirement | FDB Solution |
|---|---|
| ACID transactions | Strict serializable isolation |
| Ordered key-value | Range queries efficient (prefix scans) |
| Multi-region HA | Built-in replication, failover |
| Proven at scale | Snowflake metadata store (millions QPS) |
| Rust binding | foundationdb-rs (community maintained) |
| Open source | Apache 2.0 |

### 6.2 Key-Value Schema Design

```
FoundationDB Keyspace Layout:
(All keys are UTF-8 strings, values are serialized with bincode)

/catalog/
├── {db_id}                                    → Database {name, created_at, owner}
├── {db_id}/{schema_id}                        → Schema {name, created_at}
└── {db_id}/{schema_id}/{table_id}             → Table {name, columns[], properties}

/table/
├── {table_id}/meta                            → TableMeta {columns, partition_spec, ...}
├── {table_id}/mp/
│   ├── {mp_id}/{version}                      → MicroPartitionMeta {s3_path, stats, ...}
│   └── {mp_id}/{version}/stats                → ColumnStats[] (detailed)
├── {table_id}/active_mps                      → [mp_id:version, ...] (current visible set)
├── {table_id}/versions/
│   └── {timestamp}                            → [mp_id:version, ...] (snapshot at time T)
└── {table_id}/mp_count                        → u64 (total MPs, for quick stats)

/txn/
├── {txn_id}                                   → Transaction {status, commit_ts, tables[]}
├── active                                     → [txn_id, ...] (currently active)
└── next_id                                    → u64 (auto-increment)

/stream/
├── {stream_id}/meta                           → StreamMeta {table_id, type, created_at}
├── {stream_id}/offset                         → StreamOffset {last_consumed_ts, last_consumed_mp}
└── {stream_id}/changes/
    └── {change_id}                            → ChangeRecord {op, row_data, txn_id}

/user/
├── {user_id}                                  → User {name, password_hash, roles[], ...}
└── by_name/{username}                         → user_id (lookup index)

/role/
├── {role_id}                                  → Role {name, grants[]}
└── {role_id}/grants/
    └── {object_id}                            → Grant {privileges, with_grant_option}

/clone/
└── {clone_table_id}                           → CloneMeta {source_table_id, clone_ts}

/warehouse/
├── {wh_id}                                    → Warehouse {name, size, auto_suspend, ...}
└── {wh_id}/workers                            → [worker_id, ...]

/query_cache/
└── {query_hash}                               → CachedResult {result_path, table_versions, created_at}
```

### 6.3 Key Operations

```rust
// Get active MPs for a table (most common operation)
async fn get_active_mps(table_id: u64) -> Vec<MicroPartitionMeta> {
    // Range scan: /table/{table_id}/mp/ with filter active=true
    fdb_get_range(
        format!("/table/{}/mp/", table_id),
        |mp| mp.active == true
    ).await
}

// Get MPs active at specific timestamp (Time Travel)
async fn get_mps_at_timestamp(table_id: u64, ts: Timestamp) -> Vec<MicroPartitionMeta> {
    // Method 1: Use pre-computed version snapshot
    // /table/{table_id}/versions/{ts} → [mp_id:version, ...]

    // Method 2: Scan all MPs, filter by commit_ts and superseded_by
    fdb_get_range(
        format!("/table/{}/mp/", table_id),
        |mp| {
            mp.commit_ts <= ts &&           // created before or at ts
            (mp.superseded_by.is_none() ||  // still active, OR
             get_mp(mp.superseded_by).commit_ts > ts)  // superseded after ts
        }
    ).await
}

// Atomic transaction commit (DML)
async fn commit_transaction(txn: Transaction) -> Result<()> {
    fdb_run_transaction(|fdb_tx| {
        // 1. Verify no conflicts (snapshot_ts still valid)
        for table_id in txn.affected_tables {
            let current = fdb_tx.get(format!("/table/{}/version", table_id));
            if current > txn.snapshot_ts {
                return Err(Conflict);
            }
        }

        // 2. Atomically apply all changes
        for mp in txn.new_mps {
            fdb_tx.insert(format!("/table/{}/mp/{}/{}", ...), mp.serialize());
        }
        for (old_mp, new_mp) in txn.superseded {
            fdb_tx.update(old_mp.key(), |mp| mp.superseded_by = Some(new_mp));
        }
        for table_id in txn.affected_tables {
            fdb_tx.update(format!("/table/{}/active_mps", table_id), |list| {
                list.apply_changes(txn.mp_changes);
            });
            fdb_tx.increment(format!("/table/{}/version", table_id));
        }

        // 3. Record transaction
        fdb_tx.insert(format!("/txn/{}", txn.id), txn.serialize());
        fdb_tx.set(format!("/txn/{}/commit_ts", txn.id), now());

        Ok(())
    }).await
}
```

---

## 7. Coordinator Layer — Query Planning & Optimization

### 7.1 Query Processing Pipeline

```
Client SQL: "SELECT c.name, SUM(o.amount) FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.dt > '2026-06-01' GROUP BY c.name"
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. SQL Parser (sqlparser-rs)                                │
│    Input:  SQL text                                         │
│    Output: AST (SelectStmt { ... })                         │
│    Custom: Parse @stage syntax, AT(TIMESTAMP => ...),      │
│            CREATE STREAM, CREATE TABLE ... CLONE            │
├─────────────────────────────────────────────────────────────┤
│ 2. Analyzer                                                 │
│    Input:  AST                                              │
│    Output: Resolved logical plan                            │
│    Steps:                                                   │
│    a. Resolve table/column names → physical IDs             │
│    b. Check privileges (RBAC)                               │
│    c. Type checking + type coercion                         │
│    d. Resolve Nova custom syntax (@stage, Time Travel)      │
│    e. Determine cacheability (no non-deterministic funcs)   │
├─────────────────────────────────────────────────────────────┤
│ 3. Cache Check (Query Result Cache)                         │
│    Input:  Normalized SQL + table versions                  │
│    Output: Cache HIT → return result immediately            │
│            Cache MISS → continue to optimizer               │
├─────────────────────────────────────────────────────────────┤
│ 4. Optimizer (CBO)                                          │
│    Input:  Logical plan + table statistics                  │
│    Output: Optimized physical plan                          │
│    Steps:                                                   │
│    a. Rule-based: predicate pushdown, projection pushdown   │
│    b. Metadata pruning: skip MPs where max(dt) < date       │
│    c. Join reordering (cost-based)                          │
│    d. Runtime filter injection (Bloom filter)               │
│    e. Distributed plan: shuffle/broadcast/colocated         │
│    f. Statistics: row count, distinct, histograms           │
├─────────────────────────────────────────────────────────────┤
│ 5. Physical Planner                                         │
│    Input:  Optimized plan                                   │
│    Output: Fragment tree (for worker dispatch)              │
│    Steps:                                                   │
│    a. Fragment plan into worker-parallel chunks             │
│    b. Assign scan ranges (which MPs to read per worker)     │
│    c. Plan shuffle partitioning for joins                   │
│    d. Determine pipeline boundaries                         │
├─────────────────────────────────────────────────────────────┤
│ 6. Scheduler                                                │
│    Input:  Fragment tree                                    │
│    Output: Query results                                    │
│    Steps:                                                   │
│    a. Dispatch fragments to workers (gRPC)                  │
│    b. Track execution progress                              │
│    c. Collect partial results from workers                  │
│    d. Merge results                                         │
│    e. Store result in cache                                 │
│    f. Return to client                                      │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 Optimizer Detail

```rust
struct NovaOptimizer {
    // DataFusion base optimizer (rule-based + cost-based)
    base: datafusion::optimizer::Optimizer,

    // Nova custom optimization rules
    custom_rules: Vec<Box<dyn OptimizationRule>>,
}

trait OptimizationRule {
    fn try_optimize(&self, plan: &LogicalPlan, stats: &TableStats) -> Option<LogicalPlan>;
}

// Rule 1: Micro-Partition Pruning (zone maps)
struct MPPruningRule;

impl OptimizationRule for MPPruningRule {
    fn try_optimize(&self, plan: &LogicalPlan, stats: &TableStats) -> Option<LogicalPlan> {
        // For each Scan node with a filter predicate:
        // 1. Get all active MPs for the table
        // 2. For each MP, check if filter predicate can possibly match
        //    (using min/max stats from MP metadata)
        // 3. Remove MPs that cannot match from the scan list
        //
        // Example: WHERE dt > '2026-06-01'
        //   MP-001: max(dt) = '2026-05-15' → SKIP (no match possible)
        //   MP-002: max(dt) = '2026-06-10' → KEEP
        //   MP-003: max(dt) = '2026-06-20' → KEEP
        //   Result: skip 1 of 3 MPs → 33% less I/O
    }
}

// Rule 2: Runtime Filter Injection
struct RuntimeFilterRule;

impl OptimizationRule for RuntimeFilterRule {
    fn try_optimize(&self, plan: &LogicalPlan, stats: &TableStats) -> Option<LogicalPlan> {
        // For HashJoin: build Bloom filter on join key
        // Push Bloom filter to probe-side scan as pre-filter
        //
        // Example: JOIN orders ON customer_id = customers.id
        //   WHERE customers.region = 'west' (selective)
        //   1. Build side: customers (filtered, small)
        //   2. Create Bloom filter on customers.id
        //   3. Push to orders scan: skip orders where customer_id NOT IN bloom
        //   Result: maybe skip 90% of orders → 10x faster
    }
}

// Rule 3: Colocated Join Detection
struct ColocatedJoinRule;

impl OptimizationRule for ColocatedJoinRule {
    fn try_optimize(&self, plan: &LogicalPlan, stats: &TableStats) -> Option<LogicalPlan> {
        // If both tables are distributed by the same key as the join key:
        // → Colocated join (no network shuffle needed)
        //
        // Example: orders DISTRIBUTED BY HASH(customer_id)
        //          customers DISTRIBUTED BY HASH(id)
        //          JOIN ON orders.customer_id = customers.id
        //   → Colocated! Each worker joins locally, no shuffle.
    }
}

// Rule 4: Late Materialization
struct LateMaterializationRule;

impl OptimizationRule for LateMaterializationRule {
    fn try_optimize(&self, plan: &LogicalPlan, stats: &TableStats) -> Option<LogicalPlan> {
        // For SELECT with filter + projection:
        // 1. Scan only filter columns first → get matching row IDs
        // 2. Scan projection columns ONLY for matching row IDs
        //
        // Example: SELECT name, email FROM users WHERE status = 'active'
        //   1. Scan status column → filter → get row_ids (1% match)
        //   2. Scan name, email for 1% of rows only
        //   Result: read 3 columns × 100% + 2 columns × 1% = much less I/O
    }
}

// Rule 5: Dictionary Encoding Optimization
struct DictionaryOptRule;

impl OptimizationRule for DictionaryOptRule {
    fn try_optimize(&self, plan: &LogicalPlan, stats: &TableStats) -> Option<LogicalPlan> {
        // For low-cardinality string columns (< 10000 distinct):
        // 1. Dictionary encode at scan time
        // 2. Operate on encoded INT values (SIMD-friendly)
        // 3. Decode only at output
        //
        // Example: status column has 3 values ['pending', 'shipped', 'cancelled']
        //   Encode: [0, 0, 1, 2, 0, 1, ...] (1 byte per value vs 10+ bytes)
        //   Filter: WHERE status = 'shipped' → WHERE encoded = 1 (SIMD on u8)
        //   Result: 10x less memory, 4x faster filter
    }
}
```

### 7.3 Statistics Collection

```rust
struct TableStats {
    row_count: u64,
    byte_size: u64,

    // Per-column stats
    columns: HashMap<ColumnId, ColumnStats>,

    // Per-MP stats (for pruning)
    mp_stats: Vec<MicroPartitionStats>,
}

struct ColumnStats {
    distinct_count: u64,
    null_count: u64,
    min_value: ScalarValue,
    max_value: ScalarValue,

    // Advanced stats (collected via ANALYZE)
    histogram: Option<Histogram>,         // for skewed data
    most_common_values: Vec<(ScalarValue, f64)>,  // top-N values + frequency
    average_row_length: u64,
}

// Auto-collect after each DML (like StarRocks)
async fn auto_collect_stats(table_id: u64) {
    let mps = get_active_mps(table_id).await;

    let mut stats = TableStats::new();
    for mp in &mps {
        stats.row_count += mp.row_count;
        stats.byte_size += mp.byte_size;

        for (col_id, col_stats) in &mp.column_stats {
            stats.merge_column_stats(*col_id, col_stats);
        }
    }

    fdb_put(format!("/table/{}/stats", table_id), stats.serialize()).await;
}
```

---

## 8. Worker Layer — Execution Engine

### 8.1 Execution Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Nova Worker (1 per VM/container, fully stateless)            │
│                                                               │
│  Receives: QueryFragment (from Coordinator via gRPC)          │
│  Returns:  Stream<RecordBatch> (Arrow columnar batches)       │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Execution Pipeline (DataFusion + custom operators)       │ │
│  │                                                          │ │
│  │  Data flow (push-based):                                 │ │
│  │                                                          │ │
│  │  MPScanExec ──► FilterExec ──► ProjectExec ──► HashJoinExec ──► AggregateExec ──► SortExec ──► Output
│  │  (custom)      (DataFusion)  (DataFusion)    (DataFusion)    (DataFusion)     (DataFusion)    │
│  │                                                          │ │
│  │  Each operator:                                          │ │
│  │    - Receives RecordBatch (8192 rows, columnar)          │ │
│  │    - Processes vectorized (SIMD where possible)          │ │
│  │    - Pushes output to next operator                      │ │
│  │    - No intermediate materialization (pipelined)         │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Custom Operators                                         │ │
│  │                                                          │ │
│  │  1. MicroPartitionScanExec                               │ │
│  │     - Reads MPs from cache (Foyer) or S3                 │ │
│  │     - Applies metadata pruning before read               │ │
│  │     - Column pruning (read only needed columns)          │ │
│  │     - Predicate pushdown to Parquet row groups           │ │
│  │     - Runtime filter (Bloom) application                 │ │
│  │     - Late materialization (filter columns first)        │ │
│  │                                                          │ │
│  │  2. ChangeDiffExec (for Streams)                         │ │
│  │     - Reads old MP version + new MP version              │ │
│  │     - Diffs by primary key → INSERT/UPDATE/DELETE        │ │
│  │     - Outputs change records with metadata               │ │
│  │                                                          │ │
│  │  3. CloneWriteExec (for DML on cloned tables)            │ │
│  │     - Creates new MP (copy-on-write)                     │ │
│  │     - Does NOT modify shared S3 files                    │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │ Memory Manager   │  │ Cache (Foyer)    │  │ Storage      │ │
│  │                  │  │                  │  │ Adapter      │ │
│  │ Per-query limit  │  │ L3: MP data      │  │              │ │
│  │ Spill to SSD     │  │   4GB RAM        │  │ Parquet read │ │
│  │ OOM → fail query │  │   100GB SSD      │  │ S3/GCS/Azure │ │
│  └─────────────────┘  └──────────────────┘  └──────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### 8.2 Custom Operator: MicroPartitionScanExec

```rust
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream, Statistics,
};
use arrow::record_batch::RecordBatch;

struct MicroPartitionScanExec {
    table_id: u64,
    mp_list: Vec<MicroPartitionMeta>,  // pre-pruned by optimizer
    projection: Vec<usize>,            // column indices to read
    predicate: Option<Expr>,           // row-level filter
    runtime_filter: Option<BloomFilter>,  // from join build side
    cache: Arc<NovaCache>,
    batch_size: usize,                 // 8192
}

impl ExecutionPlan for MicroPartitionScanExec {
    fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let mp = &self.mp_list[partition];
        let cache_key = format!("{}:{}", mp.mp_id, mp.version);

        // 1. Check cache (Foyer L3)
        let cached = self.cache.mp_cache.get(&cache_key).await;

        if let Some(cached_batch) = cached {
            // Cache HIT — return from RAM/SSD
            return stream(cached_batch);
        }

        // 2. Cache MISS — read from S3
        let reader = ParquetReader::builder(&mp.s3_path)
            .with_columns(&self.projection)           // column pruning
            .with_predicate(&self.predicate)          // row group pruning
            .with_runtime_filter(&self.runtime_filter) // Bloom filter
            .with_batch_size(self.batch_size)
            .build();

        // 3. Stream batches, cache hot data
        let stream = reader.stream().map(|batch| {
            // Cache the batch for future queries
            self.cache.mp_cache.maybe_insert(&cache_key, &batch);
            batch
        });

        Ok(Box::pin(stream))
    }

    fn properties(&self) -> &PlanProperties {
        // Parallelism = number of MPs
        // Each MP = 1 partition (can be read in parallel)
    }
}
```

### 8.3 DataFusion Built-in Operators (Already Available)

| Operator | DataFusion Status | Notes |
|---|---|---|
| FilterExec | ✅ | Vectorized predicate evaluation |
| ProjectionExec | ✅ | Column selection + expression eval |
| HashJoinExec | ✅ | Build/probe hash join |
| NestedLoopJoinExec | ✅ | Fallback for non-equijoin |
| AggregateExec | ✅ | Two-phase aggregation (partial + final) |
| SortExec | ✅ | Sort with spill to disk |
| LimitExec | ✅ | Top-N / LIMIT |
| WindowAggExec | ✅ | Window functions (ROW_NUMBER, RANK, etc.) |
| UnionExec | ✅ | UNION ALL |
| CoalesceBatchesExec | ✅ | Merge small batches |
| RepartitionExec | ✅ | Shuffle/redistribute data |

---

## 9. Cache Architecture — Foyer Hybrid Cache

### 9.1 Three-Layer Cache Stack

```
┌──────────────────────────────────────────────────────────────┐
│                    NOVA ENGINE CACHE STACK                     │
│                                                               │
│  Query arrives                                                │
│       │                                                       │
│       ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ L1: Query Result Cache (Foyer HybridCache)              │ │
│  │                                                          │ │
│  │ Memory: 2GB (hot results)                                │ │
│  │ Disk: 50GB SSD (persisted results)                      │ │
│  │                                                          │ │
│  │ Key: hash(normalized_sql + table_versions)               │ │
│  │ Value: QueryResult { result_batches, created_at, ... }   │ │
│  │                                                          │ │
│  │ HIT → Return instantly (0-10ms)                         │ │
│  │ MISS → Continue to L2                                    │ │
│  │                                                          │ │
│  │ Auto-invalidation: table version change → new cache key │ │
│  │ TTL: 24 hours (reset on hit, max 31 days)               │ │
│  └─────────────────────────┬───────────────────────────────┘ │
│                            │ miss                             │
│  ┌─────────────────────────▼───────────────────────────────┐ │
│  │ L2: Metadata Cache (Foyer in-memory Cache)              │ │
│  │                                                          │ │
│  │ Memory: 512MB                                            │ │
│  │                                                          │ │
│  │ Key: "table:{id}:active_mps"                             │ │
│  │ Value: Vec<MicroPartitionMeta> (MP list + column stats) │ │
│  │                                                          │ │
│  │ HIT → Skip FDB read, use cached MP list                 │ │
│  │ MISS → Read from FoundationDB, cache result              │ │
│  └─────────────────────────┬───────────────────────────────┘ │
│                            │                                  │
│  ┌─────────────────────────▼───────────────────────────────┐ │
│  │ L3: Micro-Partition Cache (Foyer HybridCache)           │ │
│  │                                                          │ │
│  │ Memory: 4GB (hot MPs in RAM)                             │ │
│  │ Disk: 100GB SSD (warm MPs on local disk)                │ │
│  │                                                          │ │
│  │ Key: "{mp_id}:{version}"                                 │ │
│  │ Value: Arrow RecordBatch (columnar data)                │ │
│  │                                                          │ │
│  │ HIT → Skip S3 read, serve from RAM/SSD                  │ │
│  │ MISS → Read from S3, cache in Foyer                     │ │
│  │                                                          │ │
│  │ Compaction-aware refill: after MP merge, auto-prefetch  │ │
│  │ new MP to prevent cache miss                             │ │
│  └─────────────────────────┬───────────────────────────────┘ │
│                            │ miss                             │
│  ┌─────────────────────────▼───────────────────────────────┐ │
│  │ L4: S3 / Object Storage (infinite, immutable)            │ │
│  │                                                          │ │
│  │ Parquet micro-partition files                            │ │
│  │ Read → cache in L3 → serve query                         │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### 9.2 Foyer Implementation

```rust
use foyer::{HybridCache, HybridCacheBuilder, FsDeviceBuilder, BlockEngineConfig};

struct NovaCache {
    // L1: Query result cache (2GB RAM + 50GB SSD)
    result_cache: HybridCache<u64, QueryResult>,

    // L2: Metadata cache (512MB RAM only)
    meta_cache: Cache<String, Vec<MicroPartitionMeta>>,

    // L3: Micro-partition data cache (4GB RAM + 100GB SSD)
    mp_cache: HybridCache<String, RecordBatch>,
}

impl NovaCache {
    async fn new(ssd_path: &str) -> Self {
        // L1: Result cache
        let result_device = FsDeviceBuilder::new(format!("{ssd_path}/results"))
            .with_capacity(50 * GB)
            .build();
        let result_cache = HybridCacheBuilder::new()
            .memory(2 * GB)
            .storage()
            .with_engine_config(BlockEngineConfig::new(result_device))
            .build()
            .await;

        // L2: Metadata cache (RAM only, FDB is source of truth)
        let meta_cache = Cache::builder()
            .max_capacity(512 * MB)
            .build();

        // L3: MP data cache
        let mp_device = FsDeviceBuilder::new(format!("{ssd_path}/mps"))
            .with_capacity(100 * GB)
            .build();
        let mp_cache = HybridCacheBuilder::new()
            .memory(4 * GB)
            .storage()
            .with_engine_config(BlockEngineConfig::new(mp_device))
            .build()
            .await;

        Self { result_cache, meta_cache, mp_cache }
    }
}
```

### 9.3 Query Result Cache (Snowflake-style)

```rust
struct QueryResultCache {
    cache: HybridCache<u64, QueryResult>,
    ttl: Duration,              // 24 hours
    max_ttl: Duration,          // 31 days
}

struct QueryCacheKey {
    sql_hash: u64,                           // hash of normalized SQL
    table_versions: Vec<(TableId, u64)>,     // (table_id, version) at query time
}

struct QueryResult {
    batches: Vec<RecordBatch>,
    created_at: Timestamp,
    table_versions: Vec<(TableId, u64)>,
    row_count: u64,
    byte_size: u64,
}

impl QueryResultCache {
    async fn try_get(&self, sql: &str, tables: &[TableId]) -> Option<QueryResult> {
        // 1. Normalize SQL (lowercase, collapse whitespace)
        let normalized = normalize_sql(sql);
        if !is_cacheable(&normalized) {
            return None;  // non-deterministic functions → skip cache
        }

        let sql_hash = hash(&normalized);

        // 2. Get current table versions from FDB
        let current_versions = fdb_batch_get_table_versions(tables).await;

        // 3. Build cache key
        let key = hash_key(&QueryCacheKey {
            sql_hash,
            table_versions: current_versions.clone(),
        });

        // 4. Try cache
        if let Some(cached) = self.cache.get(&key).await {
            // Verify table versions haven't changed since cache was written
            if cached.table_versions == current_versions {
                return Some(cached);  // HIT
            }
        }

        None  // MISS
    }

    async fn put(&self, sql: &str, tables: &[(TableId, u64)], result: Vec<RecordBatch>) {
        let normalized = normalize_sql(sql);
        let sql_hash = hash(&normalized);

        let key = hash_key(&QueryCacheKey {
            sql_hash,
            table_versions: tables.to_vec(),
        });

        self.cache.insert(key, QueryResult {
            batches: result,
            created_at: now(),
            table_versions: tables.to_vec(),
            row_count: 0,  // computed
            byte_size: 0,  // computed
        });
    }
}

fn normalize_sql(sql: &str) -> String {
    sql.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_cacheable(sql: &str) -> bool {
    let non_deterministic = [
        "current_timestamp", "current_date", "current_time",
        "now()", "rand(", "uuid(", "random(",
    ];
    !non_deterministic.iter().any(|f| sql.contains(f))
}
```

### 9.4 Auto-Invalidation Mechanism

```
How cache auto-invalidates (no explicit invalidation needed):

1. Query: "SELECT COUNT(*) FROM orders"
   Table orders version: 5 (active MPs: MP-001-v3, MP-002-v1, MP-003-v1)
   → Cache key = hash("select count(*) from orders" + [(orders, 5)])
   → Execute (3s) → result = 1,000,000
   → Store in L1 cache

2. Same query again
   Table orders version: 5 (unchanged)
   → Cache key = same hash → HIT → instant return

3. INSERT INTO orders VALUES (...)
   → New MP-004-v1 created
   → Table orders version incremented to 6
   → Active MPs: MP-001-v3, MP-002-v1, MP-003-v1, MP-004-v1

4. Same query again
   Table orders version: 6 (CHANGED)
   → Cache key = hash("select count(*) from orders" + [(orders, 6)])
   → DIFFERENT hash → MISS → re-execute
   → New result = 1,000,001
   → Store new cache entry

The beauty: MVCC version tracking IS the invalidation mechanism.
No need to track "which queries depend on which tables."
JOIN queries: if ANY table changes, version changes, cache key changes, cache misses.
```

### 9.5 Performance Impact

```
Scenario: SELECT COUNT(*) FROM orders (100M rows, 14GB)

Without cache (cold):
  S3 read → Parquet decode → aggregation → result
  Time: ~3 seconds

With L3 cache (MP cached in SSD via Foyer):
  SSD read → Parquet decode → aggregation → result
  Time: ~1 second (3x faster)

With L3 cache (MP cached in RAM via Foyer):
  RAM read → aggregation → result
  Time: ~200ms (15x faster)

With L1 cache (query result cached):
  Return cached result directly
  Time: ~5ms (600x faster)
```

---

## 10. Performance Engineering

### 10.1 Performance Pillars (Research-Based)

| Pillar | Paper/Source | Nova Engine Implementation |
|---|---|---|
| **Vectorized Execution** | MonetDB/X100 (CIDR 2005) | DataFusion (Arrow RecordBatch, batch=8192) |
| **Push-Based Pipeline** | DuckDB push model (2021) | DataFusion Stream API (SendableRecordBatchStream) |
| **Late Materialization** | Abadi et al. (ICDE 2007), VLDB 2025 | Custom LateMaterializationRule + Parquet column pruning |
| **Metadata Pruning** | Snowflake zone maps, StarRocks tablet stats | MicroPartitionStats (min/max per MP, checked before S3 read) |
| **Runtime Filter** | StarRocks Global Runtime Filter | Bloom filter pushdown from join build to scan |
| **CBO** | Cascades (Graefe 1995), StarRocks CBO | DataFusion optimizer + custom rules |
| **Dictionary Encoding** | StarRocks "Operation on Encoded Data" | Custom DictionaryOptRule (operate on encoded values) |
| **Cache Hierarchy** | Snowflake result cache, RisingWave/Foyer | 3-layer Foyer cache (result → metadata → MP data) |

### 10.2 DataFusion Proven Performance

**ClickBench November 2024:**
> Apache DataFusion 43.0.0 is the fastest engine for querying Apache Parquet files.
> Faster than DuckDB, chDB, and ClickHouse.
> First time a Rust-based engine holds the top spot.

```
ClickBench (14GB Parquet, 16 vCPU, 32GB RAM, hot run):
1. DataFusion 43 (Rust)     ← #1 FASTEST
2. DuckDB (C++)
3. chDB/ClickHouse (C++)
```

### 10.3 Custom Performance Optimizations (Nova-specific)

```
┌─────────────────────────────────────────────────────────────────┐
│  Optimization                          │ Impact                │
├────────────────────────────────────────┼───────────────────────┤
│  MP Metadata Pruning (zone maps)       │ 10-100x faster scan   │
│  (skip MPs where min/max can't match)  │ for range predicates  │
├────────────────────────────────────────┼───────────────────────┤
│  Runtime Bloom Filter pushdown         │ 5-50x faster JOIN     │
│  (skip rows at scan if not in bloom)   │ for selective dims    │
├────────────────────────────────────────┼───────────────────────┤
│  Colocated Join (no network shuffle)   │ 2-5x faster JOIN      │
│  (same distribution key = local join)  │ for star schemas      │
├────────────────────────────────────────┼───────────────────────┤
│  Late Materialization                  │ 2-10x less I/O        │
│  (filter first, fetch columns later)   │ for selective queries │
├────────────────────────────────────────┼───────────────────────┤
│  Dictionary Encoding (direct ops)      │ 2-5x faster filter    │
│  (operate on INT codes, not strings)   │ for low-card columns  │
├────────────────────────────────────────┼───────────────────────┤
│  Query Result Cache (auto-invalidate)  │ 600x faster repeated  │
│  (MVCC version-based cache key)        │ queries (instant)     │
├────────────────────────────────────────┼───────────────────────┤
│  Foyer Hybrid Cache (RAM + SSD)        │ 25x more cache        │
│  (104GB effective vs 4GB RAM-only)     │ vs RAM-only           │
├────────────────────────────────────────┼───────────────────────┤
│  No JVM overhead                       │ Lower latency,        │
│  (no GC pauses, no warmup)             │ no GC stalls          │
├────────────────────────────────────────┼───────────────────────┤
│  No compaction overhead                │ No background CPU     │
│  (immutable storage, GC = delete only) │ drain, no stalls      │
└────────────────────────────────────────┴───────────────────────┘
```

### 10.4 Projected Performance Comparison

```
                     Scan+Filter    Aggregation    JOIN (2 tables)    Repeated Query
PostgreSQL (row):    1x (baseline)  1x             1x                 1x
StarRocks:          10-50x         20-100x        50-200x            1x (no result cache)
ClickHouse:          20-80x         30-100x        10-50x (weak)      1x
DuckDB:              15-50x         20-80x         20-80x             1x
DataFusion 43:       20-80x         25-100x        20-80x             1x
Nova Engine:         30-100x        30-150x        50-200x            600x (result cache)
  (+ MP pruning)     ─────────      ─────────      ─────────          ─────────
  (+ Runtime filter)                                ↑ massive gain
  (+ Colocated join)
  (+ Foyer cache)
  (+ Result cache)                                                     ↑ instant
```

---

## 11. Transaction & MVCC Model

### 11.1 Transaction Lifecycle

```
1. BEGIN
   → txn_id = fdb_get_next_txn_id()
   → snapshot_ts = fdb_get_current_timestamp()
   → Read table versions for all affected tables

2. INSERT INTO orders VALUES (...)
   → Write rows to new Parquet file (temp S3 path: s3://nova/tmp/{txn_id}/mp-new.parquet)
   → Compute column stats
   → DO NOT commit yet

3. UPDATE orders SET status='shipped' WHERE id=5
   → Find MP containing id=5 (metadata scan)
   → Read MP (Parquet read from cache or S3)
   → Modify matching rows → write NEW Parquet (temp path)
   → Record: old_mp_id → new_mp_id (pending)

4. COMMIT
   → FDB atomic transaction:
     a. Verify no conflicts (table versions unchanged since snapshot_ts)
     b. Atomically:
        - Move temp S3 files → permanent paths
        - Insert MP metadata (new versions)
        - Update active MP lists for affected tables
        - Increment table versions
        - Set commit_ts = now
        - Mark old MPs as superseded
     c. FDB commit (atomic, serializable)
   → Success → query results visible to new queries

5. If conflict (another txn modified same tables):
   → ABORT
   → Delete temp S3 files
   → Retry atau return error to client
```

### 11.2 MVCC Visibility Rules

```rust
fn is_mp_visible(mp: &MicroPartitionMeta, query_ts: Timestamp) -> bool {
    // An MP is visible to a query at query_ts if:
    // 1. mp.commit_ts <= query_ts  (committed before or at query time)
    // 2. mp.superseded_by is None  (still active)
    //    OR mp.superseded_by.commit_ts > query_ts  (superseded AFTER query time)

    mp.commit_ts <= query_ts && match mp.superseded_by {
        None => true,
        Some(next_mp_id) => {
            let next_mp = get_mp(next_mp_id);
            next_mp.commit_ts > query_ts
        }
    }
}
```

### 11.3 Isolation Level

**Snapshot Isolation:**
- Each query sees a consistent point-in-time snapshot
- No dirty reads (uncommitted data invisible)
- No non-repeatable reads (same query → same result within transaction)
- Phantom reads: prevented (snapshot includes all data at snapshot_ts)
- Write skew: possible (same as Snowflake, PostgreSQL REPEATABLE READ)

---

## 12. Snowflake Parity Features

### 12.1 Time Travel

```sql
-- Query data as of 3 hours ago
SELECT * FROM orders AT(TIMESTAMP => '2026-06-23 09:00:00');

-- Query data before a specific statement
SELECT * FROM orders BEFORE(STATEMENT => 'query_id_abc123');

-- Clone table as of yesterday
CREATE TABLE orders_yesterday CLONE orders AT(TIMESTAMP => '2026-06-22 00:00:00');
```

**Implementation:**
- Planner resolves `AT(TIMESTAMP => T)` → get MPs active at time T
- Uses MVCC visibility rules (commit_ts <= T, superseded_by.commit_ts > T)
- MPs from time T are still in S3 (if within retention period)
- GC only deletes MPs past retention (1-90 days configurable)

### 12.2 Zero-Copy Clone

```sql
-- Clone a table (instant, no data copy)
CREATE TABLE orders_dev CLONE orders;

-- Clone at specific time
CREATE TABLE orders_snapshot CLONE orders AT(TIMESTAMP => '2026-06-23 00:00:00');

-- Clone entire schema
CREATE SCHEMA dev_schema CLONE prod_schema;
```

**Implementation:**
- Clone = copy metadata entries in FDB (point to SAME S3 files)
- Zero data copy — clone and source share Parquet files
- Copy-on-write: when clone is modified, new MP created (source unaffected)
- Instant operation (metadata only, ~ms regardless of table size)

```rust
fn clone_table(source_table_id: u64, at_ts: Option<Timestamp>) -> u64 {
    let new_table_id = generate_id();
    let clone_ts = at_ts.unwrap_or(now());

    // Get active MPs (at clone_ts for time-travel clone)
    let mps = get_visible_mps(source_table_id, clone_ts);

    // Copy ALL MP metadata entries → point to SAME S3 paths
    for mp in mps {
        let cloned_mp = MicroPartitionMeta {
            mp_id: generate_id(),
            table_id: new_table_id,
            s3_path: mp.s3_path.clone(),  // SAME PATH! Zero-copy!
            row_count: mp.row_count,
            column_stats: mp.column_stats.clone(),
            commit_ts: clone_ts,
            supersedes: None,
            superseded_by: None,
            active: true,
            ..Default::default()
        };
        fdb_insert(cloned_mp);
    }

    // Store clone relationship
    fdb_insert(CloneMeta {
        clone_table_id: new_table_id,
        source_table_id,
        clone_ts,
    });

    new_table_id
}
```

### 12.3 Streams (CDC)

```sql
-- Create stream on table
CREATE STREAM orders_stream ON TABLE orders;

-- Consume changes (advances offset)
INSERT INTO orders_archive
SELECT * FROM orders_stream WHERE METADATA$ACTION = 'INSERT';

-- Stream types
CREATE STREAM orders_append ON TABLE orders APPEND_ONLY = TRUE;
```

**Implementation:**
- Stream stores offset (last consumed timestamp) in FDB
- When queried: find MPs created AFTER offset
- For each new MP:
  - If no previous version (supersedes=None) → INSERT records
  - If supersedes previous MP → diff old vs new → INSERT/UPDATE/DELETE records
- Offset advances after successful consumption

```rust
fn read_stream(stream_id: u64) -> Vec<ChangeRecord> {
    let offset = get_stream_offset(stream_id);  // last_consumed_ts
    let table_id = get_stream_table_id(stream_id);

    // Get MPs created AFTER offset
    let new_mps = get_mps_committed_after(table_id, offset.last_consumed_ts);

    let mut changes = Vec::new();

    for new_mp in new_mps {
        match new_mp.supersedes {
            None => {
                // New MP (pure INSERT)
                let data = read_parquet(&new_mp.s3_path);
                for row in data {
                    changes.push(ChangeRecord::Insert(row));
                }
            }
            Some(old_mp_id) => {
                // Updated/Deleted MP → diff
                let old_mp = get_mp(old_mp_id);
                let old_data = read_parquet(&old_mp.s3_path);
                let new_data = read_parquet(&new_mp.s3_path);

                // Diff by primary key
                let diff = diff_by_pk(old_data, new_data, &pk_columns);
                changes.extend(diff);
                // diff produces: Insert(new rows), Update(changed rows), Delete(removed rows)
            }
        }
    }

    // Advance offset
    set_stream_offset(stream_id, now());

    changes
}
```

### 12.4 Dynamic Tables (Target Lag)

```sql
-- Create dynamic table with target lag
CREATE DYNAMIC TABLE dt_orders
    TARGET_LAG = '10 minutes'
    WAREHOUSE = analytics_wh
    REFRESH_MODE = INCREMENTAL
AS
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id;
```

**Implementation:**
- Nova translates to: `CREATE MATERIALIZED VIEW mv_dt_orders REFRESH MANUAL AS ...`
- Stores `target_lag` in FDB metadata
- Background monitor checks: if (now - last_refresh) > target_lag → trigger refresh
- Refresh = `REFRESH MATERIALIZED VIEW mv_dt_orders`

### 12.5 Warehouse (Virtual Compute)

```sql
CREATE WAREHOUSE analytics_wh
    SIZE = 'MEDIUM'        -- 4 workers, 8 vCPU each
    AUTO_SUSPEND = 300     -- suspend after 5 min idle
    AUTO_RESUME = TRUE;    -- resume on query

USE WAREHOUSE analytics_wh;

SELECT * FROM orders;  -- routes to analytics_wh workers
```

**Implementation:**
- Warehouse = logical group of workers
- Auto-suspend: if no queries for N seconds → terminate all workers (no cost)
- Auto-resume: when query arrives → provision new workers (60s startup)
- Workers are stateless → no data loss on suspend/resume

---

## 13. Scalability Model

### 13.1 Scale Dimensions

| Dimension | How to Scale | Limit |
|---|---|---|
| **Query concurrency** | Add more workers | Unlimited (workers stateless) |
| **Data volume** | S3 auto-scales | Unlimited (S3) |
| **Metadata volume** | FDB cluster scale-out | Petabytes (FDB proven at Snowflake) |
| **Query parallelism** | Partition MPs across workers | # active MPs = parallelism ceiling |
| **Write throughput** | Parallel MP writes to S3 | ~3,500 PUTs/s per S3 prefix |
| **Cache capacity** | Add more SSD to workers | Linear with worker count |

### 13.2 Auto-Scaling

```rust
struct AutoScaler {
    min_workers: usize,
    max_workers: usize,
    scale_up_cpu_threshold: f64,     // 70%
    scale_down_cpu_threshold: f64,   // 20%
    scale_up_queue_depth: usize,     // 2 pending queries per worker
    auto_suspend_seconds: u64,       // 300 (5 minutes)
}

impl AutoScaler {
    async fn evaluate(&self, metrics: &WorkerPoolMetrics) -> Action {
        let avg_cpu = metrics.avg_cpu_across_workers();
        let pending_queries = metrics.pending_query_count();
        let active_workers = metrics.active_worker_count();

        // Scale UP conditions
        if avg_cpu > self.scale_up_cpu_threshold
            || pending_queries > active_workers * self.scale_up_queue_depth
        {
            if active_workers < self.max_workers {
                return Action::ScaleUp(1);
            }
        }

        // Scale DOWN conditions
        if avg_cpu < self.scale_down_cpu_threshold && active_workers > self.min_workers {
            return Action::ScaleDown(1);
        }

        // Auto-suspend (all workers idle)
        if metrics.seconds_since_last_query() > self.auto_suspend_seconds
            && active_workers > 0
        {
            return Action::SuspendAll;
        }

        // Auto-resume (query arrived, no workers)
        if pending_queries > 0 && active_workers == 0 {
            return Action::Resume(self.min_workers);
        }

        Action::Hold
    }
}
```

### 13.3 Distributed Query Execution

```
Query: SELECT c.region, SUM(o.amount) FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.region

Coordinator plan:
┌─────────────────────────────────────────────────────────┐
│ Fragment 1: Scan + Filter (orders)                      │
│   Worker 1: scan MP-001, MP-002                          │
│   Worker 2: scan MP-003, MP-004                          │
│   Worker 3: scan MP-005, MP-006                          │
│   → Shuffle by customer_id (for colocated join)          │
├─────────────────────────────────────────────────────────┤
│ Fragment 2: Scan + Filter (customers)                    │
│   Worker 1: scan all customers (broadcast if small)      │
│   → Broadcast to all workers                             │
├─────────────────────────────────────────────────────────┤
│ Fragment 3: Hash Join + Aggregate + Sort                 │
│   Worker 1: join + partial agg (partition 1)             │
│   Worker 2: join + partial agg (partition 2)             │
│   Worker 3: join + partial agg (partition 3)             │
│   → Shuffle by region to coordinator                     │
├─────────────────────────────────────────────────────────┤
│ Fragment 4: Final Aggregate + Output (Coordinator)       │
│   Merge partial results → final result → client          │
└─────────────────────────────────────────────────────────┘
```

---

## 14. High Availability

### 14.1 HA Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                      HA Architecture                            │
│                                                                │
│  Region A (Primary)              Region B (DR)                 │
│  ┌──────────────┐                ┌──────────────┐             │
│  │ FDB Cluster   │ ◄────────────► │ FDB Cluster   │             │
│  │ (3+ nodes,    │  async replica │ (3+ nodes,    │             │
│  │  Raft)        │                │  standby)     │             │
│  └──────────────┘                └──────────────┘             │
│                                                                │
│  ┌──────────────┐                ┌──────────────┐             │
│  │ S3 Bucket     │ ◄────────────► │ S3 Bucket     │             │
│  │ (cross-region │  CRR           │ (replica)     │             │
│  │  replication) │                │               │             │
│  └──────────────┘                └──────────────┘             │
│                                                                │
│  ┌──────────────┐                ┌──────────────┐             │
│  │ Coordinators  │   failover     │ Coordinators  │             │
│  │ (3 nodes,     │ ─────────────► │ (standby,     │             │
│  │  Raft leader  │                │  promoted)    │             │
│  │  election)    │                │               │             │
│  └──────────────┘                └──────────────┘             │
│                                                                │
│  ┌──────────────┐                ┌──────────────┐             │
│  │ Workers       │   re-provision │ Workers       │             │
│  │ (stateless,   │ ─────────────► │ (auto-provision│            │
│  │  auto-scaling)│                │  from image)  │             │
│  └──────────────┘                └──────────────┘             │
└───────────────────────────────────────────────────────────────┘
```

### 14.2 HA Per Layer

| Layer | HA Strategy | RTO | RPO |
|---|---|---|---|
| **Coordinator** | Raft consensus (3 nodes), leader election (openraft) | < 10s | 0 (sync) |
| **FoundationDB** | 3+ nodes, multi-region async replication | < 30s | < 1s (async) |
| **S3** | Cross-region replication (CRR) | < 5min | < 1min |
| **Workers** | Stateless, auto-replace on failure | < 60s | N/A (stateless) |
| **Cache** | Foyer disk cache persists across worker restarts | Immediate | N/A (rebuilt from S3) |

### 14.3 Coordinator Failover

```rust
// Raft consensus: 3 coordinator nodes, 1 leader, 2 followers
// Leader handles all queries, followers replicate state

// On leader failure:
// 1. Followers detect heartbeat timeout (10s)
// 2. Follower with most up-to-date log starts election
// 3. Wins majority → becomes new leader
// 4. New leader starts accepting queries
// 5. Workers reconnect to new leader (via service discovery / DNS)

// On follower failure:
// 1. Leader detects missing heartbeat
// 2. Leader continues serving queries (majority still intact)
// 3. Replacement follower provisioned
// 4. New follower syncs from leader
```

### 14.4 Worker Failure Recovery

```rust
// Workers are stateless — failure recovery is trivial
fn on_worker_failure(worker_id: WorkerId, in_flight_queries: Vec<QueryId>) {
    // 1. Mark worker as dead
    workers.remove(&worker_id);

    // 2. Re-schedule in-flight query fragments to other workers
    for query_id in in_flight_queries {
        let unfinished_fragments = get_unfinished_fragments(query_id);
        for fragment in unfinished_fragments {
            schedule_on_available_worker(fragment);
        }
    }

    // 3. Auto-provision replacement worker (if below min_workers)
    if workers.len() < min_workers {
        provision_new_worker();
    }

    // No data loss — all data in S3, all metadata in FDB
    // Cache (Foyer SSD) may persist if worker VM is reused
}
```

---

## 15. Complete Tech Stack

```
┌─────────────────────────────────────────────────────────────┐
│              NOVA ENGINE — COMPLETE TECH STACK               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Language:       Rust (edition 2024)                        │
│  Build:          cargo + just (task runner)                 │
│  Testing:        cargo test + proptest + cargo-nextest      │
│  Linting:        clippy + rustfmt                           │
│                                                              │
│  ── SQL Layer (Coordinator) ──                              │
│  SQL Parser:     sqlparser-rs (ANSI SQL + custom dialect)  │
│  Optimizer:      datafusion::optimizer + custom rules      │
│  Planner:        datafusion::physical_planner               │
│                                                              │
│  ── Execution Layer (Worker) ──                             │
│  Execution:      DataFusion (vectorized, push-based)       │
│  Custom Ops:     MicroPartitionScanExec, ChangeDiffExec    │
│  UDF:            PyO3 (embedded Python interpreter)        │
│                                                              │
│  ── Storage Layer ──                                        │
│  Parquet R/W:    parquet crate (Arrow-native)              │
│  Object Store:   object_store (S3/GCS/Azure/Local)         │
│  Metadata:       FoundationDB (foundationdb-rs)            │
│  Dev Metadata:   sled (embedded, for local testing)        │
│                                                              │
│  ── Cache Layer ──                                          │
│  Hybrid Cache:   foyer (RAM + SSD hybrid cache)            │
│  In-Memory:      foyer::Cache (or moka for simple cases)   │
│                                                              │
│  ── Networking ──                                           │
│  RPC:            tonic (gRPC, Coordinator ↔ Worker)        │
│  HTTP API:       axum                                       │
│  MySQL Protocol: mysql_wire (MySQL wire compatibility)     │
│                                                              │
│  ── Consensus & HA ──                                       │
│  Raft:           openraft (coordinator leader election)    │
│                                                              │
│  ── Auth & Security ──                                      │
│  Password:       argon2 (password hashing)                 │
│  TLS:            rustls                                     │
│                                                              │
│  ── Observability ──                                        │
│  Logging:        tracing + tracing-subscriber              │
│  Metrics:        prometheus + metrics crate                │
│  Tracing:        opentelemetry + jaeger                    │
│                                                              │
│  ── Config ──                                               │
│  Config:         figment (TOML + env)                      │
│                                                              │
│  ── Deployment ──                                           │
│  Container:      Docker (multi-stage build)                │
│  Orchestration:  Kubernetes (Helm charts)                  │
│  CI/CD:          GitHub Actions                             │
│                                                              │
│  ── Python Interop ──                                       │
│  Binding:        PyO3 (Python ↔ Rust FFI)                  │
│  UDF:            Python UDFs via embedded interpreter      │
│  Admin UI:       FastAPI (existing Nova backend)           │
│                                                              │
│  ── Benchmarks ──                                           │
│  ClickBench:     https://github.com/ClickHouse/ClickBench  │
│  TPC-H:          datafusion benchmark suite                │
│  TPC-DS:         datafusion benchmark suite                │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Crate Dependency Graph

```
nova-engine (workspace)
├── nova-coordinator
│   ├── sqlparser-rs        # SQL parsing
│   ├── datafusion          # optimizer, planner
│   ├── tonic               # gRPC server (receive queries, dispatch to workers)
│   ├── axum                # REST API
│   ├── mysql_wire          # MySQL protocol server
│   ├── openraft            # Raft consensus
│   ├── foundationdb        # metadata store
│   ├── foyer               # query result cache
│   ├── argon2              # password hashing
│   └── tracing             # logging
│
├── nova-worker
│   ├── datafusion          # execution engine
│   ├── parquet             # Parquet R/W
│   ├── object_store        # S3/GCS/Azure
│   ├── foyer               # MP cache (hybrid)
│   ├── tonic               # gRPC client (receive fragments from coordinator)
│   ├── pyo3                # Python UDF
│   └── tracing             # logging
│
├── nova-common
│   ├── arrow               # Arrow types (shared)
│   ├── tonic               # protobuf definitions (shared)
│   ├── bincode             # serialization
│   └── thiserror           # error types
│
├── nova-storage
│   ├── parquet             # Parquet writer (MP creation)
│   ├── object_store        # S3/GCS/Azure
│   ├── foundationdb        # metadata operations
│   └── foyer               # cache integration
│
└── nova-cli
    ├── clap                # CLI argument parsing
    └── nova-common         # shared types
```

---

## 16. Development Roadmap

### Phase 1: Foundation (Month 1-3)
**Goal: "Can write and read data"**

```
├── Project setup: cargo workspace, CI, Docker
├── Metadata store (FDB/sled) schema + CRUD
├── Micro-partition writer (Arrow → Parquet → S3)
├── Micro-partition reader (S3 → Parquet → Arrow)
├── Basic SQL: CREATE TABLE, INSERT, SELECT (no optimizer)
├── MySQL protocol server (basic)
└── Benchmark: basic scan speed vs PostgreSQL
```

**Deliverable:** Can create table, insert data, run SELECT with filter.

### Phase 2: Query Engine (Month 4-5)
**Goal: "Fast analytical queries"**

```
├── DataFusion integration (scan, filter, project, join, agg, sort)
├── Custom MicroPartitionScanExec operator
├── Column pruning + predicate pushdown to Parquet
├── MP metadata pruning (zone maps: skip MPs by min/max stats)
├── Basic CBO (statistics, join reordering)
├── Parallel scan across multiple MPs
└── Benchmark: ClickBench single-node (target: match DataFusion)
```

**Deliverable:** Full SQL support, competitive with DataFusion on ClickBench.

### Phase 3: Snowflake Features (Month 6-7)
**Goal: "Time Travel, Clone, Streams"**

```
├── MVCC: version chains, snapshot isolation
├── Time Travel: AT(TIMESTAMP => ...) / BEFORE(STATEMENT => ...)
├── Zero-Copy Clone: CREATE TABLE ... CLONE
├── UPDATE/DELETE: copy-on-write MPs
├── Garbage collection (retention-based, background)
├── Streams: CREATE STREAM, SELECT FROM stream
├── Optional: MP merging (small MPs → large MP)
└── Benchmark: verify Time Travel correctness
```

**Deliverable:** All Snowflake parity features working.

### Phase 4: Distributed (Month 8-9)
**Goal: "Scale out to multiple nodes"**

```
├── Coordinator Raft consensus (openraft)
├── Worker pool (gRPC dispatch via tonic)
├── Distributed scan (MP partitioned across workers)
├── Shuffle join (partition by join key)
├── Colocated join (no shuffle for same-key tables)
├── Broadcast join (small table broadcast)
├── Adaptive join selection (runtime stats)
├── Auto-scaling (worker provision/decommission)
├── Warehouse concept (virtual compute clusters)
├── Auto-suspend/resume
└── Benchmark: TPC-H distributed
```

**Deliverable:** Multi-node cluster, elastic scaling.

### Phase 5: CBO Enhancement (Month 8-9, parallel with Phase 4)
**Goal: "Beat StarRocks optimizer"**

```
├── Advanced join reorder (beyond left-deep, bushy plans)
├── CTE reuse + materialization
├── Subquery decorrelation
├── Runtime filter injection (Bloom filter) in plan
├── Statistics: histograms, most-common-values
├── Cost model: CPU + memory + network
├── Dictionary encoding optimization (direct ops on encoded data)
├── Late materialization rule (selective column fetch)
└── Benchmark: TPC-DS (99 queries)
```

**Deliverable:** Optimizer competitive with StarRocks CBO.

### Phase 6: Cache & Polish (Month 10)
**Goal: "Production ready"**

```
├── Foyer hybrid cache integration (3-layer: result + metadata + MP)
├── Query result cache (Snowflake-style auto-invalidation)
├── RBAC: users, roles, grants
├── Auth: password (argon2), optional LDAP/OIDC
├── Backup/restore (snapshot FDB + S3)
├── Monitoring (Prometheus metrics, Grafana dashboards)
├── Nova UI integration (FastAPI → Nova Engine API)
├── HA: multi-AZ, FDB replication
├── Load testing + optimization
└── Benchmark: full ClickBench + TPC-DS + real workloads
```

**Deliverable:** Production-ready Nova Engine v0.1.0.

---

## Appendix A: Research References

| Paper/Source | Relevance |
|---|---|
| MonetDB/X100: Hyper-Pipelining Query Execution (CIDR 2005) | Vectorized execution model |
| Vectorwise: Beyond Column Stores (IEEE 2013) | Vectorized execution commercialization |
| Snowflake SIGMOD 2016 | Cloud-native DW architecture, micro-partitions |
| Snowflake NSDI 2020 | Elastic query engine on disaggregated storage |
| Snowflake Springer 2025 | Lessons learned, immutable files, background maintenance |
| Velox: Meta's Unified Execution Engine (VLDB 2022) | Reusable C++ execution components |
| Push vs Pull: Is It Really a Myth? | Push-based execution analysis |
| DuckDB Push-Based Execution (CMU 2023) | Push-based model implementation details |
| Materialization Strategies in Column-Oriented DBMS (ICDE 2007) | Late materialization |
| Selective Late Materialization (VLDB 2025) | Modern late materialization in DuckDB |
| Vertica: C-Store 7 Years Later (VLDB 2012) | Sideways information passing, late materialization |
| StarRocks CBO Documentation | Cascades framework, 99 TPC-DS support |
| StarRocks Vectorized Engine Blog | 7 categories of vectorization optimization |
| DataFusion ClickBench Blog (Nov 2024) | Fastest single-node Parquet engine |
| Foyer: Hybrid Cache for Rust | RisingWave case study, compaction-aware refill |
| Snowflake Query Result Cache Documentation | 24-hour cache, auto-invalidation |
| Snowflake FoundationDB Migration Blog | Metadata store at scale |
| FoundationDB Documentation | ACID KV store, ordered keys, multi-region |
