# CLAUDE.md — nova-core Claude Code Guide

> This file provides Claude Code specific guidance for working on nova-core.
> It is read automatically by Claude Code at the start of every session.

---

## Project Identity

**nova-core** is a Rust-native analytical query engine. NOT a UI, NOT a fork, NOT an OLTP engine.
It is the core engine for the Nova platform, built from scratch on Apache Arrow + DataFusion.

---

## Quick Context (Read First)

Before writing any code, read these files in order:

1. **`AGENTS.md`** — Full project guide (architecture, conventions, scope)
2. **`ROADMAP.md`** — Current phase and what's in scope
3. **`docs/design/architecture.md`** — Complete architecture document

---

## Current Phase

**Phase 1: Foundation (Month 1-3)**

Scope: cargo workspace, FDB metadata, Parquet R/W, basic SQL, MySQL protocol.

Do NOT implement: Time Travel, Clone, Streams, distributed, CBO, cache.

---

## Build & Test Commands

```bash
# Build
cargo build --release

# Test (use nextest if available)
cargo test --all
cargo nextest run --all

# Lint (warnings are errors)
cargo clippy --all -- -D warnings

# Format
cargo fmt --all

# Format check (CI mode)
cargo fmt --all -- --check

# Run specific crate tests
cargo test -p nova-storage
cargo test -p nova-coordinator

# Run benchmarks
cargo bench -p nova-storage
```

---

## Architecture Rules (Never Violate)

### 1. Immutable Storage

```
✅ INSERT → write new Parquet MP to S3
✅ UPDATE → read MP, modify, write NEW MP (old retained)
✅ DELETE → read MP, remove rows, write NEW MP (old retained)
❌ NEVER modify a Parquet file in S3
❌ NEVER update rows in-place
```

### 2. MVCC Timestamps

Every micro-partition MUST have:
- `commit_ts` — when the transaction that created it committed
- `supersedes` — previous MP it replaces (None for pure inserts)
- `superseded_by` — next MP that replaces it (None = active)

### 3. Stateless Workers

Workers hold NO persistent state. All data in S3, all metadata in FDB.
Worker crash = re-schedule fragments. No data loss.

### 4. Push-Based Execution

Use DataFusion's `SendableRecordBatchStream` (push model).
Do NOT use Volcano-style `next()` pull model.

### 5. No Compaction

Nova uses GC (delete expired S3 files), not compaction (merge files).
Optional MP merging is for optimization only, not correctness.

---

## Crate Boundaries

```
nova-common     → Types, errors, protobuf (NO business logic)
nova-storage    → MP writer/reader, FDB metadata ops (NO SQL)
nova-coordinator → Parser, optimizer, planner, scheduler (NO execution)
nova-worker     → DataFusion execution, cache, storage I/O (NO SQL parsing)
nova-cli        → Binary entry points (NO business logic)
```

**Dependencies flow:** cli → coordinator/worker → storage → common

Do NOT create circular dependencies. If coordinator needs storage types, they go in `nova-common`.

---

## Implementation Patterns

### Writing a Micro-Partition

```rust
// CORRECT
async fn write_mp(table_id: u64, batches: Vec<RecordBatch>) -> Result<MicroPartitionMeta> {
    let mp_id = generate_id();
    let s3_path = format!("s3://nova/tables/{}/mp-{}-v1.parquet", table_id, mp_id);

    // 1. Write Parquet to S3
    let stats = parquet_writer::write(&s3_path, &batches).await?;

    // 2. Insert metadata into FDB
    let mp_meta = MicroPartitionMeta {
        mp_id,
        table_id,
        s3_path,
        commit_ts: now(),
        column_stats: stats,
        supersedes: None,
        superseded_by: None,
        active: true,
        ..Default::default()
    };
    fdb_insert(mp_meta.clone()).await?;

    Ok(mp_meta)
}

// WRONG — never modify existing MP
async fn update_mp(mp_id: u64, changes: Vec<Change>) -> Result<()> {
    let path = get_s3_path(mp_id);
    parquet_writer::modify_in_place(&path, changes).await?; // ❌ NEVER
}
```

### FoundationDB Operations

```rust
// CORRECT — range scan with prefix
let mps: Vec<MicroPartitionMeta> = fdb_get_range(
    format!("/table/{}/mp/", table_id),
    |meta| meta.active
).await?;

// WRONG — don't treat FDB as SQL
let mps = fdb_sql("SELECT * FROM mps WHERE table_id = ?", table_id).await?; // ❌
```

### DataFusion Custom Operator

```rust
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

struct MicroPartitionScanExec {
    mp_list: Vec<MicroPartitionMeta>,
    projection: Vec<usize>,
    predicate: Option<Expr>,
}

impl ExecutionPlan for MicroPartitionScanExec {
    fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // Push-based: return a stream that yields RecordBatches
        // ...
    }
}
```

### Cache with Foyer

```rust
// CORRECT — check cache before S3
async fn read_mp(&self, mp_id: u64) -> Result<RecordBatch> {
    let key = format!("{}:{}", mp_id, version);

    if let Some(cached) = self.cache.mp_cache.get(&key).await {
        return Ok(cached);  // cache HIT
    }

    let batch = self.storage.read_parquet(&s3_path).await?;
    self.cache.mp_cache.insert(key, batch.clone()).await;
    Ok(batch)
}

// WRONG — always read from S3
async fn read_mp(&self, mp_id: u64) -> Result<RecordBatch> {
    self.storage.read_parquet(&s3_path).await?  // ❌ no cache
}
```

---

## Testing Conventions

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mp_writer_creates_valid_parquet() {
        // Test that MP writer produces valid Parquet with correct stats
    }

    #[tokio::test]
    async fn test_fdb_metadata_crud() {
        // Test FDB insert/get/delete for MP metadata
    }

    #[tokio::test]
    async fn test_mp_pruning_with_range_predicate() {
        // Test that optimizer skips MPs where max(col) < predicate value
    }
}
```

- Unit tests in `src/**/tests.rs` or `src/**_test.rs`
- Integration tests in `tests/` directory
- Benchmarks in `benches/` directory using `criterion`
- Every public function must have tests
- Async tests use `#[tokio::test]`

---

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum NovaStorageError {
    #[error("micro-partition not found: table={table_id}, mp={mp_id}")]
    MpNotFound { table_id: u64, mp_id: u64 },

    #[error("FDB transaction conflict: txn_id={txn_id}")]
    TransactionConflict { txn_id: u64 },

    #[error("Parquet write failed: {source}")]
    ParquetWriteFailed {
        #[from]
        source: parquet::errors::ParquetError,
    },

    #[error("S3 operation failed: {source}")]
    S3OperationFailed {
        #[from]
        source: object_store::Error,
    },
}

pub type Result<T> = std::result::Result<T, NovaStorageError>;
```

- Use `thiserror` for error enums
- Use `?` operator for error propagation
- NEVER use `.unwrap()` in production code (tests are OK)
- Use `.context()` / `.map_err()` to add context when needed

---

## Commit Convention

```
feat(storage): implement micro-partition writer
fix(coordinator): fix CBO crash on empty table
test(worker): add cache hit/miss tests
docs(guide): update getting-started.md
refactor(common): consolidate error types
chore(ci): add clippy to CI pipeline
```

Format: `type(scope): description`

Types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `perf`, `style`
Scopes: `storage`, `coordinator`, `worker`, `common`, `cli`, `ci`, `docs`

---

## Performance Mindset

nova-core is a performance-critical system. Always think about:

1. **Allocations** — Avoid unnecessary `Vec` allocations in hot paths. Use `SmallVec` or reuse buffers.
2. **Zero-copy** — Use `Arc<RecordBatch>` to share data without cloning. Foyer provides zero-copy cache abstraction.
3. **SIMD** — Prefer operations on `PrimitiveArray<T>` that can be auto-vectorized by LLVM.
4. **Cache locality** — Batch processing (8192 rows) should fit in L1 cache (~32KB for INT64).
5. **Async I/O** — Use `tokio` for all I/O (S3, FDB). Never block the executor thread.
6. **Memory pool** — Workers have memory limits. Operators must spill to SSD when over budget.

---

## What NOT to Do

1. ❌ Don't add crates not in the tech stack without discussion
2. ❌ Don't implement features from future phases
3. ❌ Don't modify Parquet files in-place
4. ❌ Don't store data on workers
5. ❌ Don't use `unwrap()` in non-test code
6. `.unwrap()` ❌
7. ❌ Don't skip tests
8. ❌ Don't create circular crate dependencies
9. ❌ Don't store credentials in code — use `figment` config
10. ❌ Don't add JVM/C++ components — pure Rust only
11. ❌ Don't implement compaction — use GC (delete) + optional MP merge
12. ❌ Don't treat FDB as SQL — it's a KV store, use range scans
13. ❌ Don't use pull-based execution — always push-based
14. ❌ Don't forget MVCC timestamps on MPs
15. ❌ Don't implement manual cache invalidation — MVCC version is the invalidation key
16. ❌ Don't confuse nova-core with Nova UI — this is the engine, not the console

---

## When You're Done

After completing a task:

1. Run `cargo test --all` — all tests pass
2. Run `cargo clippy --all -- -D warnings` — no warnings
3. Run `cargo fmt --all -- --check` — formatted correctly
4. Update ROADMAP.md if a milestone is completed
5. Commit with conventional commit format
6. If implementing a new pattern, update the skill: `skills/nova-core-development/SKILL.md`
