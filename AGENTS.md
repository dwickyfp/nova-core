# AGENTS.md — nova-core Guide for AI Coding Agents

> **READ THIS FILE COMPLETELY before making any changes to nova-core.**
> This is the single source of truth for any AI agent working on this project.

---

## What is nova-core?

nova-core is a **Rust-native analytical query engine** built on Apache Arrow + DataFusion. It is the core engine for the Nova platform (a Snowflake-grade management console).

**It is NOT:**
- A UI layer (that's the separate Nova frontend/backend)
- A fork of StarRocks (it is built from scratch on DataFusion)
- An OLTP engine (pure OLAP/columnar)
- A streaming engine (not a Flink/Kafka replacement)

**It IS:**
- A columnar OLAP query engine with Snowflake-grade features
- Built in Rust for memory safety and performance
- Designed around **immutable micro-partition storage** (Parquet in S3)
- Powered by DataFusion (the #1 fastest Parquet query engine per ClickBench 2024)

---

## Architecture Summary (MUST READ)

```
┌─────────────────────────────────────────────────────────────┐
│  CLIENT (MySQL protocol / REST API / Nova UI)              │
├─────────────────────────────────────────────────────────────┤
│  COORDINATOR (Rust, Raft HA)                               │
│  SQL Parser → Analyzer → CBO → Planner → Scheduler         │
│  Metadata Manager · Transaction Manager · Cache Manager    │
├─────────────────────────────────────────────────────────────┤
│  WORKER (Rust, stateless, auto-scale)                      │
│  DataFusion execution · Foyer hybrid cache · Storage I/O   │
├─────────────────────────────────────────────────────────────┤
│  STORAGE                                                   │
│  FoundationDB (metadata) · S3/MinIO (immutable Parquet MPs)│
└─────────────────────────────────────────────────────────────┘
```

**Full architecture doc:** [`docs/design/architecture.md`](docs/design/architecture.md) — **READ THIS before implementing anything.**

### Core Design Principles (NEVER VIOLATE)

1. **Immutable Storage** — Micro-partitions (Parquet files) are NEVER modified. UPDATE/DELETE creates new MPs; old ones retained for Time Travel. No in-place updates.
2. **Vectorized Push-Based Execution** — All operators process data in columnar batches (Arrow RecordBatch, default 8192 rows). Push model, not pull (Volcano).
3. **Separation of Compute & Storage** — Workers are stateless. All data in S3, all metadata in FoundationDB. Workers can be added/removed without data loss.
4. **Metadata-Driven Pruning** — Before reading Parquet files, check column min/max stats in FoundationDB metadata. Skip MPs that cannot match query predicates.
5. **MVCC Everywhere** — Every micro-partition has a commit timestamp. Queries see a consistent snapshot. This enables Time Travel, Clone, Streams, and result cache auto-invalidation.

---

## Project Structure

```
nova-core/
├── Cargo.toml                  # Workspace root
├── crates/
│   ├── nova-common/            # Shared types, errors, protobuf
│   ├── nova-coordinator/       # SQL parsing, optimization, scheduling
│   ├── nova-worker/            # Execution engine, cache, storage I/O
│   ├── nova-storage/           # Micro-partition R/W, metadata ops
│   └── nova-cli/               # CLI binary (nova-server, nova-worker)
├── docs/
│   ├── design/architecture.md  # Complete architecture (READ FIRST)
│   ├── guide/                  # Getting started, coding standards
│   └── research/papers.md      # Research references
├── skills/
│   └── nova-core-development/  # Agent skill (load this!)
│       └── SKILL.md
├── references/                 # Tech choices, perf targets, parity
└── ROADMAP.md                  # Development phases (MUST READ)
```

---

## Development Phase (CURRENT STATUS)

**We are in Phase 1: Foundation.**

See [ROADMAP.md](ROADMAP.md) for what's done, what's next, and what's blocked.

### Phase 1 Scope (DO NOT implement features from later phases)

✅ DO:
- Set up cargo workspace
- Implement FoundationDB metadata schema + CRUD
- Implement micro-partition writer (Arrow → Parquet → S3)
- Implement micro-partition reader (S3 → Parquet → Arrow)
- Basic SQL: CREATE TABLE, INSERT, SELECT (no optimizer yet)
- MySQL protocol server (basic)
- Unit tests + integration tests

❌ DON'T:
- Implement Time Travel, Clone, Streams (Phase 3)
- Implement distributed execution (Phase 4)
- Implement advanced CBO (Phase 5)
- Implement query result cache (Phase 6)
- Build UI or API endpoints (that's Nova frontend, not nova-core)
- Add dependencies not listed in the tech stack

---

## Coding Conventions

### Rust Style

```rust
// Type hints always
fn find_mp(table_id: u64, mp_id: u64) -> Result<Option<MicroPartitionMeta>> { ... }

// Use thiserror for error types
#[derive(Debug, thiserror::Error)]
enum NovaError {
    #[error("micro-partition not found: {mp_id}")]
    MpNotFound { mp_id: u64 },
    #[error("FDB transaction conflict: {txn_id}")]
    TransactionConflict { txn_id: u64 },
}

// Use Arc for shared state
struct Worker {
    cache: Arc<NovaCache>,
    storage: Arc<StorageAdapter>,
}

// Async everywhere (tokio runtime)
async fn read_mp(mp_id: u64) -> Result<RecordBatch> { ... }

// Document public APIs with rustdoc
/// Reads a micro-partition from cache or S3.
///
/// Checks Foyer hybrid cache first. On miss, reads from S3
/// and caches the result.
async fn read_mp(&self, mp_id: u64) -> Result<RecordBatch> { ... }

// Tests for all non-trivial logic
#[cfg(test)]
mod tests {
    #[test]
    fn test_mp_pruning() { ... }
}
```

### Naming Conventions

| Item | Convention | Example |
|---|---|---|
| Crates | `nova-{name}` | `nova-coordinator`, `nova-storage` |
| Modules | `snake_case` | `metadata`, `mp_writer`, `cache` |
| Structs | `PascalCase` | `MicroPartitionMeta`, `NovaCache` |
| Traits | `PascalCase` | `ExecutionPlan`, `OptimizationRule` |
| Functions | `snake_case` | `read_mp`, `prune_mps`, `commit_txn` |
| Constants | `SCREAMING_SNAKE` | `DEFAULT_BATCH_SIZE`, `MIN_MP_SIZE` |
| Enums | `PascalCase`, variants `PascalCase` | `JoinStrategy::Colocated` |
| Files | `snake_case.rs` | `mp_writer.rs`, `metadata.rs` |

### Dependencies Policy

- **Stdlib first** — if Rust stdlib can do it, don't add a crate
- **No new dependencies without justification** — every new crate must be discussed
- **Apache-2.0 or MIT license only**
- **Must be actively maintained** (commit within last 6 months)

### Testing Policy

- Every public function has unit tests
- Integration tests in `tests/` directory per crate
- Benchmark tests with `criterion` crate for performance-critical paths
- Tests run in CI: `cargo nextest run --all`

---

## Tech Stack (DO NOT substitute)

| Component | Crate | Status |
|---|---|---|
| SQL Parser | `sqlparser-rs` | Fixed |
| Query Engine | `DataFusion` | Fixed |
| Columnar Format | `arrow` | Fixed |
| Storage Format | `parquet` | Fixed |
| Metadata Store | `foundationdb` (prod), `sled` (dev) | Fixed |
| Object Storage | `object_store` | Fixed |
| Hybrid Cache | `foyer` | Fixed |
| Consensus | `openraft` | Fixed |
| RPC | `tonic` | Fixed |
| HTTP API | `axum` | Fixed |
| MySQL Protocol | `mysql_wire` | Fixed (evaluate alternatives if needed) |
| Python UDF | `pyo3` | Fixed |
| Runtime | `tokio` | Fixed |
| Serialization | `bincode` | Fixed |
| Error handling | `thiserror` | Fixed |
| Logging | `tracing` | Fixed |

**Do not add new dependencies without explicit approval.** If a feature needs a new crate, document why existing crates can't handle it.

---

## Key Concepts Glossary

| Term | Meaning |
|---|---|
| **MP** | Micro-Partition — a single immutable Parquet file in S3 (16-64MB) |
| **MVCC** | Multi-Version Concurrency Control — each MP has commit_ts, queries see consistent snapshot |
| **COW** | Copy-on-Write — UPDATE creates new MP, doesn't modify original |
| **GC** | Garbage Collection — delete expired MPs from S3 after retention period |
| **FDB** | FoundationDB — the metadata store (ACID KV) |
| **L1/L2/L3 Cache** | L1=Query Result, L2=Metadata, L3=MP Data (all via Foyer) |
| **Coordinator** | The "brain" — SQL parsing, optimization, scheduling, metadata |
| **Worker** | The "muscle" — execution engine, stateless, auto-scalable |
| **Warehouse** | Virtual compute cluster (group of workers) with auto-suspend/resume |
| **Time Travel** | Query historical data via `AT(TIMESTAMP => ...)` using MVCC version chains |
| **Clone** | Zero-copy table duplication via metadata copy (same S3 files) |
| **Stream** | CDC object that tracks table changes via MP version diff |
| **Late Materialization** | Delay fetching columns until after filtering, to reduce I/O |
| **Runtime Filter** | Bloom filter pushed from join build-side to scan-side for pre-filtering |

---

## Agent Workflow

When working on nova-core, follow this workflow:

1. **READ** this AGENTS.md file completely
2. **READ** [`docs/design/architecture.md`](docs/design/architecture.md) for full architecture context
3. **READ** [ROADMAP.md](ROADMAP.md) to understand current phase and scope
4. **LOAD** the `nova-core-development` skill: `skill_view(name='nova-core-development')`
5. **CHECK** current phase scope — do NOT implement features from future phases
6. **WRITE** code following the coding conventions above
7. **TEST** all changes: `cargo test --all`
8. **LINT**: `cargo clippy --all -- -D warnings`
9. **FORMAT**: `cargo fmt --all`
9. **COMMIT** with conventional commits: `feat(storage): implement MP writer`, `fix(coordinator): fix CBO crash on empty table`

---

## Common Mistakes to Avoid

1. **Modifying Parquet files in-place** — NEVER. Always create new MPs.
2. **Adding GC dependencies** — GC is independent; don't couple it to query execution.
3. **Storing data on workers** — Workers are stateless. All data in S3, all metadata in FDB.
4. **Using `unwrap()` in production code** — Use `?` with proper error types.
5. **Skipping tests** — Every public function needs tests. No exceptions.
6. **Adding dependencies without discussion** — Justify every new crate.
7. **Implementing future-phase features** — Stay in scope. Check ROADMAP.md.
8. **Mixing nova-core with Nova UI** — nova-core is the engine only. UI is separate.
9. **Using pull-based execution** — Always push-based (DataFusion Stream API).
10. **Forgetting MVCC timestamps** — Every MP MUST have commit_ts. No exceptions.
11. **Treating FDB as a SQL database** — FDB is a key-value store. Use range scans, not SQL.
12. **Storing credentials in code** — Use `figment` config (TOML + env vars).
13. **Ignoring cache invalidation** — Query result cache auto-invalidates via MVCC version. Never manual invalidation.
14. **Implementing compaction** — Nova uses GC (delete), not compaction (merge). Only optional MP merging for small files.
15. **Adding JVM or C++ components** — Pure Rust only. Python only via PyO3 for UDFs.

---

## How to Run

```bash
# Build
cargo build --release

# Run tests
cargo test --all
cargo nextest run --all

# Run clippy
cargo clippy --all -- -D warnings

# Format check
cargo fmt --all -- --check

# Run local dev cluster
docker compose -f docker/docker-compose.yml up -d

# Start coordinator
./target/release/nova-cli server --config config.toml

# Start worker
./target/release/nova-cli worker --config config.toml

# Connect
mysql -h 127.0.0.1 -P 4406 -u root
```

---

## Skill Reference

Load the `nova-core-development` skill for detailed implementation patterns:

```
skill_view(name='nova-core-development')
```

This skill contains:
- Micro-partition writer/reader implementation patterns
- FoundationDB schema operations
- DataFusion custom operator patterns
- Foyer cache integration patterns
- Test patterns and benchmarks
