# nova-core

Rust-native, Snowflake-inspired analytical query engine built on Apache Arrow + DataFusion.

## Quick Start

```bash
# 1. Start infrastructure (MinIO + FoundationDB)
docker compose up -d

# 2. Build (default: sled backend for dev)
cargo build --release

# 3. Run
./target/release/nova server --config config.toml

# 4. Connect via MySQL protocol
mysql -h 127.0.0.1 -P 3306 -u root
```

## Metadata Backend

nova-core supports two metadata backends, controlled by `config.toml`:

### Sled (default, dev)
Embedded KV store. Zero external dependencies. Single binary.

```toml
[metadata]
backend = "sled"
sled_path = "./data/nova-meta"
```

### FoundationDB (production)
Distributed ACID KV store. Requires Docker.

```toml
[metadata]
backend = "fdb"
fdb_cluster_file = "docker:docker@127.0.0.1:4500"
```

Build with FDB support:
```bash
cargo build --release --features nova-cli/fdb-backend
```

Start FoundationDB via Docker:
```bash
docker compose up -d fdb
# Initialize FDB database (first time only):
docker compose run --rm init
```

## Architecture

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

Full architecture: [`docs/design/architecture.md`](docs/design/architecture.md)

## Tech Stack

| Component | Crate | Notes |
|---|---|---|
| Language | Rust 2024 | Memory safety, no GC |
| SQL Parser | sqlparser-rs 0.52 | |
| Query Engine | DataFusion 45 | #1 ClickBench Nov 2024 |
| Columnar | Arrow 54 + Parquet 54 | |
| Metadata (prod) | FoundationDB 7.4 | ACID distributed KV |
| Metadata (dev) | sled 0.34 | Embedded KV |
| Object Storage | object_store 0.11 | S3/MinIO |
| Cache | foyer 0.16 | RAM + SSD hybrid |
| Consensus | openraft 0.9 | Coordinator HA |
| RPC | tonic 0.12 | gRPC |
| MySQL Protocol | Custom (10 modules) | Production-grade |

## Project Structure

```
nova-core/
├── crates/
│   ├── nova-common/         # Shared types, errors
│   ├── nova-coordinator/    # SQL parsing, optimization, scheduling, MySQL protocol
│   ├── nova-worker/         # Execution engine, custom DataFusion operators
│   ├── nova-storage/        # Micro-partition R/W, metadata (sled + FDB)
│   └── nova-cli/            # CLI binary (nova server)
├── docker-compose.yml       # FoundationDB + MinIO
├── config.toml              # Server configuration
├── docs/design/             # Architecture documentation
└── ROADMAP.md               # Development phases
```

## Implementation Status

### Phase 1: Foundation ✅
- [x] Cargo workspace (5 crates, 15K+ LOC)
- [x] SledMetadataStore (815 lines, fully implemented)
- [x] FdbMetadataStore (590 lines, feature-gated `fdb-backend`)
- [x] FoundationDB Docker container + init
- [x] MpWriter (Arrow → Parquet → S3)
- [x] MpReader (S3 → Parquet → Arrow)
- [x] SQL Parser (CREATE DATABASE/TABLE, INSERT, SELECT, UPDATE, DELETE)
- [x] Analyzer (name resolution, type checking)
- [x] Executor (DDL, DML, SELECT with WHERE)
- [x] MySQL wire protocol (10 modules, 40 tests, production-grade)
- [x] Config switch (sled/fdb via config.toml)
- [x] 258 tests

### Phase 2: Query Engine ✅
- [x] NovaOptimizer (171 lines) — MP pruning wired into exec_select
- [x] QueryPlanner (pass-through for single-node)
- [x] QueryScheduler (local execution via Executor)
- [x] NovaEngine pipeline: Parser → Analyzer → Planner → Scheduler → Executor
- [x] MP pruning active: WHERE filter → skip MPs that can't match
- [x] build_arrow_schema: NovaType → Arrow DataType mapping
- [x] 5 new tests (optimizer, planner, scheduler)
- [ ] DataFusion SessionContext integration (future)
- [ ] CBO join reordering wired (code exists, not connected)
- [ ] Statistics collection wired (code exists, not connected)

### Phase 3: Snowflake Features ⚠️ (partial)
- [x] Transaction Manager (MVCC, 248 lines) — NOT wired
- [x] UPDATE/DELETE (COW in executor)
- [x] Time Travel (get_mps_at_timestamp)
- [ ] Clone (metadata exists, no SQL syntax)
- [ ] Streams (metadata exists, no SQL syntax)
- [ ] GC (delete expired MPs)
- [ ] Wire txn manager into executor

### Phase 4: Distributed ⚠️ (code exists, NOT wired)
- [x] Raft (209 lines) — NOT wired
- [x] Distributed exec (321 lines) — NOT wired
- [x] Worker Pool (283 lines) — NOT wired
- [x] Auto-scaling (284 lines) — NOT wired
- [ ] gRPC protobuf definitions
- [ ] Worker binary (nova worker command)
- [ ] Wire worker pool → scheduler → distributed exec

### Phase 5: CBO Enhancement ⚠️ (code exists, NOT wired)
- [x] Runtime Filter / Bloom (255 lines) — NOT wired
- [x] Late Materialization (289 lines) — NOT wired
- [x] Advanced Stats: Histograms + MCV (342 lines) — NOT wired
- [ ] Dictionary Encoding
- [ ] Wire all into optimizer pipeline

### Phase 6: Cache & Polish ⚠️ (code exists, NOT wired)
- [x] 3-layer Foyer Cache (485 lines) — NOT wired
- [x] Result Cache (269 lines) — NOT wired
- [x] RBAC (361 lines) — NOT wired
- [x] Monitoring / Prometheus (391 lines) — NOT wired
- [x] HA / Health Check (360 lines) — NOT wired
- [x] Backup / Restore (355 lines) — NOT wired
- [ ] Auth — STUB (20 lines)
- [ ] storage/cache.rs — STUB (24 lines)
- [ ] Wire all into server startup

### Phase 7: MySQL Wire Protocol ✅
- [x] Production-grade MySQL protocol (10 modules, 40 tests)
- [x] Handshake V10, auth (mysql_native_password + caching_sha2_password)
- [x] COM_QUERY, COM_PING, COM_QUIT, COM_INIT_DB, COM_FIELD_LIST
- [x] CLIENT_QUERY_ATTRIBUTES support
- [x] ResultSet, column definitions, EOF/OK packets
- [x] mysql-connector-python (use_pure=True) compatibility verified

## Build Commands

```bash
# Default build (sled backend)
cargo build --release

# FDB build
cargo build --release --features nova-cli/fdb-backend

# Tests
cargo test -p nova-common
cargo test -p nova-storage
cargo test -p nova-coordinator

# Lint
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
```

## Docker Infrastructure

```yaml
# docker-compose.yml services:
- fdb:        FoundationDB 7.4.0  (port 4500)
- minio:      MinIO S3            (ports 9000, 9001)
- init:       FDB database init   (one-shot)
- createbucket: MinIO bucket init (one-shot)
```

## License

Apache-2.0
