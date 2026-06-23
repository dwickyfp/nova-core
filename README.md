# nova-core

> A Rust-native, cloud-native analytical query engine — built on Apache Arrow + DataFusion, inspired by Snowflake's architecture.
> Time Travel · Zero-Copy Clone · Streams (CDC) · MVCC · Vectorized Execution · Hybrid Cache

---

## What is nova-core?

nova-core is the **core query engine** for the Nova platform. It is NOT a management UI — it is the engine itself: storage, metadata, query planning, and execution.

### Why does it exist?

Existing open-source OLAP engines (StarRocks, ClickHouse) have fundamental architectural limitations that prevent Snowflake-grade features:

| Feature | StarRocks | ClickHouse | Snowflake | nova-core |
|---|---|---|---|---|
| Time Travel | ❌ (mutable storage) | ❌ | ✅ | ✅ (immutable MPs) |
| Zero-Copy Clone | ❌ | ❌ | ✅ | ✅ (metadata copy) |
| Streams (CDC) | ❌ (binlog dying) | ❌ | ✅ | ✅ (version diff) |
| Query Result Cache | ❌ | ❌ | ✅ | ✅ (MVCC auto-invalidate) |
| Memory Safety | ⚠️ (C++ UB) | ⚠️ (C++ UB) | ⚠️ (C++ UB) | ✅ (Rust) |
| No GC Pauses | ❌ (Java FE) | ✅ | ❌ (Java) | ✅ (Rust) |
| Open Source | ✅ | ✅ | ❌ | ✅ |
| Self-Hosted | ✅ | ✅ | ❌ | ✅ |

### The core insight

Snowflake's features (Time Travel, Clone, Streams) come from **immutable micro-partition storage**, not from the programming language. nova-core adopts this architecture:

```
Data is stored as IMMUTABLE Parquet files (micro-partitions) in S3/MinIO.
UPDATE/DELETE = create a NEW micro-partition (old one retained for Time Travel).
Clone = copy metadata only (same S3 files, zero data copy).
Stream = diff old vs new micro-partition versions.
Result cache = keyed by MVCC table version (auto-invalidates on data change).
```

### Performance foundation

nova-core is built on **Apache DataFusion** — the #1 fastest single-node engine for querying Parquet files (ClickBench, November 2024), beating DuckDB and ClickHouse. It is the first Rust-based engine to hold the top spot.

---

## Architecture (30-second overview)

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

---

## Tech Stack

| Component | Technology | Why |
|---|---|---|
| Language | **Rust** (edition 2024) | Memory safety, no GC, performance |
| SQL Parser | `sqlparser-rs` | ANSI SQL, extensible dialect |
| Query Engine | `DataFusion` | #1 ClickBench, vectorized, push-based |
| Columnar Format | `Apache Arrow` | Industry standard, zero-copy |
| Storage Format | `Parquet` | Columnar, compressed, stats in footer |
| Metadata Store | `FoundationDB` | ACID KV, proven at Snowflake scale |
| Object Storage | `object_store` crate | S3/GCS/Azure/MinIO abstraction |
| Hybrid Cache | `foyer` | RAM + SSD, used by RisingWave |
| Consensus | `openraft` | Coordinator HA, leader election |
| RPC | `tonic` (gRPC) | Coordinator ↔ Worker communication |
| HTTP API | `axum` | REST API for Nova UI |
| MySQL Protocol | `mysql_wire` | MySQL wire compatibility |
| Python UDF | `PyO3` | In-process Python FFI |
| Auth | `argon2` | Password hashing |

---

## Project Structure

```
nova-core/
├── Cargo.toml                  # Workspace root
├── README.md                   # This file
├── AGENTS.md                   # Guide for AI coding agents
├── CLAUDE.md                   # Claude Code specific guide
├── ROADMAP.md                  # Development roadmap (all phases)
├── Justfile                    # Task runner commands
│
├── crates/
│   ├── nova-common/            # Shared types, errors, protobuf
│   ├── nova-coordinator/       # SQL parsing, optimization, scheduling
│   ├── nova-worker/            # Execution engine, cache, storage I/O
│   ├── nova-storage/           # Micro-partition R/W, metadata ops
│   └── nova-cli/               # CLI binary (nova-server, nova-worker)
│
├── docs/
│   ├── design/
│   │   └── architecture.md     # Complete architecture document
│   ├── guide/
│   │   ├── getting-started.md  # Setup, build, first query
│   │   ├── contributing.md     # How to contribute
│   │   └── coding-standards.md # Rust conventions, patterns
│   └── research/
│       └── papers.md           # Research paper references
│
├── skills/
│   └── nova-core-development/  # Agent skill for nova-core dev
│       └── SKILL.md
│
├── references/
│   ├── tech-choices.md         # Why each technology was chosen
│   ├── performance-targets.md # Benchmark targets and methodology
│   └── snowflake-parity.md    # Feature parity tracking
│
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI
│
└── docker/
    ├── Dockerfile.coordinator
    ├── Dockerfile.worker
    └── docker-compose.yml      # Local dev: 1 coordinator + 2 workers + MinIO + FDB
```

---

## Quick Start

```bash
# Clone
git clone git@github.com:dwickyfp/nova-core.git
cd nova-core

# Build
cargo build --release

# Run local dev cluster (MinIO + FoundationDB + 1 coordinator + 2 workers)
docker compose -f docker/docker-compose.yml up -d

# Connect via MySQL protocol
mysql -h 127.0.0.1 -P 4406 -u root

# Create table and query
CREATE TABLE orders (id INT, amount DECIMAL(10,2), status VARCHAR(20), dt DATE);
INSERT INTO orders VALUES (1, 500.00, 'pending', '2026-06-23');
SELECT * FROM orders WHERE status = 'pending';

# Time Travel (query data as of 1 hour ago)
SELECT * FROM orders AT(TIMESTAMP => '2026-06-23 10:00:00');

# Zero-copy clone
CREATE TABLE orders_dev CLONE orders;

# Stream (CDC)
CREATE STREAM orders_stream ON TABLE orders;
SELECT * FROM orders_stream;
```

---

## Development Status

**Phase 1: Foundation** — In Progress

See [ROADMAP.md](ROADMAP.md) for detailed milestones.

---

## License

Apache License 2.0

---

## Research Foundation

This project is built on decades of database systems research. See [`docs/research/papers.md`](docs/research/papers.md) for the full list of papers that inform the architecture.

Key influences:
- **MonetDB/X100** (CIDR 2005) — Vectorized execution model
- **Snowflake** (SIGMOD 2016, NSDI 2020) — Cloud-native architecture, immutable micro-partitions
- **C-Store / Vertica** (VLDB 2012) — Late materialization, sideways information passing
- **DuckDB** (CMU 2023) — Push-based execution model
- **Apache DataFusion** (ClickBench 2024) — Fastest Rust Parquet engine
- **Foyer / RisingWave** — Hybrid cache for object storage
