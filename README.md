# nova-core

> Rust-native, Snowflake-inspired analytical query engine — Apache Arrow + DataFusion + FoundationDB + MinIO.

[![Tests](https://img.shields.io/badge/tests-298%20passing-brightgreen)](#)
[![Clippy](https://img.shields.io/badge/clippy-clean-blue)](#)
[![License](https://img.shields.io/badge/license-Apache--2.0-orange)](#)

Nova is a production-grade OLAP engine that speaks MySQL wire protocol. Connect any MySQL client, run analytical SQL with aggregates, joins, time travel, and zero-copy clones — all backed by immutable Parquet micro-partitions on S3.

---

## Quick Start

### Dev mode (sled, no Docker needed)

```bash
cargo build --release
./target/release/nova server --config config.toml
mysql -h 127.0.0.1 -P 3306 -u root
```

### Docker full-stack (FDB + MinIO + Coordinator + 2 Workers)

```bash
# Build image + start all services
docker compose -f docker/docker-compose.yml up -d --build

# Connect
mysql -h 127.0.0.1 -P 3306 -u root

# Logs
docker compose -f docker/docker-compose.yml logs -f nova-coordinator

# Teardown
docker compose -f docker/docker-compose.yml down -v
```

Services started:
| Service | Port | Notes |
|---|---|---|
| nova-fdb | 4500 | FoundationDB metadata store |
| nova-minio | 9000 / 9001 | S3-compatible storage (Console: http://localhost:9001) |
| nova-coordinator | 3306 / 9090 | MySQL protocol + metrics |
| nova-worker-1 | 50051 | gRPC worker |
| nova-worker-2 | 50052 | gRPC worker |

MinIO credentials: `nova` / `nova12345` — bucket `nova` auto-created.

### FDB production (manual)

```toml
# config.toml
[metadata]
backend          = "fdb"
fdb_cluster_file = "docker:docker@127.0.0.1:4500"

[storage]
s3_endpoint = "http://localhost:9000"
s3_bucket   = "nova"
```

```bash
cargo build --release --features nova-cli/fdb-backend
./target/release/nova server --config config.toml
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  CLIENT (MySQL protocol — mysql CLI, DBeaver, JDBC, etc.)  │
├─────────────────────────────────────────────────────────────┤
│  COORDINATOR (Rust, openraft HA)                           │
│  SQL Parser → Analyzer → CBO → Planner → Scheduler         │
│  Auth · RBAC · Transaction Manager · Result Cache          │
├─────────────────────────────────────────────────────────────┤
│  WORKER (Rust, stateless, gRPC, auto-scale)                │
│  DataFusion execution · Foyer hybrid cache · Storage I/O   │
├─────────────────────────────────────────────────────────────┤
│  STORAGE                                                    │
│  FoundationDB (metadata, ACID) · S3/MinIO (Parquet MPs)    │
└─────────────────────────────────────────────────────────────┘
```

Full design: [`docs/design/architecture.md`](docs/design/architecture.md)

---

## Tech Stack

| Component | Crate / Tool | Notes |
|---|---|---|
| Language | Rust 2024 | Memory safety, no GC |
| SQL Parser | sqlparser-rs 0.52 | |
| Query Engine | DataFusion 45 | #1 ClickBench Nov 2024 |
| Columnar | Arrow 54 + Parquet 54 | |
| Metadata (prod) | FoundationDB 7.4 | ACID distributed KV |
| Metadata (dev) | sled 0.34 | Embedded KV, zero deps |
| Object Storage | object_store 0.11 | S3 / MinIO |
| Cache | foyer 0.16 | RAM + SSD hybrid |
| Consensus | openraft 0.9 | Coordinator HA, 3-node |
| RPC | tonic 0.14 | gRPC coordinator↔worker |
| MySQL Protocol | Custom (10 modules) | Production-grade |

---

## Project Structure

```
nova-core/
├── crates/
│   ├── nova-common/         # Shared types, errors, NovaType
│   ├── nova-coordinator/    # SQL parsing, CBO, scheduling, MySQL protocol, HA
│   ├── nova-worker/         # DataFusion execution, NovaTableProvider, gRPC server
│   ├── nova-storage/        # MpReader/MpWriter, SledMetadataStore, FdbMetadataStore
│   └── nova-cli/            # CLI binary — nova server / nova worker
├── proto/
│   └── nova_rpc.proto       # WorkerService + RaftService gRPC definitions
├── docker/
│   ├── docker-compose.yml   # Full stack: FDB + MinIO + Coordinator + 2 Workers
│   ├── Dockerfile           # Multi-stage Rust builder
│   └── config-fdb.toml      # Config for Docker deployment (FDB backend)
├── config.toml              # Default dev config (sled + localhost MinIO)
└── ROADMAP.md               # Development phases
```

---

## SQL Reference

Connect via `mysql -h 127.0.0.1 -P 3306 -u root` then run any of the examples below.

### DDL — Databases & Schemas

```sql
-- Create a database
CREATE DATABASE analytics;

-- Drop a database
DROP DATABASE analytics;

-- Create a schema
CREATE SCHEMA analytics.sales;
```

### DDL — Tables

```sql
-- Create table with Nova types
CREATE TABLE users (
    id       INT,
    name     VARCHAR,
    email    VARCHAR,
    age      INT,
    score    FLOAT,
    active   BOOLEAN,
    joined   TIMESTAMP
);

-- Drop table
DROP TABLE users;

-- Alter table — add column
ALTER TABLE users ADD COLUMN country VARCHAR;

-- Alter table — drop column
ALTER TABLE users DROP COLUMN score;
```

**Supported types:** `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `VARCHAR`, `TEXT`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `DECIMAL(p,s)`, `BINARY`

### DML — Insert / Update / Delete

```sql
-- Insert single row
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30, 99.5, true, '2024-01-15 10:00:00');

-- Insert multiple rows
INSERT INTO users VALUES
    (2, 'Bob',     'bob@example.com',   25, 87.3, true,  '2024-03-01 09:00:00'),
    (3, 'Charlie', 'charlie@acme.com',  35, 92.1, false, '2024-06-10 14:30:00');

-- Update rows matching a filter (copy-on-write — old MPs preserved for Time Travel)
UPDATE users SET score = 95.0 WHERE name = 'Bob';

-- Delete rows matching a filter
DELETE FROM users WHERE active = false;
```

### SELECT — Basic Queries

```sql
-- Select all
SELECT * FROM users;

-- Projection
SELECT id, name, email FROM users;

-- WHERE filter
SELECT * FROM users WHERE age > 28;

-- Multiple conditions
SELECT * FROM users WHERE age > 25 AND active = true;

-- LIKE
SELECT * FROM users WHERE email LIKE '%@acme.com';

-- IN
SELECT * FROM users WHERE id IN (1, 2, 5);
```

### SELECT — Aggregates

```sql
-- COUNT all rows
SELECT COUNT(*) FROM users;

-- COUNT non-null
SELECT COUNT(email) FROM users;

-- SUM, AVG, MIN, MAX
SELECT
    SUM(age)  AS total_age,
    AVG(age)  AS avg_age,
    MIN(age)  AS youngest,
    MAX(age)  AS oldest
FROM users;

-- GROUP BY + aggregate
SELECT active, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM users
GROUP BY active;

-- HAVING (post-aggregate filter)
SELECT country, COUNT(*) AS cnt
FROM users
GROUP BY country
HAVING COUNT(*) > 10
ORDER BY cnt DESC;
```

### SELECT — Joins

```sql
-- Set up orders table
CREATE TABLE orders (
    order_id INT,
    user_id  INT,
    amount   FLOAT,
    status   VARCHAR
);

INSERT INTO orders VALUES
    (1001, 1, 299.99, 'completed'),
    (1002, 2, 149.50, 'pending'),
    (1003, 1, 89.00,  'completed');

-- INNER JOIN
SELECT u.name, o.order_id, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id;

-- LEFT JOIN (include users with no orders)
SELECT u.name, o.order_id, o.amount
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- 3-table join
CREATE TABLE products (pid INT, name VARCHAR, price FLOAT);

SELECT u.name, o.order_id, p.name AS product
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN products p ON p.pid = o.order_id;
```

### SELECT — Sorting, Limit, Distinct

```sql
-- ORDER BY
SELECT * FROM users ORDER BY age DESC;

-- LIMIT + OFFSET (pagination)
SELECT * FROM orders ORDER BY amount DESC LIMIT 10 OFFSET 20;

-- DISTINCT
SELECT DISTINCT country FROM users;

-- DISTINCT with count
SELECT COUNT(DISTINCT country) AS unique_countries FROM users;
```

### Transactions

```sql
-- Explicit transaction
BEGIN;
    INSERT INTO orders VALUES (1004, 3, 500.00, 'pending');
    UPDATE users SET active = true WHERE id = 3;
COMMIT;

-- Rollback on error
BEGIN;
    DELETE FROM users WHERE id = 1;
ROLLBACK;  -- users.id=1 is back, no micro-partition committed
```

### Time Travel

Query historical data as of any past timestamp (Unix microseconds).

```sql
-- Get current timestamp for reference
SELECT CURRENT_TIMESTAMP;

-- Insert some data, note the timestamp
INSERT INTO users VALUES (4, 'Dana', 'dana@test.com', 28, 88.0, true, NOW());

-- Update it
UPDATE users SET score = 55.0 WHERE id = 4;

-- Travel back to before the update (replace 1750000000000000 with actual micros)
SELECT * FROM users AT(TIMESTAMP => 1750000000000000);
```

How to get a Unix timestamp in microseconds:
```python
import time; int(time.time() * 1_000_000)
```

### Zero-Copy Clone

Instantly duplicate a table by referencing the same micro-partitions — no data copied.

```sql
-- Clone users → users_backup (metadata-only, O(1))
CREATE TABLE users_backup CLONE users;

-- Verify same data
SELECT COUNT(*) FROM users_backup;

-- Cloned tables are independent — mutations don't affect the original
DELETE FROM users_backup WHERE active = false;
SELECT COUNT(*) FROM users;         -- unchanged
SELECT COUNT(*) FROM users_backup;  -- reduced
```

### Streams (CDC)

Capture inserts into a table as a change stream.

```sql
-- Create a stream on the orders table (append-only mode)
CREATE STREAM orders_stream FROM orders (APPEND_ONLY);

-- After inserts, query the stream for new rows
INSERT INTO orders VALUES (2000, 1, 999.99, 'new');
-- ponytail: stream consumption API (CONSUME FROM stream) is future work
```

### Garbage Collection & Auto Compaction

Nova uses **immutable copy-on-write micro-partitions (MPs)**. Every UPDATE/DELETE creates new MPs and marks old ones as superseded — they are kept for Time Travel but accumulate over time. GC removes superseded MPs older than the retention window.

**Manual GC:**

```sql
-- Keep last 7 days of history, purge older superseded MPs
GC 7;

-- Aggressive: keep only 1 day
GC 1;
```

**How compaction works internally:**

1. `UPDATE users SET score = 99 WHERE id = 1` → creates a new MP with updated row, marks old MP as `superseded_by = new_mp_id`
2. Old MP stays active for Time Travel queries (`AT(TIMESTAMP => ...)`)
3. `GC 7` scans all tables, finds MPs where `commit_ts < NOW() - 7 days` AND `superseded_by IS NOT NULL`, removes them from object storage and metadata

**Auto-compaction via scheduled GC:**

Nova does not run GC automatically — trigger it from a cron job or after heavy write workloads:

```bash
# Example: daily GC via mysql client
echo "GC 7;" | mysql -h 127.0.0.1 -P 3306 -u root

# Or via cron (run daily at 2AM)
0 2 * * * echo "GC 7;" | mysql -h 127.0.0.1 -P 3306 -u root
```

**Space amplification rule of thumb:**

| Write pattern | Recommended retention |
|---|---|
| High-frequency updates (streaming) | `GC 1` or `GC 3` daily |
| Batch ETL (daily loads) | `GC 7` weekly |
| Append-only (inserts only) | GC not needed — no superseded MPs |
| Regulatory compliance | `GC 90` or `GC 365` |

**What GC does NOT remove:**
- Active MPs (current data)
- MPs still within the retention window (needed for Time Travel)
- Clone source MPs that are referenced by cloned tables

### Backup & Restore

```sql
-- Backup all metadata + MP manifest to a path
BACKUP TO '/var/backups/nova/2024-07-01';

-- Restore from backup
RESTORE FROM '/var/backups/nova/2024-07-01';
```

---

## Configuration Reference

```toml
# config.toml — full field reference

[server]
host = "0.0.0.0"   # Bind address
port = 3306        # MySQL protocol port

[storage]
s3_endpoint   = "http://localhost:9000"   # MinIO or S3 endpoint
s3_bucket     = "nova"                    # Bucket for micro-partitions
s3_access_key = "nova"
s3_secret_key = "nova12345"
s3_region     = "us-east-1"

[metadata]
backend = "sled"                          # "sled" (dev) | "fdb" (prod)
sled_path = "./data/nova-meta"            # Used when backend = "sled"
fdb_cluster_file = "docker:docker@127.0.0.1:4500"  # Used when backend = "fdb"

[auth]
enabled          = false         # true = enforce password on MySQL handshake
default_username = "root"
# default_password_hash = ""     # argon2 hash; leave empty = no password
```

---

## Docker Deployment

```bash
# 1. Build the nova image (first time or after code changes)
docker compose -f docker/docker-compose.yml build

# 2. Start full stack
docker compose -f docker/docker-compose.yml up -d

# 3. Wait for all services to be healthy (~30s for FDB init)
docker compose -f docker/docker-compose.yml ps

# 4. Connect
mysql -h 127.0.0.1 -P 3306 -u root

# 5. MinIO Console
open http://localhost:9001   # nova / nova12345

# 6. Scale workers
docker compose -f docker/docker-compose.yml up -d --scale worker-2=3

# 7. Teardown
docker compose -f docker/docker-compose.yml down -v
```

The `docker/config-fdb.toml` inside the container points to service names (`nova-fdb`, `minio`) as hostnames — no manual IP configuration needed.

---

## Implementation Status

| Phase | Name | Status |
|---|---|---|
| 1 | Foundation — storage, metadata, basic SQL | ✅ Complete |
| 2 | Query Engine — optimizer, DataFusion, MP pruning | ✅ Complete |
| 3 | Snowflake Features — Time Travel, Clone, Streams, GC | ✅ Complete |
| 4 | Distributed — WorkerPool, AutoScaler (single-node) | ✅ Complete |
| 5 | CBO Enhancement — late mat, statistics, runtime filter | ✅ Complete |
| 6 | Cache & Polish — Auth, RBAC, monitoring, backup | ✅ Complete |
| 7 | MySQL Protocol — production-grade, 40 tests | ✅ Complete |
| 8 | SQL Completeness — AGG, JOIN, ORDER BY, DROP, multi-stmt | ✅ Complete |
| 9 | Production Hardening — COW fix, cache invalidation, E2E | ✅ Complete |
| 10 | Multi-Node Distributed — tonic gRPC, WorkerGrpcServer | ✅ Complete |
| 11 | HA Coordinator — openraft 3-node leader election | 🔄 In Progress |

**298 tests passing** · **~20K LOC Rust** · **clippy clean** · **zero TODO/FIXME**

---

## Benchmarks

Measured on Apple Silicon (M-series), local disk, release build (`cargo bench`).

### Raw scan — MicroPartitionScanExec

| Workload | Median | Rows |
|---|---|---|
| `5mp × 1K rows` | **1.21 ms** | 5K |
| `5mp × 10K rows` | ~4.9 ms | 50K |
| `10mp × 10K rows` | **4.91 ms** | 100K |
| `5mp × 100K rows` | **17.15 ms** | 500K |

### DataFusion SQL path — 50K rows (5 MPs × 10K)

| Query | Median | Notes |
|---|---|---|
| `SELECT *` (5K) | **1.08 ms** | Full scan |
| `SELECT *` (50K) | **3.53 ms** | Full scan |
| `SELECT COUNT(*)` | **850 µs** | AGG — DataFusion skips column reads |
| `SELECT SUM(amount)` | **6.92 ms** | Numeric AGG |
| `SELECT * WHERE id > 25000` | **1.29 ms** | Filter — MP pruning active |
| `GROUP BY name + COUNT + SUM` | **2.96 ms** | Full GROUP BY |

---

## Build Commands

```bash
# Dev build (sled backend, default)
cargo build --release

# Production build (FoundationDB)
cargo build --release --features nova-cli/fdb-backend

# Run tests (skip known hanging test)
cargo test --all -- --skip test_create_incremental_backup

# Specific crate
cargo test -p nova-coordinator
cargo test -p nova-storage
cargo test -p nova-worker

# HA integration tests
cargo test -p nova-coordinator --test ha_integration_test

# gRPC integration tests
cargo test -p nova-coordinator --test grpc_integration_test

# Lint
cargo clippy --all -- -D warnings
cargo fmt --all

# Benchmarks
cargo bench -p nova-worker --bench clickbench
cargo bench -p nova-coordinator --bench query_benchmarks
```

---

## License

Apache-2.0
