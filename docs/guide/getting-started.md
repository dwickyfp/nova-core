# Getting Started — nova-core

> Setup, build, and run your first query on nova-core.

---

## Prerequisites

- **Rust** 1.75+ (use [rustup](https://rustup.rs))
- **Docker** + Docker Compose (for local dev cluster)
- **MySQL client** (for connecting via MySQL protocol)
- **Just** (task runner, optional but recommended: `cargo install just`)

---

## 1. Clone & Build

```bash
git clone git@github.com:dwickyfp/nova-core.git
cd nova-core

# Build (release mode for performance)
cargo build --release

# Or use Just
just build
```

## 2. Start Local Dev Cluster

The local dev cluster includes: FoundationDB + MinIO (S3-compatible) + 1 coordinator + 2 workers.

```bash
docker compose -f docker/docker-compose.yml up -d

# Verify
docker compose -f docker/docker-compose.yml ps
# Should show: fdb, minio, nova-coordinator, nova-worker-1, nova-worker-2
```

### Access Local Services

| Service | URL | Credentials |
|---|---|---|
| FoundationDB | localhost:4500 | (none) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API | http://localhost:9000 | minioadmin / minioadmin |
| Nova MySQL | localhost:4406 | root / (no password) |
| Nova REST API | http://localhost:8080 | (none) |

## 3. Connect & Query

```bash
# Connect via MySQL CLI
mysql -h 127.0.0.1 -P 4406 -u root

# Run your first query
nova> CREATE DATABASE demo;
nova> USE demo;
nova> CREATE TABLE orders (id INT, amount DECIMAL(10,2), status VARCHAR(20), dt DATE);
nova> INSERT INTO orders VALUES (1, 500.00, 'pending', '2026-06-23');
nova> SELECT * FROM orders WHERE status = 'pending';
```

## 4. Try Snowflake Features (Phase 3+)

```sql
-- Time Travel (query data as of 1 hour ago)
SELECT * FROM orders AT(TIMESTAMP => '2026-06-23 10:00:00');

-- Zero-copy clone
CREATE TABLE orders_dev CLONE orders;

-- Stream (CDC)
CREATE STREAM orders_stream ON TABLE orders;
SELECT * FROM orders_stream;
```

## 5. Run Tests

```bash
# All tests
cargo test --all

# Or with nextest (faster, better output)
cargo nextest run --all

# Specific crate
cargo test -p nova-storage

# Benchmarks
cargo bench -p nova-storage
```

## 6. Development Workflow

```bash
# Format
cargo fmt --all

# Lint (warnings are errors)
cargo clippy --all -- -D warnings

# Pre-commit check
just precommit
```

---

## Troubleshooting

### FoundationDB connection refused

```bash
# Check FDB is running
docker exec nova-fdb fdbcli status

# If not running, restart
docker compose -f docker/docker-compose.yml restart fdb
```

### MinIO connection refused

```bash
# Check MinIO is running
docker logs nova-minio

# Recreate buckets
docker exec nova-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec nova-minio mc mb local/nova
```

### Build errors

```bash
# Clean build
cargo clean
cargo build --release

# Update dependencies
cargo update
```
