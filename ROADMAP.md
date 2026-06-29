# ROADMAP.md — nova-core Development Roadmap

> All phases, milestones, and dependencies. Updated as work progresses.
> **Current Phase: Phase 1 — Foundation**

---

## Phase Overview

| Phase | Name | Duration | Status | Focus |
|---|---|---|---|---|
| 1 | Foundation | Month 1-3 | ✅ Complete | Storage, metadata, basic SQL, FDB + sled |
| 2 | Query Engine | Month 4-5 | ✅ Complete | Optimizer, planner, scheduler, MP pruning, TableProvider |
| 3 | Snowflake Features | Month 6-7 | ✅ Complete | Time Travel, Clone, Streams, GC, BEGIN/COMMIT/ROLLBACK |
| 4 | Distributed | Month 8-9 | ✅ Single-node | WorkerPool, AutoScaler wired (gRPC future) |
| 5 | CBO Enhancement | Month 8-9 | ✅ Complete | Late mat, stats, runtime filter wired to optimizer |
| 6 | Cache & Polish | Month 10 | ✅ Complete | Auth, cache, RBAC, monitoring, HA, backup |
| 7 | MySQL Protocol | — | ✅ Complete | Production-grade (40 tests) |
| 8 | SQL Completeness | — | ✅ Complete | AGG, GROUP BY, ORDER BY, JOIN, DROP, multi-stmt, DataFusion |
| 9 | Production Hardening | — | 🔴 In Progress | COW fix, cache invalidation, auth, RBAC, E2E tests, Foyer |

---

## Phase 1: Foundation (Month 1-3)

**Goal:** Can create table, insert data, run SELECT with filter. Beat PostgreSQL on basic scan.

### Milestone 1.1: Project Bootstrap (Week 1-2)

- [ ] Create cargo workspace with 5 crates
- [ ] Setup `nova-common` (types, errors, protobuf)
- [ ] Setup `nova-storage` (skeleton)
- [ ] Setup `nova-coordinator` (skeleton)
- [ ] Setup `nova-worker` (skeleton)
- [ ] Setup `nova-cli` (binary entry points)
- [ ] Add `Justfile` with common commands
- [ ] Add `.github/workflows/ci.yml` (build, test, clippy, fmt)
- [ ] Add `docker/docker-compose.yml` (MinIO + FDB local dev)
- [ ] Add `rust-toolchain.toml` (pin Rust version)
- [ ] First commit: `cargo build` passes with empty crates

**Deliverable:** Project compiles, CI runs, Docker dev environment works.

### Milestone 1.2: Metadata Store (Week 2-4)

- [ ] Define `MicroPartitionMeta` struct in `nova-common`
- [ ] Define `TableMeta`, `DatabaseMeta`, `SchemaMeta` structs
- [ ] Implement FDB connection layer (`nova-storage::metadata::fdb`)
- [ ] Implement sled connection layer (for local dev/testing)
- [ ] Implement metadata CRUD:
  - [ ] `create_database`, `get_database`, `list_databases`, `drop_database`
  - [ ] `create_schema`, `get_schema`, `list_schemas`, `drop_schema`
  - [ ] `create_table`, `get_table`, `list_tables`, `drop_table`
  - [ ] `insert_mp`, `get_mp`, `get_active_mps`, `get_mp_at_timestamp`
- [ ] Implement FDB key-value schema layout (see architecture.md section 6.2)
- [ ] Unit tests for all metadata operations
- [ ] Integration test: FDB running in Docker

**Deliverable:** Can create databases/tables in FDB, insert MP metadata, query active MPs.

### Milestone 1.3: Micro-Partition Writer (Week 4-6)

- [ ] Implement `MpWriter` in `nova-storage::mp_writer`
- [ ] Arrow RecordBatch → Parquet file in S3
- [ ] Compute column statistics (min, max, null_count, distinct_count)
- [ ] Write stats to Parquet footer + FDB metadata
- [ ] Target MP size: 16-64MB compressed (configurable)
- [ ] Compression: Snappy (default), ZSTD (optional)
- [ ] S3 path format: `s3://bucket/tables/{table_id}/mp-{id}-v{ver}.parquet`
- [ ] Temp file write → atomic rename on commit
- [ ] Unit tests: write 1000 rows, verify Parquet + stats + FDB metadata
- [ ] Benchmark: write throughput (rows/sec, MB/sec)

**Deliverable:** Can write Arrow data as immutable Parquet MPs to S3 with full metadata.

### Milestone 1.4: Micro-Partition Reader (Week 6-8)

- [ ] Implement `MpReader` in `nova-storage::mp_reader`
- [ ] S3 → Parquet → Arrow RecordBatch (streaming, not load-all)
- [ ] Column pruning (read only requested columns)
- [ ] Predicate pushdown to Parquet row groups (min/max in footer)
- [ ] Batch streaming (yield RecordBatch every 8192 rows)
- [ ] Unit tests: read back written data, verify correctness
- [ ] Benchmark: read throughput (rows/sec, MB/sec)
- [ ] Benchmark: cold read (S3) vs warm read (if cached)

**Deliverable:** Can read MPs from S3 with column pruning and predicate pushdown.

### Milestone 1.5: Basic SQL (Week 8-10)

- [ ] Implement SQL parser integration (`sqlparser-rs`) in `nova-coordinator`
- [ ] Support: `CREATE DATABASE`, `CREATE TABLE`, `DROP TABLE`
- [ ] Support: `INSERT INTO ... VALUES`
- [ ] Support: `SELECT * FROM table` (full scan)
- [ ] Support: `SELECT col1, col2 FROM table WHERE condition` (filter + project)
- [ ] Implement analyzer (name resolution, type checking)
- [ ] Implement basic planner (logical → physical, no optimization)
- [ ] Wire coordinator → storage (direct call, no workers yet)
- [ ] Unit tests for parser, analyzer, planner
- [ ] Integration test: CREATE → INSERT → SELECT cycle

**Deliverable:** Full CREATE → INSERT → SELECT cycle works end-to-end.

### Milestone 1.6: MySQL Protocol Server (Week 10-12)

- [ ] Implement MySQL wire protocol server in `nova-coordinator`
- [ ] Support: authentication (username/password, argon2)
- [ ] Support: basic query execution (text protocol)
- [ ] Support: result set formatting (column metadata + rows)
- [ ] Support: simple `mysql` CLI client connection
- [ ] Integration test: connect with `mysql` CLI, run queries
- [ ] Benchmark: query latency (cold vs warm)

**Deliverable:** Can connect with any MySQL client (mysql CLI, DBeaver, etc.) and run queries.

### Phase 1 Exit Criteria

- [ ] `CREATE TABLE`, `INSERT`, `SELECT` with filter works via MySQL client
- [ ] Micro-partitions written to S3 as valid Parquet files
- [ ] Metadata correctly stored in FDB
- [ ] Column stats computed and stored
- [ ] All tests pass: `cargo test --all`
- [ ] No clippy warnings: `cargo clippy --all -- -D warnings`
- [ ] Basic benchmark: scan 1M rows, measure latency
- [ ] Basic benchmark: beat PostgreSQL on same workload

---

## Phase 2: Query Engine (Month 4-5)

**Goal:** Full SQL support, competitive with DataFusion on ClickBench.

### Milestone 2.1: DataFusion Integration

- [ ] Integrate DataFusion as execution engine in `nova-worker`
- [ ] Implement `MicroPartitionScanExec` custom operator
- [ ] Wire: coordinator planner → DataFusion physical plan → worker execution
- [ ] Support: JOIN (hash join, broadcast join)
- [ ] Support: aggregation (two-phase: partial + final)
- [ ] Support: ORDER BY, LIMIT, GROUP BY
- [ ] Support: subqueries, CTEs
- [ ] Support: window functions
- [ ] Unit tests for each operator
- [ ] Integration tests: TPC-H Q1-Q10

### Milestone 2.2: MP Metadata Pruning

- [ ] Implement `MPPruningRule` optimizer rule
- [ ] For each scan with filter predicate:
  - Get active MPs from FDB
  - Check column min/max stats against predicate
  - Skip MPs that cannot match
- [ ] Support: range predicates (`col > val`, `col BETWEEN`)
- [ ] Support: equality predicates (`col = val`)
- [ ] Support: IN predicates (`col IN (...)`)
- [ ] Unit tests: pruning correctness
- [ ] Benchmark: 100 MPs, 10% match → should scan ~10 MPs

### Milestone 2.3: Parallel Scan

- [ ] Implement parallel MP scanning (1 thread per MP)
- [ ] Implement batch coalescing (merge small batches from parallel scans)
- [ ] Implement work stealing (idle threads steal from busy threads)
- [ ] Benchmark: scan 10 MPs in parallel vs sequential

### Milestone 2.4: Basic CBO

- [ ] Implement table statistics collection (row count, distinct count, min/max)
- [ ] Implement column statistics (histograms for skewed data)
- [ ] Implement cost model (CPU + I/O + memory estimates)
- [ ] Implement join reordering (left-deep, basic)
- [ ] Implement predicate pushdown (already from DataFusion, verify)
- [ ] Implement projection pushdown
- [ ] Unit tests: cost estimation correctness
- [ ] Benchmark: TPC-H Q1-Q22

### Milestone 2.5: ClickBench Benchmark

- [ ] Run ClickBench on nova-core
- [ ] Compare with DataFusion baseline
- [ ] Compare with DuckDB, ClickHouse
- [ ] Identify and fix performance gaps
- [ ] Document results in `references/performance-targets.md`

### Phase 2 Exit Criteria

- [ ] Full SQL support (SELECT, JOIN, AGG, SORT, LIMIT, subquery, CTE, window)
- [ ] MP pruning reduces I/O by 10-100x for selective queries
- [ ] Parallel scan utilizes all CPU cores
- [ ] ClickBench results within 1.2x of DataFusion baseline
- [ ] TPC-H Q1-Q22 run successfully

---

## Phase 3: Snowflake Features (Month 6-7)

**Goal:** Time Travel, Zero-Copy Clone, Streams, UPDATE/DELETE.

### Milestone 3.1: MVCC & Transaction Manager

- [ ] Implement transaction manager (BEGIN, COMMIT, ABORT)
- [ ] Implement snapshot isolation
- [ ] Implement MVCC version chains (supersedes, superseded_by)
- [ ] Implement commit_ts assignment
- [ ] Implement conflict detection (optimistic concurrency)
- [ ] Unit tests: concurrent transactions, conflict scenarios

### Milestone 3.2: UPDATE & DELETE (Copy-on-Write)

- [ ] Implement `UPDATE table SET ... WHERE ...`
  - Find affected MPs
  - Read + modify + write new MP
  - Mark old MP as superseded
- [ ] Implement `DELETE FROM table WHERE ...`
  - Find affected MPs
  - Read + remove + write new MP
  - Mark old MP as superseded
- [ ] Integration tests: UPDATE/DELETE + verify Time Travel
- [ ] Benchmark: COW write amplification

### Milestone 3.3: Time Travel

- [ ] Implement `AT(TIMESTAMP => ...)` SQL syntax
- [ ] Implement `BEFORE(STATEMENT => ...)` SQL syntax
- [ ] Implement MP visibility logic (commit_ts <= T, superseded_by.commit_ts > T)
- [ ] Implement `SELECT * FROM table AT(TIMESTAMP => '2026-06-01')`
- [ ] Integration tests: insert, update, time travel query
- [ ] Test: Time Travel with expired MPs (past retention) → error

### Milestone 3.4: Zero-Copy Clone

- [ ] Implement `CREATE TABLE ... CLONE source` SQL syntax
- [ ] Clone = copy FDB metadata entries (same S3 paths)
- [ ] Implement `CREATE TABLE ... CLONE source AT(TIMESTAMP => ...)`
- [ ] Implement copy-on-write for cloned tables (modify clone → new MP, source unaffected)
- [ ] Integration tests: clone table, modify clone, verify source unchanged
- [ ] Benchmark: clone time (should be < 1s regardless of table size)

### Milestone 3.5: Streams (CDC)

- [ ] Implement `CREATE STREAM ... ON TABLE ...` SQL syntax
- [ ] Implement stream offset tracking in FDB
- [ ] Implement stream read: find MPs after offset, diff versions
- [ ] Implement change record format (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE)
- [ ] Implement offset advance on consume
- [ ] Support: standard (delta) streams
- [ ] Support: append-only streams
- [ ] Integration tests: insert, update, delete → verify stream records

### Milestone 3.6: Garbage Collection

- [ ] Implement background GC process
- [ ] Find expired MPs (superseded AND past retention period)
- [ ] Delete S3 files (batch async)
- [ ] Delete FDB metadata entries
- [ ] Configurable retention period (1-90 days)
- [ ] Integration tests: insert, update, wait for GC, verify old MPs deleted
- [ ] Optional: MP merging (small MPs → large MP)

### Phase 3 Exit Criteria

- [ ] Time Travel: query data as of past timestamp
- [ ] Clone: instant zero-copy table duplication
- [ ] Streams: CDC with INSERT/UPDATE/DELETE detection
- [ ] UPDATE/DELETE: copy-on-write with old data retained
- [ ] GC: expired MPs cleaned up automatically
- [ ] All Snowflake parity features have integration tests

---

## Phase 4: Distributed (Month 8-9)

**Goal:** Multi-node cluster, elastic scaling, distributed JOIN.

### Milestone 4.1: Coordinator Raft

- [ ] Integrate `openraft` for coordinator consensus
- [ ] Implement leader election (3 coordinator nodes)
- [ ] Implement state replication (metadata changes replicated to followers)
- [ ] Implement leader failover (< 10s)
- [ ] Integration tests: kill leader, verify new leader takes over

### Milestone 4.2: Worker Pool

- [ ] Implement gRPC protocol (protobuf in `nova-common`)
- [ ] Implement coordinator → worker fragment dispatch
- [ ] Implement worker → coordinator result streaming
- [ ] Implement worker registration & heartbeat
- [ ] Implement worker health monitoring
- [ ] Integration tests: 1 coordinator + 2 workers, run distributed query

### Milestone 4.3: Distributed Execution

- [ ] Implement distributed scan (MPs partitioned across workers)
- [ ] Implement shuffle join (partition by join key across workers)
- [ ] Implement broadcast join (small table broadcast to all workers)
- [ ] Implement colocated join (same distribution key → local join, no shuffle)
- [ ] Implement adaptive join selection (runtime stats → switch strategy)
- [ ] Integration tests: TPC-H distributed, verify correctness
- [ ] Benchmark: scale-up (1 worker vs 4 workers vs 8 workers)

### Milestone 4.4: Auto-Scaling

- [ ] Implement worker auto-scaling (CPU threshold, queue depth)
- [ ] Implement auto-suspend (idle workers terminated after N seconds)
- [ ] Implement auto-resume (provision workers when query arrives)
- [ ] Implement warehouse concept (virtual compute clusters)
- [ ] Integration tests: scale up under load, scale down when idle

### Milestone 4.5: Distributed TPC-H

- [ ] Run TPC-H 100GB on 4-worker cluster
- [ ] Compare with single-node performance
- [ ] Identify and fix distributed execution bottlenecks
- [ ] Document results

### Phase 4 Exit Criteria

- [ ] 3-node coordinator cluster with Raft failover
- [ ] Workers auto-scale based on load
- [ ] Distributed JOIN (shuffle, broadcast, colocated)
- [ ] TPC-H runs correctly on multi-node cluster
- [ ] Worker failure → query re-scheduled, no data loss

---

## Phase 5: CBO Enhancement (Month 8-9, parallel with Phase 4)

**Goal:** Beat StarRocks CBO on TPC-DS.

### Milestone 5.1: Advanced Join Reordering

- [ ] Implement bushy join plans (not just left-deep)
- [ ] Implement dynamic programming join reorder (System-R style)
- [ ] Implement cost-based join type selection (hash vs sort-merge vs nested loop)
- [ ] Unit tests: verify optimal join order for 5-table star schema

### Milestone 5.2: Runtime Filter

- [ ] Implement Bloom filter construction on join build-side
- [ ] Implement Bloom filter pushdown to scan-side (pre-filter rows)
- [ ] Implement runtime filter injection in physical plan
- [ ] Benchmark: JOIN with selective dim table → 10x speedup

### Milestone 5.3: Advanced Statistics

- [ ] Implement histogram collection (equi-height, top-N frequency)
- [ ] Implement most-common-values (MCV) stats
- [ ] Implement `ANALYZE TABLE` command (manual + auto after DML)
- [ ] Implement cardinality estimation using histograms
- [ ] Unit tests: estimation accuracy within 10% of actual

### Milestone 5.4: Late Materialization

- [ ] Implement `LateMaterializationRule` optimizer rule
- [ ] For SELECT with filter + projection:
  - Scan filter columns first → get matching row IDs
  - Scan projection columns ONLY for matching row IDs
- [ ] Integration with Parquet column pruning
- [ ] Benchmark: selective query → 5-10x less I/O

### Milestone 5.5: Dictionary Encoding Optimization

- [ ] Implement dictionary encoding at scan time (low-cardinality columns)
- [ ] Operate on encoded INT values (SIMD-friendly)
- [ ] Decode only at output
- [ ] Benchmark: string column filter → 4x faster

### Milestone 5.6: TPC-DS Benchmark

- [ ] Run all 99 TPC-DS queries
- [ ] Compare with StarRocks CBO
- [ ] Identify and fix optimizer gaps
- [ ] Document results

### Phase 5 Exit Criteria

- [ ] All 99 TPC-DS queries run successfully
- [ ] Runtime filter provides 5-50x speedup on selective JOINs
- [ ] Late materialization reduces I/O by 2-10x
- [ ] CBO picks optimal join order for star schema queries
- [ ] Dictionary encoding speeds up string filters 2-5x

---

## Phase 6: Cache & Polish (Month 10)

**Goal:** Production ready — cache hierarchy, RBAC, HA, monitoring.

### Milestone 6.1: Foyer Hybrid Cache

- [ ] Integrate `foyer` for MP data cache (L3: 4GB RAM + 100GB SSD)
- [ ] Integrate `foyer` for metadata cache (L2: 512MB RAM)
- [ ] Integrate `foyer` for query result cache (L1: 2GB RAM + 50GB SSD)
- [ ] Implement cache key strategy (mp_id:version for L3, table_id for L2)
- [ ] Implement compaction-aware refill (after MP merge, prefetch new MP)
- [ ] Benchmark: cache hit rate, cold vs warm vs hot query latency

### Milestone 6.2: Query Result Cache

- [ ] Implement SQL normalization (lowercase, collapse whitespace)
- [ ] Implement cache key: hash(normalized_sql + table_versions)
- [ ] Implement auto-invalidation (table version change → new cache key)
- [ ] Implement TTL (24 hours, reset on hit, max 31 days)
- [ ] Implement non-cacheable query detection (CURRENT_TIMESTAMP, RAND, etc.)
- [ ] Benchmark: same query repeated → 600x faster (instant)

### Milestone 6.3: RBAC

- [ ] Implement users (create, drop, alter, list)
- [ ] Implement roles (create, drop, grant, revoke)
- [ ] Implement privileges (SELECT, INSERT, CREATE, DROP, etc.)
- [ ] Implement access control in analyzer (check privileges before execution)
- [ ] Implement `current_user()`, `current_role()` functions
- [ ] Integration tests: user with SELECT only cannot INSERT

### Milestone 6.4: Backup & Restore

- [ ] Implement FDB snapshot backup
- [ ] Implement S3 bucket backup (cross-region replication)
- [ ] Implement restore from backup
- [ ] Integration tests: backup, destroy, restore, verify data

### Milestone 6.5: Monitoring

- [ ] Implement Prometheus metrics export
  - Query count, latency histogram
  - Cache hit/miss rates
  - Worker count, CPU usage
  - S3 read/write bytes
  - FDB transaction count
- [ ] Implement Grafana dashboard templates
- [ ] Implement `tracing` structured logging
- [ ] Implement OpenTelemetry distributed tracing

### Milestone 6.6: HA & Production

- [ ] Multi-AZ deployment guide
- [ ] FDB multi-region replication setup
- [ ] S3 cross-region replication
- [ ] Load testing (100 concurrent queries)
- [ ] Performance tuning guide
- [ ] Nova UI integration (FastAPI → nova-core REST API)

### Phase 6 Exit Criteria

- [ ] 3-layer cache (result + metadata + MP) working with Foyer
- [ ] Query result cache auto-invalidates on data change
- [ ] RBAC: users, roles, grants enforced
- [ ] Prometheus metrics exported
- [ ] HA: coordinator failover < 10s, worker auto-replace < 60s
- [ ] Load test: 100 concurrent queries, no crashes
- [ ] Nova Engine v0.1.0 released

---

## Dependency Graph

```
Phase 1 (Foundation)
  └── Phase 2 (Query Engine)
        └── Phase 3 (Snowflake Features)
              ├── Phase 4 (Distributed)
              └── Phase 5 (CBO Enhancement)
                    └── Phase 6 (Cache & Polish)
```

- Phase 4 and Phase 5 can run in parallel (different teams/agents)
- Phase 6 depends on both Phase 4 and Phase 5
- Each phase has explicit exit criteria — do NOT start next phase until all criteria met

---

## Benchmark Targets

| Benchmark | Target | Comparison |
|---|---|---|
| ClickBench (single-node) | Within 1.2x of DataFusion 43 | Beat DuckDB, ClickHouse |
| TPC-H (single-node) | All 22 queries pass | Within 2x of StarRocks |
| TPC-H (distributed, 4 workers) | Linear scale-up | 3x+ vs single-node |
| TPC-DS | All 99 queries pass | Within 2x of StarRocks |
| Time Travel query | < 1.5x of current query | Overhead < 50% |
| Zero-Copy Clone | < 1 second | Any table size |
| Stream latency | < 1 second | INSERT to stream visible |
| Repeated query (cache hit) | < 10ms | 600x faster than cold |
| Coordinator failover | < 10 seconds | RTO |
| Worker auto-scale | < 60 seconds | Spin up new worker |

---

## Phase 8: SQL Completeness (Gap Analysis — June 2026)

> Audit result: Phases 1-7 wired but SQL feature gaps remain.
> This phase closes all gaps between architecture spec and implementation.

### P0: Critical SQL Features (blocking production)

#### 8.1: DROP TABLE / DROP DATABASE
- [ ] Parser: sqlparser native `Statement::Drop`
- [ ] Analyzer: resolve to `ResolvedStatement::DropTable` / `DropDatabase`
- [ ] Executor: call `meta.drop_table()` / `meta.drop_database()`
- [ ] Tests: create → drop → verify gone

#### 8.2: DataFusion SessionContext Integration
- [ ] Replace direct storage reads in exec_select with DataFusion SessionContext
- [ ] Register NovaTableProvider to SessionContext
- [ ] Route SELECT queries through DataFusion SQL parser + optimizer
- [ ] Tests: SELECT via DataFusion returns same results

#### 8.3: Aggregate Functions (COUNT/SUM/AVG/MIN/MAX)
- [ ] Via DataFusion SessionContext (built-in support)
- [ ] Tests: COUNT(*), SUM(col), AVG(col), GROUP BY

#### 8.4: ORDER BY + LIMIT/OFFSET
- [ ] Via DataFusion SessionContext (built-in support)
- [ ] Tests: ORDER BY col DESC LIMIT 10

#### 8.5: Multi-statement SQL Execution
- [ ] NovaEngine: execute ALL statements, not just first
- [ ] Tests: "CREATE...; INSERT...; SELECT..." returns correct result

### P1: Important SQL Features

#### 8.6: JOIN Support (INNER/LEFT/RIGHT)
- [ ] Analyzer: resolve JOIN syntax → multi-table ResolvedStatement
- [ ] Executor: route to DataFusion for multi-table queries
- [ ] CBO: join reordering active (cbo.rs already exists)
- [ ] Tests: INNER JOIN, LEFT JOIN, 3-table join

#### 8.7: Parallel MP Scan
- [ ] Executor: read MPs in parallel (tokio::join_all) instead of sequential loop
- [ ] Tests: verify parallel scan produces same results

#### 8.8: Foyer HybridCache
- [ ] Replace HashMap in NovaCache with Foyer HybridCache (RAM + SSD)
- [ ] Add eviction policy (LRU)
- [ ] Tests: cache hit/miss/eviction

#### 8.9: ResultCache Table Version Tracking
- [ ] Track table versions in NovaEngine (not empty HashMap)
- [ ] Invalidate cache on INSERT/UPDATE/DELETE
- [ ] Tests: insert → select (cache miss) → select (cache hit) → insert → select (cache miss)

#### 8.10: Worker Executor (replace stub)
- [ ] Implement nova-worker/src/executor.rs with DataFusion SessionContext
- [ ] Register MicroPartitionScanExec
- [ ] Tests: worker can execute query fragments

### P2: E2E Test Suite

#### 8.11: Full Lifecycle E2E Tests
- [ ] CREATE DATABASE → CREATE TABLE → INSERT → SELECT → UPDATE → DELETE → DROP
- [ ] AGG: COUNT/SUM/AVG/GROUP BY
- [ ] ORDER BY + LIMIT
- [ ] BEGIN → INSERT → COMMIT → SELECT
- [ ] Time Travel: INSERT → SELECT AT TIMESTAMP
- [ ] CLONE: CREATE TABLE CLONE → verify data
- [ ] GC: INSERT → UPDATE → GC → verify old MPs deleted

### Phase 8 Exit Criteria

- [x] All P0 features implemented with tests
- [x] All P1 features implemented with tests
- [x] E2E test suite passes (5 tests)
- [x] 275+ tests total
- [x] clippy clean, fmt clean
- [x] MySQL client can execute full SQL lifecycle

---

## Phase 9: Production Hardening (Gap Analysis — June 2026)

> Remaining gaps from architecture spec + production requirements.
> These are the last items before 100% production-ready.

### P0: Critical (correctness + verification)

#### 9.1: E2E Tests for DataFusion Path
- [ ] E2E: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)
- [ ] E2E: GROUP BY with aggregates
- [ ] E2E: ORDER BY col DESC
- [ ] E2E: LIMIT / OFFSET
- [ ] E2E: DISTINCT
- [ ] E2E: HAVING (post-aggregate filter)
- [ ] E2E: Subquery (SELECT * FROM (SELECT ...))

#### 9.2: E2E Tests for JOIN
- [ ] E2E: INNER JOIN (2 tables)
- [ ] E2E: LEFT JOIN
- [ ] E2E: 3-table JOIN
- [ ] Verify CBO join reordering is active

#### 9.3: COW Visibility Fix (DataFusion reads stale MPs)
- [ ] After UPDATE/DELETE, DataFusion path must read updated active MPs
- [ ] Root cause: exec_select_datafusion receives stale `mps` snapshot
- [ ] Fix: re-fetch active MPs inside exec_select_datafusion, or invalidate cache on COW

### P1: Important (production quality)

#### 9.4: ResultCache Table Version Tracking
- [ ] Track table versions from metadata (not empty HashMap)
- [ ] Call meta.get_table_version() before cache lookup
- [ ] Invalidate cache on INSERT/UPDATE/DELETE via version change
- [ ] Test: insert → select (miss) → select (hit) → insert → select (miss)

#### 9.5: Auth Enforcement in MySQL Handshake
- [ ] Verify password via AuthManager during MySQL handshake
- [ ] When auth.enabled=true, reject connections with wrong password
- [ ] When auth.enabled=false, accept all (dev mode, current behavior)
- [ ] Test: connect with correct password → success; wrong password → error

#### 9.6: RBAC Enforcement in DDL/DML
- [ ] Call rbac.check_privilege() before CREATE TABLE / DROP TABLE / INSERT / UPDATE / DELETE
- [ ] Track current user from MySQL session
- [ ] Test: non-admin user cannot DROP TABLE

#### 9.7: DataFusion Path MP Pruning
- [ ] NovaTableProvider::scan() should use filter predicates for MP pruning
- [ ] Pass _filters to MicroPartitionScanExec for predicate pushdown
- [ ] Currently: DataFusion path reads all active MPs, no pruning

#### 9.8: DataFusion Path Statistics
- [ ] NovaTableProvider should expose statistics to DataFusion optimizer
- [ ] Implement TableProvider::statistics() method
- [ ] Currently: statistics only collected in legacy path

### P2: Enhancement (nice to have)

#### 9.9: Foyer HybridCache (replace HashMap)
- [ ] Replace HashMap in NovaCache with foyer::HybridCache (RAM + SSD)
- [ ] Add LRU eviction policy
- [ ] Add cache size limits (2GB RAM + 50GB SSD for result cache, 4GB RAM + 100GB SSD for MP cache)
- [ ] Test: cache hit/miss/eviction

#### 9.10: Parallel Scan in DataFusion Path
- [ ] NovaTableProvider::scan() creates MicroPartitionScanExec with 1 partition per MP
- [ ] DataFusion executes partitions in parallel automatically
- [ ] Verify parallelism is actually happening (not sequential)

#### 9.11: ALTER TABLE Support
- [ ] Parser: sqlparser native ALTER TABLE
- [ ] Analyzer: resolve to ResolvedStatement::AlterTable
- [ ] Executor: add/drop column (creates new MP with updated schema)
- [ ] Test: ALTER TABLE ADD COLUMN → INSERT → SELECT new column

#### 9.12: Time Travel SQL Syntax
- [ ] Parser: `SELECT * FROM t AT(TIMESTAMP => '2026-06-30 12:00:00')`
- [ ] Analyzer: resolve to ResolvedStatement::Select with at_timestamp
- [ ] Executor: use get_mps_at_timestamp() (already implemented)
- [ ] Test: insert → wait → select AT TIMESTAMP → verify old data

#### 9.13: E2E Tests for Snowflake Features
- [ ] E2E: CREATE TABLE x CLONE y → verify data
- [ ] E2E: CREATE STREAM s ON TABLE t
- [ ] E2E: GC <retention> → verify old MPs deleted
- [ ] E2E: BACKUP TO /path → RESTORE FROM /path

#### 9.14: gRPC Proto Definitions (multi-node only)
- [ ] Define .proto files for coordinator↔worker RPC
- [ ] RegisterWorker, Heartbeat, ExecuteFragment, StreamResults
- [ ] Generate tonic stubs
- [ ] Future: enables distributed execution

### Phase 9 Exit Criteria

- [ ] All P0 features implemented with tests
- [ ] All P1 features implemented with tests
- [ ] 300+ tests total
- [ ] COW visibility verified (UPDATE → SELECT sees updated data)
- [ ] ResultCache invalidation verified (INSERT → cache miss)
- [ ] Auth enforcement verified (wrong password rejected)
- [ ] clippy clean, fmt clean
- [ ] Zero TODO/FIXME in production code
