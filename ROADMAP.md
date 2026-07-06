# ROADMAP.md — nova-core Development Roadmap

> All phases, milestones, and dependencies. Updated as work progresses.
> **Current Phase: Phase 1 — Foundation**

---

## Phase Overview

| Phase | Name | Duration | Status | Focus |
|---|---|---|---|---|
| 1 | Foundation | Month 1-3 | ✅ Complete | Storage, metadata, basic SQL, FDB |
| 2 | Query Engine | Month 4-5 | ✅ Complete | Optimizer, planner, scheduler, MP pruning, TableProvider |
| 3 | Snowflake Features | Month 6-7 | ✅ Complete | Time Travel, Clone, Streams, GC, BEGIN/COMMIT/ROLLBACK |
| 4 | Distributed | Month 8-9 | ✅ Single-node | WorkerPool, AutoScaler wired (gRPC future) |
| 5 | CBO Enhancement | Month 8-9 | ✅ Complete | Late mat, stats, runtime filter wired to optimizer |
| 6 | Cache & Polish | Month 10 | ✅ Complete | Auth, cache, RBAC, monitoring, HA, backup |
| 7 | MySQL Protocol | — | ✅ Complete | Production-grade (40 tests) |
| 8 | SQL Completeness | — | ✅ Complete | AGG, GROUP BY, ORDER BY, JOIN, DROP, multi-stmt, DataFusion |
| 9 | Production Hardening | — | ✅ Complete | COW fix, cache invalidation, auth, RBAC, E2E tests, Foyer |
| 10 | Multi-Node Distributed | — | ✅ Complete | tonic gRPC stubs, WorkerGrpcServer, WorkerClientPool, FragmentDispatcher::dispatch_via_grpc |
| 11 | HA Coordinator | — | ✅ Complete | openraft 3-node leader election, RaftRpcServer, RaftNetwork transport |
| 12 | Auto-Compaction | — | ✅ Complete | background GC + MP merge |
| 13 | Dynamic Tables | — | ✅ Complete | CREATE/ALTER/DROP/SHOW DYNAMIC TABLE + scheduler |
| 14 | Production Deployment | — | ✅ Complete | Docker Rust 1.91, fixed worker gRPC args |
| 15 | Distributed Cluster Wire-up | — | ✅ Complete | Raft gRPC server mounted, AutoScaler loop wired |
| 16 | Enterprise Security & Governance | — | 🚧 In Progress | Snowflake-style RBAC, policies, tags, audit, identity |
| 17 | Enterprise Task Orchestration | — | 📋 Planned | Snowflake-style TASK, scheduler, stream triggers, DAGs, RBAC, audit |
| 18 | Snowflake MERGE INTO | — | 📋 Planned | Snowflake-compatible MERGE subset, deterministic duplicate handling, RBAC, TASK integration |

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
- [ ] Use FoundationDB for local dev/testing and production
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

- [x] `CREATE TABLE`, `INSERT`, `SELECT` with filter works via MySQL client
- [x] Micro-partitions written to S3 as valid Parquet files
- [x] Metadata correctly stored in FDB
- [x] Column stats computed and stored
- [x] All tests pass: `cargo test --all`
- [x] No clippy warnings: `cargo clippy --all -- -D warnings`
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

- [x] Full SQL support (SELECT, JOIN, AGG, SORT, LIMIT, subquery, CTE, window)
- [x] MP pruning reduces I/O by 10-100x for selective queries
- [x] Parallel scan utilizes all CPU cores
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

- [x] Implement `CREATE STREAM ... ON TABLE ...` SQL syntax
- [x] Implement stream offset tracking in FDB
- [x] Implement durable CDC change-log metadata in FDB and Parquet payloads in object storage
- [x] Implement consuming stream `SELECT` with compare-and-set offset commit
- [x] Implement preview reads with `WITH (COMMIT = FALSE)`
- [x] Implement Snowflake-compatible `SYSTEM$STREAM_HAS_DATA('<stream>')`
- [x] Implement change record format (`INSERT`, `DELETE`; UPDATE as DELETE+INSERT with `METADATA$ISUPDATE = true`)
- [x] Support: standard (delta) streams
- [x] Integration tests: insert, update, delete → verify stream records, metadata columns, RBAC, preview, consume offsets
- [x] Reject append-only stream syntax; Nova supports standard delta streams only

### Milestone 3.6: Garbage Collection

- [ ] Implement background GC process
- [ ] Find expired MPs (superseded AND past retention period)
- [ ] Delete S3 files (batch async)
- [ ] Delete FDB metadata entries
- [ ] Configurable retention period (1-90 days)
- [ ] Integration tests: insert, update, wait for GC, verify old MPs deleted
- [ ] Optional: MP merging (small MPs → large MP)

### Phase 3 Exit Criteria

- [x] Time Travel: query data as of past timestamp
- [x] Clone: instant zero-copy table duplication
- [x] Streams: CDC with INSERT/UPDATE/DELETE detection
- [x] UPDATE/DELETE: copy-on-write with old data retained
- [x] GC: expired MPs cleaned up automatically
- [x] All Snowflake parity features have integration tests

---

## Phase 4: Distributed (Month 8-9)

**Goal:** Multi-node cluster, elastic scaling, distributed JOIN.

### Milestone 4.1: Coordinator Raft

- [x] Integrate `openraft` for coordinator consensus
- [x] Implement leader election (3 coordinator nodes)
- [x] Implement state replication (metadata changes replicated to followers)
- [x] Implement leader failover (< 10s)
- [x] Integration tests: kill leader, verify new leader takes over

### Milestone 4.2: Worker Pool

- [x] Implement gRPC protocol (protobuf in `nova-common`)
- [x] Implement coordinator → worker fragment dispatch
- [x] Implement worker → coordinator result streaming
- [x] Implement worker registration & heartbeat
- [x] Implement worker health monitoring
- [x] Integration tests: 1 coordinator + 2 workers, run distributed query

### Milestone 4.3: Distributed Execution

- [x] Implement distributed scan (MPs partitioned across workers)
- [x] Implement shuffle join (partition by join key across workers)
- [x] Implement broadcast join (small table broadcast to all workers)
- [x] Implement colocated join (same distribution key → local join, no shuffle)
- [x] Implement adaptive join selection (runtime stats → switch strategy)
- [x] Integration tests: TPC-H distributed, verify correctness
- [x] Benchmark: scale-up (1 worker vs 4 workers vs 8 workers)

### Milestone 4.4: Auto-Scaling

- [x] Implement worker auto-scaling (CPU threshold, queue depth)
- [x] Implement auto-suspend (idle workers terminated after N seconds)
- [x] Implement auto-resume (provision workers when query arrives)
- [x] Implement warehouse concept (virtual compute clusters)
- [x] Integration tests: scale up under load, scale down when idle

### Milestone 4.5: Distributed TPC-H

- [x] Run TPC-H 100GB on 4-worker cluster
- [x] Compare with single-node performance
- [x] Identify and fix distributed execution bottlenecks
- [x] Document results

### Phase 4 Exit Criteria

- [x] 3-node coordinator cluster with Raft failover
- [x] Workers auto-scale based on load
- [x] Distributed JOIN (shuffle, broadcast, colocated)
- [x] TPC-H runs correctly on multi-node cluster
- [x] Worker failure → query re-scheduled, no data loss

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

> Baseline RBAC only. Enterprise-grade Snowflake-style authorization, governance policies, audit, and identity hardening are tracked in Phase 16.

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

## Phase 16: Enterprise Security & Governance

**Goal:** Snowflake-inspired enterprise authorization, policy enforcement, governance catalog, audit, and identity hardening.

> Design reference: `docs/design/enterprise-rbac-roadmap.md`. Phase 6/9 RBAC is treated as baseline enforcement; this phase is the enterprise-grade security and governance program.

### Milestone 16.1: FDB-backed RBAC Foundation

- [ ] Replace in-memory user/role/grant state with FoundationDB-backed security metadata
- [ ] Define stable securable object identities: account, database, schema, table, dynamic table, stream, view, role, user, policy, tag, warehouse
- [ ] Store object ownership as role-owned metadata, not user-owned metadata
- [ ] Assign every new object to the session primary role as owner
- [ ] Migrate legacy objects without ownership metadata to `ACCOUNTADMIN` ownership
- [x] Implement role-to-user grants and role-to-role hierarchy grants
- [x] Implement role inheritance traversal with cycle detection and recursion/depth guards
- [ ] Add security epoch and bump it on user, role, grant, revoke, ownership, policy, and tag changes
- [ ] Add authorization cache keyed by security epoch
- [ ] Add negative tests for missing grants, stale cache, role hierarchy cycles, and dropped roles

### Milestone 16.2: Snowflake-style Session Roles

- [ ] Implement per-session `SecurityContext` passed through parser, analyzer, planner, executor, DataFusion path, and background jobs
- [ ] Remove unsafe global `current_user` executor state for concurrent MySQL sessions
- [ ] Implement default role resolution at login: requested role → user default role → `PUBLIC`
- [ ] Implement `USE ROLE <role>` with validation that the role is granted to the user
- [ ] Implement `USE SECONDARY ROLES NONE | ALL` and reserve explicit secondary role lists
- [ ] Implement `current_user()`, `current_role()`, `current_secondary_roles()`, and `is_role_in_session()`
- [ ] Enforce Snowflake-style rule: `CREATE` authorization and new object ownership use primary role only
- [x] Enforce non-CREATE authorization using primary role plus active secondary roles and inherited roles
- [ ] Add tests for role switching, secondary role activation, inherited privileges, and unauthorized role activation

### Milestone 16.3: Grant Management

- [ ] Implement SQL support for `GRANT <privileges> ON <object> TO ROLE <role>`
- [ ] Implement SQL support for `REVOKE <privileges> ON <object> FROM ROLE <role>`
- [ ] Implement `GRANT ROLE <role> TO USER <user>` and `REVOKE ROLE <role> FROM USER <user>`
- [ ] Implement `GRANT ROLE <child> TO ROLE <parent>` and `REVOKE ROLE <child> FROM ROLE <parent>`
- [ ] Implement `GRANT OWNERSHIP ON <object> TO ROLE <role>` with transfer semantics
- [ ] Support `WITH GRANT OPTION` for object privileges where applicable
- [ ] Implement `MANAGE_GRANTS` account privilege for centralized grant administration
- [ ] Make `DROP` and destructive `ALTER` require `OWNERSHIP` rather than a generic `DROP` privilege
- [ ] Implement managed access schemas where only schema owner or `MANAGE_GRANTS` can grant on contained objects
- [ ] Implement future grants: `GRANT ... ON FUTURE TABLES|VIEWS|STREAMS|DYNAMIC TABLES IN SCHEMA|DATABASE ...`
- [ ] Implement all-object grants: `GRANT ... ON ALL TABLES|VIEWS|STREAMS|DYNAMIC TABLES IN SCHEMA|DATABASE ...`
- [ ] Implement `SHOW GRANTS`, `SHOW GRANTS TO ROLE`, `SHOW GRANTS ON <object>`, and grant visibility filtering
- [ ] Add tests for ownership transfer, managed access schemas, grant option, revoke behavior, and future grant application

### Milestone 16.4: Database Roles

- [ ] Implement account roles and database roles as distinct role scopes
- [ ] Implement `CREATE DATABASE ROLE <db>.<role>` and `DROP DATABASE ROLE <db>.<role>`
- [ ] Restrict database role privileges to objects in the same database
- [ ] Allow database roles to be granted to account roles
- [ ] Allow database roles to be granted to other database roles in the same database with cycle detection
- [ ] Prevent database roles from becoming active primary or secondary session roles directly
- [ ] Make database role grants contribute privileges through the active account role hierarchy
- [ ] Reserve database role semantics for future secure data sharing
- [ ] Add tests for cross-database denial, hierarchy inheritance, and session activation denial

### Milestone 16.5: Row Access Policies

- [ ] Implement row access policy metadata as schema-level securable objects
- [ ] Implement `CREATE ROW ACCESS POLICY <name> AS (...) RETURNS BOOLEAN -> <expr>`
- [ ] Implement `ALTER ROW ACCESS POLICY`, `DROP ROW ACCESS POLICY`, `SHOW ROW ACCESS POLICIES`, and `DESC ROW ACCESS POLICY`
- [ ] Implement `ALTER TABLE|VIEW ... ADD ROW ACCESS POLICY <policy> ON (<columns>)`
- [ ] Implement policy owner execution context for mapping-table lookups
- [ ] Rewrite DataFusion logical plans to inject policy filters before user predicates where required
- [ ] Apply row access policies to `SELECT`, and to rows selected by `UPDATE`, `DELETE`, and future `MERGE`
- [ ] Ensure row access policies do not silently bypass MP pruning correctness; pruning must remain conservative when policy columns are involved
- [ ] Implement policy reference metadata for tables, views, and dynamic tables
- [ ] Define Time Travel semantics: data snapshot uses requested timestamp, but policy and mapping tables are evaluated at query time
- [ ] Apply source table policies when streams read protected tables
- [ ] Add tests for simple role predicates, mapping-table predicates, nested table/view policies, Time Travel, streams, and denied rows

### Milestone 16.6: Column Masking Policies

- [ ] Implement masking policy metadata as schema-level securable objects
- [ ] Implement `CREATE MASKING POLICY <name> AS (val <type>) RETURNS <type> -> <expr>`
- [ ] Implement conditional masking with `USING` columns
- [ ] Enforce policy input and output type compatibility
- [ ] Implement `ALTER TABLE|VIEW ... MODIFY COLUMN ... SET|UNSET MASKING POLICY`
- [ ] Implement `ALTER MASKING POLICY`, `DROP MASKING POLICY`, `SHOW MASKING POLICIES`, and `DESC MASKING POLICY`
- [ ] Rewrite DataFusion logical plans so masking applies wherever protected columns are referenced: projection, filters, joins, grouping, ordering, aggregates, CTAS, and unload paths
- [ ] Ensure direct column masking takes precedence over tag-based masking
- [ ] Redact sensitive policy internals in `EXPLAIN`, errors, and query profiles
- [ ] Define clone behavior for policy assignments: table clone retains policy mapping; schema/database clone maps to cloned policies when self-contained
- [ ] Add tests for masked SELECT, masked WHERE/JOIN anti-bypass behavior, conditional masking, CTAS, clone, streams, and explain redaction

### Milestone 16.7: Tags & Classification

- [ ] Implement tag metadata as securable schema-level objects
- [ ] Implement `CREATE TAG`, `ALTER TAG`, `DROP TAG`, `SHOW TAGS`, and `DESC TAG`
- [ ] Implement object and column tag assignments via `ALTER ... SET TAG` and `UNSET TAG`
- [ ] Support allowed tag values and validation
- [ ] Implement tag reference catalog views for objects and columns
- [ ] Implement tag-based masking policy bindings
- [ ] Enforce direct masking policy precedence over tag-based masking policy
- [ ] Implement `CLASSIFY TABLE` to detect likely PII/PHI/PCI/secrets using sampled values and column metadata without logging raw sensitive samples
- [ ] Store classification results with confidence score, suggested tags, reviewer, and applied status
- [ ] Add governance coverage metrics: untagged sensitive columns, protected columns, policy coverage, and stale classification results
- [ ] Add tests for tag assignment, tag-based masking, precedence, allowed values, classification suggestions, and redacted logs

### Milestone 16.8: Audit, Access History, and Lineage

- [ ] Implement append-only security audit log in FDB or dedicated audit storage for GRANT, REVOKE, OWNERSHIP, policy, tag, login, and denied access events
- [ ] Implement query access history with query id, timestamp, user, primary role, secondary roles, warehouse, status, and client metadata
- [ ] Record direct objects accessed by each query
- [ ] Record base objects accessed through views, dynamic tables, policies, UDFs, and future secure objects
- [ ] Record column-level access for direct and base objects
- [ ] Record objects modified by DDL and DML, including column lineage for CTAS, INSERT SELECT, UPDATE, DELETE, and future MERGE
- [ ] Record policies referenced by each query: row access policies, masking policies, and tag-based policies
- [ ] Preserve audit records for failed authorization checks without leaking protected object or policy internals to unauthorized users
- [ ] Add queryable system views: `ACCOUNT_USAGE.ACCESS_HISTORY`, `ACCOUNT_USAGE.QUERY_HISTORY`, `ACCOUNT_USAGE.GRANTS_TO_ROLES`, `ACCOUNT_USAGE.POLICY_REFERENCES`, and `INFORMATION_SCHEMA.POLICY_REFERENCES`
- [ ] Define retention, truncation, and redaction behavior for large audit records
- [ ] Add tests for SELECT, JOIN, CTAS, CLONE, UPDATE, DELETE, policy-protected queries, denied queries, and audit redaction

### Milestone 16.9: Enterprise Auth & Network Controls

- [ ] Implement password policies: minimum length, complexity, expiry, reuse prevention, and forced reset
- [ ] Implement failed-login throttling, account lockout, and admin unlock
- [ ] Implement login history with success/failure reason, client address, protocol, and user agent where available
- [ ] Implement session expiry, idle timeout, and explicit session revocation
- [ ] Add service users for automation with non-interactive authentication controls
- [ ] Add key-pair or JWT authentication path for service users
- [ ] Design OIDC/SAML external identity integration without adding dependencies until approved
- [ ] Design SCIM-compatible provisioning API for users, roles, and external groups
- [ ] Implement account-level and user-level network policies with allowed and blocked CIDR ranges
- [ ] Enforce network policies consistently in MySQL protocol and REST/API entry points
- [ ] Add tests for lockout, password policy, session revocation, service auth, and denied network access

### Milestone 16.10: Security Integration Across Nova Features

- [ ] Make result cache keys include user, active roles, security epoch, policy epoch, and table versions unless a result is proven safely shareable
- [ ] Invalidate result cache on grants, revokes, ownership transfer, role changes, policy changes, tag changes, and security epoch changes
- [ ] Enforce row access and masking policies in DataFusion, legacy execution, MySQL protocol, REST/API, worker fragments, and background jobs
- [ ] Define Time Travel security semantics for RBAC, masking, row policies, and policy mapping tables
- [ ] Define Clone security semantics for grants, ownership, row policies, masking policies, tags, and future grants
- [ ] Define Streams security semantics for table policies and CDC visibility
- [ ] Ensure Dynamic Table refresh runs as `SYSTEM` user with the dynamic table owner role and no accidental admin bypass
- [ ] Ensure backup/restore preserves users, roles, grants, ownership, policies, tags, security epoch, and audit continuity atomically
- [ ] Ensure HA coordinator failover and multi-node workers use FDB security source of truth, not stale local security state
- [ ] Add end-to-end bypass tests across cache, Time Travel, Clone, Streams, Dynamic Tables, distributed execution, and failover

### Phase 16 Exit Criteria

- [ ] All users, roles, grants, ownership, policies, tags, and security epochs are persisted in FoundationDB and shared by all coordinators
- [ ] Role hierarchy, primary roles, secondary roles, ownership, managed access schemas, grant option, future grants, and database roles are enforced
- [ ] Row access policies and column masking policies are enforced in the DataFusion path without bypass via filters, joins, aggregates, CTAS, streams, or Time Travel
- [ ] Tags, tag-based masking, classification results, and governance coverage views are queryable
- [ ] Access history records direct objects, base objects, columns, modified objects, policies referenced, denied events, and lineage for supported SQL operations
- [ ] Result cache and authorization caches are security-aware and invalidate on security or policy changes
- [ ] Enterprise auth controls cover password policy, login history, lockout, session expiry/revocation, service users, and network policies
- [ ] Backup/restore and HA preserve security metadata and never fall back to local-only security state
- [ ] Negative bypass tests pass for non-admin DDL/DML, revoked privileges, role switching, stale cache, policy-protected data, clone, streams, dynamic tables, and distributed execution
- [ ] Documentation explains Snowflake-inspired differences and Nova-specific constraints without claiming full Snowflake compatibility

---

## Phase 17: Enterprise Task Orchestration

**Goal:** Implement Snowflake-inspired `TASK` orchestration for scheduled SQL, stream-triggered ELT, and enterprise workflow DAGs with HA scheduling, transactional stream consumption, RBAC, audit, and observability.

> Planning reference: `GUIDE_TASK_PLANNING.md`. This phase builds on Phase 3 Streams, Phase 10 multi-node execution, Phase 11 HA coordinator, Phase 13 Dynamic Tables, and Phase 16 enterprise security. It must preserve immutable micro-partitions, MVCC stream offsets, stateless workers, and FDB as the source of truth.

### Milestone 17.1: Task Catalog and FoundationDB Metadata

- [ ] Define `TaskMeta`, `TaskVersionMeta`, `TaskRunMeta`, `TaskGraphRunMeta`, `TaskDependencyMeta`, `TaskLeaseMeta`, and `TaskScheduleMeta` in `nova-common`
- [ ] Add FoundationDB tuple-key layout for task definitions, task name lookup, task graph edges, task versions, run history, scheduler leases, stream trigger indexes, and audit references
- [ ] Persist task owner role, optional `EXECUTE AS USER`, warehouse/serverless config, schedule config, `WHEN` expression text, SQL body, session parameters, timeout, retry, failure suspension, and comments
- [ ] Store task definitions as versioned immutable snapshots so running tasks keep their original version while later DDL creates a new version
- [ ] Add atomic create/replace/drop/alter metadata operations with duplicate-name checks, graph validation, stream trigger index maintenance, and run-history retention boundaries
- [ ] Add tests for key ordering, duplicate tasks, task version snapshots, dependency edges, stream trigger indexes, lease conflicts, rollback behavior, and metadata migration safety

### Milestone 17.2: SQL Surface for TASK DDL, DCL, and Introspection

- [ ] Implement `CREATE TASK`, `CREATE OR REPLACE TASK`, `CREATE TASK IF NOT EXISTS`, and `CREATE OR ALTER TASK` with interval schedules, cron schedules, `WHEN`, `AFTER`, `FINALIZE`, timeout, retry, overlap policy, session parameters, and comments
- [ ] Implement `ALTER TASK ... RESUME|SUSPEND`, `SET`, `UNSET`, `MODIFY AS`, `MODIFY WHEN`, `REMOVE WHEN`, `ADD AFTER`, `REMOVE AFTER`, `SET FINALIZE`, and `UNSET FINALIZE`
- [ ] Implement `DROP TASK`, `SHOW TASKS`, `DESC TASK`, and `EXECUTE TASK` for manual runs
- [ ] Implement task graph helper functions: `SYSTEM$TASK_DEPENDENTS_ENABLE`, `TASK_DEPENDENTS`, `SYSTEM$TASK_RUNTIME_INFO`, `SYSTEM$SET_RETURN_VALUE`, and `SYSTEM$GET_PREDECESSOR_RETURN_VALUE`
- [ ] Validate Snowflake-style restrictions: child tasks cannot define schedules, finalizer tasks cannot define schedules or children, graph members must share schema and owner role, root must be suspended before graph mutation, and task rename is not supported
- [ ] Add parser, analyzer, executor, MySQL protocol, and integration tests for valid DDL, invalid combinations, graph changes, manual execution, introspection visibility, and error messages

### Milestone 17.3: Scheduler Core, Cron, and HA Leases

- [ ] Implement a coordinator-owned `TaskScheduler` that scans due resumed root and standalone tasks, evaluates trigger conditions, and dispatches run records without storing execution state on workers
- [ ] Support interval schedules with resume-time base interval semantics and cron schedules using five-field Snowflake-style expressions plus explicit timezone
- [ ] Store `next_scheduled_time`, base interval time, skipped-run decisions, queued time, query start time, completion time, and scheduled source (`SCHEDULE`, `TRIGGER`, `EXECUTE_TASK`, `MANUAL RETRY`, `AUTOMATIC RETRY`)
- [ ] Implement HA-safe scheduler leases in FoundationDB so only the elected coordinator or lease holder schedules a given task/graph run
- [ ] Enforce no-overlap behavior for standalone scheduled tasks, and graph overlap policies: `NO_OVERLAP`, `ALLOW_CHILD_OVERLAP`, and `ALLOW_ALL_OVERLAP`
- [ ] Add backpressure controls for max queued runs, max concurrent task runs per warehouse, max account-level task concurrency, and bounded scheduler scan work
- [ ] Add deterministic unit tests for interval calculation, cron next-time calculation, daylight-saving edge cases, overlap skipping, lease conflicts, coordinator failover, and backpressure

### Milestone 17.4: Stream-triggered Tasks and `SYSTEM$STREAM_HAS_DATA`

- [ ] Implement metadata-only `SYSTEM$STREAM_HAS_DATA('<stream_name>')` using stream offset versus current source object version without scanning micro-partition data
- [ ] Design the function to avoid false negatives when stream change data exists while allowing documented false positives for net-zero changes and selective view streams
- [ ] Add stream trigger indexes so table commits can mark dependent tasks due without broad catalog scans
- [ ] Support pure triggered tasks with no `SCHEDULE` and scheduled tasks that use `WHEN SYSTEM$STREAM_HAS_DATA(...)` as a skip condition
- [ ] Enforce transactional stream consumption: offsets advance only when a committed DML/CTAS/COPY-like transaction consumes the stream; failed task runs must not advance offsets
- [ ] Support multiple streams on the same table for independent consumers and document that a single stream should not be consumed by multiple tasks unless shared-offset behavior is intentional
- [ ] Add stale-stream protection hooks, health-check behavior for idle triggered tasks, and explicit false-positive consumption guidance
- [ ] Add tests for insert/update/delete CDC, empty streams, false-positive offset advancement, failed task rollback, multiple consumers, triggered task batching, and stream staleness prevention

### Milestone 17.5: Task Graph Execution, Retry, and Finalizers

- [ ] Implement graph run groups with root task version snapshots, child scheduling after predecessor success, skipped child semantics, and finalizer execution after graph completion or failure
- [ ] Support parallel child execution when multiple child tasks share a predecessor and enforce predecessor completion before multi-parent child execution
- [ ] Implement manual retry from the latest failed task and automatic retry via `TASK_AUTO_RETRY_ATTEMPTS`
- [ ] Implement `SUSPEND_TASK_AFTER_NUM_FAILURES` for standalone tasks and root task graphs with consecutive failure tracking that excludes skipped/cancelled/indeterminate system failures
- [ ] Implement task run timeout and graph timeout semantics, with child timeout overriding root timeout for that child
- [ ] Persist predecessor return values and task runtime context for use in downstream `WHEN` conditions and task bodies
- [ ] Add tests for DAG ordering, parallel branches, multi-parent joins, skipped children, finalizer success/failure, automatic retry, manual retry, timeout, and failure auto-suspension

### Milestone 17.6: Enterprise RBAC, Ownership, and Execution Identity

- [ ] Add task securable object type and privileges: `CREATE TASK` on schema, `OWNERSHIP`, `OPERATE`, `MONITOR`, `USAGE` on task, `EXECUTE TASK` on account, and `EXECUTE MANAGED TASK` for serverless task execution
- [ ] Run tasks by default as a system service using the task owner role, not the interactive user that resumed the task
- [ ] Support `EXECUTE AS USER <user>` with strict impersonation checks: owner role must have `IMPERSONATE` on the user and the user must be granted the owner role
- [ ] Re-check owner role privileges at resume and before each run, including warehouse usage for user-managed tasks and privileges required by the SQL body
- [ ] Enforce deny-by-default behavior for missing task, missing owner role, revoked warehouse access, revoked stream/table access, revoked account task execution privileges, and stale security epoch
- [ ] Record security context in run history: system user or execute-as user, owner role, active secondary-role policy, security epoch, warehouse, client/source, and authorization failures
- [ ] Add negative tests for unauthorized create, unauthorized resume/suspend, revoked owner role privileges, denied stream read, denied target DML, execute-as impersonation denial, and background-job bypass attempts

### Milestone 17.7: Observability, History, Audit, and Cost Controls

- [ ] Implement `INFORMATION_SCHEMA.TASK_HISTORY`, task graph history, and account usage views for task runs, graph runs, serverless task usage, task versions, and dependency metadata
- [ ] Emit Prometheus metrics for scheduled runs, triggered runs, skipped runs, failed runs, queue latency, execution latency, retry counts, scheduler lease conflicts, stream trigger lag, and task concurrency
- [ ] Add structured `tracing` spans for scheduler decisions, `WHEN` evaluation, dispatch, query execution, stream offset advancement, graph transitions, and RBAC decisions
- [ ] Add append-only audit events for create, alter, drop, resume, suspend, execute, retry, auto-suspend, denied access, execute-as usage, and task-owned object access
- [ ] Track cost attribution for user-managed warehouse tasks and serverless task estimates using execution duration, queued time, warehouse size, and run source
- [ ] Add retention and redaction rules for task SQL text, errors, configs, comments, session parameters, and metadata so secrets are never logged or exposed to unauthorized roles
- [ ] Add tests for history filters, visibility rules, audit redaction, metrics labels, task graph run grouping, serverless usage records, and failed authorization observability

### Milestone 17.8: Enterprise Hardening and Compatibility Boundaries

- [ ] Define idempotency guidance for task authors and ensure scheduler-generated run ids prevent duplicate graph runs during coordinator failover
- [ ] Ensure task execution uses stateless workers and can be rescheduled safely after worker crash without committing partial stream offset advancement
- [ ] Integrate result-cache and authorization-cache invalidation with task-owned DDL/DML, stream consumption, security epoch changes, and task version changes
- [ ] Define backup/restore behavior for task metadata, graph dependencies, run history retention, leases, trigger indexes, and security/audit continuity
- [ ] Document Nova-specific compatibility boundaries: no external notification integrations in the initial implementation, no arbitrary OS cron, no worker-local state, and no claim of complete Snowflake compatibility
- [ ] Add chaos and recovery tests for coordinator failover, worker crash, FDB transaction conflicts, duplicate trigger events, clock skew, large task graphs, and high-frequency streams

### Phase 17 Exit Criteria

- [ ] `CREATE/ALTER/DROP/SHOW/DESC/EXECUTE TASK` works through the MySQL protocol with interval schedules, cron schedules, `WHEN`, `AFTER`, finalizers, retry, timeout, and graph metadata
- [ ] Scheduler is HA-safe through FoundationDB leases and does not duplicate scheduled graph runs across coordinator failover
- [ ] `SYSTEM$STREAM_HAS_DATA` is metadata-only, avoids false negatives, supports documented false positives, and integrates with transactional stream consumption
- [ ] Stream-triggered tasks process committed CDC data without advancing stream offsets on failed task runs
- [ ] Task graph execution supports parallel children, multi-parent joins, skipped children, finalizers, automatic retry, manual retry, and failure auto-suspension
- [ ] RBAC denies unauthorized task creation, operation, execution, stream access, target DML, warehouse usage, and execute-as impersonation by default
- [ ] Task run history, graph history, metrics, tracing, audit, and cost attribution are queryable and redacted according to security policy
- [ ] Backup/restore, cache invalidation, worker crash recovery, coordinator failover, and FDB transaction conflicts preserve task correctness
- [ ] Documentation in `GUIDE_TASK_PLANNING.md` explains technology choices, metadata layout, execution flow, RBAC, test strategy, and Nova-specific Snowflake compatibility boundaries

---

## Phase 18: Snowflake MERGE INTO

**Goal:** Implement a Snowflake-compatible Phase 18 subset of `MERGE INTO` for conditional insert, update, and delete workflows with deterministic duplicate handling, copy-on-write micro-partition updates, enterprise RBAC, auditability, and safe execution from interactive SQL and TASK runs.

> Planning reference: `GUIDE_MERGE_INTO_PLANNING.md`. This phase builds on Phase 3 MVCC/COW DML, Phase 8 DataFusion SQL completeness, Phase 9 cache invalidation, Phase 16 enterprise security, and Phase 17 task orchestration. It must preserve immutable micro-partitions, FoundationDB atomic metadata commits, stateless workers, and deny-by-default authorization.

### Milestone 18.1: Snowflake Semantics and Compatibility Boundaries

- [ ] Document supported syntax: `MERGE INTO <target> USING <source> ON <join_expr> { matchedClause | notMatchedClause } ...`
- [ ] Support `WHEN MATCHED [AND predicate] THEN UPDATE SET ...`, `WHEN MATCHED [AND predicate] THEN UPDATE ALL BY NAME`, and `WHEN MATCHED [AND predicate] THEN DELETE`
- [ ] Support `WHEN NOT MATCHED [AND predicate] THEN INSERT [(columns)] VALUES (...)` and `WHEN NOT MATCHED [AND predicate] THEN INSERT ALL BY NAME`
- [ ] Enforce clause reachability: a catch-all `WHEN MATCHED` or `WHEN NOT MATCHED` clause without `AND` must be last for that clause type
- [ ] Implement Snowflake-style duplicate source behavior for target rows, including deterministic delete-only cases, exactly-one-update cases, and default errors for ambiguous update/delete conflicts
- [ ] Implement a Phase 18 parameter surface for `ERROR_ON_NONDETERMINISTIC_MERGE`, defaulting to `TRUE`, with at least session-scoped `FALSE` support for nondeterministic compatibility mode; account/user resolution can plug into the Phase 16 parameter model when available
- [ ] Document unsupported initial scope explicitly: no `WHEN NOT MATCHED BY SOURCE`, no `RETURNING`, no multi-target merge, no worker-local state, and no claim of complete Snowflake compatibility

### Milestone 18.2: Parser, Analyzer, and Resolved AST

- [ ] Evaluate sqlparser-rs native `MERGE` AST support and add a Nova pre-parser only if required for Snowflake-specific `ALL BY NAME` gaps
- [ ] Add `ResolvedStatement::Merge` with target relation, source relation or subquery, join expression, ordered clauses, action expressions, and all-by-name flags
- [ ] Resolve target/source aliases, column references, expression types, and source-only versus target/source expression restrictions for insert/update actions
- [ ] Validate duplicate target column assignments, generated/read-only column constraints when applicable, `ALL BY NAME` column count/name equality, and clause ordering before execution
- [ ] Preserve original SQL text and normalized clause metadata for task version snapshots, audit, query history, and error reporting
- [ ] Add parser/analyzer tests for valid mixed MERGE, aliasing, subquery source, unreachable clauses, invalid column references, invalid all-by-name inputs, and multiple statement handling

### Milestone 18.3: Logical Planning and Execution Strategy

- [ ] Build a logical MERGE plan that joins target and source once, classifies matched and not-matched rows, evaluates clauses in order, and records one intended action per target row/source row pair
- [ ] Ensure unmatched source duplicate rows are all inserted when no target row matches, matching Snowflake deterministic insert behavior
- [ ] Detect ambiguous matched actions before committing changes when `ERROR_ON_NONDETERMINISTIC_MERGE=TRUE`
- [ ] Provide a guarded nondeterministic mode for `ERROR_ON_NONDETERMINISTIC_MERGE=FALSE` with explicit tracing/audit that the selected source row/action is undefined
- [ ] Use DataFusion for source evaluation, join evaluation, predicate evaluation, projection, and expression computation while preserving Nova's MVCC snapshot and metadata pruning rules
- [ ] Return Snowflake-style row count summaries for inserted, updated, and deleted rows through executor, scheduler, MySQL protocol, and task run history
- [ ] Add tests for simple update, insert-only, delete-only, mixed clauses, duplicate source conflicts, deterministic duplicate delete, deterministic single update, and all duplicate inserts

### Milestone 18.4: Copy-on-Write Storage, MVCC, and FDB Atomicity

- [ ] Reuse UPDATE/DELETE copy-on-write micro-partition rewriting for matched updates/deletes and INSERT micro-partition writing for not-matched inserts
- [ ] Commit all MERGE effects atomically in FoundationDB: new MP metadata, superseded MP metadata, table version increments, transaction records, stream change metadata, and audit references
- [ ] Preserve statement-level snapshot semantics so source duplicate inserts do not see rows inserted earlier in the same MERGE statement
- [ ] Ensure failed MERGE statements roll back new metadata visibility and do not advance source stream offsets, task offsets, result-cache versions, or audit success records
- [ ] Add conflict detection for concurrent MERGE/UPDATE/DELETE on the same target table or affected MPs, with deterministic retry/idempotency behavior for FDB transaction retries
- [ ] Invalidate result cache using table version changes; rely on security epoch changes, not ordinary MERGE DML, for authorization cache invalidation
- [ ] Add tests for COW visibility, time travel before/after MERGE, clone isolation, stream CDC output, FDB conflict retry, rollback on error, and cache invalidation

### Milestone 18.5: Enterprise RBAC, Governance, and Audit

- [ ] Require `SELECT` on every source table/view/stream referenced by the source relation or subquery
- [ ] Require target privileges statically for every action clause present after validation: `INSERT` for insert clauses, `UPDATE` for update clauses, and `DELETE` for delete clauses; authorization must not depend on row contents or whether a clause happens to match at runtime
- [ ] Enforce row access policies and masking policies for source rows and target rows selected by MERGE without bypass through join predicates, clause predicates, or all-by-name expansion, and define whether policy-hidden target rows are denied or treated as not matched before implementation
- [ ] Deny by default when user, active role, target object, source object, privilege, policy, or security epoch resolution is missing or stale
- [ ] Record query access history and lineage for source objects read, target table modified, columns read, columns updated, rows inserted/updated/deleted, policies referenced, and denied authorization attempts
- [ ] Redact SQL text, expression values, and policy internals in errors, audit, and task run history according to Phase 16 security rules
- [ ] Add negative tests for missing source SELECT, missing target INSERT/UPDATE/DELETE, revoked privileges after planning, policy-protected rows, masked join/update expressions, MySQL protocol execution, and background task bypass attempts

### Milestone 18.6: TASK Integration and Stream-Triggered MERGE Workflows

- [ ] Allow task SQL bodies to contain MERGE statements and persist the MERGE SQL in immutable task version snapshots
- [ ] Execute MERGE tasks with the Phase 17 task security context: system service user plus task owner role by default, or validated `EXECUTE AS USER` when configured
- [ ] Re-check owner role privileges before each task run, including source SELECT, stream access, target DML privileges, warehouse usage, and account-level task execution privileges
- [ ] Integrate MERGE with stream-triggered tasks so `SYSTEM$STREAM_HAS_DATA` can trigger CDC upsert workflows without scanning MP data unnecessarily
- [ ] Advance source stream offsets only when the task transaction commits successfully; failed MERGE task runs must leave source stream offsets unchanged for retry
- [ ] Store MERGE row counts, nondeterminism errors, authorization failures, table versions, source stream offsets consumed, and target table versions in task run history
- [ ] Add tests for scheduled MERGE task, stream-triggered MERGE task, failed MERGE retry without offset advancement, revoked owner role DML privilege, execute-as impersonation denial, and coordinator failover during a queued MERGE task

### Milestone 18.7: Observability, Benchmarks, and Hardening

- [ ] Emit tracing spans for parse/analyze/plan, source evaluation, join classification, duplicate detection, COW rewrite, FDB commit, cache invalidation, RBAC decisions, and task execution context
- [ ] Add metrics for MERGE rows scanned, matched, inserted, updated, deleted, duplicate-conflict errors, rewritten MPs, write amplification, commit latency, and task-trigger latency
- [ ] Add benchmark scenarios for small upserts, large CDC batches, high-duplicate source data, all-by-name merges, selective joins with MP pruning, and task-driven stream consumption
- [ ] Define performance guardrails for MP rewrite amplification, memory usage during join/action classification, and backpressure for very large source relations
- [ ] Add compatibility tests comparing documented Snowflake examples to Nova expected results where behavior is in scope
- [ ] Document operational guidance: source de-duplication with `GROUP BY`, idempotent task MERGE design, lock/conflict behavior, and unsupported Snowflake features

### Phase 18 Exit Criteria

- [ ] `MERGE INTO` works through parser, analyzer, executor, scheduler, MySQL protocol, and task execution paths
- [ ] Matched UPDATE/DELETE, not-matched INSERT, multiple ordered clauses, `ALL BY NAME`, source subqueries, aliases, and row-count output are covered by tests
- [ ] Duplicate source behavior matches Snowflake-compatible deterministic rules and defaults to error for nondeterministic update/delete conflicts
- [ ] MERGE preserves immutable MP copy-on-write, MVCC time travel, clone isolation, stream CDC correctness, result-cache invalidation, and FDB atomicity
- [ ] RBAC and governance deny unauthorized interactive and background MERGE execution by default, with negative bypass tests
- [ ] TASK integration supports scheduled and stream-triggered MERGE workflows without advancing source stream offsets on failed runs
- [ ] Interactive and task `MERGE ... USING <stream>` advance source stream offsets only after the same transaction commits successfully
- [ ] Concurrent MERGE/UPDATE/DELETE on the same target table or affected MPs has tested conflict, retry, and idempotency behavior
- [ ] Documentation in `GUIDE_MERGE_INTO_PLANNING.md` explains references, implementation steps, RBAC, task integration, tests, and Nova-specific compatibility boundaries

---

## Dependency Graph

```
Phase 1 (Foundation)
  └── Phase 2 (Query Engine)
        └── Phase 3 (Snowflake Features)
              ├── Phase 4 (Distributed)
              └── Phase 5 (CBO Enhancement)
                    └── Phase 6 (Cache & Polish)
                          └── Phase 16 (Enterprise Security & Governance)
                                └── Phase 17 (Enterprise Task Orchestration)
                                      └── Phase 18 (Snowflake MERGE INTO)
```

This graph is condensed; the bullets below call out additional completed-phase capabilities such as Phase 8 SQL completeness, Phase 9 cache invalidation, Phase 10 distributed execution, Phase 11 HA, and Phase 13 Dynamic Tables where relevant.

- Phase 4 and Phase 5 can run in parallel (different teams/agents)
- Phase 6 depends on both Phase 4 and Phase 5
- Phase 16 depends on baseline SQL execution, DataFusion integration, RBAC enforcement, cache invalidation, dynamic tables, HA, and distributed execution being available
- Phase 17 depends on Streams, Dynamic Tables scheduler patterns, HA coordinator leadership, distributed execution, and Phase 16 enterprise RBAC/security context being available
- Phase 18 depends on COW UPDATE/DELETE/INSERT, MVCC/streams, DataFusion SQL execution, result-cache invalidation, Phase 16 enterprise RBAC, and Phase 17 task execution context being available
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
| Task scheduled-run jitter | < 5 seconds p95 | Due time to queued run under normal load |
| Task stream-trigger latency | < 5 seconds p95 | Source commit to task queued for triggered tasks |
| Task HA duplicate prevention | 0 duplicate graph runs | Coordinator failover and FDB lease conflict tests |
| MERGE CDC batch | Deterministic correctness first | Insert/update/delete counts match expected results |
| MERGE COW amplification | Measured and bounded | Rewritten MPs and latency reported per benchmark |
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
- [x] E2E: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)
- [x] E2E: GROUP BY with aggregates
- [x] E2E: ORDER BY col DESC
- [x] E2E: LIMIT / OFFSET
- [x] E2E: DISTINCT
- [x] E2E: HAVING (post-aggregate filter)
- [ ] E2E: Subquery (SELECT * FROM (SELECT ...))

#### 9.2: E2E Tests for JOIN
- [x] E2E: INNER JOIN (2 tables)
- [x] E2E: LEFT JOIN
- [x] E2E: 3-table JOIN
- [ ] Verify CBO join reordering is active

#### 9.3: COW Visibility Fix (DataFusion reads stale MPs)
- [x] After UPDATE/DELETE, DataFusion path must read updated active MPs
- [x] Root cause: exec_select_datafusion receives stale `mps` snapshot
- [x] Fix: re-fetch active MPs inside exec_select_datafusion, or invalidate cache on COW

### P1: Important (production quality)

#### 9.4: ResultCache Table Version Tracking
- [x] Track table versions from metadata (not empty HashMap)
- [x] Call meta.get_table_version() before cache lookup
- [x] Invalidate cache on INSERT/UPDATE/DELETE via version change
- [x] Test: insert → select (miss) → select (hit) → insert → select (miss)

#### 9.5: Auth Enforcement in MySQL Handshake
- [x] Verify password via AuthManager during MySQL handshake
- [x] When auth.enabled=true, reject connections with wrong password
- [x] When auth.enabled=false, accept all (dev mode, current behavior)
- [x] Test: connect with correct password → success; wrong password → error

#### 9.6: RBAC Enforcement in DDL/DML
> Baseline DDL/DML enforcement only. Phase 16 replaces this with FDB-backed role ownership, role hierarchy, managed access schemas, policy enforcement, and security-aware cache invalidation.

- [x] Call rbac.check_privilege() before CREATE TABLE / DROP TABLE / INSERT / UPDATE / DELETE
- [x] Track current user from MySQL session
- [ ] Test: non-admin user cannot DROP TABLE

#### 9.7: DataFusion Path MP Pruning
- [x] NovaTableProvider::scan() should use filter predicates for MP pruning
- [x] Pass _filters to MicroPartitionScanExec for predicate pushdown
- [x] Currently: DataFusion path reads all active MPs, no pruning

#### 9.8: DataFusion Path Statistics
- [x] NovaTableProvider should expose statistics to DataFusion optimizer
- [x] Implement TableProvider::statistics() method
- [x] Currently: statistics only collected in legacy path

### P2: Enhancement (nice to have)

#### 9.9: Foyer HybridCache (replace HashMap)
- [x] Add LRU eviction to NovaCache (max 500 MP entries, max 1000 result entries)
- [ ] Replace HashMap with foyer::HybridCache (RAM + SSD) — future (async init complexity)
- [x] Test: cache hit/miss/eviction (3 tests)

#### 9.10: Parallel Scan in DataFusion Path
- [x] NovaTableProvider::scan() creates MicroPartitionScanExec with 1 partition per MP
- [x] DataFusion executes partitions in parallel (target_partitions=1 for single-node)

#### 9.11: ALTER TABLE Support
- [x] Parser: sqlparser native ALTER TABLE
- [x] Analyzer: resolve to ResolvedStatement::AlterTable with AlterAction
- [x] Executor: ADD COLUMN / DROP COLUMN (metadata-only, existing MPs unchanged)

#### 9.12: Time Travel SQL Syntax
- [x] Parser: `SELECT * FROM t AT(TIMESTAMP => <unix_micros>)`
- [x] Analyzer: detect __tt_ prefix, resolve to Select with at_timestamp
- [x] Executor: uses get_mps_at_timestamp() (already implemented)

#### 9.13: E2E Tests for Snowflake Features
- [x] E2E: CREATE TABLE x CLONE y → verify success
- [x] E2E: GC <retention> → verify success
- [x] E2E: BACKUP TO /path → verify graceful handling

#### 9.14: gRPC Proto Definitions (multi-node only)
- [x] Define .proto files for coordinator↔worker RPC
- [x] RegisterWorker, Heartbeat, ExecuteFragment, StreamResults
- [ ] Generate tonic stubs (future — when distributed mode is needed)

### Phase 9 Exit Criteria

- [x] All P0 features implemented with tests
- [x] All P1 features implemented with tests
- [x] 300+ tests total
- [x] COW visibility verified (UPDATE → SELECT sees updated data)
- [x] ResultCache invalidation verified (INSERT → cache miss)
- [x] Auth enforcement verified (wrong password rejected)
- [x] clippy clean, fmt clean
- [x] Zero TODO/FIXME in production code
