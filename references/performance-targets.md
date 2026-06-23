# Performance Targets & Benchmark Methodology

> nova-core performance goals, benchmark suites, and comparison methodology.

---

## 1. Benchmark Suites

| Benchmark | Purpose | Phase | Target |
|---|---|---|---|
| **ClickBench** | Single-node scan/filter/agg on Parquet | Phase 2 | Within 1.2x of DataFusion 43 |
| **TPC-H** (100GB) | Analytical queries with JOINs | Phase 2-4 | All 22 queries, within 2x of StarRocks |
| **TPC-DS** (100GB) | Complex analytical queries | Phase 5 | All 99 queries, within 2x of StarRocks |
| **Time Travel overhead** | MVCC version lookup overhead | Phase 3 | < 1.5x of current-time query |
| **Clone speed** | Zero-copy clone latency | Phase 3 | < 1 second, any table size |
| **Stream latency** | INSERT to stream visibility | Phase 3 | < 1 second |
| **Cache hit ratio** | Query result cache effectiveness | Phase 6 | > 80% for repeated queries |
| **Distributed scale-up** | Multi-worker speedup | Phase 4 | 3x+ with 4 workers vs 1 |
| **Coordinator failover** | HA recovery time | Phase 4 | < 10 seconds |
| **Worker auto-scale** | Provisioning time | Phase 4 | < 60 seconds |

---

## 2. Performance Targets

### 2.1 Query Latency (Single-Node, 16 vCPU, 32GB RAM)

```
Scenario: 100M rows, 14GB Parquet data (ClickBench scale)

                          Cold (first)    Warm (cached MP)   Hot (result cached)
Scan + Filter:            < 3 seconds     < 1 second          < 10ms
Aggregation (COUNT/SUM):  < 3 seconds     < 1 second          < 10ms
JOIN (2 tables):          < 5 seconds     < 2 seconds         < 10ms
Complex JOIN (5 tables):  < 10 seconds    < 5 seconds         < 10ms
```

### 2.2 Write Throughput

```
INSERT (batch, 8192 rows):     > 500K rows/sec
INSERT (single row):           > 10K rows/sec
UPDATE (COW, 1% selectivity):  > 50K rows/sec
DELETE (COW, 1% selectivity):  > 50K rows/sec
MP write (50MB Parquet):       < 500ms
```

### 2.3 Cache Performance

```
L1 (Query Result Cache):
  Hit rate (repeated queries):  > 80%
  Hit latency:                  < 10ms
  Miss latency:                 = full query execution

L2 (Metadata Cache):
  Hit rate:                     > 95% (metadata rarely changes)
  Hit latency:                  < 1ms (RAM)
  Miss latency:                 < 5ms (FDB read)

L3 (MP Data Cache):
  Hit rate (hot data):          > 70%
  Hit latency (RAM):            < 200ms per MP
  Hit latency (SSD):            < 1s per MP
  Miss latency (S3):            < 3s per MP
```

### 2.4 Distributed Performance

```
Scale-up efficiency (TPC-H Q1, 100GB):
  1 worker:   1x (baseline)
  2 workers:  1.8x (> 90% efficiency)
  4 workers:  3.2x (> 80% efficiency)
  8 workers:  5.6x (> 70% efficiency)
```

### 2.5 Snowflake Feature Overhead

```
Time Travel query:       < 1.5x of current-time query (version lookup overhead)
Zero-Copy Clone:         < 1 second (metadata only)
Stream read:             < 1 second latency from INSERT
GC background:           < 5% CPU when active, 0% when idle
```

### 2.6 Resource Usage

```
Memory per worker:       < 8GB (including cache)
Startup time (worker):   < 5 seconds (Rust, no JVM warmup)
Binary size:             < 50MB (single binary)
Coordinator memory:      < 4GB (metadata + planning)
```

---

## 3. Comparison Methodology

### 3.1 Fair Comparison Rules

1. **Same hardware:** All benchmarks run on identical VMs (same vCPU, RAM, disk)
2. **Same data:** Identical dataset (ClickBench hits dataset, TPC-H/DS at same scale factor)
3. **Same format:** Compare Parquet-to-Parquet (don't compare our Parquet vs StarRocks native format)
4. **Cold + Warm + Hot:** Run each query 3 times:
   - Cold: first run, no cache, OS page cache cleared
   - Warm: second run, MP data may be cached
   - Hot: third run, query result may be cached
5. **Single-node:** Phase 2-3 benchmarks are single-node only (fair comparison with DuckDB/ClickHouse)
6. **Distributed:** Phase 4+ benchmarks use 4-worker cluster (fair comparison with StarRocks)

### 3.2 Systems to Compare Against

| System | Version | Benchmark | Notes |
|---|---|--- Parquet query speed | ClickBench winner Nov 2024 |
| DuckDB | latest | ClickBench, TPC-H | C++ baseline |
| ClickHouse | latest | ClickBench | C++ baseline, weak at JOINs |
| StarRocks | 4.1.x | TPC-H, TPC-DS | Java/C++ baseline, industry leader |
| PostgreSQL | 16+ | TPC-H | OLTP baseline (should be 10-100x slower) |

### 3.3 Metrics to Record

For each query:
- **Wall clock time** (ms)
- **CPU time** (user + system)
- **Peak memory** (MB)
- **Bytes read from S3** (MB)
- **Cache hit/miss** (L1, L2, L3)
- **MP pruning ratio** (MPs scanned / total MPs)
- **Worker count** (for distributed)

---

## 4. Performance Optimization Techniques

### 4.1 Techniques Already in DataFusion (Free)

| Technique | Impact | Source |
|---|---|---|
| Vectorized execution (batch=8192) | 10-100x vs row-at-a-time | MonetDB/X100 |
| Push-based pipeline | Better cache efficiency, backpressure | DuckDB |
| Parquet predicate pushdown | Skip row groups by min/max | Parquet spec |
| Parquet projection pushdown | Read only needed columns | Parquet spec |
| Parallel scan | Utilize all CPU cores | DataFusion |
| Two-phase aggregation | Reduce shuffle data | DataFusion |
| Memory pool + spill | Avoid OOM, spill to SSD | DataFusion |
| SIMD auto-vectorization | 16-32 values per instruction | LLVM |

### 4.2 Nova-Specific Optimizations

| Technique | Phase | Expected Impact |
|---|---|---|
| MP metadata pruning (zone maps) | Phase 2 | 10-100x faster range scans |
| Runtime Bloom filter pushdown | Phase 5 | 5-50x faster JOINs |
| Colocated join (no shuffle) | Phase 4 | 2-5x faster star schema JOINs |
| Late materialization | Phase 5 | 2-10x less I/O |
| Dictionary encoding direct ops | Phase 5 | 2-5x faster string filters |
| Foyer hybrid cache (RAM+SSD) | Phase 6 | 25x more cache than RAM-only |
| Query result cache | Phase 6 | 600x faster repeated queries |
| No JVM overhead | All phases | Lower latency, no GC stalls |
| No compaction overhead | All phases | No background CPU drain |

---

## 5. Benchmark Execution

### 5.1 ClickBench

```bash
# Setup: load ClickBench dataset (14GB, 100M rows) as Parquet
# https://github.com/ClickHouse/ClickBench

# Run all 43 queries
./benchmarks/run_clickbench.sh --engine nova-core --runs 3

# Compare
./benchmarks/compare.sh --engines nova-core,datafusion,duckdb,clickhouse
```

### 5.2 TPC-H

```bash
 # TPC-H 100GB
./benchmarks/run_tpch.sh --engine nova-core --scale 100 --queries all

# Compare with StarRocks
./benchmarks/run_tpch.sh --engine starrocks --scale 100 --queries all
```

### 5.3 Time Travel Overhead

```bash
# 1. Run current-time query (baseline)
SELECT COUNT(*) FROM orders;  -- measure time

# 2. Run Time Travel query (1 hour ago)
SELECT COUNT(*) FROM orders AT(TIMESTAMP => now() - interval '1 hour');  -- measure time

# 3. Compare: Time Travel should be < 1.5x of current-time
```

### 5.4 Clone Speed

```bash
# 1. Create large table (100GB)
CREATE TABLE large_table AS SELECT * FROM generate_series(1, 1000000000);

# 2. Clone and measure time
\timing on
CREATE TABLE large_clone CLONE large_table;  -- should be < 1 second

# 3. Verify zero-copy (S3 file count should not increase)
aws s3 ls s3://nova-bucket/tables/ | wc -l  -- should be same
```

### 5.5 Cache Effectiveness

```bash
# 1. Cold query (no cache)
SELECT COUNT(*) FROM orders WHERE dt > '2026-01-01';  -- measure time (T1)

# 2. Warm query (MP cached)
SELECT COUNT(*) FROM orders WHERE dt > '2026-01-01';  -- measure time (T2)

# 3. Hot query (result cached)
SELECT COUNT(*) FROM orders WHERE dt > '2026-01-01';  -- measure time (T3)

# T1 > T2 > T3
# T3 should be < 10ms
```

---

## 6. Performance Regression Testing

### 6.1 CI Performance Gate

```yaml
# .github/workflows/perf.yml
name: Performance Regression
on: [pull_request]
jobs:
  perf:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run ClickBench subset
        run: ./benchmarks/run_clickbench.sh --queries Q1,Q6,Q12,Q18 --runs 1
      - name: Compare with main branch
        run: ./benchmarks/compare_with_main.sh --threshold 1.2
        # Fail if any query is > 1.2x slower than main branch
```

### 6.2 Benchmark Storage

- Benchmark results stored in `benchmarks/results/` as JSON
- Historical trends tracked in Grafana dashboard
- Regression threshold: 1.2x slower than previous release = fail CI

---

## 7. Hardware Specifications for Benchmarks

### Standard Benchmark Machine

```
Cloud: AWS EC2 c6a.4xlarge (or equivalent)
CPU: 16 vCPU (AMD EPYC 7R13)
RAM: 32 GB
Disk: 500GB NVMe SSD (for cache)
Network: 10 Gbps
OS: Ubuntu 24.04 LTS
```

### Production Reference Architecture

```
Small:  4 vCPU, 16GB RAM, 100GB SSD   (dev/test)
Medium: 16 vCPU, 32GB RAM, 500GB SSD  (production small)
Large:  32 vCPU, 64GB RAM, 1TB SSD    (production large)
```
