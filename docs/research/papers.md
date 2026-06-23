# Research Papers & References

> Academic papers and technical references that inform nova-core's architecture.

---

## Foundational Papers

### Vectorized Execution

| Paper | Authors | Year | Relevance |
|---|---|---|---|
| [MonetDB/X100: Hyper-Pipelining Query Execution](https://www.cidrdb.org/cidr2005/papers/P19.pdf) | Boncz, Zukowski, Nes | 2005 | Vectorized execution model — process data in columnar batches (vectors) that fit in CPU cache. 1-2 orders of magnitude faster than tuple-at-a-time. |
| [Vectorwise: Beyond Column Stores](http://sites.computer.org/debull/A12mar/vectorwise.pdf) | Boncz et al. | 2013 | Commercialization of X100. Lazy vectorized expression evaluation, SIMD predicate evaluation, NULL-processing optimizations. |
| [MonetDB: Two Decades of Research](https://ir.cwi.nl/pub/19929/19929B.pdf) | Boncz et al. | 2013 | Overview of column-store innovations: vertical fragmentation, cache-conscious execution, adaptive indexing. |

### Cloud-Native Architecture

| Paper | Authors | Year | Relevance |
|---|---|---|---|
| [The Snowflake Elastic Data Warehouse](https://dl.acm.org/doi/10.1145/2882903.2903741) | Craige et al. | 2016 (SIGMOD) | Multi-cluster shared-data architecture. Separation of compute and storage. Immutable micro-partitions in S3. Time Travel, zero-copy clone. |
| [Building An Elastic Query Engine on Disaggregated Storage](https://www.usenix.org/conference/nsdi20/presentation/vuppalapati) | Vuppalapati et al. | 2020 (NSDI) | Snowflake operational experience. 70M queries over 14 days. Design implications of cloud infrastructure. |
| [Building a Data Management System for the Cloud: Lessons Learned](https://link.springer.com/article/10.1007/s13222-025-00494-9) | Snowflake team | 2025 (Springer) | 10-year retrospective. Immutable files, background maintenance, cloud-specific optimization. |

### Query Optimization

| Paper | Authors | Year | Relevance |
|---|---|---|---|
| [The Volcano Optimizer Generator](https://15721.courses.cs.cmu.edu/spring2023/slides/22-duckdb.pdf) | Graefe | 1995 | Cascades framework — basis for modern CBO (StarRocks, Snowflake, PostgreSQL). |
| [ORCA: A Modular Query Optimizer](https://15721.courses.cs.cmu.edu/spring2023/slides/22-duckdb.pdf) | Palkert et al. | 2010 | Modular optimizer architecture. Influenced StarRocks CBO. |

### Execution Models

| Paper | Authors | Year | Relevance |
|---|---|---|---|
| [Push vs Pull: Is It Really a Myth?](https://arxiv.org/pdf/1610.09166) | Markl et al. | 2017 | Analysis of push-based vs pull-based execution. Neither is clear winner, but push enables better parallelism. |
| [DuckDB Push-Based Execution](https://15721.courses.cs.cmu.edu/spring2023/slides/22-duckdb.pdf) | Raasveldt | 2023 (CMU) | DuckDB's migration from pull to push. Pipeline parallelism, operator isolation. |
| [Velox: Meta's Unified Execution Engine](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf) | Pedreira et al. | 2022 (VLDB) | Reusable C++ execution components. Vectorized kernels, LLVM expression engine. |

### Late Materialization

| Paper | Authors | Year | Relevance |
|---|---|---|---|
| [Materialization Strategies in a Column-Oriented DBMS](https://www.cs.umd.edu/~abadi/papers/abadiicde2007.pdf) | Abadi et al. | 2007 (ICDE) | When to stitch columns into tuples. Late materialization advantages for selective queries. |
| [Selective Late Materialization in Modern Analytical Databases](https://www.vldb.org/pvldb/vol18/p4616-liu.pdf) | Liu et al. | 2025 (VLDB) | Modern SLM approach integrated into DuckDB. Outperforms MariaDB ColumnStore by 50%. |
| [The Vertica Analytic Database: C-Store 7 Years Later](https://andrew.nerdnetworks.org/pdf/p1790_andrewlamb_vldb2012.pdf) | Lamb et al. | 2012 (VLDB) | Sideways information passing, late materialization in production. |

### Storage & Compression

| Paper | Authors | Year | Relevance |
|---|---|---|---|
| [C-Store: A Column-oriented DBMS](https://www.vldb.org/conf/2005/paper12.pdf) | Stonebraker et al. | 2005 (VLDB) | Pioneering column-store. Projections, sort orders, compression. |
| [Cache-Conscious Columnar Data](https://ir.cwi.nl/pub/11098) | Zukowski et al. | 2005 | In-cache vectorized processing. Cooperative scans, lightweight compression. |

---

## Industry References

### DataFusion (nova-core's execution engine)

| Reference | Relevance |
|---|---|
| [DataFusion: Fastest Single-Node Parquet Engine (ClickBench 2024)](https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/) | DataFusion 43 beat DuckDB and ClickHouse. First Rust engine to hold top spot. |
| [DataFusion ClickBench Performance Analysis](https://alamb.github.io/datafusion-benchmarking/) | Ongoing performance tracking across releases. |
| [DataFusion Documentation](https://datafusion.apache.org/) | API docs, user guide, developer guide. |

### StarRocks (competitor, architectural reference)

| Reference | Relevance |
|---|---|
| [Deep Dive: StarRocks Vectorized Engine](https://medium.com/starrocks-engineering/deep-dive-how-starrocks-built-a-high-performance-vectorized-engine-156ab9c38328) | 7 categories of vectorization optimization. SIMD, column pool, adaptive optimization. |
| [StarRocks CBO Documentation](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/) | Cascades-based CBO. 99 TPC-DS queries. Statistics collection. |
| [How to Build an Extremely Fast Analytical Database](https://medium.com/starrocks-engineering/how-to-build-an-extremely-fast-analytical-database-part3-78c5a06b88ed) | CBO + vectorized execution + runtime filters. |

### Snowflake (architectural inspiration)

| Reference | Relevance |
|---|---|
| [Snowflake Time Travel & Fail-safe](https://docs.snowflake.com/en/user-guide/data-availability.html) | 1-90 day retention, query historical data, restore dropped objects. |
| [Snowflake Query Result Cache](https://docs.snowflake.com/en/user-guide/querying-persisted-results) | 24-hour cache, auto-invalidation on data change, 31-day max. |
| [Snowflake FoundationDB Migration](https://medium.com/snowflake/migrating-snowflakes-metadata-with-no-downtime-ca90604b677c) | FDB as metadata store, multi-region replication, zero-downtime migration. |
| [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables/) | Target lag, refresh modes (INCREMENTAL, FULL, AUTO, ADAPTIVE). |

### Foyer / RisingWave (cache reference)

| Reference | Relevance |
|---|---|
| [Foyer: Hybrid Cache for Rust](https://github.com/foyer-rs/foyer) | Hybrid RAM + SSD cache. Zero-copy abstraction. Compaction-aware refill. |
| [RisingWave Case Study with Foyer](https://foyer-rs.github.io/foyer/docs/case-study/risingwave) | Production use of Foyer for S3-based streaming database. Reduced S3 access, improved performance. |
| [The Case for Hybrid Cache for Object Stores](https://risingwave.com/blog/the-case-for-hybrid-cache-for-object-stores/) | Why hybrid cache is critical for S3-based systems. |

### FoundationDB (metadata store)

| Reference | Relevance |
|---|---|
| [FoundationDB Documentation](https://apple.github.io/foundationdb/) | ACID KV store, ordered keys, multi-region replication. |
| [Snowflake Metadata powered by FDB](https://news.ycombinator.com/item?id=16880379) | Snowflake's use of FDB for metadata. Not a SQL layer — object mapping on KV. |

---

## nova-core Architecture Document

| Document | Relevance |
|---|---|
| [nova-core Architecture](../design/architecture.md) | Complete architecture: storage, metadata, coordinator, worker, cache, MVCC, HA. |

---

## Paper-to-Feature Mapping

| nova-core Feature | Inspired By | Key Insight |
|---|---|---|
| Vectorized execution (batch=8192) | MonetDB/X100 (2005) | Process columnar batches that fit in L1 cache |
| Push-based pipeline | DuckDB (2021) | Data-driven flow enables pipeline parallelism |
| Immutable micro-partitions | Snowflake (2016) | Never modify files → Time Travel + Clone + Streams |
| Late materialization | C-Store (2005), Vertica (2012) | Delay column fetch until after filtering |
| MP metadata pruning (zone maps) | Snowflake zone maps | Skip files by checking min/max stats before read |
| Runtime Bloom filter | StarRocks Global Runtime Filter | Push join filter to scan for pre-filtering |
| Query result cache | Snowflake result cache | MVCC version = natural cache invalidation key |
| Hybrid cache (RAM+SSD) | Foyer / RisingWave | 25x more cache capacity than RAM-only |
| FoundationDB metadata | Snowflake | ACID KV for metadata, proven at scale |
| Colocated join | StarRocks colocate | Same distribution key → local join, no shuffle |
| Dictionary encoding ops | StarRocks "Operation on Encoded Data" | Operate on INT codes, decode only at output |
