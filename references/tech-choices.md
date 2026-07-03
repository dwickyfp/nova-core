# Technology Choices — nova-core

> Detailed rationale for every technology choice in nova-core.
> Each decision includes alternatives considered and why they were rejected.

---

## 1. Language: Rust

| Aspect | Detail |
|---|---|
| **Chosen** | Rust (edition 2024) |
| **Alternatives** | C++, Java, Go, Zig |
| **Decision date** | June 2026 |

### Why Rust?

| Factor | Rust | C++ | Java | Go |
|---|---|---|---|---|
| Memory safety | ✅ Compile-time | ❌ UB risk | ✅ GC | ✅ GC |
| Zero-cost abstractions | ✅ | ✅ | ❌ JVM overhead | ❌ GC + interface dispatch |
| No GC pauses | ✅ | ✅ | ❌ | ❌ |
| Async ecosystem | ✅ tokio | ⚠️ | ✅ | ✅ |
| Cargo (dependency mgmt) | ✅ best-in-class | ❌ CMake | ✅ Maven | ✅ go mod |
| Arrow/DataFusion native | ✅ | ✅ (Arrow C++) | ✅ (Arrow Java) | ⚠️ (bindings) |
| DB engine precedent | RisingWave, InfluxDB | StarRocks, ClickHouse | StarRocks FE, Snowflake | CockroachDB, TiDB |

### Why not C++?

- Memory safety: C++ has undefined behavior risk. StarRocks BE (C++) has latent bugs.
- Build system: CMake is painful. Cargo is superior.
- Dependency management: vcpkg/conan are inferior to Cargo.
- The performance gap is negligible (Rust LLVM vs C++ LLVM).

### Why not Java?

- GC pauses: StarRocks FE (Java) has GC stalls. Snowflake uses Java for Cloud Services but admits they'd use a different language today.
- Memory overhead: JVM object overhead is 2-5x vs native.
- Startup time: JVM warmup. Rust = instant startup.

### Why not Go?

- GC pauses: Go's GC has improved but still pauses.
- Performance: Go's interface dispatch and GC make it slower than Rust for compute-intensive workloads.
- Generics: Go generics (1.18+) are limited compared to Rust's trait system.
- No zero-copy ecosystem: Arrow/Parquet in Go is less mature.

### Why not Zig?

- Ecosystem: Zig's package manager and async ecosystem are still nascent.
- Community: Smaller community, fewer DB libraries.
- Arrow/Parquet: No mature Zig bindings.

---

## 2. Query Engine: Apache DataFusion

| Aspect | Detail |
|---|---|
| **Chosen** | Apache DataFusion 43+ |
| **Alternatives** | Build from scratch, DuckDB (C++), Velox (C++), ClickHouse (C++), Peloton |
| **Decision date** | June 2026 |

### Why DataFusion?

1. **#1 ClickBench (November 2024)** — fastest single-node Parquet engine, beating DuckDB and ClickHouse
2. **Rust-native** — built on Arrow, zero-copy integration
3. **Extensible** — trait-based operator/optimizer/planner extension
4. **80% ready** — scan, filter, project, join, agg, sort, window, union all built-in
5. **Active community** — InfluxData, DataBend, GreptimeDB, RisingWave all use it
6. **Open source** — Apache 2.0

### Why not build execution engine from scratch?

- 80+ operators and 200+ functions would take 2+ years to implement
- DataFusion already has vectorized, push-based execution
- DataFusion's Parquet reader is the most sophisticated open-source reader

### Why not DuckDB?

- C++ (not Rust): would need FFI, loses memory safety
- Not extensible: DuckDB is a monolithic engine, not a library
- ClickBench: DataFusion beat DuckDB in November 2024

### Why not Velox?

- C++ (not Rust)
- Designed as a library for Meta's internal systems, not standalone engine
- No Rust bindings

---

## 3. Metadata Store: FoundationDB

| Aspect | Detail |
|---|---|
| **Chosen** | FoundationDB for dev and production |
| **Alternatives** | PostgreSQL, etcd, TiKV, FoundationDB (production), RocksDB |
| **Decision date** | June 2026 |

### Why FoundationDB?

1. **Proven at Snowflake scale** — metadata store for millions of queries per day
2. **ACID transactions** — strict serializable isolation
3. **Ordered key-value** — efficient prefix range scans
4. **Multi-region replication** — built-in HA
5. **Open source** — Apache 2.0
6. **Rust binding** — `foundationdb-rs` (community maintained)

### Why not PostgreSQL?

- Not a key-value store: our metadata is KV-oriented (prefix scans), not relational
- Scale limits: PostgreSQL single-node limits metadata throughput
- No multi-region native replication

### Why not etcd?

- Small data limit (~8GB): our metadata can grow to hundreds of GB
- Not designed for high-write throughput
- Primarily for configuration, not database metadata

### Why not TiKV?

- More complex to operate than FDB
- Less proven for metadata workloads (TiKV is designed for TiDB data, not metadata)
- Rust-native, but FDB has Snowflake's endorsement

### Why not FoundationDB for production?

- Still in beta
- No HA (single-node only)
- No multi-region replication
- Good for local development and testing

---

## 4. Storage Format: Parquet

| Aspect | Detail |
|---|Parquet (Apache Parquet) |
| **Alternatives** | Custom format, ORC, Arrow IPC |
| **Decision date** | June 202 |

### Why Parquet?

1. **Industry standard** — widest tooling support (Spark, Hive, Iceberg, Pandas, DuckDB)
2. **Arrow-native** — parquet crate converts to Arrow RecordBatch natively
3. **Column statistics** — min/max/null_count in footer for pruning
4. **Compression** — Snappy (fast) and ZSTD (high ratio) support
5. **Dictionary encoding** — built-in low-cardinality compression
6. DataFusion's Parquet reader is #1 (per ClickBench blog)

### Why not custom format?

- Massive engineering effort with marginal gain
- Loses ecosystem compatibility (Iceberg, Spark, Pandas)
- Parquet already has all needed features (columnar, stats, compression)

### Why not ORC?

- Smaller ecosystem than Parquet
- No mature Rust reader (Parquet has `parquet` crate)
- No significant advantage over Parquet

### Why not Arrow IPC?

- Not compressed by default
- No column statistics in footer
- Designed for in-process data exchange, not persistent storage

---

## 5. Object Storage: object_store crate

| Aspect | Detail |
|---|---|
| **Chosen** | `object_store` crate (Apache Arrow project) |
| **Alternatives** | aws-sdk-s3, rusoto, minio-rs |
| **Decision date** | June 2026 |

### Why object_store?

1. **Multi-cloud** — S3, GCS, Azure Blob, local file, all behind one trait
2. **Arrow project** — maintained by Apache Arrow/DataFusion team
3. **Async** — full tokio async support
3. **Get/put/delete/range** — all needed operations
4. **Streaming** — streaming reads for large files
5. **No cloud-vendor lock-in** — switch between S3/MinIO/GCS/Azure without code changes

### Why not aws-sdk-s3?

- AWS-only: would lock us to S3
- Heavier dependency (entire AWS SDK)
- object_store provides S3 compatibility via MinIO

---

## 2. Query Engine: Apache DataFusion

| Aspect | Detail |
|---|---|
| **Chosen** | Apache DataFusion 43+ |
| **Alternatives** | Build from scratch, DuckDB (C++), Velox (C++), ClickHouse (C++), Peloton |
| **Arrow/Parquet** | No mature Zig bindings. |

### Why DataFusion?

1. **#1 ClickBench (November 202 strategy**

| Choice | Rationale |
|---|---|
| Micro-partition size: 16-64MB | Matches Snowflake's MP size. Balanced for S3 read efficiency and write granularity.
| Immutable Parquet files | Enables Time Travel, Clone, Streams, MVCC. No compaction.
| Column statistics in footer + FDB | Dual storage: Parquet footer for row group pruning, FDB for MP-level pruning.
| Snappy compression (default) | Fastest decompression. ZSTD optional for higher ratio.
| Dictionary encoding (Parquet built-in) | Low-cardinality columns compressed automatically.

---

## 7. Cache: Foyer

| Aspect | Detail |
|---|---|
| **Chosen** | foyer (hybrid memory + disk cache) |
| **Alternatives** | moka (in-memory only), RocksDB (as cache), CacheLib (C++), Caffeine (Java) |
| **Decision date** | June 2026 |

### Why Foyer?

1. **Hybrid cache** — RAM (hot) + SSD (warm), 10-1000x larger than RAM-only
2. **Rust-native** — zero-copy abstraction via Rust type system
3. **Proven by RisingWave** — production streaming database using S3 as primary storage
4. **Compaction-aware refill** — after MP merge, auto-prefetch new MP to prevent cache miss
5. **Fearless concurrency** — lock-free, thread-safe
6. **Plug-and-play eviction** — LRU, LFU, or custom

### Why not moka?

- In-memory only: no disk tier
- 25x less effective cache capacity vs Foyer hybrid (4GB RAM vs 4GB RAM + 100GB SSD)
- No compaction-aware refill

### Why not RocksDB as cache?

- Not designed as a cache (no LRU eviction, no TTL)
- Block-based, not object-based
- Heavier dependency

---

## 8. Consensus: openraft

| Aspect | Detail |
|---|---|
| **Chosen** | openraft |
| **Alternatives** | tikv/raft-rs, etcd-raft, custom Raft |
| **Decision date** | June 2026 |

### Why openraft?

1. **Rust-native** — pure Rust, no FFI
2. **Active development** — recent commits, responsive maintainers
3. **Flexible** — generic over node ID, log entry, and state machine types
4. **Well-documented** — comprehensive docs and examples
5. **Used by real projects** — production deployments

### Why not tikv/raft-rs?

- Tightly coupled to TiKV ecosystem
- Harder to use standalone
- Less flexible type system

---

## 9. RPC: tonic (gRPC)

| Aspect | Coordinator ↔ Worker communication
| **Alternatives** | reqwest (HTTP/JSON), tarpc, postcard+TCP, quinn (QUIC) |
| **Decision date** | June 2 |

### Why tonic (gRPC)?

1. **Bidirectional streaming** — needed for result streaming (worker → coordinator)
2. **Strong typing** — protobuf definitions shared via nova-common
3. **Performance** — HTTP/2 multiplexing, binary protocol
4. **Ecosystem** — tracing integration, interceptors, middleware
5. **Arrow flight compatible** — if we later adopt Arrow Flight for client protocol

### Why not HTTP/JSON?

- No native streaming (would need SSE/WebSockets)
- JSON serialization overhead for large payloads
- No strong typing without code generation

---

## 10. MySQL Protocol: mysql_wire

| Aspect | Detail |
|---|---|
| **Chosen** | mysql_wire crate (evaluate alternatives during Phase 1) |
| **Alternatives** | `mysql_async` server, custom implementation, PostgreSQL wire protocol |
| **Decision date** | June 2026 (evaluate in Phase 1) |

### Why MySQL protocol?

1. **Tool compatibility** — DBeaver, DataGrip, mysql CLI, MySQL Workbench all work
2. **Familiarity** — users already know MySQL protocol
3. **StarRocks compatibility** — StarRocks uses MySQL protocol, easy migration for users

### Evaluation criteria for crate selection

- Completeness: handshake, auth, query, result set, prepared statements
- Streaming: support large result sets without buffering
- Rust-native: async, no FFI
- Maintenance: active development, no unresolved security issues

---

## 11. Python UDF: PyO3

| Aspect | Detail |
|---|---|
| **Chosen** | PyO3 (Python ↔ Rust FFI) |
| **Alternatives** | Separate Python process (subprocess), gRPC UDF service, Java UDF |
| **Decision date** | June 202 |

### Why PyO3?

1. **In-process** — UDF runs in the same process as worker, no network overhead
2. **Rust-native** — direct memory sharing between Rust and Python
3. **Arrow integration** — PyArrow can share Arrow buffers with Rust Arrow (zero-copy)
4. **Existing ecosystem** — Python data science libraries (pandas, numpy, scikit-learn)
5. **Nova UI uses Python** — Nova backend is FastAPI, Python UDFs can be shared

### Why not separate process?

- Network overhead: every UDF call = 1 round trip
- Latency: process startup + IPC for each UDF call
- Complexity: process management, error handling

---

## 12. Runtime: tokio

| Aspect | Detail |
|---|---|
| **Chosen** | tokio (async runtime) |
| **Alternatives** | async-std, smol, glommio (thread-per-core) |
| **Decision date** | June 2026 |

### Why tokio?

1. **De facto standard** — most Rust async crates depend on tokio
2. **Work-stealing scheduler** — optimal for mixed I/O + compute workloads
3. **Ecosystem** — tracing, tonic, axum, object_store all tokio-based
4. **Performance** — battle-tested at scale
5. **Features** — `tokio::spawn`, `tokio::select!`, `tokio::sync::mpsc`, etc.

---

## 13. Serialization: bincode

| Aspect | Detail |
|---|---|
| **Chosen** | bincode (for FDB values + internal RPC) |
| **Alternatives** | JSON, MessagePack, Protocol Buffers, Postcard |
| **Decision date** | June 2026 |

### Why bincode?

1. **Fast** — binary format, no string parsing
2. **Compact** — minimal overhead (length prefix + raw bytes)
3. **Rust-native** — serde-compatible, works with any Serialize/Deserialize type
4. **No schema needed** — type-safe via Rust's type system

### Why not Protocol Buffers?

- Heavier: requires .proto files and code generation
- Less flexible for internal types
- bincode is sufficient for FDB values and internal RPC

### Why not JSON?

- Slow: string parsing, whitespace overhead
- Larger: string representation of numbers, booleans
- No binary data support without base64

---

## 14. Error Handling: thiserror

| Aspect | Detail |
|---|---|
| **Chosen** | thiserror + anyhow (for application-level errors) |
| **Alternatives** | anyhow only, snafu, custom error system |
| **Decision date** | June 2026 |

### Why thiserror?

1. **Library errors** — thiserror provides ergonomic enum-based error types
2. `#[from]` auto-conversion from underlying errors
3. **Display** — automatic `Display` implementation with `#[error("...")]`
4. **Application errors** — anyhow for cases where error type doesn't matter

---

## 15. Configuration: figment

| Aspect | Detail |
|---|---|
| **Ch config format
| **Alternatives** | config crate, toml crate, envonly
| **Decision date** | June 2026 |

### Why figment?

1. **Multi-source** — TOML file + environment variables + CLI args
2. **Layered** — file defaults < file < env vars < CLI args
3. **Typed** — deserialize into Rust structs via serde
4. **TOML support** — human-readable config files

---

## 16. Observability: tracing + Prometheus + OpenTelemetry

| Aspect | Detail |
|---|---|
| **Chosen** | `tracing` (structured logging) + `metrics` + `prometheus` (metrics) + `opentelemetry` (distributed tracing) |
| **Alternatives** | log crate, slog, custom metrics |
| **Decision date** | June 2026 |

### Why tracing?

1. **Structured logging** — JSON output with spans, fields, timestamps
2. **Span hierarchy** — query → fragment → operator → batch
3. **Integration** — tonic, axum, tokio all support tracing
4. **Performance** — zero-cost when disabled

---

## Summary: Complete Tech Stack

| # | Component | Crate/Technology | License | Status |
|---|---|---|---|---|
| 1 | Language | Rust 2024 | MIT/Apache-2.0 | Fixed |
| 2 | Query Engine | DataFusion 43+ | Apache-2.0 | Fixed |
| 3 | Metadata Store | FoundationDB | Apache-2.0 | Fixed |
| 4 | Storage Format | Parquet | Apache-2.0 | Fixed |
| 5 | Object Storage | object_store | Apache-2.0/MIT | Fixed |
| 6 | Hybrid Cache | foyer | Apache-2.0 | Fixed |
| 7 | Consensus | openraft | MIT/Apache-2.0 | Fixed |
| 8 | RPC | tonic (gRPC) | MIT | Fixed |
| 9 | MySQL Protocol | mysql_wire (evaluate) | MIT | Provisional |
| 10 | Python UDF | PyO3 | Apache-2.0 | Fixed |
| 1 | Runtime | tokio | MIT | Fixed |
| 12 | Serialization | bincode | MIT | Fixed |
| 13 | Error Handling | thiserror + anyhow | MIT/Apache-2.0 | Fixed |
| 14 | Config | figment | MIT/Apache-2.0 | Fced |
| 15 | Observability | tracing + prometheus + opentelemetry | MIT | Fixed |
| 16 | Columnar Format | Apache Arrow | Apache-2.0 | Fixed |
| 17 | SQL Parser | sqlparser-rs | Apache-2.0 | Fixed |
| 16 | HTTP API | axum | MIT | Fixed |
