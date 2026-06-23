# Snowflake Feature Parity Tracking

> Track nova-core's progress toward Snowflake feature parity.
> Each feature has: Snowflake behavior, nova-core approach, status, and test coverage.

---

## Feature Status Legend

| Status | Meaning |
|---|---|
| ✅ | Implemented and tested |
| 🟡 | Partially implemented |
| ⬜ | Not started (check ROADMAP.md for phase) |
| ❌ | Cannot implement (architectural limitation) |
| 🔄 | Different approach (documented) |

---

## 1. Time Travel

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| `SELECT ... AT(TIMESTAMP => ...)` | Query data at past timestamp | MVCC version chain lookup | ⬜ (Phase 3) |
| `SELECT ... BEFORE(STATEMENT => ...)` | Query before specific statement | Same MVCC mechanism | ⬜ (Phase 3) |
| Configurable retention | 1 day (standard), 90 days (Enterprise) | 1-90 days configurable | ⬜ (Phase 3) |
| `UNDROP TABLE/SCHEMA/DATABASE` | Restore dropped objects | FDB metadata restore + S3 files retained | ⬜ (Phase 3) |
| Fail-safe (7-day DR) | Snowflake-managed recovery | ❌ Not applicable (self-hosted, use S3 CRR) | 🔄 |

**nova-core approach:** Immutable micro-partitions with commit_ts. Query at timestamp T = find MPs where commit_ts ≤ T AND (superseded_by is None OR superseded_by.commit_ts > T). GC only deletes MPs past retention.

---

## 2. Zero-Copy Clone

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| `CREATE TABLE ... CLONE source` | Metadata copy, same micro-partitions | Same — copy FDB metadata, share S3 files | ⬜ (Phase 3) |
| Clone at past timestamp | `CLONE source AT(TIMESTAMP => ...)` | Same MVCC mechanism | ⬜ (Phase 3) |
| Clone schema/database | Clone all child objects | FDB range copy + metadata | ⬜ (Phase 3) |
| Copy-on-write for clone modifications | New MPs for clone, source unaffected | Same COW mechanism | ⬜ (Phase 3) |
| Instant clone (< 1s) | Metadata only, no data copy | Same — FDB metadata operation | ⬜ (Phase 3) |

---

## 3. Streams (CDC)

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| `CREATE STREAM ... ON TABLE` | Stream object tracks DML changes | FDB offset + MP version diff | ⬜ (Phase 3) |
| Standard (delta) stream | INSERT + UPDATE + DELETE | MP version diff by PK | ⬜ (Phase 3) |
| Append-only stream | INSERT only | Filter: only MPs with supersedes=None | ⬜ (Phase 3) |
| Insert-only stream (external tables) | For external/Iceberg tables | ⬜ (future, after external catalog) | ⬜ |
| Stream offset advance | Offset advances on DML consume | Same — FDB offset update on consume | ⬜ (Phase 3) |
| `METADATA$ACTION` column | INSERT/DELETE metadata | `_binlog_op` equivalent | ⬜ (Phase 3) |
| `CHANGES` clause (read-only) | Query changes without stream | Direct MP version diff query | ⬜ (future) |
| Stream on views | Track view underlying table changes | ⬜ (future) | ⬜ |
| Stream on dynamic tables | Track dynamic table changes | ⬜ (future, after dynamic tables) | ⬜ |

---

## 4. Dynamic Tables (Materialized Views with Target Lag)

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| `CREATE DYNAMIC TABLE ... TARGET_LAG = '...'` | Staleness target scheduling | Background monitor + REFRESH MV | ⬜ (Phase 3) |
| `REFRESH_MODE = INCREMENTAL` | Incremental refresh | MP version diff + incremental agg | ⬜ (Phase 5) |
| `REFRESH_MODE = FULL` | Full refresh | Full scan + recompute | ⬜ (Phase 3) |
| `REFRESH_MODE = AUTO` | Auto-select incremental/full | Heuristic based on query pattern | ⬜ (Phase 5) |
| `TARGET_LAG = DOWNSTREAM` | Refresh only when downstream needs | Reverse dependency graph | ⬜ (future) |
| `ALTER DYNAMIC TABLE REFRESH` | Manual refresh trigger | Same | ⬜ (Phase 3) |
| Pipeline ordering | Automatic dependency tracking | FDB dependency graph | ⬜ (Phase 5) |
| Dual warehouse | Init vs refresh warehouse | Warehouse assignment per task | ⬜ (Phase 6) |

---

## 5. Query Result Cache

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| 24-hour result cache | Auto-cache query results | Foyer L1 cache (2GB RAM + 50GB SSD) | ⬜ (Phase 6) |
| Auto-invalidation on data change | Micro-partition version check | MVCC table version in cache key | ⬜ (Phase 6) |
| Exact SQL match required | Normalized SQL text match | Same — normalize + hash | ⬜ (Phase 6) |
| Non-deterministic function exclusion | CURRENT_TIMESTAMP, RAND, etc. | Same — blocklist | ⬜ (Phase 6) |
| Cache reset on hit (24h extension) | Reset TTL on cache hit | Same — Foyer TTL | ⬜ (Phase 6) |
| 31-day max retention | Max from first execution | Same | ⬜ (Phase 6) |
| `RESULT_SCAN` function | Query previous result as table | Cached result scan | ⬜ (future) |
| Cross-session cache sharing | All sessions share cache | Same — coordinator-level cache | ⬜ (Phase 6) |
| Cross-warehouse cache sharing | All warehouses share cache | Same — cache at coordinator, not worker | ⬜ (Phase 6) |

---

## 6. Tasks & Orchestration

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| `CREATE TASK ... SCHEDULE ...` | Scheduled ETL tasks | StarRocks SUBMIT TASK (via Nova layer) | ⬜ (future) |
| Task DAG (task graph) | Root task + child tasks + branches | Nova orchestrator (Python layer) | ⬜ (future) |
| Serverless tasks | Snowflake-managed compute | ❌ (self-hosted, use worker pool) | 🔄 |
| Conditional branching | `SYSTEM$RETURN_VALUES()` | Nova orchestrator | ⬜ (future) |
| Task finalizer | Cleanup task after DAG | Nova orchestrator | ⬜ (future) |
| Flexible scheduling | `SCHEDULING_MODE = FLEXIBLE` | ❌ (not planned) | ⬜ |

**Note:** Task DAG is more suitable for the Nova Python layer (orchestration), not nova-core engine. nova-core provides the execution; Nova orchestrates.

---

## 7. Snowpipe (Continuous Ingestion)

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| Auto-ingest Snowpipe | Event notification (S3 SQS) | Pipe with `AUTO_INGEST = TRUE` + polling | ⬜ (future) |
| Snowpipe Streaming | Row-based, 10GB/s, 5s latency | ❌ (not planned for v1) | ⬜ |
| Error notifications | SNS/Event Grid | Webhook or alerting | ⬜ (future) |
| In-flight transforms | COPY transforms | INSERT INTO ... SELECT FROM FILES() | ⬜ (future) |
| Default pipe per table | Auto-created per table | ❌ (not planned) | ⬜ |

---

## 8. Virtual Warehouses

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| `CREATE WAREHOUSE` | Virtual compute cluster | Worker pool + auto-scale | ⬜ (Phase 4) |
| Auto-suspend | Suspend after idle N seconds | Same — terminate idle workers | ⬜ (Phase 4) |
| Auto-resume | Resume on query arrival | Same — provision workers on demand | ⬜ (Phase 4) |
| Warehouse sizing | X-Small to 6X-Large | Worker count + VM size | ⬜ (Phase 4) |
| Multi-cluster warehouse | Horizontal scaling | Auto-scaling worker pool | ⬜ (Phase 4) |
| Serverless compute | Snowflake-managed | ❌ (self-hosted) | 🔄 |
| Query acceleration | Offload parts of query | ❌ (not planned for v1) | ⬜ |

---

## 9. Data Governance

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| Dynamic Data Masking | `CREATE MASKING POLICY` | StarRocks native (via Nova UI) | 🔄 (Nova layer) |
| Row Access Policies | `CREATE ROW ACCESS POLICY` | StarRocks native (via Nova UI) | 🔄 (Nova layer) |
| Object Tagging | Key-value tags on objects | Nova NOVA_SYSTEM tables | 🔄 (Nova layer) |
| Column-level lineage | `ACCESS_HISTORY` view | Audit log parsing (heuristic) | ⬜ (future) |
| Data lineage visualization | Native lineage graph | Nova UI (audit log parse) | ⬜ (future) |

---

## 10. Data Sharing

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| Secure shares | Provider → consumer, zero-copy | ❌ (not native, needs Nova layer) | ⬜ (future) |
| Reader accounts | Provider-managed consumer accounts | ❌ (not applicable, self-hosted) | 🔄 |
| Secure views | Security policy on views | ❌ (not planned for v1) | ⬜ |
| Cross-cloud sharing | Share across AWS/Azure/GCP | ❌ (not planned for v1) | ⬜ |

---

## 11. Stored Procedures

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| Snowflake Scripting | SQL extension (IF/CASE/FOR/WHILE) | ❌ (Nova Python layer) | 🔄 |
| Multi-language handlers | Python, Java, Scala, JavaScript | Python UDF via PyO3 | ⬜ (Phase 6) |
| Anonymous blocks | `BEGIN ... END;` | ❌ (Nova Python layer) | 🔄 |
| `RETURNS TABLE(...)` | Return table from procedure | ❌ (Nova Python layer) | 🔄 |

**Note:** Stored procedures are more suitable for the Nova Python layer (orchestration + procedural logic), not nova-core engine. nova-core provides SQL execution; Nova provides procedural wrappers.

---

## 12. Hybrid Tables (HTAP)

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| Transactional + Analytical | Row store + columnar, ACID | Columnar-only (pure OLAP) | ❌ |
| Enforced PK/FK/UNIQUE | Row-level enforcement | PK via MVCC (not enforced at write) | ❌ |
| Secondary indexes | Explicit secondary indexes | ❌ (columnar, use bloom filter) | ❌ |
| Row-level locks | Row-level locking | ❌ (MVCC snapshot isolation) | ❌ |

**nova-core is pure OLAP.** HTAP is out of scope. Use PostgreSQL for OLTP, nova-core for OLAP.

---

## 13. Backup & Recovery

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| Time Travel (restore) | Query + clone historical data | Same (Time Travel + Clone) | ⬜ (Phase 3) |
| Fail-safe | 7-day Snowflake-managed DR | S3 cross-region replication | 🔄 |
| `BACKUP SNAPSHOT` | StarRocks-style backup to repo | FDB snapshot + S3 bucket backup | ⬜ (Phase 6) |
| `RESTORE SNAPSHOT` | Restore from repo | FDB restore + S3 restore | ⬜ (Phase 6) |
| `RECOVER TABLE` | Recycle bin (deleted objects) | FDB metadata restore (S3 files retained during retention) | ⬜ (Phase 3) |

---

## 14. Authentication & RBAC

| Aspect | Snowflake | nova-core | Status |
|---|---|---|---|
| Users, roles, grants | Full RBAC | FDB-based RBAC | ⬜ (Phase 6) |
| Password policies | Expiration, complexity, lockout | Configurable | ⬜ (Phase 6) |
| Network policies | IP allowlist/blocklist | ❌ (not planned for v1) | ⬜ |
| SCIM integration | Automated user provisioning | ❌ (not planned for v1) | ⬜ |
| SSO (SAML, OIDC) | Enterprise SSO | ❌ (not planned for v1) | ⬜ |
| Key pair auth | RSA key pair authentication | ⬜ (future) | ⬜ |

---

## Summary Scoreboard

| Category | Features Total | ✅/🟡 | ⬜ | ❌/🔄 |
|---|---|---|---|---|
| Time Travel | 5 | 0 | 4 | 1 (🔄 Fail-safe) |
| Zero-Copy Clone | 5 | 0 | 5 | 0 |
| Streams (CDC) | 9 | 0 | 8 | 1 (🔄 future) |
| Dynamic Tables | 8 | 0 | 7 | 1 (🔄 future) |
| Query Result Cache | 9 | 0 | 9 | 0 |
| Tasks & Orchestration | 6 | 0 | 3 | 3 (🔄 Nova layer) |
| Snowpipe | 5 | 0 | 4 | 1 (❌ streaming) |
| Virtual Warehouses | 7 | 0 | 5 | 2 (🔄 serverless) |
| Data Governance | 5 | 0 | 1 | 4 (🔄 Nova layer) |
| Data Sharing | 4 | 0 | 3 | 1 (🔄 not applicable) |
| Stored Procedures | 4 | 0 | 1 | 3 (🔄 Nova layer) |
| Hybrid Tables (HTAP) | 4 | 0 | 0 | 4 (❌ out of scope) |
| Backup & Recovery | 5 | 0 | 4 | 1 (🔄 Fail-safe) |
| Auth & RBAC | 6 | 0 | 4 | 2 (⬜ future) |
| **TOTAL** | **82** | **0** | **62** | **20** |

**Target for v0.1.0 (Phase 1-6):** 62 features implemented (76% parity)
