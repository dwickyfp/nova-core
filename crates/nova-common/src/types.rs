//! Core types shared across nova-core crates.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Identifiers ──

pub type DatabaseId = u64;
pub type SchemaId = u64;
pub type TableId = u64;
pub type MpId = u64;
pub type TxnId = u64;
pub type StreamId = u64;
pub type UserId = u64;
pub type RoleId = u64;
pub type ColumnId = u32;

/// Microsecond-precision timestamp.
pub type Timestamp = u64;

// ── Enums ──

/// Compression codec for Parquet files.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum Compression {
    #[default]
    Snappy,
    Zstd,
    Lz4,
}

/// Supported SQL data types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NovaType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal { precision: u8, scale: i8 },
    Utf8,
    Date32,
    Timestamp,
    Binary,
    List(Box<NovaType>),
}

// ── Micro-Partition Metadata ──

/// Column-level statistics stored per micro-partition.
/// Used for zone-map pruning (skip MPs where min/max can't match predicate).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value in this MP for this column.
    pub min_value: Option<Vec<u8>>,
    /// Maximum value in this MP for this column.
    pub max_value: Option<Vec<u8>>,
    /// Number of NULL values.
    pub null_count: u64,
    /// Number of distinct values (estimated).
    pub distinct_count: u64,
    /// Uncompressed byte size of this column in this MP.
    pub byte_size: u64,
}

/// Metadata for a single micro-partition (stored in FDB).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicroPartitionMeta {
    /// Globally unique MP ID.
    pub mp_id: MpId,
    /// Table this MP belongs to.
    pub table_id: TableId,
    /// Logical partition ID (None if unpartitioned).
    pub partition_id: Option<u64>,
    /// Version number (1, 2, 3, ...).
    pub version: u64,

    /// Immutable S3 path (e.g. "s3://nova/tables/123/mp-456-v1.parquet").
    pub s3_path: String,
    /// Temp S3 path (before commit). None after commit.
    pub s3_temp_path: Option<String>,
    /// Number of rows in this MP.
    pub row_count: u64,
    /// Compressed byte size.
    pub byte_size: u64,
    /// Compression codec used.
    pub compression: Compression,

    /// Column-level statistics (for zone-map pruning).
    pub column_stats: HashMap<ColumnId, ColumnStats>,

    // ── MVCC ──
    /// When the transaction that created this MP committed.
    pub commit_ts: Timestamp,
    /// Transaction that created this MP.
    pub txn_id: TxnId,
    /// Previous version MP ID (None for pure inserts).
    pub supersedes: Option<MpId>,
    /// Next version MP ID (None = active, still visible).
    pub superseded_by: Option<MpId>,
    /// Whether this MP is currently active (visible to queries).
    pub active: bool,
}

// ── Table / Database / Schema ──

/// Database metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMeta {
    pub id: DatabaseId,
    pub name: String,
    pub created_at: Timestamp,
    pub owner: UserId,
}

/// Schema metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMeta {
    pub id: SchemaId,
    pub db_id: DatabaseId,
    pub name: String,
    pub created_at: Timestamp,
}

/// Column definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: NovaType,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub comment: Option<String>,
}

/// Table metadata (stored in FDB).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub id: TableId,
    pub db_id: DatabaseId,
    pub schema_id: SchemaId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub created_at: Timestamp,
    pub owner: UserId,
    pub comment: Option<String>,
    /// Current version (incremented on each write transaction).
    pub version: u64,
    /// Table-level properties (e.g. replication_num, compression).
    pub properties: HashMap<String, String>,
}

// ── Transaction ──

/// Transaction status.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed,
    Aborted,
}

/// Transaction metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMeta {
    pub txn_id: TxnId,
    pub status: TxnStatus,
    pub snapshot_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
    pub affected_tables: Vec<TableId>,
}

// ── Stream ──

/// Stream metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMeta {
    pub stream_id: StreamId,
    pub table_id: TableId,
    pub name: String,
    pub append_only: bool,
    pub created_at: Timestamp,
}

/// Stream offset (last consumed position).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOffset {
    pub last_consumed_ts: Timestamp,
    pub last_consumed_mp: Option<MpId>,
}

/// Change record (for CDC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub action: ChangeAction,
    pub row_data: Vec<u8>,
    pub mp_id: MpId,
    pub txn_id: TxnId,
}

/// CDC action type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChangeAction {
    Insert,
    UpdateBefore,
    UpdateAfter,
    Delete,
}

// ── Clone ──

/// Clone metadata (tracks zero-copy clone relationships).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloneMeta {
    pub clone_table_id: TableId,
    pub source_table_id: TableId,
    pub clone_ts: Timestamp,
}

// ── Warehouse ──

/// Virtual warehouse configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseMeta {
    pub id: u64,
    pub name: String,
    pub size: WarehouseSize,
    pub auto_suspend_seconds: u64,
    pub auto_resume: bool,
    pub created_at: Timestamp,
}

/// Warehouse size (T-shirt sizes like Snowflake).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WarehouseSize {
    XSmall,
    Small,
    Medium,
    Large,
    XLarge,
}

// ── Config ──

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub coordinator: CoordinatorConfig,
    pub worker: WorkerConfig,
    pub storage: StorageConfig,
    pub metadata: MetadataConfig,
    pub cache: CacheConfig,
    pub auth: AuthConfig,
}

/// Coordinator-specific config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    pub mysql_port: u16,
    pub http_port: u16,
    pub grpc_port: u16,
    pub raft_port: u16,
}

/// Worker-specific config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub grpc_port: u16,
    pub batch_size: usize,
    pub memory_limit_bytes: u64,
    pub cache_ssd_path: String,
}

/// Object storage config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

/// Metadata store config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    pub fdb_cluster_file: String,
    pub retention_days: u32,
}

/// Cache config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub result_cache_memory_bytes: u64,
    pub result_cache_ssd_bytes: u64,
    pub mp_cache_memory_bytes: u64,
    pub mp_cache_ssd_bytes: u64,
    pub meta_cache_memory_bytes: u64,
}

/// Auth config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub enabled: bool,
    pub default_username: String,
    pub default_password_hash: String,
}

// ── Helpers ──

/// Generate a new unique ID (based on UUID v4).
pub fn generate_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    // Mix with random bits from UUID
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let rand = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    ts.wrapping_add(rand)
}

/// Get current timestamp in microseconds.
pub fn now_micros() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
