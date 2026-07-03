//! MetadataStore — abstract trait + implementations for metadata storage.
//!
//! The MetadataStore trait defines the interface for all metadata operations.
//! Implementations:
//! - `SledMetadataStore` — embedded KV store for local dev/testing (default)
//! - `FdbMetadataStore` — FoundationDB for production (feature = "fdb-backend")

#[cfg(feature = "sled-backend")]
pub mod sled_store;

#[cfg(feature = "fdb-backend")]
pub mod fdb_store;

use async_trait::async_trait;
use nova_common::{Result, *};

/// Abstract interface for metadata storage operations.
///
/// All metadata (databases, schemas, tables, micro-partitions, transactions)
/// flows through this trait. Implementations must be Send + Sync for
/// concurrent access.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    // ══════════════════════════════════════════════════════════════
    //  DATABASE OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Create a new database.
    async fn create_database(&self, db: DatabaseMeta) -> Result<()>;

    /// Get a database by ID.
    async fn get_database(&self, id: DatabaseId) -> Result<Option<DatabaseMeta>>;

    /// List all databases.
    async fn list_databases(&self) -> Result<Vec<DatabaseMeta>>;

    /// Drop a database by ID. Fails if database has schemas.
    async fn drop_database(&self, id: DatabaseId) -> Result<()>;

    // ══════════════════════════════════════════════════════════════
    //  SCHEMA OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Create a new schema in a database.
    async fn create_schema(&self, schema: SchemaMeta) -> Result<()>;

    /// Get a schema by database + schema ID.
    async fn get_schema(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
    ) -> Result<Option<SchemaMeta>>;

    /// List all schemas in a database.
    async fn list_schemas(&self, db_id: DatabaseId) -> Result<Vec<SchemaMeta>>;

    /// Drop a schema. Fails if schema has tables.
    async fn drop_schema(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<()>;

    // ══════════════════════════════════════════════════════════════
    //  TABLE OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Create a new table.
    async fn create_table(&self, table: TableMeta) -> Result<()>;

    /// Get a table by fully qualified ID.
    async fn get_table(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        table_id: TableId,
    ) -> Result<Option<TableMeta>>;

    /// List all tables in a schema.
    async fn list_tables(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<Vec<TableMeta>>;

    /// Drop a table. Deletes all associated micro-partitions.
    async fn drop_table(&self, table_id: TableId) -> Result<()>;

    // ══════════════════════════════════════════════════════════════
    //  MICRO-PARTITION OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Insert a new micro-partition metadata entry.
    async fn insert_mp(&self, mp: MicroPartitionMeta) -> Result<()>;

    /// Get a specific micro-partition by ID.
    async fn get_mp(&self, mp_id: MpId) -> Result<Option<MicroPartitionMeta>>;

    /// Get all active (currently visible) micro-partitions for a table.
    async fn get_active_mps(&self, table_id: TableId) -> Result<Vec<MicroPartitionMeta>>;

    /// Get micro-partitions visible at a specific timestamp (Time Travel).
    async fn get_mps_at_timestamp(
        &self,
        table_id: TableId,
        ts: Timestamp,
    ) -> Result<Vec<MicroPartitionMeta>>;

    /// Mark an MP as superseded by a new MP (for UPDATE/DELETE COW).
    async fn mark_superseded(&self, old_mp_id: MpId, new_mp_id: MpId) -> Result<()>;

    /// Delete expired micro-partitions (for GC).
    async fn delete_mp(&self, mp_id: MpId) -> Result<()>;

    /// Get table's current version number.
    async fn get_table_version(&self, table_id: TableId) -> Result<u64>;

    /// Increment table version (called on each write transaction commit).
    async fn increment_table_version(&self, table_id: TableId) -> Result<u64>;

    // ══════════════════════════════════════════════════════════════
    //  TRANSACTION OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Begin a new transaction. Returns a unique transaction ID.
    async fn begin_transaction(&self) -> Result<TxnId>;

    /// Commit a transaction (atomically apply all changes).
    async fn commit_transaction(&self, txn_id: TxnId) -> Result<()>;

    /// Abort a transaction (rollback all pending changes).
    async fn abort_transaction(&self, txn_id: TxnId) -> Result<()>;

    /// Get transaction metadata.
    async fn get_transaction(&self, txn_id: TxnId) -> Result<Option<TransactionMeta>>;

    // ══════════════════════════════════════════════════════════════
    //  STREAM OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Create a new stream on a table.
    async fn create_stream(&self, stream: StreamMeta) -> Result<()>;

    /// Get stream metadata.
    async fn get_stream(&self, stream_id: StreamId) -> Result<Option<StreamMeta>>;

    /// Get stream offset (last consumed position).
    async fn get_stream_offset(&self, stream_id: StreamId) -> Result<Option<StreamOffset>>;

    /// Update stream offset after consumption.
    async fn set_stream_offset(&self, stream_id: StreamId, offset: StreamOffset) -> Result<()>;

    // ══════════════════════════════════════════════════════════════
    //  CLONE OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Record a clone relationship (source → clone).
    async fn create_clone(&self, clone: CloneMeta) -> Result<()>;

    /// Get clone metadata for a cloned table.
    async fn get_clone(&self, clone_table_id: TableId) -> Result<Option<CloneMeta>>;

    // ══════════════════════════════════════════════════════════════
    //  DYNAMIC TABLE OPERATIONS
    // ══════════════════════════════════════════════════════════════

    /// Create a new dynamic table entry.
    async fn create_dynamic_table(&self, dt: DynamicTableMeta) -> Result<()>;

    /// Get a dynamic table by ID.
    async fn get_dynamic_table(&self, dt_id: TableId) -> Result<Option<DynamicTableMeta>>;

    /// List all dynamic tables in a database.
    async fn list_dynamic_tables(&self, db_id: DatabaseId) -> Result<Vec<DynamicTableMeta>>;

    /// Update dynamic table metadata (e.g. after refresh).
    async fn update_dynamic_table(&self, dt: DynamicTableMeta) -> Result<()>;

    /// Drop a dynamic table by ID.
    async fn drop_dynamic_table(&self, dt_id: TableId) -> Result<()>;
}
