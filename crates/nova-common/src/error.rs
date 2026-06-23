//! Error types shared across all nova-core crates.

use thiserror::Error;

/// Top-level error type for nova-core.
#[derive(Debug, Error)]
pub enum NovaError {
    // ── Storage ──
    #[error("micro-partition not found: table={table_id}, mp={mp_id}")]
    MpNotFound { table_id: u64, mp_id: u64 },

    #[error("table not found: {table_name}")]
    TableNotFound { table_name: String },

    #[error("database not found: {db_name}")]
    DatabaseNotFound { db_name: String },

    #[error("schema not found: {schema_name}")]
    SchemaNotFound { schema_name: String },

    // ── Transaction ──
    #[error("transaction conflict: txn_id={txn_id}")]
    TransactionConflict { txn_id: u64 },

    #[error("transaction aborted: {reason}")]
    TransactionAborted { reason: String },

    // ── Metadata ──
    #[error("FDB error: {source}")]
    FdbError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("metadata serialization error: {source}")]
    MetadataSerialization { source: bincode::Error },

    // ── Parquet ──
    #[error("parquet error: {source}")]
    ParquetError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("arrow error: {source}")]
    ArrowError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // ── Object Store ──
    #[error("object store error: {source}")]
    ObjectStoreError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // ── SQL ──
    #[error("SQL parse error: {message}")]
    SqlParseError { message: String },

    #[error("SQL analysis error: {message}")]
    SqlAnalysisError { message: String },

    // ── Auth ──
    #[error("authentication failed: {reason}")]
    AuthFailed { reason: String },

    #[error("permission denied: user={user}, action={action}")]
    PermissionDenied { user: String, action: String },

    // ── Internal ──
    #[error("internal error: {message}")]
    Internal { message: String },
}

/// Result type alias for nova-core operations.
pub type Result<T> = std::result::Result<T, NovaError>;
