//! MetadataStore — FoundationDB (production) / sled (dev) metadata operations.

// TODO: Phase 1 Milestone 1.2 — implement FDB + sled CRUD

/// Metadata store for catalog, table, and micro-partition metadata.
pub struct MetadataStore {
    // TODO: Phase 1 — add FDB/sled connection
}

impl MetadataStore {
    /// Creates a new MetadataStore.
    pub fn new() -> Self {
        Self {}
    }

    // TODO: Phase 1 Milestone 1.2
    // pub async fn create_database(&self, db: DatabaseMeta) -> Result<()> { ... }
    // pub async fn get_database(&self, id: DatabaseId) -> Result<Option<DatabaseMeta>> { ... }
    // pub async fn list_databases(&self) -> Result<Vec<DatabaseMeta>> { ... }
    // pub async fn drop_database(&self, id: DatabaseId) -> Result<()> { ... }
    //
    // pub async fn create_table(&self, table: TableMeta) -> Result<()> { ... }
    // pub async fn get_table(&self, db: DatabaseId, schema: SchemaId, id: TableId) -> Result<Option<TableMeta>> { ... }
    // pub async fn list_tables(&self, db: DatabaseId, schema: SchemaId) -> Result<Vec<TableMeta>> { ... }
    // pub async fn drop_table(&self, id: TableId) -> Result<()> { ... }
    //
    // pub async fn insert_mp(&self, mp: MicroPartitionMeta) -> Result<()> { ... }
    // pub async fn get_mp(&self, mp_id: MpId) -> Result<Option<MicroPartitionMeta>> { ... }
    // pub async fn get_active_mps(&self, table_id: TableId) -> Result<Vec<MicroPartitionMeta>> { ... }
    // pub async fn get_mps_at_timestamp(&self, table_id: TableId, ts: Timestamp) -> Result<Vec<MicroPartitionMeta>> { ... }
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}
