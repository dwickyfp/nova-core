// SledMetadataStore - embedded KV store for local dev/testing.
//
// Uses sled (pure Rust embedded KV) as the metadata backend.
// Stores all metadata as serialized key-value pairs.
//
// Key layout:
//   /db/{id}                          -> DatabaseMeta
//   /schema/{db_id}/{schema_id}       -> SchemaMeta
//   /table/{db_id}/{schema_id}/{id}   -> TableMeta
//   /mp/{mp_id}                       -> MicroPartitionMeta
//   /table_mps/{table_id}/{mp_id}     -> ()  (index: table -> MP)
//   /table_version/{table_id}         -> u64
//   /txn/{txn_id}                     -> TransactionMeta
//   /stream/{stream_id}               -> StreamMeta
//   /stream_offset/{stream_id}        -> StreamOffset
//   /clone/{clone_table_id}           -> CloneMeta
//   /next_id/{category}               -> u64  (auto-increment)

use async_trait::async_trait;
use nova_common::{NovaError, Result, *};
use sled::Db;
use std::path::Path;

use super::MetadataStore;

/// Embedded KV metadata store for local development and testing.
pub struct SledMetadataStore {
    db: Db,
}

impl SledMetadataStore {
    /// Open or create a sled database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(path).map_err(|e| NovaError::Internal {
            message: format!("sled open failed: {}", e),
        })?;
        Ok(Self { db })
    }

    /// Create an in-memory sled database (for testing).
    pub fn open_temporary() -> Result<Self> {
        let config = sled::Config::new().temporary(true);
        let db = config.open().map_err(|e| NovaError::Internal {
            message: format!("sled open failed: {}", e),
        })?;
        Ok(Self { db })
    }

    /// Get the next unique ID for a category (auto-increment).
    fn next_id(&self, category: &str) -> Result<u64> {
        let key = format!("/next_id/{}", category);
        let old = self
            .db
            .get(&key)
            .map_err(|e| NovaError::Internal {
                message: format!("sled get failed: {}", e),
            })?
            .map(|v| {
                let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                u64::from_be_bytes(bytes)
            })
            .unwrap_or(0);
        let new = old + 1;
        self.db
            .insert(&key, &new.to_be_bytes()[..])
            .map_err(|e| NovaError::Internal {
                message: format!("sled insert failed: {}", e),
            })?;
        Ok(new)
    }

    fn serialize<T: serde::Serialize>(val: &T) -> Result<Vec<u8>> {
        bincode::serialize(val).map_err(|e| NovaError::MetadataSerialization { source: e })
    }

    fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        bincode::deserialize(bytes).map_err(|e| NovaError::MetadataSerialization { source: e })
    }

    fn get_value<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.db.get(key).map_err(|e| NovaError::Internal {
            message: format!("sled get failed: {}", e),
        })? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    fn insert_value<T: serde::Serialize>(&self, key: &str, val: &T) -> Result<()> {
        let bytes = Self::serialize(val)?;
        self.db
            .insert(key, bytes)
            .map_err(|e| NovaError::Internal {
                message: format!("sled insert failed: {}", e),
            })?;
        Ok(())
    }

    fn delete_key(&self, key: &str) -> Result<()> {
        self.db.remove(key).map_err(|e| NovaError::Internal {
            message: format!("sled delete failed: {}", e),
        })?;
        Ok(())
    }

    fn scan_prefix<T: serde::de::DeserializeOwned>(&self, prefix: &str) -> Result<Vec<T>> {
        let mut results = Vec::new();
        for item in self.db.scan_prefix(prefix) {
            let (_, val) = item.map_err(|e| NovaError::Internal {
                message: format!("sled scan failed: {}", e),
            })?;
            results.push(Self::deserialize(&val)?);
        }
        Ok(results)
    }
}

impl SledMetadataStore {
    /// Get ALL MPs for a table (including superseded ones). Used by Time Travel.
    pub(crate) async fn get_all_mps(&self, table_id: TableId) -> Result<Vec<MicroPartitionMeta>> {
        let prefix = format!("/table_mps/{}/", table_id);
        let mut results = Vec::new();
        for item in self.db.scan_prefix(&prefix) {
            let (key, _) = item.map_err(|e| NovaError::Internal {
                message: format!("sled scan failed: {}", e),
            })?;
            let key_str = String::from_utf8_lossy(&key);
            if let Some(Ok(mp_id)) = key_str.split('/').next_back().map(|s| s.parse::<u64>())
                && let Some(mp) = self.get_mp(mp_id).await?
            {
                results.push(mp);
            }
        }
        Ok(results)
    }
}

#[async_trait]
impl MetadataStore for SledMetadataStore {
    // DATABASE

    async fn create_database(&self, mut db: DatabaseMeta) -> Result<()> {
        if db.id == 0 {
            db.id = self.next_id("database")?;
        }
        let key = format!("/db/{}", db.id);
        self.insert_value(&key, &db)
    }

    async fn get_database(&self, id: DatabaseId) -> Result<Option<DatabaseMeta>> {
        let key = format!("/db/{}", id);
        self.get_value(&key)
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseMeta>> {
        self.scan_prefix("/db/")
    }

    async fn drop_database(&self, id: DatabaseId) -> Result<()> {
        let key = format!("/db/{}", id);
        if self.get_value::<DatabaseMeta>(&key)?.is_none() {
            return Err(NovaError::DatabaseNotFound {
                db_name: id.to_string(),
            });
        }
        let schemas = self.scan_prefix::<SchemaMeta>(&format!("/schema/{}/", id))?;
        if !schemas.is_empty() {
            return Err(NovaError::Internal {
                message: format!(
                    "cannot drop database {}: {} schemas still exist",
                    id,
                    schemas.len()
                ),
            });
        }
        self.delete_key(&key)
    }

    // SCHEMA

    async fn create_schema(&self, mut schema: SchemaMeta) -> Result<()> {
        if schema.id == 0 {
            schema.id = self.next_id("schema")?;
        }
        let key = format!("/schema/{}/{}", schema.db_id, schema.id);
        self.insert_value(&key, &schema)
    }

    async fn get_schema(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
    ) -> Result<Option<SchemaMeta>> {
        let key = format!("/schema/{}/{}", db_id, schema_id);
        self.get_value(&key)
    }

    async fn list_schemas(&self, db_id: DatabaseId) -> Result<Vec<SchemaMeta>> {
        let prefix = format!("/schema/{}/", db_id);
        self.scan_prefix(&prefix)
    }

    async fn drop_schema(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<()> {
        let key = format!("/schema/{}/{}", db_id, schema_id);
        if self.get_value::<SchemaMeta>(&key)?.is_none() {
            return Err(NovaError::SchemaNotFound {
                schema_name: schema_id.to_string(),
            });
        }
        let tables = self.scan_prefix::<TableMeta>(&format!("/table/{}/{}/", db_id, schema_id))?;
        if !tables.is_empty() {
            return Err(NovaError::Internal {
                message: format!(
                    "cannot drop schema {}/{}: {} tables still exist",
                    db_id,
                    schema_id,
                    tables.len()
                ),
            });
        }
        self.delete_key(&key)
    }

    // TABLE

    async fn create_table(&self, mut table: TableMeta) -> Result<()> {
        if table.id == 0 {
            table.id = self.next_id("table")?;
        }
        let key = format!("/table/{}/{}/{}", table.db_id, table.schema_id, table.id);
        self.insert_value(&key, &table)?;
        let ver_key = format!("/table_version/{}", table.id);
        self.db
            .insert(&ver_key, &0u64.to_be_bytes()[..])
            .map_err(|e| NovaError::Internal {
                message: format!("sled insert failed: {}", e),
            })?;
        Ok(())
    }

    async fn get_table(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        table_id: TableId,
    ) -> Result<Option<TableMeta>> {
        let key = format!("/table/{}/{}/{}", db_id, schema_id, table_id);
        self.get_value(&key)
    }

    async fn list_tables(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<Vec<TableMeta>> {
        let prefix = format!("/table/{}/{}/", db_id, schema_id);
        self.scan_prefix(&prefix)
    }

    async fn drop_table(&self, table_id: TableId) -> Result<()> {
        let tables = self.scan_prefix::<TableMeta>("/table/")?;
        let table = tables
            .iter()
            .find(|t| t.id == table_id)
            .ok_or(NovaError::TableNotFound {
                table_name: table_id.to_string(),
            })?;
        let key = format!("/table/{}/{}/{}", table.db_id, table.schema_id, table.id);
        self.delete_key(&key)?;
        let ver_key = format!("/table_version/{}", table_id);
        self.delete_key(&ver_key)?;
        let mp_prefix = format!("/table_mps/{}/", table_id);
        let mp_keys: Vec<String> = self
            .db
            .scan_prefix(&mp_prefix)
            .filter_map(|r| r.ok())
            .map(|(k, _)| String::from_utf8_lossy(&k).into_owned())
            .collect();
        for mp_key in mp_keys {
            if let Some(Ok(mp_id)) = mp_key.split('/').next_back().map(|s| s.parse::<u64>()) {
                self.delete_key(&format!("/mp/{}", mp_id))?;
            }
            self.delete_key(&mp_key)?;
        }
        Ok(())
    }

    // MICRO-PARTITION

    async fn insert_mp(&self, mut mp: MicroPartitionMeta) -> Result<()> {
        if mp.mp_id == 0 {
            mp.mp_id = self.next_id("mp")?;
        }
        let key = format!("/mp/{}", mp.mp_id);
        self.insert_value(&key, &mp)?;
        let idx_key = format!("/table_mps/{}/{}", mp.table_id, mp.mp_id);
        self.insert_value(&idx_key, &())?;
        Ok(())
    }

    async fn get_mp(&self, mp_id: MpId) -> Result<Option<MicroPartitionMeta>> {
        let key = format!("/mp/{}", mp_id);
        self.get_value(&key)
    }

    async fn get_active_mps(&self, table_id: TableId) -> Result<Vec<MicroPartitionMeta>> {
        let prefix = format!("/table_mps/{}/", table_id);
        let mut results = Vec::new();
        for item in self.db.scan_prefix(&prefix) {
            let (key, _) = item.map_err(|e| NovaError::Internal {
                message: format!("sled scan failed: {}", e),
            })?;
            let key_str = String::from_utf8_lossy(&key);
            if let Some(Ok(mp_id)) = key_str.split('/').next_back().map(|s| s.parse::<u64>())
                && let Some(mp) = self.get_mp(mp_id).await?
                && mp.active
            {
                results.push(mp);
            }
        }
        Ok(results)
    }

    async fn get_mps_at_timestamp(
        &self,
        table_id: TableId,
        ts: Timestamp,
    ) -> Result<Vec<MicroPartitionMeta>> {
        // Scan ALL MPs for this table (not just active), needed for Time Travel
        let all_mps = self.get_all_mps(table_id).await?;
        let mut visible = Vec::new();
        for mp in all_mps {
            if mp.commit_ts <= ts {
                match mp.superseded_by {
                    None => visible.push(mp),
                    Some(next_id) => {
                        if let Some(next_mp) = self.get_mp(next_id).await? {
                            if next_mp.commit_ts > ts {
                                visible.push(mp);
                            }
                        } else {
                            visible.push(mp);
                        }
                    }
                }
            }
        }
        Ok(visible)
    }

    async fn mark_superseded(&self, old_mp_id: MpId, new_mp_id: MpId) -> Result<()> {
        if let Some(mut old_mp) = self.get_mp(old_mp_id).await? {
            old_mp.superseded_by = Some(new_mp_id);
            old_mp.active = false;
            let key = format!("/mp/{}", old_mp_id);
            self.insert_value(&key, &old_mp)?;
        }
        if let Some(mut new_mp) = self.get_mp(new_mp_id).await? {
            new_mp.supersedes = Some(old_mp_id);
            let key = format!("/mp/{}", new_mp_id);
            self.insert_value(&key, &new_mp)?;
        }
        Ok(())
    }

    async fn delete_mp(&self, mp_id: MpId) -> Result<()> {
        if let Some(mp) = self.get_mp(mp_id).await? {
            let idx_key = format!("/table_mps/{}/{}", mp.table_id, mp_id);
            self.delete_key(&idx_key)?;
            let key = format!("/mp/{}", mp_id);
            self.delete_key(&key)?;
        }
        Ok(())
    }

    async fn get_table_version(&self, table_id: TableId) -> Result<u64> {
        let key = format!("/table_version/{}", table_id);
        match self.db.get(&key).map_err(|e| NovaError::Internal {
            message: format!("sled get failed: {}", e),
        })? {
            Some(bytes) => {
                let arr: [u8; 8] = bytes.as_ref().try_into().unwrap_or([0; 8]);
                Ok(u64::from_be_bytes(arr))
            }
            None => Ok(0),
        }
    }

    async fn increment_table_version(&self, table_id: TableId) -> Result<u64> {
        let current = self.get_table_version(table_id).await?;
        let new = current + 1;
        let key = format!("/table_version/{}", table_id);
        self.db
            .insert(&key, &new.to_be_bytes()[..])
            .map_err(|e| NovaError::Internal {
                message: format!("sled insert failed: {}", e),
            })?;
        Ok(new)
    }

    // TRANSACTION

    async fn begin_transaction(&self) -> Result<TxnId> {
        let txn_id = self.next_id("txn")?;
        let txn = TransactionMeta {
            txn_id,
            status: TxnStatus::Active,
            snapshot_ts: now_micros(),
            commit_ts: None,
            affected_tables: Vec::new(),
        };
        let key = format!("/txn/{}", txn_id);
        self.insert_value(&key, &txn)?;
        Ok(txn_id)
    }

    async fn commit_transaction(&self, txn_id: TxnId) -> Result<()> {
        let key = format!("/txn/{}", txn_id);
        let mut txn: TransactionMeta = self.get_value(&key)?.ok_or(NovaError::Internal {
            message: format!("transaction {} not found", txn_id),
        })?;
        txn.status = TxnStatus::Committed;
        txn.commit_ts = Some(now_micros());
        self.insert_value(&key, &txn)
    }

    async fn abort_transaction(&self, txn_id: TxnId) -> Result<()> {
        let key = format!("/txn/{}", txn_id);
        let mut txn: TransactionMeta = self.get_value(&key)?.ok_or(NovaError::Internal {
            message: format!("transaction {} not found", txn_id),
        })?;
        txn.status = TxnStatus::Aborted;
        self.insert_value(&key, &txn)
    }

    async fn get_transaction(&self, txn_id: TxnId) -> Result<Option<TransactionMeta>> {
        let key = format!("/txn/{}", txn_id);
        self.get_value(&key)
    }

    // STREAM

    async fn create_stream(&self, mut stream: StreamMeta) -> Result<()> {
        if stream.stream_id == 0 {
            stream.stream_id = self.next_id("stream")?;
        }
        let key = format!("/stream/{}", stream.stream_id);
        self.insert_value(&key, &stream)?;
        let offset = StreamOffset {
            last_consumed_ts: 0,
            last_consumed_mp: None,
        };
        let offset_key = format!("/stream_offset/{}", stream.stream_id);
        self.insert_value(&offset_key, &offset)?;
        Ok(())
    }

    async fn get_stream(&self, stream_id: StreamId) -> Result<Option<StreamMeta>> {
        let key = format!("/stream/{}", stream_id);
        self.get_value(&key)
    }

    async fn get_stream_offset(&self, stream_id: StreamId) -> Result<Option<StreamOffset>> {
        let key = format!("/stream_offset/{}", stream_id);
        self.get_value(&key)
    }

    async fn set_stream_offset(&self, stream_id: StreamId, offset: StreamOffset) -> Result<()> {
        let key = format!("/stream_offset/{}", stream_id);
        self.insert_value(&key, &offset)
    }

    // CLONE

    async fn create_clone(&self, clone: CloneMeta) -> Result<()> {
        let key = format!("/clone/{}", clone.clone_table_id);
        self.insert_value(&key, &clone)
    }

    async fn get_clone(&self, clone_table_id: TableId) -> Result<Option<CloneMeta>> {
        let key = format!("/clone/{}", clone_table_id);
        self.get_value(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> SledMetadataStore {
        SledMetadataStore::open_temporary().unwrap()
    }

    fn test_db(id: u64) -> DatabaseMeta {
        DatabaseMeta {
            id,
            name: format!("test_db_{}", id),
            created_at: now_micros(),
            owner: 1,
        }
    }

    fn test_schema(id: u64, db_id: u64) -> SchemaMeta {
        SchemaMeta {
            id,
            db_id,
            name: format!("test_schema_{}", id),
            created_at: now_micros(),
        }
    }

    fn test_table(id: u64, db_id: u64, schema_id: u64) -> TableMeta {
        TableMeta {
            id,
            db_id,
            schema_id,
            name: format!("test_table_{}", id),
            columns: vec![
                ColumnDef {
                    id: 0,
                    name: "id".to_string(),
                    data_type: NovaType::Int64,
                    nullable: false,
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    id: 1,
                    name: "name".to_string(),
                    data_type: NovaType::Utf8,
                    nullable: true,
                    default_value: None,
                    comment: None,
                },
            ],
            created_at: now_micros(),
            owner: 1,
            comment: None,
            version: 0,
            properties: Default::default(),
        }
    }

    fn test_mp(table_id: u64, version: u64) -> MicroPartitionMeta {
        MicroPartitionMeta {
            mp_id: 0,
            table_id,
            partition_id: None,
            version,
            s3_path: format!("s3://nova/tables/{}/mp-{}.parquet", table_id, version),
            s3_temp_path: None,
            row_count: 1000,
            byte_size: 1024 * 1024,
            compression: Compression::Snappy,
            column_stats: Default::default(),
            commit_ts: now_micros(),
            txn_id: 1,
            supersedes: None,
            superseded_by: None,
            active: true,
        }
    }

    #[tokio::test]
    async fn test_create_and_get_database() {
        let store = test_store();
        let db = test_db(0);
        store.create_database(db).await.unwrap();
        let got = store.get_database(1).await.unwrap().unwrap();
        assert_eq!(got.name, "test_db_0");
        assert_eq!(got.id, 1);
    }

    #[tokio::test]
    async fn test_list_databases() {
        let store = test_store();
        store.create_database(test_db(0)).await.unwrap();
        store.create_database(test_db(0)).await.unwrap();
        store.create_database(test_db(0)).await.unwrap();
        let dbs = store.list_databases().await.unwrap();
        assert_eq!(dbs.len(), 3);
    }

    #[tokio::test]
    async fn test_drop_database() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.drop_database(100).await.unwrap();
        let got = store.get_database(100).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn test_drop_database_fails_if_has_schemas() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(1, 100)).await.unwrap();
        let result = store.drop_database(100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_nonexistent_database() {
        let store = test_store();
        let got = store.get_database(999).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn test_create_and_get_schema() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(0, 100)).await.unwrap();
        let got = store.get_schema(100, 1).await.unwrap().unwrap();
        assert_eq!(got.name, "test_schema_0");
    }

    #[tokio::test]
    async fn test_list_schemas() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(0, 100)).await.unwrap();
        store.create_schema(test_schema(0, 100)).await.unwrap();
        let schemas = store.list_schemas(100).await.unwrap();
        assert_eq!(schemas.len(), 2);
    }

    #[tokio::test]
    async fn test_drop_schema() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(1, 100)).await.unwrap();
        store.drop_schema(100, 1).await.unwrap();
        let got = store.get_schema(100, 1).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn test_drop_schema_fails_if_has_tables() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(1, 100)).await.unwrap();
        store.create_table(test_table(0, 100, 1)).await.unwrap();
        let result = store.drop_schema(100, 1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_and_get_table() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(1, 100)).await.unwrap();
        store.create_table(test_table(0, 100, 1)).await.unwrap();
        let got = store.get_table(100, 1, 1).await.unwrap().unwrap();
        assert_eq!(got.name, "test_table_0");
        assert_eq!(got.columns.len(), 2);
    }

    #[tokio::test]
    async fn test_list_tables() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(1, 100)).await.unwrap();
        store.create_table(test_table(0, 100, 1)).await.unwrap();
        store.create_table(test_table(0, 100, 1)).await.unwrap();
        let tables = store.list_tables(100, 1).await.unwrap();
        assert_eq!(tables.len(), 2);
    }

    #[tokio::test]
    async fn test_drop_table() {
        let store = test_store();
        store.create_database(test_db(100)).await.unwrap();
        store.create_schema(test_schema(1, 100)).await.unwrap();
        store.create_table(test_table(50, 100, 1)).await.unwrap();
        store.drop_table(50).await.unwrap();
        let got = store.get_table(100, 1, 50).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn test_insert_and_get_mp() {
        let store = test_store();
        let mp = test_mp(1, 1);
        store.insert_mp(mp).await.unwrap();
        let got = store.get_mp(1).await.unwrap().unwrap();
        assert_eq!(got.table_id, 1);
        assert_eq!(got.row_count, 1000);
        assert!(got.active);
    }

    #[tokio::test]
    async fn test_get_active_mps() {
        let store = test_store();
        store.insert_mp(test_mp(1, 1)).await.unwrap();
        store.insert_mp(test_mp(1, 2)).await.unwrap();
        store.insert_mp(test_mp(1, 3)).await.unwrap();
        let active = store.get_active_mps(1).await.unwrap();
        assert_eq!(active.len(), 3);
    }

    #[tokio::test]
    async fn test_mark_superseded() {
        let store = test_store();
        store.insert_mp(test_mp(1, 1)).await.unwrap();
        store.insert_mp(test_mp(1, 2)).await.unwrap();
        store.mark_superseded(1, 2).await.unwrap();
        let old = store.get_mp(1).await.unwrap().unwrap();
        assert!(!old.active);
        assert_eq!(old.superseded_by, Some(2));
        let new = store.get_mp(2).await.unwrap().unwrap();
        assert_eq!(new.supersedes, Some(1));
    }

    #[tokio::test]
    async fn test_get_mps_at_timestamp() {
        let store = test_store();
        let mut mp1 = test_mp(1, 1);
        mp1.commit_ts = 1000;
        store.insert_mp(mp1).await.unwrap();
        let mut mp2 = test_mp(1, 2);
        mp2.commit_ts = 2000;
        store.insert_mp(mp2).await.unwrap();
        store.mark_superseded(1, 2).await.unwrap();

        let at_t1 = store.get_mps_at_timestamp(1, 1000).await.unwrap();
        assert_eq!(at_t1.len(), 1);
        assert_eq!(at_t1[0].mp_id, 1);

        let at_t2 = store.get_mps_at_timestamp(1, 2000).await.unwrap();
        assert_eq!(at_t2.len(), 1);
        assert_eq!(at_t2[0].mp_id, 2);
    }

    #[tokio::test]
    async fn test_delete_mp() {
        let store = test_store();
        store.insert_mp(test_mp(1, 1)).await.unwrap();
        store.delete_mp(1).await.unwrap();
        let got = store.get_mp(1).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn test_table_version() {
        let store = test_store();
        assert_eq!(store.get_table_version(1).await.unwrap(), 0);
        let v = store.increment_table_version(1).await.unwrap();
        assert_eq!(v, 1);
        let v = store.increment_table_version(1).await.unwrap();
        assert_eq!(v, 2);
        assert_eq!(store.get_table_version(1).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let store = test_store();
        let txn_id = store.begin_transaction().await.unwrap();
        assert!(txn_id > 0);
        let txn = store.get_transaction(txn_id).await.unwrap().unwrap();
        assert_eq!(txn.status, TxnStatus::Active);
        store.commit_transaction(txn_id).await.unwrap();
        let txn = store.get_transaction(txn_id).await.unwrap().unwrap();
        assert_eq!(txn.status, TxnStatus::Committed);
        assert!(txn.commit_ts.is_some());
    }

    #[tokio::test]
    async fn test_transaction_abort() {
        let store = test_store();
        let txn_id = store.begin_transaction().await.unwrap();
        store.abort_transaction(txn_id).await.unwrap();
        let txn = store.get_transaction(txn_id).await.unwrap().unwrap();
        assert_eq!(txn.status, TxnStatus::Aborted);
    }

    #[tokio::test]
    async fn test_stream_create_and_offset() {
        let store = test_store();
        let stream = StreamMeta {
            stream_id: 0,
            table_id: 1,
            name: "orders_stream".to_string(),
            append_only: false,
            created_at: now_micros(),
        };
        store.create_stream(stream).await.unwrap();
        let got = store.get_stream(1).await.unwrap().unwrap();
        assert_eq!(got.name, "orders_stream");
        let offset = store.get_stream_offset(1).await.unwrap().unwrap();
        assert_eq!(offset.last_consumed_ts, 0);
        store
            .set_stream_offset(
                1,
                StreamOffset {
                    last_consumed_ts: 5000,
                    last_consumed_mp: Some(42),
                },
            )
            .await
            .unwrap();
        let offset = store.get_stream_offset(1).await.unwrap().unwrap();
        assert_eq!(offset.last_consumed_ts, 5000);
        assert_eq!(offset.last_consumed_mp, Some(42));
    }

    #[tokio::test]
    async fn test_clone_create_and_get() {
        let store = test_store();
        let clone = CloneMeta {
            clone_table_id: 200,
            source_table_id: 100,
            clone_ts: now_micros(),
        };
        store.create_clone(clone).await.unwrap();
        let got = store.get_clone(200).await.unwrap().unwrap();
        assert_eq!(got.source_table_id, 100);
    }
}
