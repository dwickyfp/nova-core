// FdbMetadataStore — FoundationDB metadata store for production.
//
// Uses FoundationDB (ACID distributed KV) as the metadata backend.
// Same key layout as FdbMetadataStore but with FDB tuples for keys.
// All operations use FDB transactions for atomicity.
//
// Key layout (FDB tuples packed via Subspace):
//   ("db", id)                          -> DatabaseMeta
//   ("schema", db_id, schema_id)        -> SchemaMeta
//   ("table", db_id, schema_id, id)     -> TableMeta
//   ("mp", mp_id)                        -> MicroPartitionMeta
//   ("table_mps", table_id, mp_id)       -> ()  (index: table -> MP)
//   ("table_version", table_id)          -> u64
//   ("txn", txn_id)                      -> TransactionMeta
//   ("stream", stream_id)                -> StreamMeta
//   ("stream_offset", stream_id)         -> StreamOffset
//   ("clone", clone_table_id)            -> CloneMeta
//   ("next_id", category)                -> u64  (auto-increment)

use async_trait::async_trait;
use fdb::RangeOption;
use fdb::tuple::{Subspace, TuplePack};
use foundationdb as fdb;
use nova_common::{NovaError, Result, *};
use std::{
    path::Path,
    sync::{Arc, Mutex, Once, OnceLock},
};

use super::MetadataStore;

static FDB_NETWORK: OnceLock<Mutex<Option<Box<fdb::api::NetworkAutoStop>>>> = OnceLock::new();
static FDB_NETWORK_ATEXIT: Once = Once::new();

#[cfg(unix)]
unsafe extern "C" {
    fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
}

extern "C" fn shutdown_fdb_network() {
    if let Some(network) = FDB_NETWORK.get()
        && let Ok(mut guard) = network.lock()
    {
        let _ = guard.take();
    }
}

fn register_fdb_network_shutdown() {
    #[cfg(unix)]
    unsafe {
        let _ = atexit(shutdown_fdb_network);
    }
}

/// FoundationDB metadata store for production deployments.
pub struct FdbMetadataStore {
    pub(crate) db: Arc<fdb::Database>,
    pub(crate) subspace: Subspace,
    _network: &'static fdb::api::NetworkAutoStop, // ponytail: one FDB network per process; leak to avoid shutdown-order crashes.
}

impl FdbMetadataStore {
    pub(crate) fn pack(&self, tuple: &impl TuplePack) -> Vec<u8> {
        self.subspace.pack(tuple)
    }

    fn network() -> &'static fdb::api::NetworkAutoStop {
        let slot = FDB_NETWORK.get_or_init(|| Mutex::new(None));
        let mut guard = slot.lock().unwrap();
        if guard.is_none() {
            *guard = Some(Box::new(unsafe { fdb::boot() }));
            FDB_NETWORK_ATEXIT.call_once(register_fdb_network_shutdown);
        }
        let network = guard
            .as_ref()
            .map(|network| &**network as *const fdb::api::NetworkAutoStop)
            .expect("FDB network initialized");
        drop(guard);
        unsafe { &*network }
    }

    /// Connect to a FoundationDB cluster using either a cluster file path or raw cluster contents.
    pub fn open(cluster_file: &str) -> Result<Self> {
        let network = Self::network();
        let cluster_file = Self::cluster_file_path(cluster_file)?;
        let db = fdb::Database::new(Some(&cluster_file)).map_err(|e| NovaError::Internal {
            message: format!("FDB connect failed: {}", e),
        })?;
        Ok(Self {
            db: Arc::new(db),
            subspace: Subspace::from_bytes(b"nova"),
            _network: network,
        })
    }

    /// Connect with default cluster file.
    pub fn open_default() -> Result<Self> {
        let network = Self::network();
        let db = fdb::Database::default().map_err(|e| NovaError::Internal {
            message: format!("FDB connect failed: {}", e),
        })?;
        Ok(Self {
            db: Arc::new(db),
            subspace: Subspace::from_bytes(b"nova"),
            _network: network,
        })
    }

    #[doc(hidden)]
    pub fn open_test(cluster_file: &str, subspace: Vec<u8>) -> Result<Self> {
        let network = Self::network();
        let cluster_file = Self::cluster_file_path(cluster_file)?;
        let db = fdb::Database::new(Some(&cluster_file)).map_err(|e| NovaError::Internal {
            message: format!("FDB connect failed: {}", e),
        })?;
        Ok(Self {
            db: Arc::new(db),
            subspace: Subspace::from_bytes(subspace),
            _network: network,
        })
    }

    // ── Helpers ──

    fn cluster_file_path(cluster_file: &str) -> Result<String> {
        if Path::new(cluster_file).exists() || !cluster_file.contains('@') {
            return Ok(cluster_file.to_string());
        }
        let path = std::env::temp_dir().join("nova-core-fdb.cluster");
        std::fs::write(&path, cluster_file).map_err(|e| NovaError::Internal {
            message: format!("FDB cluster file write failed: {}", e),
        })?;
        Ok(path.to_string_lossy().into_owned())
    }

    pub(crate) fn serialize<T: serde::Serialize>(val: &T) -> Result<Vec<u8>> {
        bincode::serialize(val).map_err(|e| NovaError::MetadataSerialization { source: e })
    }

    pub(crate) fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        bincode::deserialize(bytes).map_err(|e| NovaError::MetadataSerialization { source: e })
    }

    /// Get prefix range for a subspace category.
    pub(crate) fn category_range(&self, prefix: &impl TuplePack) -> (Vec<u8>, Vec<u8>) {
        let packed = self.subspace.pack(prefix);
        let mut end = packed.clone();
        // Increment last byte to get exclusive upper bound
        if let Some(last) = end.last_mut() {
            *last = last.wrapping_add(1);
        }
        (packed, end)
    }

    /// Simple single-key get via FDB read transaction.
    pub(crate) async fn fdb_get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let result = self
            .db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                async move {
                    trx.get(&key, false)
                        .await
                        .map(|v| v.map(|s| s.to_vec()))
                        .map_err(fdb::FdbBindingError::from)
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB get failed: {}", e),
            })?;
        Ok(result)
    }

    /// Simple single-key set via FDB read-write transaction.
    pub(crate) async fn fdb_set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    trx.set(&key, &value);
                    Ok::<_, fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB set failed: {}", e),
            })?;
        Ok(())
    }

    /// Simple single-key clear via FDB transaction.
    pub(crate) async fn fdb_clear(&self, key: Vec<u8>) -> Result<()> {
        self.db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                async move {
                    trx.clear(&key);
                    Ok::<_, fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB clear failed: {}", e),
            })?;
        Ok(())
    }

    /// Range scan via FDB. Returns all key-value pairs in range.
    pub(crate) async fn fdb_get_range(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let result = self
            .db
            .run(|trx, _maybe_committed| {
                let opt = RangeOption::from((start.clone(), end.clone()));
                async move {
                    let values = trx
                        .get_range(&opt, 1_000_000, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?;
                    let result: Vec<(Vec<u8>, Vec<u8>)> = values
                        .iter()
                        .map(|kv| (kv.key().to_vec(), kv.value().to_vec()))
                        .collect();
                    Ok(result)
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB get_range failed: {}", e),
            })?;
        Ok(result)
    }

    /// Commit a small metadata mutation batch atomically.
    pub(crate) async fn fdb_write_batch(
        &self,
        sets: Vec<(Vec<u8>, Vec<u8>)>,
        clears: Vec<Vec<u8>>,
        bump_epoch: bool,
    ) -> Result<u64> {
        let epoch_key = self.pack(&("security_epoch",));
        self.db
            .run(|trx, _maybe_committed| {
                let sets = sets.clone();
                let clears = clears.clone();
                let epoch_key = epoch_key.clone();
                async move {
                    for (key, value) in &sets {
                        trx.set(key, value);
                    }
                    for key in &clears {
                        trx.clear(key);
                    }
                    if !bump_epoch {
                        return Ok::<u64, fdb::FdbBindingError>(0);
                    }
                    let current = trx
                        .get(&epoch_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0);
                    let next = current + 1;
                    trx.set(&epoch_key, &next.to_be_bytes()[..]);
                    Ok(next)
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB write batch failed: {}", e),
            })
    }

    /// Atomically assert keys are absent, then apply a small metadata mutation batch.
    pub(crate) async fn fdb_checked_write_batch(
        &self,
        must_not_exist: Vec<Vec<u8>>,
        sets: Vec<(Vec<u8>, Vec<u8>)>,
        clears: Vec<Vec<u8>>,
        bump_epoch: bool,
    ) -> Result<u64> {
        let epoch_key = self.pack(&("security_epoch",));
        self.db
            .run(|trx, _maybe_committed| {
                let must_not_exist = must_not_exist.clone();
                let sets = sets.clone();
                let clears = clears.clone();
                let epoch_key = epoch_key.clone();
                async move {
                    for key in &must_not_exist {
                        if trx
                            .get(key, false)
                            .await
                            .map_err(fdb::FdbBindingError::from)?
                            .is_some()
                        {
                            return Err(fdb::FdbBindingError::CustomError(Box::new(
                                std::io::Error::new(
                                    std::io::ErrorKind::AlreadyExists,
                                    "FDB checked write precondition failed",
                                ),
                            )));
                        }
                    }
                    for (key, value) in &sets {
                        trx.set(key, value);
                    }
                    for key in &clears {
                        trx.clear(key);
                    }
                    if !bump_epoch {
                        return Ok::<u64, fdb::FdbBindingError>(0);
                    }
                    let current = trx
                        .get(&epoch_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0);
                    let next = current + 1;
                    trx.set(&epoch_key, &next.to_be_bytes()[..]);
                    Ok(next)
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB checked write batch failed: {}", e),
            })
    }

    /// Atomic increment of a counter key.
    pub(crate) async fn fdb_atomic_inc(&self, key: Vec<u8>) -> Result<u64> {
        let result = self
            .db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                async move {
                    let current = trx
                        .get(&key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0);
                    let new_val = current + 1;
                    trx.set(&key, &new_val.to_be_bytes()[..]);
                    Ok::<u64, fdb::FdbBindingError>(new_val)
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB atomic inc failed: {}", e),
            })?;
        Ok(result)
    }
}

#[async_trait]
impl MetadataStore for FdbMetadataStore {
    // ══════════════════════════════════════════════════════════════
    //  DATABASE OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn create_database(&self, db: DatabaseMeta) -> Result<()> {
        let key = self.pack(&("db", db.id));
        let val = Self::serialize(&db)?;
        self.fdb_set(key, val).await
    }

    async fn get_database(&self, id: DatabaseId) -> Result<Option<DatabaseMeta>> {
        let key = self.pack(&("db", id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseMeta>> {
        let (start, end) = self.category_range(&"db");
        let kvs = self.fdb_get_range(start, end).await?;
        kvs.into_iter()
            .map(|(_, v)| Self::deserialize(&v))
            .collect()
    }

    async fn drop_database(&self, id: DatabaseId) -> Result<()> {
        let schemas = self.list_schemas(id).await?;
        if !schemas.is_empty() {
            return Err(NovaError::Internal {
                message: format!("Cannot drop database {}: has {} schemas", id, schemas.len()),
            });
        }
        let key = self.pack(&("db", id));
        self.fdb_clear(key).await
    }

    // ══════════════════════════════════════════════════════════════
    //  SCHEMA OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn create_schema(&self, schema: SchemaMeta) -> Result<()> {
        let key = self.pack(&("schema", schema.db_id, schema.id));
        let val = Self::serialize(&schema)?;
        self.fdb_set(key, val).await
    }

    async fn get_schema(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
    ) -> Result<Option<SchemaMeta>> {
        let key = self.pack(&("schema", db_id, schema_id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn list_schemas(&self, db_id: DatabaseId) -> Result<Vec<SchemaMeta>> {
        let (start, end) = self.category_range(&("schema", db_id));
        let kvs = self.fdb_get_range(start, end).await?;
        kvs.into_iter()
            .map(|(_, v)| Self::deserialize(&v))
            .collect()
    }

    async fn drop_schema(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<()> {
        let tables = self.list_tables(db_id, schema_id).await?;
        if !tables.is_empty() {
            return Err(NovaError::Internal {
                message: format!("Cannot drop schema: has {} tables", tables.len()),
            });
        }
        let key = self.pack(&("schema", db_id, schema_id));
        self.fdb_clear(key).await
    }

    // ══════════════════════════════════════════════════════════════
    //  TABLE OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn create_table(&self, mut table: TableMeta) -> Result<()> {
        if table.id == 0 {
            let key = self.pack(&("next_id", "table"));
            table.id = self.fdb_atomic_inc(key).await?;
        }
        let key = self.pack(&("table", table.db_id, table.schema_id, table.id));
        let val = Self::serialize(&table)?;
        self.fdb_set(key, val).await
    }

    async fn get_table(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        table_id: TableId,
    ) -> Result<Option<TableMeta>> {
        let key = self.pack(&("table", db_id, schema_id, table_id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn list_tables(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<Vec<TableMeta>> {
        let (start, end) = self.category_range(&("table", db_id, schema_id));
        let kvs = self.fdb_get_range(start, end).await?;
        kvs.into_iter()
            .map(|(_, v)| Self::deserialize(&v))
            .collect()
    }

    async fn drop_table(&self, table_id: TableId) -> Result<()> {
        // Get active MPs to clean up
        let active_mps = self.get_active_mps(table_id).await?;

        // Clear table version
        let ver_key = self.pack(&("table_version", table_id));
        self.fdb_clear(ver_key).await?;

        // Clear MP index range + MP metadata
        let (mp_start, mp_end) = self.category_range(&("table_mps", table_id));
        // Clear range via single transaction
        self.db
            .run(|trx, _| {
                let mp_start = mp_start.clone();
                let mp_end = mp_end.clone();
                let mp_keys: Vec<Vec<u8>> = active_mps
                    .iter()
                    .map(|mp| self.pack(&("mp", mp.mp_id)))
                    .collect();
                async move {
                    trx.clear_range(&mp_start, &mp_end);
                    for k in &mp_keys {
                        trx.clear(k);
                    }
                    Ok::<_, fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB drop_table failed: {}", e),
            })?;

        Ok(())
    }

    // ══════════════════════════════════════════════════════════════
    //  MICRO-PARTITION OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn insert_mp(&self, mp: MicroPartitionMeta) -> Result<()> {
        let mp_key = self.pack(&("mp", mp.mp_id));
        let idx_key = self.pack(&("table_mps", mp.table_id, mp.mp_id));
        let val = Self::serialize(&mp)?;

        // Both writes in a single transaction
        self.db
            .run(|trx, _| {
                let mp_key = mp_key.clone();
                let idx_key = idx_key.clone();
                let val = val.clone();
                async move {
                    trx.set(&mp_key, &val);
                    trx.set(&idx_key, b"");
                    Ok::<_, fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB insert_mp failed: {}", e),
            })?;
        Ok(())
    }

    async fn get_mp(&self, mp_id: MpId) -> Result<Option<MicroPartitionMeta>> {
        let key = self.pack(&("mp", mp_id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn get_active_mps(&self, table_id: TableId) -> Result<Vec<MicroPartitionMeta>> {
        let (start, end) = self.category_range(&("table_mps", table_id));
        let kvs = self.fdb_get_range(start, end).await?;

        let mut mps = Vec::new();
        for (key_bytes, _) in &kvs {
            // Index entries have empty values. Key format: ("nova", "table_mps", table_id, mp_id)
            // Unpack mp_id from key
            let unpacked: (String, TableId, MpId) =
                self.subspace
                    .unpack(key_bytes)
                    .map_err(|e| NovaError::Internal {
                        message: format!("FDB tuple unpack failed: {}", e),
                    })?;
            let mp_id = unpacked.2;
            if let Some(mp) = self.get_mp(mp_id).await?
                && mp.active
                && mp.superseded_by.is_none()
            {
                mps.push(mp);
            }
        }
        Ok(mps)
    }

    async fn get_mps_at_timestamp(
        &self,
        table_id: TableId,
        ts: Timestamp,
    ) -> Result<Vec<MicroPartitionMeta>> {
        // Get ALL MPs for table (including inactive/superseded)
        let (start, end) = self.category_range(&("table_mps", table_id));
        let kvs = self.fdb_get_range(start, end).await?;

        let mut all_mps = Vec::new();
        for (key_bytes, _) in &kvs {
            let unpacked: (String, TableId, MpId) =
                self.subspace
                    .unpack(key_bytes)
                    .map_err(|e| NovaError::Internal {
                        message: format!("FDB tuple unpack failed: {}", e),
                    })?;
            let mp_id = unpacked.2;
            if let Some(mp) = self.get_mp(mp_id).await? {
                all_mps.push(mp);
            }
        }

        // Filter by MVCC visibility
        let mut visible = Vec::new();
        for mp in all_mps {
            if mp.commit_ts <= ts {
                match mp.superseded_by {
                    None => visible.push(mp),
                    Some(next_id) => {
                        if let Some(next_mp) = self.get_mp(next_id).await?
                            && next_mp.commit_ts > ts
                        {
                            visible.push(mp);
                        }
                    }
                }
            }
        }
        Ok(visible)
    }

    async fn mark_superseded(&self, old_mp_id: MpId, new_mp_id: MpId) -> Result<()> {
        let old_mp = self.get_mp(old_mp_id).await?.ok_or(NovaError::MpNotFound {
            table_id: 0,
            mp_id: old_mp_id,
        })?;
        let mut updated = old_mp;
        updated.superseded_by = Some(new_mp_id);
        updated.active = false;

        let key = self.pack(&("mp", old_mp_id));
        let val = Self::serialize(&updated)?;
        self.fdb_set(key, val).await
    }

    async fn delete_mp(&self, mp_id: MpId) -> Result<()> {
        if let Some(mp) = self.get_mp(mp_id).await? {
            let mp_key = self.pack(&("mp", mp_id));
            let idx_key = self.pack(&("table_mps", mp.table_id, mp_id));

            self.db
                .run(|trx, _| {
                    let mp_key = mp_key.clone();
                    let idx_key = idx_key.clone();
                    async move {
                        trx.clear(&mp_key);
                        trx.clear(&idx_key);
                        Ok::<_, fdb::FdbBindingError>(())
                    }
                })
                .await
                .map_err(|e| NovaError::Internal {
                    message: format!("FDB delete_mp failed: {}", e),
                })?;
        }
        Ok(())
    }

    async fn get_table_version(&self, table_id: TableId) -> Result<u64> {
        let key = self.pack(&("table_version", table_id));
        match self.fdb_get(key).await? {
            Some(bytes) => {
                let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
                Ok(u64::from_be_bytes(arr))
            }
            None => Ok(0),
        }
    }

    async fn increment_table_version(&self, table_id: TableId) -> Result<u64> {
        let key = self.pack(&("table_version", table_id));
        self.fdb_atomic_inc(key).await
    }

    // ══════════════════════════════════════════════════════════════
    //  TRANSACTION OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn begin_transaction(&self) -> Result<TxnId> {
        let key = self.pack(&("next_id", "txn"));
        let txn_id = self.fdb_atomic_inc(key).await?;

        let meta = TransactionMeta {
            txn_id,
            status: TxnStatus::Active,
            snapshot_ts: now_micros(),
            commit_ts: None,
            affected_tables: Vec::new(),
        };
        let key = self.pack(&("txn", txn_id));
        let val = Self::serialize(&meta)?;
        self.fdb_set(key, val).await?;
        Ok(txn_id)
    }

    async fn commit_transaction(&self, txn_id: TxnId) -> Result<()> {
        let meta = self
            .get_transaction(txn_id)
            .await?
            .ok_or(NovaError::Internal {
                message: format!("Transaction {} not found", txn_id),
            })?;
        let mut updated = meta;
        updated.status = TxnStatus::Committed;
        updated.commit_ts = Some(now_micros());

        let key = self.pack(&("txn", txn_id));
        let val = Self::serialize(&updated)?;
        self.fdb_set(key, val).await
    }

    async fn abort_transaction(&self, txn_id: TxnId) -> Result<()> {
        let meta = self
            .get_transaction(txn_id)
            .await?
            .ok_or(NovaError::Internal {
                message: format!("Transaction {} not found", txn_id),
            })?;
        let mut updated = meta;
        updated.status = TxnStatus::Aborted;

        let key = self.pack(&("txn", txn_id));
        let val = Self::serialize(&updated)?;
        self.fdb_set(key, val).await
    }

    async fn get_transaction(&self, txn_id: TxnId) -> Result<Option<TransactionMeta>> {
        let key = self.pack(&("txn", txn_id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  STREAM OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn create_stream(&self, stream: StreamMeta) -> Result<()> {
        let key = self.pack(&("stream", stream.stream_id));
        let val = Self::serialize(&stream)?;
        self.fdb_set(key, val).await
    }

    async fn get_stream(&self, stream_id: StreamId) -> Result<Option<StreamMeta>> {
        let key = self.pack(&("stream", stream_id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn get_stream_offset(&self, stream_id: StreamId) -> Result<Option<StreamOffset>> {
        let key = self.pack(&("stream_offset", stream_id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn set_stream_offset(&self, stream_id: StreamId, offset: StreamOffset) -> Result<()> {
        let key = self.pack(&("stream_offset", stream_id));
        let val = Self::serialize(&offset)?;
        self.fdb_set(key, val).await
    }

    // ══════════════════════════════════════════════════════════════
    //  CLONE OPERATIONS
    // ══════════════════════════════════════════════════════════════

    async fn create_clone(&self, clone: CloneMeta) -> Result<()> {
        let key = self.pack(&("clone", clone.clone_table_id));
        let val = Self::serialize(&clone)?;
        self.fdb_set(key, val).await
    }

    async fn get_clone(&self, clone_table_id: TableId) -> Result<Option<CloneMeta>> {
        let key = self.pack(&("clone", clone_table_id));
        match self.fdb_get(key).await? {
            Some(bytes) => Ok(Some(Self::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn create_dynamic_table(&self, dt: DynamicTableMeta) -> Result<()> {
        self.fdb_set(self.pack(&("dynamic_table", dt.id)), Self::serialize(&dt)?)
            .await
    }

    async fn get_dynamic_table(&self, dt_id: TableId) -> Result<Option<DynamicTableMeta>> {
        self.fdb_get(self.pack(&("dynamic_table", dt_id)))
            .await?
            .map(|v| Self::deserialize(&v))
            .transpose()
    }

    async fn list_dynamic_tables(&self, db_id: DatabaseId) -> Result<Vec<DynamicTableMeta>> {
        let (start, end) = self.category_range(&"dynamic_table");
        let mut out = Vec::new();
        for (_, v) in self.fdb_get_range(start, end).await? {
            let dt: DynamicTableMeta = Self::deserialize(&v)?;
            if dt.db_id == db_id {
                out.push(dt);
            }
        }
        Ok(out)
    }

    async fn update_dynamic_table(&self, dt: DynamicTableMeta) -> Result<()> {
        self.create_dynamic_table(dt).await
    }

    async fn drop_dynamic_table(&self, dt_id: TableId) -> Result<()> {
        self.fdb_clear(self.pack(&("dynamic_table", dt_id))).await
    }
}
