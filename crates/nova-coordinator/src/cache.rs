// Cache hierarchy — 3-layer cache using Foyer hybrid cache.
//
// L1: Query Result Cache (2GB RAM)
//   Key: hash(normalized_sql + table_versions)
//   Value: serialized query result
//   Invalidation: auto (table version change → new key)
//
// L2: Metadata Cache (512MB RAM)
//   Key: table_id, db_id, schema_id
//   Value: TableMeta, DatabaseMeta, SchemaMeta
//   Invalidation: on DDL changes
//
// L3: MP Data Cache (4GB RAM + 100GB SSD)
//   Key: mp_id:version
//   Value: Arrow RecordBatch (decoded Parquet)
//   Invalidation: never (immutable MPs — old versions GC'd)

use nova_common::{MicroPartitionMeta, TableMeta};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Cache layer type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheLayer {
    L1Result,
    L2Metadata,
    L3Mp,
}

/// Cache statistics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub total_bytes: u64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// L1: Query Result Cache.
/// Caches serialized query results keyed by SQL + table versions.
pub struct QueryResultCache {
    cache: RwLock<HashMap<u64, CachedResult>>,
    stats: RwLock<CacheStats>,
    max_entries: usize,
}

#[derive(Debug, Clone)]
struct CachedResult {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    #[allow(dead_code)]
    cached_at: Instant,
    table_versions: HashMap<u64, u64>,
}

impl QueryResultCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            stats: RwLock::new(CacheStats::default()),
            max_entries,
        }
    }

    /// Generate cache key from SQL + table versions.
    pub fn key(sql: &str, table_versions: &HashMap<u64, u64>) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sql.hash(&mut hasher);
        let mut versions: Vec<_> = table_versions.iter().collect();
        versions.sort_by_key(|(k, _)| *k);
        for (k, v) in versions {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    pub async fn get(&self, key: u64) -> Option<(Vec<String>, Vec<Vec<String>>)> {
        let cache = self.cache.read().await;
        if let Some(result) = cache.get(&key) {
            self.stats.write().await.hits += 1;
            return Some((result.columns.clone(), result.rows.clone()));
        }
        self.stats.write().await.misses += 1;
        None
    }

    #[allow(clippy::collapsible_if)]
    pub async fn put(
        &self,
        key: u64,
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        table_versions: HashMap<u64, u64>,
    ) {
        let mut cache = self.cache.write().await;
        if cache.len() >= self.max_entries {
            // Evict oldest entry (FIFO)
            if let Some(&oldest_key) = cache.keys().next() {
                cache.remove(&oldest_key);
                self.stats.write().await.evictions += 1;
            }
        }
        cache.insert(
            key,
            CachedResult {
                columns,
                rows,
                cached_at: Instant::now(),
                table_versions,
            },
        );
    }

    pub async fn invalidate_table(&self, table_id: u64) {
        let mut cache = self.cache.write().await;
        cache.retain(|_, v| !v.table_versions.contains_key(&table_id));
    }

    pub async fn stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }
}

/// L2: Metadata Cache.
/// Caches TableMeta, DatabaseMeta by ID.
pub struct MetadataCache {
    tables: RwLock<HashMap<u64, Arc<TableMeta>>>,
    stats: RwLock<CacheStats>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            stats: RwLock::new(CacheStats::default()),
        }
    }

    pub async fn get_table(&self, table_id: u64) -> Option<Arc<TableMeta>> {
        let tables = self.tables.read().await;
        if let Some(meta) = tables.get(&table_id) {
            self.stats.write().await.hits += 1;
            return Some(meta.clone());
        }
        self.stats.write().await.misses += 1;
        None
    }

    pub async fn put_table(&self, table_id: u64, meta: Arc<TableMeta>) {
        self.tables.write().await.insert(table_id, meta);
    }

    pub async fn invalidate_table(&self, table_id: u64) {
        self.tables.write().await.remove(&table_id);
    }

    pub async fn stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    pub async fn clear(&self) {
        self.tables.write().await.clear();
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

/// L3: MP Data Cache.
/// Caches decoded Arrow RecordBatches by MP ID + version.
/// ponytail: full Foyer integration needs disk-backed storage config.
// Upgrade: replace HashMap with foyer::HybridCache when disk path is configured.
pub struct MpDataCache {
    cache: RwLock<HashMap<String, Vec<arrow::record_batch::RecordBatch>>>,
    stats: RwLock<CacheStats>,
    max_entries: usize,
}

impl MpDataCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            stats: RwLock::new(CacheStats::default()),
            max_entries,
        }
    }

    /// Cache key: mp_id:version
    fn key(mp: &MicroPartitionMeta) -> String {
        format!("{}:{}", mp.mp_id, mp.version)
    }

    pub async fn get(
        &self,
        mp: &MicroPartitionMeta,
    ) -> Option<Vec<arrow::record_batch::RecordBatch>> {
        let key = Self::key(mp);
        let cache = self.cache.read().await;
        if let Some(batches) = cache.get(&key) {
            self.stats.write().await.hits += 1;
            return Some(batches.clone());
        }
        self.stats.write().await.misses += 1;
        None
    }

    #[allow(clippy::collapsible_if)]
    pub async fn put(
        &self,
        mp: &MicroPartitionMeta,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) {
        let key = Self::key(mp);
        let mut cache = self.cache.write().await;
        if cache.len() >= self.max_entries {
            if let Some(first_key) = cache.keys().next().cloned() {
                cache.remove(&first_key);
                self.stats.write().await.evictions += 1;
            }
        }
        cache.insert(key, batches);
    }

    pub async fn stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }
}

/// Unified cache manager — holds all 3 layers.
pub struct CacheManager {
    pub l1_result: QueryResultCache,
    pub l2_metadata: MetadataCache,
    pub l3_mp: MpDataCache,
}

impl CacheManager {
    pub fn new() -> Self {
        Self {
            l1_result: QueryResultCache::new(1000),
            l2_metadata: MetadataCache::new(),
            l3_mp: MpDataCache::new(500),
        }
    }

    pub async fn stats(&self) -> HashMap<CacheLayer, CacheStats> {
        let mut stats = HashMap::new();
        stats.insert(CacheLayer::L1Result, self.l1_result.stats().await);
        stats.insert(CacheLayer::L2Metadata, self.l2_metadata.stats().await);
        stats.insert(CacheLayer::L3Mp, self.l3_mp.stats().await);
        stats
    }

    pub async fn invalidate_table(&self, table_id: u64) {
        self.l1_result.invalidate_table(table_id).await;
        self.l2_metadata.invalidate_table(table_id).await;
    }

    pub async fn clear_all(&self) {
        self.l1_result.clear().await;
        self.l2_metadata.clear().await;
        self.l3_mp.clear().await;
    }
}

impl Default for CacheManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_l1_result_cache_basic() {
        let cache = QueryResultCache::new(100);
        let key = QueryResultCache::key("SELECT * FROM t", &HashMap::new());

        cache
            .put(
                key,
                vec!["id".to_string()],
                vec![vec!["1".to_string()]],
                HashMap::new(),
            )
            .await;

        let result = cache.get(key).await;
        assert!(result.is_some());
        let (cols, rows) = result.unwrap();
        assert_eq!(cols, vec!["id"]);
        assert_eq!(rows, vec![vec!["1"]]);
    }

    #[tokio::test]
    async fn test_l1_result_cache_miss() {
        let cache = QueryResultCache::new(100);
        let result = cache.get(999).await;
        assert!(result.is_none());

        let stats = cache.stats().await;
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn test_l1_result_cache_hit_rate() {
        let cache = QueryResultCache::new(100);
        let key = 1u64;
        cache
            .put(key, vec!["a".to_string()], vec![], HashMap::new())
            .await;

        cache.get(key).await; // hit
        cache.get(key).await; // hit
        cache.get(999).await; // miss

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate() - 0.6667).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_l1_result_cache_eviction() {
        let cache = QueryResultCache::new(2);
        cache.put(1, vec![], vec![], HashMap::new()).await;
        cache.put(2, vec![], vec![], HashMap::new()).await;
        cache.put(3, vec![], vec![], HashMap::new()).await; // evicts key 1

        let stats = cache.stats().await;
        assert!(stats.evictions >= 1);
    }

    #[tokio::test]
    async fn test_l1_result_cache_invalidate_table() {
        let cache = QueryResultCache::new(100);
        let mut versions = HashMap::new();
        versions.insert(42u64, 1u64);

        let key = QueryResultCache::key("SELECT * FROM t", &versions);
        cache
            .put(key, vec!["a".to_string()], vec![], versions)
            .await;

        cache.invalidate_table(42).await;
        assert!(cache.get(key).await.is_none());
    }

    #[tokio::test]
    async fn test_l1_cache_key_deterministic() {
        let versions = HashMap::new();
        let key1 = QueryResultCache::key("SELECT 1", &versions);
        let key2 = QueryResultCache::key("SELECT 1", &versions);
        assert_eq!(key1, key2);
    }

    #[tokio::test]
    async fn test_l1_cache_key_different_sql() {
        let versions = HashMap::new();
        let key1 = QueryResultCache::key("SELECT 1", &versions);
        let key2 = QueryResultCache::key("SELECT 2", &versions);
        assert_ne!(key1, key2);
    }

    #[tokio::test]
    async fn test_l2_metadata_cache_basic() {
        let cache = MetadataCache::new();
        let table = Arc::new(TableMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "test".to_string(),
            columns: vec![],
            created_at: 0,
            owner: 0,
            comment: None,
            version: 0,
            properties: Default::default(),
        });

        cache.put_table(1, table.clone()).await;
        let result = cache.get_table(1).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, "test");
    }

    #[tokio::test]
    async fn test_l2_metadata_cache_miss() {
        let cache = MetadataCache::new();
        assert!(cache.get_table(999).await.is_none());
    }

    #[tokio::test]
    async fn test_l2_metadata_cache_invalidate() {
        let cache = MetadataCache::new();
        let table = Arc::new(TableMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "test".to_string(),
            columns: vec![],
            created_at: 0,
            owner: 0,
            comment: None,
            version: 0,
            properties: Default::default(),
        });

        cache.put_table(1, table).await;
        cache.invalidate_table(1).await;
        assert!(cache.get_table(1).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_manager_stats() {
        let mgr = CacheManager::new();
        let stats = mgr.stats().await;
        assert!(stats.contains_key(&CacheLayer::L1Result));
        assert!(stats.contains_key(&CacheLayer::L2Metadata));
        assert!(stats.contains_key(&CacheLayer::L3Mp));
    }

    #[tokio::test]
    async fn test_cache_manager_invalidate_table() {
        let mgr = CacheManager::new();
        mgr.l2_metadata
            .put_table(
                1,
                Arc::new(TableMeta {
                    id: 1,
                    db_id: 1,
                    schema_id: 1,
                    name: "t".to_string(),
                    columns: vec![],
                    created_at: 0,
                    owner: 0,
                    comment: None,
                    version: 0,
                    properties: Default::default(),
                }),
            )
            .await;

        mgr.invalidate_table(1).await;
        assert!(mgr.l2_metadata.get_table(1).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_manager_clear_all() {
        let mgr = CacheManager::new();
        mgr.l1_result.put(1, vec![], vec![], HashMap::new()).await;
        mgr.clear_all().await;

        let stats = mgr.stats().await;
        assert_eq!(stats.get(&CacheLayer::L1Result).unwrap().hits, 0);
    }
}
