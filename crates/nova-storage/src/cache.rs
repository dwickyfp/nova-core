//! NovaCache — 2-layer foyer in-memory cache for query results and MP data.
//!
//! L1: Query Result Cache  (foyer::Cache<String, Vec<RecordBatch>>, LRU, 1000 entries)
//! L2: MP Data Cache       (foyer::Cache<u64,    Vec<RecordBatch>>, LRU,  500 entries)
//!
//! ponytail: SSD tier via foyer::HybridCache — add when working set > RAM.
//!   Upgrade path: replace Cache::new() with HybridCacheBuilder::new().memory().storage().build().await
//!   and make NovaCache::new() async (needs async bootstrap in coordinator/worker init).

use arrow::record_batch::RecordBatch;
use foyer::{Cache, CacheBuilder};

pub struct NovaCache {
    result_cache: Cache<String, Vec<RecordBatch>>,
    mp_cache: Cache<u64, Vec<RecordBatch>>,
}

impl NovaCache {
    pub fn new() -> Self {
        Self {
            result_cache: CacheBuilder::new(1000).build(),
            mp_cache: CacheBuilder::new(500).build(),
        }
    }

    pub fn get_mp(&self, mp_id: u64) -> Option<Vec<RecordBatch>> {
        self.mp_cache.get(&mp_id).map(|e| e.value().clone())
    }

    pub fn put_mp(&self, mp_id: u64, batch: Vec<RecordBatch>) {
        self.mp_cache.insert(mp_id, batch);
    }

    pub fn get_result(&self, key: &str) -> Option<Vec<RecordBatch>> {
        self.result_cache
            .get(&key.to_string())
            .map(|e| e.value().clone())
    }

    pub fn put_result(&self, key: String, result: Vec<RecordBatch>) {
        self.result_cache.insert(key, result);
    }

    /// Returns (mp_entries, result_entries).
    pub fn stats(&self) -> (usize, usize) {
        (self.mp_cache.usage(), self.result_cache.usage())
    }
}

impl Default for NovaCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn one_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn test_mp_cache_roundtrip() {
        let cache = NovaCache::new();
        assert!(cache.get_mp(42).is_none());
        cache.put_mp(42, vec![one_batch()]);
        let got = cache.get_mp(42).unwrap();
        assert_eq!(got[0].num_rows(), 3);
    }

    #[test]
    fn test_result_cache_roundtrip() {
        let cache = NovaCache::new();
        assert!(cache.get_result("SELECT 1").is_none());
        cache.put_result("SELECT 1".to_string(), vec![one_batch()]);
        assert_eq!(cache.get_result("SELECT 1").unwrap()[0].num_rows(), 3);
    }

    #[test]
    fn test_stats() {
        let cache = NovaCache::new();
        cache.put_mp(1, vec![one_batch()]);
        cache.put_result("q".to_string(), vec![one_batch()]);
        let (mp, res) = cache.stats();
        assert!(mp >= 1);
        assert!(res >= 1);
    }
}
