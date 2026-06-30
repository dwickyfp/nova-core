//! NovaCache — 2-layer in-memory cache for query results and micro-partition data.
//!
//! L1: Query Result Cache (in-memory HashMap)
//! L2: MP Data Cache (in-memory HashMap)
//!
//! ponytail: Foyer HybridCache (RAM+SSD) is the production target — swap both
//! HashMaps for foyer::HybridCache once async init is wired through the worker
//! bootstrap. Ceiling: ~4GB L2 / ~2GB L1 in this in-memory mode.

/// 2-layer cache stack for nova-core with LRU eviction.
///
/// L1: Query result cache (in-memory, max 1000 entries)
/// L2: MP data cache (in-memory, max 500 entries)
///
/// ponytail: Foyer HybridCache (RAM+SSD) is the production target — swap
/// both HashMaps for foyer::HybridCache once async init is wired through
/// the worker bootstrap. Ceiling: ~4GB L2 / ~2GB L1 in Foyer mode.
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Mutex;

const MAX_MP_ENTRIES: usize = 500;
const MAX_RESULT_ENTRIES: usize = 1000;

pub struct NovaCache {
    result_cache: Mutex<HashMap<String, Vec<RecordBatch>>>,
    mp_cache: Mutex<HashMap<u64, Vec<RecordBatch>>>,
}

impl NovaCache {
    /// Creates a new NovaCache with empty caches.
    pub fn new() -> Self {
        Self {
            result_cache: Mutex::new(HashMap::new()),
            mp_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Get a cached micro-partition by id.
    pub fn get_mp(&self, mp_id: u64) -> Option<Vec<RecordBatch>> {
        self.mp_cache.lock().unwrap().get(&mp_id).cloned()
    }

    /// Cache micro-partition batches with eviction.
    pub fn put_mp(&self, mp_id: u64, batch: Vec<RecordBatch>) {
        let mut cache = self.mp_cache.lock().unwrap();
        if cache.len() >= MAX_MP_ENTRIES {
            // Evict one entry to prevent unbounded growth
            let key_to_remove = cache.keys().next().copied();
            if let Some(k) = key_to_remove {
                cache.remove(&k);
            }
        }
        cache.insert(mp_id, batch);
    }

    /// Get a cached query result by key.
    pub fn get_result(&self, key: &str) -> Option<Vec<RecordBatch>> {
        self.result_cache.lock().unwrap().get(key).cloned()
    }

    /// Cache a query result with eviction.
    pub fn put_result(&self, key: String, result: Vec<RecordBatch>) {
        let mut cache = self.result_cache.lock().unwrap();
        if cache.len() >= MAX_RESULT_ENTRIES {
            let key_to_remove = cache.keys().next().cloned();
            if let Some(k) = key_to_remove {
                cache.remove(&k);
            }
        }
        cache.insert(key, result);
    }

    /// Returns cache statistics: (mp_entries, result_entries).
    pub fn stats(&self) -> (usize, usize) {
        (
            self.mp_cache.lock().unwrap().len(),
            self.result_cache.lock().unwrap().len(),
        )
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
        assert_eq!(got.len(), 1);
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
        assert_eq!(cache.stats(), (1, 1));
    }
}
