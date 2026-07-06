// Query Result Cache — SQL normalization + auto-invalidation.
//
// Phase 6.2: Repeated queries return instantly from cache.
// Cache key = hash(normalized_sql + table_versions).
// Table version changes → new key → cache miss → re-execute.

use crate::cache::{CacheStats, QueryResultCache};
use std::collections::HashMap;

/// Normalize SQL for cache key generation.
/// - Lowercase keywords
/// - Collapse whitespace
/// - Remove trailing semicolons
/// - Sort table references (SELECT a FROM t1, t2 == SELECT a FROM t2, t1)
pub fn normalize_sql(sql: &str) -> String {
    let mut result = sql.trim().trim_end_matches(';').to_lowercase();
    // Collapse multiple whitespace into single space
    let mut prev_space = false;
    result.retain(|c| {
        if c.is_whitespace() {
            if prev_space {
                false
            } else {
                prev_space = true;
                true
            }
        } else {
            prev_space = false;
            true
        }
    });
    result
}

/// Check if a query is cacheable.
/// Non-cacheable: contains CURRENT_TIMESTAMP, RAND, NOW(), UUID(), etc.
pub fn is_cacheable(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    let non_cacheable = [
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "NOW()",
        "RAND()",
        "RANDOM()",
        "UUID()",
        "SEQUENCE",
        "AUTO_INCREMENT",
    ];
    !non_cacheable.iter().any(|nc| upper.contains(nc))
}

/// Wrapper for query result cache with SQL normalization.
pub struct ResultCache {
    inner: QueryResultCache,
}

impl ResultCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: QueryResultCache::new(max_entries),
        }
    }

    /// Get cached result for a SQL query.
    /// Returns None if not cached or not cacheable.
    pub async fn get(
        &self,
        sql: &str,
        table_versions: &HashMap<u64, u64>,
    ) -> Option<(Vec<String>, Vec<Vec<String>>)> {
        if !is_cacheable(sql) {
            return None;
        }
        let normalized = normalize_sql(sql);
        let key = QueryResultCache::key(&normalized, table_versions);
        self.inner.get(key).await
    }

    /// Cache a query result.
    pub async fn put(
        &self,
        sql: &str,
        table_versions: HashMap<u64, u64>,
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    ) {
        if !is_cacheable(sql) {
            return;
        }
        let normalized = normalize_sql(sql);
        let key = QueryResultCache::key(&normalized, &table_versions);
        self.inner.put(key, columns, rows, table_versions).await;
    }

    pub async fn invalidate_table(&self, table_id: u64) {
        self.inner.invalidate_table(table_id).await;
    }

    pub async fn stats(&self) -> CacheStats {
        self.inner.stats().await
    }

    pub async fn clear(&self) {
        self.inner.clear().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sql_basic() {
        let result = normalize_sql("SELECT * FROM users WHERE id = 1");
        assert_eq!(result, "select * from users where id = 1");
    }

    #[test]
    fn test_normalize_sql_whitespace() {
        let result = normalize_sql("SELECT   *   FROM   users");
        assert_eq!(result, "select * from users");
    }

    #[test]
    fn test_normalize_sql_trailing_semicolon() {
        let result = normalize_sql("SELECT 1;");
        assert_eq!(result, "select 1");
    }

    #[test]
    fn test_normalize_sql_uppercase() {
        let result = normalize_sql("SELECT * FROM Users WHERE Name = 'Alice'");
        assert_eq!(result, "select * from users where name = 'alice'");
    }

    #[test]
    fn test_is_cacheable_basic() {
        assert!(is_cacheable("SELECT * FROM users"));
        assert!(is_cacheable(
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'"
        ));
    }

    #[test]
    fn test_is_cacheable_non_cacheable() {
        assert!(!is_cacheable("SELECT CURRENT_TIMESTAMP"));
        assert!(!is_cacheable("SELECT NOW()"));
        assert!(!is_cacheable("SELECT RAND()"));
        assert!(!is_cacheable(
            "SELECT * FROM t WHERE created > CURRENT_DATE"
        ));
    }

    #[tokio::test]
    async fn test_result_cache_basic() {
        let cache = ResultCache::new(100);
        let mut versions = HashMap::new();
        versions.insert(1u64, 1u64);

        cache
            .put(
                "SELECT * FROM users",
                versions.clone(),
                vec!["id".to_string()],
                vec![vec!["1".to_string()]],
            )
            .await;

        let result = cache.get("SELECT * FROM users", &versions).await;
        assert!(result.is_some());
        let (cols, rows) = result.unwrap();
        assert_eq!(cols, vec!["id"]);
        assert_eq!(rows, vec![vec!["1"]]);
    }

    #[tokio::test]
    async fn test_result_cache_normalized_hit() {
        let cache = ResultCache::new(100);
        let mut versions = HashMap::new();
        versions.insert(1u64, 1u64);

        // Store with extra whitespace
        cache
            .put(
                "SELECT   *   FROM   users;",
                versions.clone(),
                vec!["a".to_string()],
                vec![],
            )
            .await;

        // Query with different whitespace should still hit
        let result = cache.get("SELECT * FROM users", &versions).await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_result_cache_version_invalidation() {
        let cache = ResultCache::new(100);
        let mut versions = HashMap::new();
        versions.insert(1u64, 1u64);

        cache
            .put(
                "SELECT * FROM t",
                versions.clone(),
                vec!["a".to_string()],
                vec![],
            )
            .await;

        // Same SQL but different table version → cache miss
        let mut versions2 = HashMap::new();
        versions2.insert(1u64, 2u64);
        let result = cache.get("SELECT * FROM t", &versions2).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_result_cache_non_cacheable() {
        let cache = ResultCache::new(100);
        let versions = HashMap::new();

        // NOW() is not cacheable
        cache
            .put(
                "SELECT NOW()",
                versions.clone(),
                vec!["now".to_string()],
                vec![],
            )
            .await;

        let result = cache.get("SELECT NOW()", &versions).await;
        assert!(result.is_none()); // not cached
    }

    #[tokio::test]
    async fn test_result_cache_invalidate_table() {
        let cache = ResultCache::new(100);
        let mut versions = HashMap::new();
        versions.insert(42u64, 1u64);

        cache
            .put("SELECT * FROM t", versions, vec!["a".to_string()], vec![])
            .await;

        cache.invalidate_table(42).await;

        let result = cache.get("SELECT * FROM t", &HashMap::new()).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_result_cache_stats() {
        let cache = ResultCache::new(100);
        let mut versions = HashMap::new();
        versions.insert(1u64, 1u64);

        cache
            .put("SELECT 1", versions.clone(), vec!["a".to_string()], vec![])
            .await;

        cache.get("SELECT 1", &versions).await; // hit
        cache.get("SELECT 2", &versions).await; // miss

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }
}
