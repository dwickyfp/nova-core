//! Coordinator cache — query result cache (Snowflake-style).

// TODO: Phase 6 — implement with foyer

pub struct CoordinatorCache;

impl CoordinatorCache {
    pub fn new() -> Self {
        Self
    }
    // TODO: Phase 6
    // pub async fn try_get(&self, sql: &str, tables: &[TableId]) -> Option<QueryResult> { ... }
    // pub async fn put(&self, sql: &str, tables: &[(TableId, u64)], result: QueryResult) { ... }
}

impl Default for CoordinatorCache {
    fn default() -> Self {
        Self::new()
    }
}
