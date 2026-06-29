// QueryEngine trait — allows MySQL server to execute SQL via any backend.

use crate::executor::QueryResult;
use nova_common::Result;

#[async_trait::async_trait]
pub trait QueryEngine: Send + Sync {
    /// Parse + analyze + execute raw SQL.
    async fn execute_sql(&self, sql: &str, current_db: &str) -> Result<QueryResult>;
}
