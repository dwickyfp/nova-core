// QueryEngine trait — allows MySQL server to execute SQL via any backend.

use crate::executor::QueryResult;
use nova_common::{Result, RoleId, SecurityContext, UserMeta};

#[async_trait::async_trait]
pub trait QueryEngine: Send + Sync {
    /// Parse + analyze + execute raw SQL.
    async fn execute_sql(
        &self,
        sql: &str,
        current_db: &str,
        security: &SecurityContext,
    ) -> Result<QueryResult>;

    /// List all database names (for SHOW DATABASES).
    async fn list_databases(&self, security: &SecurityContext) -> Result<Vec<String>>;

    /// List all table names in the given database (for SHOW TABLES).
    async fn list_tables(&self, db: &str, security: &SecurityContext) -> Result<Vec<String>>;

    /// Read persistent user metadata for MySQL authentication.
    async fn user_for_auth(&self, username: &str) -> Result<Option<UserMeta>>;

    /// Build session security context from persistent security metadata.
    async fn security_context_for_user(&self, username: &str) -> Result<SecurityContext>;

    /// Resolve a role name for USE ROLE.
    async fn role_id_by_name(&self, role: &str) -> Result<Option<RoleId>>;
}
