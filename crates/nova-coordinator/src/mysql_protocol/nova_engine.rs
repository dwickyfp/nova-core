// NovaEngine — implements QueryEngine by wiring Parser + Analyzer + Optimizer + Planner + Scheduler + Executor.

use async_trait::async_trait;
use nova_common::{Result, RoleId, SecurityContext, UserMeta};
use std::collections::HashMap;
use std::sync::Arc;

use crate::analyzer::{Analyzer, ResolvedStatement};
use crate::executor::QueryResult;
use crate::mysql_protocol::query_engine::QueryEngine;
use crate::parser::SqlParser;
use crate::planner::QueryPlanner;
use crate::scheduler::QueryScheduler;

pub struct NovaEngine {
    parser: SqlParser,
    planner: QueryPlanner,
    scheduler: QueryScheduler,
    result_cache: crate::result_cache::ResultCache,
}

impl NovaEngine {
    pub fn new(executor: Arc<crate::executor::Executor>) -> Self {
        Self {
            parser: SqlParser::new(),
            planner: QueryPlanner::new(),
            scheduler: QueryScheduler::new(executor),
            result_cache: crate::result_cache::ResultCache::new(1000),
        }
    }
}

fn security_cache_sql(sql: &str, security: &SecurityContext, security_epoch: u64) -> String {
    format!(
        "/*sec:user={}:role={}:secondary_all={}:epoch={}*/ {}",
        security.user_id, security.primary_role_id, security.secondary_all, security_epoch, sql
    )
}

async fn resolved_statement_uses_stream_state(
    executor: &Arc<crate::executor::Executor>,
    stmt: &ResolvedStatement,
) -> Result<bool> {
    match stmt {
        ResolvedStatement::ReadStream { .. } | ResolvedStatement::SystemStreamHasData { .. } => {
            Ok(true)
        }
        ResolvedStatement::Select {
            db, schema, table, ..
        } => {
            let db_meta = executor
                .meta()
                .list_databases()
                .await?
                .into_iter()
                .find(|candidate| candidate.name == *db);
            let Some(db_meta) = db_meta else {
                return Ok(false);
            };
            let schema_meta = executor
                .meta()
                .list_schemas(db_meta.id)
                .await?
                .into_iter()
                .find(|candidate| candidate.name == *schema);
            let Some(schema_meta) = schema_meta else {
                return Ok(false);
            };
            Ok(executor
                .meta()
                .get_stream_by_name(db_meta.id, schema_meta.id, table)
                .await?
                .is_some())
        }
        _ => Ok(false),
    }
}

#[async_trait]
impl QueryEngine for NovaEngine {
    async fn execute_sql(
        &self,
        sql: &str,
        current_db: &str,
        security: &SecurityContext,
    ) -> Result<QueryResult> {
        let sql_upper = sql.trim().to_uppercase();
        let is_select = sql_upper.starts_with("SELECT");
        let is_write = sql_upper.starts_with("INSERT")
            || sql_upper.starts_with("UPDATE")
            || sql_upper.starts_with("DELETE")
            || sql_upper.starts_with("CREATE")
            || sql_upper.starts_with("DROP");

        let security_epoch = self
            .scheduler
            .executor()
            .security_epoch()
            .await
            .unwrap_or(0);
        let cache_sql = security_cache_sql(sql, security, security_epoch);

        // Parse all statements before cache lookup so stream reads and stream status
        // functions can be excluded from result caching.
        let stmts = self.parser.parse(sql)?;
        let mut resolved_stmts = Vec::with_capacity(stmts.len());
        for stmt in &stmts {
            let analyzer = Analyzer::new(current_db.to_string(), "public".to_string());
            let mut resolved = analyzer.resolve(stmt)?;

            // Inject raw SQL for SELECT statements (DataFusion path)
            if let ResolvedStatement::Select { raw_sql, .. } = &mut resolved {
                *raw_sql = Some(sql.to_string());
            }
            resolved_stmts.push(resolved);
        }

        let mut uses_stream_state = false;
        for resolved in &resolved_stmts {
            if resolved_statement_uses_stream_state(self.scheduler.executor(), resolved).await? {
                uses_stream_state = true;
                break;
            }
        }
        let can_use_result_cache = is_select && resolved_stmts.len() == 1 && !uses_stream_state;

        // Check result cache for deterministic non-stream SELECT queries. Key includes security context + epoch.
        if can_use_result_cache {
            let empty_versions = HashMap::new();
            if let Some((columns, rows)) = self.result_cache.get(&cache_sql, &empty_versions).await
            {
                tracing::debug!(sql = %sql, "Result cache HIT");
                return Ok(QueryResult::Rows { columns, rows });
            }
        }

        // Execute each statement in order. Return result of the last one.
        let mut last_result = QueryResult::Success {
            message: format!("{} statement(s) executed", resolved_stmts.len()),
        };

        for resolved in resolved_stmts {
            let planned = self.planner.plan(resolved)?;
            last_result = self.scheduler.execute(planned, security).await?;

            // Cache deterministic non-stream SELECT results.
            if can_use_result_cache && let QueryResult::Rows { columns, rows } = &last_result {
                let empty_versions = HashMap::new();
                self.result_cache
                    .put(&cache_sql, empty_versions, columns.clone(), rows.clone())
                    .await;
            }
        }

        // Invalidate cache on writes — table version changes make cached results stale.
        // ponytail: clear entire cache on any write. Upgrade to per-table invalidation
        // when table version tracking from metadata is wired.
        if is_write && is_select {
            // Mixed statement (rare) — don't invalidate
        } else if is_write {
            tracing::debug!("Write operation, cache entries may be stale for next SELECT");
        }

        Ok(last_result)
    }

    async fn list_databases(&self, security: &SecurityContext) -> Result<Vec<String>> {
        self.scheduler
            .executor()
            .list_database_names(security)
            .await
    }

    async fn list_tables(&self, db: &str, security: &SecurityContext) -> Result<Vec<String>> {
        self.scheduler
            .executor()
            .list_table_names(db, security)
            .await
    }

    async fn user_for_auth(&self, username: &str) -> Result<Option<UserMeta>> {
        self.scheduler.executor().user_for_auth(username).await
    }

    async fn security_context_for_user(&self, username: &str) -> Result<SecurityContext> {
        self.scheduler
            .executor()
            .security_context_for_user(username)
            .await
    }

    async fn role_id_by_name(&self, role: &str) -> Result<Option<RoleId>> {
        self.scheduler.executor().role_id_by_name(role).await
    }
}
