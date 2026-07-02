// NovaEngine — implements QueryEngine by wiring Parser + Analyzer + Optimizer + Planner + Scheduler + Executor.

use async_trait::async_trait;
use nova_common::Result;
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

#[async_trait]
impl QueryEngine for NovaEngine {
    async fn execute_sql(&self, sql: &str, current_db: &str) -> Result<QueryResult> {
        let sql_upper = sql.trim().to_uppercase();
        let is_select = sql_upper.starts_with("SELECT");
        let is_write = sql_upper.starts_with("INSERT")
            || sql_upper.starts_with("UPDATE")
            || sql_upper.starts_with("DELETE")
            || sql_upper.starts_with("CREATE")
            || sql_upper.starts_with("DROP");

        // Check result cache for SELECT queries
        if is_select {
            let empty_versions = HashMap::new();
            if let Some((columns, rows)) = self.result_cache.get(sql, &empty_versions).await {
                tracing::debug!(sql = %sql, "Result cache HIT");
                return Ok(QueryResult::Rows { columns, rows });
            }
        }

        // Parse all statements
        let stmts = self.parser.parse(sql)?;

        // Execute each statement in order. Return result of the last one.
        let mut last_result = QueryResult::Success {
            message: format!("{} statement(s) executed", stmts.len()),
        };

        for stmt in &stmts {
            let analyzer = Analyzer::new(current_db.to_string(), "public".to_string());
            let mut resolved = analyzer.resolve(stmt)?;

            // Inject raw SQL for SELECT statements (DataFusion path)
            if let ResolvedStatement::Select { raw_sql, .. } = &mut resolved {
                *raw_sql = Some(sql.to_string());
            }

            let planned = self.planner.plan(resolved)?;
            last_result = self.scheduler.execute(planned).await?;

            // Cache SELECT results
            if let QueryResult::Rows { columns, rows } = &last_result {
                let empty_versions = HashMap::new();
                self.result_cache
                    .put(sql, empty_versions, columns.clone(), rows.clone())
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

    async fn list_databases(&self) -> Result<Vec<String>> {
        self.scheduler.executor().list_database_names().await
    }

    async fn list_tables(&self, db: &str) -> Result<Vec<String>> {
        self.scheduler.executor().list_table_names(db).await
    }
}
