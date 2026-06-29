// NovaEngine — implements QueryEngine by wiring Parser + Analyzer + Optimizer + Planner + Scheduler + Executor.

use async_trait::async_trait;
use nova_common::Result;
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
        // Check result cache for SELECT queries
        if sql.trim().to_uppercase().starts_with("SELECT") {
            let empty_versions = std::collections::HashMap::new();
            if let Some((columns, rows)) = self.result_cache.get(sql, &empty_versions).await {
                tracing::debug!(sql = %sql, "Result cache HIT");
                return Ok(QueryResult::Rows { columns, rows });
            }
        }

        // 1. Parse all statements
        let stmts = self.parser.parse(sql)?;

        // Execute each statement in order. Return result of the last one.
        let mut last_result = QueryResult::Success {
            message: format!("{} statement(s) executed", stmts.len()),
        };

        for stmt in &stmts {
            // 2. Analyze (name resolution, type checking)
            let analyzer = Analyzer::new(current_db.to_string(), "public".to_string());
            let mut resolved = analyzer.resolve(stmt)?;

            // Inject raw SQL for SELECT statements (DataFusion path)
            if let ResolvedStatement::Select { raw_sql, .. } = &mut resolved {
                *raw_sql = Some(sql.to_string());
            }

            // 3. Plan (pass-through for single-node)
            let planned = self.planner.plan(resolved)?;

            // 4. Schedule + Execute
            last_result = self.scheduler.execute(planned).await?;

            // 5. Cache SELECT results
            if let QueryResult::Rows { columns, rows } = &last_result {
                let empty_versions = std::collections::HashMap::new();
                self.result_cache
                    .put(sql, empty_versions, columns.clone(), rows.clone())
                    .await;
            }
        }

        Ok(last_result)
    }
}
