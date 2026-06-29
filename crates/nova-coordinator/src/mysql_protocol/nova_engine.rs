// NovaEngine — implements QueryEngine by wiring Parser + Analyzer + Optimizer + Planner + Scheduler + Executor.

use async_trait::async_trait;
use nova_common::Result;
use std::sync::Arc;

use crate::analyzer::Analyzer;
use crate::executor::QueryResult;
use crate::mysql_protocol::query_engine::QueryEngine;
use crate::parser::SqlParser;
use crate::planner::QueryPlanner;
use crate::scheduler::QueryScheduler;

pub struct NovaEngine {
    parser: SqlParser,
    planner: QueryPlanner,
    scheduler: QueryScheduler,
}

impl NovaEngine {
    pub fn new(executor: Arc<crate::executor::Executor>) -> Self {
        Self {
            parser: SqlParser::new(),
            planner: QueryPlanner::new(),
            scheduler: QueryScheduler::new(executor),
        }
    }
}

#[async_trait]
impl QueryEngine for NovaEngine {
    async fn execute_sql(&self, sql: &str, current_db: &str) -> Result<QueryResult> {
        // 1. Parse
        let stmts = self.parser.parse(sql)?;
        let stmt = stmts
            .first()
            .ok_or_else(|| nova_common::NovaError::SqlParseError {
                message: "empty SQL".to_string(),
            })?;

        // 2. Analyze (name resolution, type checking)
        let analyzer = Analyzer::new(current_db.to_string(), "public".to_string());
        let resolved = analyzer.resolve(stmt)?;

        // 3. Optimize (MP pruning, CBO rules)
        // MP pruning happens inside executor.exec_select() via optimizer
        // For now, optimizer is applied at the executor level (see Executor)

        // 4. Plan (pass-through for single-node)
        let planned = self.planner.plan(resolved)?;

        // 5. Schedule + Execute
        self.scheduler.execute(planned).await
    }
}
