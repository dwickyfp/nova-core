// NovaEngine — implements QueryEngine by wiring Parser + Analyzer + Executor.

use async_trait::async_trait;
use nova_common::Result;
use std::sync::Arc;

use crate::analyzer::Analyzer;
use crate::executor::{Executor, QueryResult};
use crate::mysql_protocol::query_engine::QueryEngine;
use crate::parser::SqlParser;

pub struct NovaEngine {
    parser: SqlParser,
    executor: Arc<Executor>,
}

impl NovaEngine {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self {
            parser: SqlParser::new(),
            executor,
        }
    }
}

#[async_trait]
impl QueryEngine for NovaEngine {
    async fn execute_sql(&self, sql: &str, current_db: &str) -> Result<QueryResult> {
        let stmts = self.parser.parse(sql)?;
        let stmt = stmts
            .first()
            .ok_or_else(|| nova_common::NovaError::SqlParseError {
                message: "empty SQL".to_string(),
            })?;

        let analyzer = Analyzer::new(current_db.to_string(), "public".to_string());
        let resolved = analyzer.resolve(stmt)?;

        self.executor.execute(resolved).await
    }
}
