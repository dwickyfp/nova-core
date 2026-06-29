//! Optimizer — applies MP pruning, CBO, and optimizer rules to resolved statements.
//!
//! For single-node (Phase 2), the optimizer applies:
//! 1. MP pruning — skip MPs whose zone-map stats can't match the WHERE predicate
//! 2. (Future) CBO join reordering, late materialization, runtime filter pushdown

use nova_common::{MicroPartitionMeta, Result};
use sqlparser::ast::{BinaryOperator, Expr, Value};

use crate::analyzer::{ResolvedFilter, ResolvedStatement};
use crate::mp_pruning;
use crate::optimizer_rules::{LateMaterializationPlan, plan_late_materialization};
use crate::statistics;

/// Nova optimizer: applies pruning and optimization rules.
pub struct NovaOptimizer;

impl NovaOptimizer {
    pub fn new() -> Self {
        Self
    }

    /// Optimize a resolved statement by applying pruning rules.
    /// Returns the pruned MP list for SELECT queries.
    pub fn optimize_select(
        &self,
        stmt: &ResolvedStatement,
        mps: &[MicroPartitionMeta],
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<Vec<MicroPartitionMeta>> {
        if let ResolvedStatement::Select { filter, .. } = stmt {
            match filter {
                Some(f) => {
                    let expr = resolved_filter_to_expr(f);
                    Ok(mp_pruning::prune_mps(mps, &expr, schema))
                }
                None => Ok(mps.to_vec()),
            }
        } else {
            Ok(mps.to_vec())
        }
    }

    /// Plan late materialization for a SELECT with filter + projection.
    /// Returns a plan that describes which columns to read first (filter) vs later (projection).
    pub fn plan_late_materialization(
        &self,
        stmt: &ResolvedStatement,
    ) -> Option<LateMaterializationPlan> {
        if let ResolvedStatement::Select {
            projection, filter, ..
        } = stmt
        {
            let filter_col = filter.as_ref().map(|f| vec![f.column.clone()])?;
            let column_sizes: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            plan_late_materialization(&filter_col, projection, 0.1, &column_sizes)
        } else {
            None
        }
    }

    /// Collect table statistics for CBO.
    pub fn collect_stats(
        &self,
        mps: &[MicroPartitionMeta],
        table: &nova_common::TableMeta,
        schema: &arrow::datatypes::SchemaRef,
    ) -> datafusion::common::stats::Statistics {
        statistics::collect_table_stats(mps, table, schema)
    }
}

impl Default for NovaOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a ResolvedFilter back to sqlparser Expr for use with mp_pruning.
fn resolved_filter_to_expr(filter: &ResolvedFilter) -> Expr {
    let col = Expr::Identifier(sqlparser::ast::Ident::new(&filter.column));
    let val = match &filter.value {
        crate::analyzer::ResolvedExpr::Int64(v) => Expr::Value(Value::Number(v.to_string(), false)),
        crate::analyzer::ResolvedExpr::Float64(v) => {
            Expr::Value(Value::Number(v.to_string(), false))
        }
        crate::analyzer::ResolvedExpr::String(v) => {
            Expr::Value(Value::SingleQuotedString(v.clone()))
        }
        crate::analyzer::ResolvedExpr::Boolean(v) => Expr::Value(Value::Boolean(*v)),
        crate::analyzer::ResolvedExpr::Null => Expr::Value(Value::Null),
    };

    let op = match filter.op.as_str() {
        "=" => BinaryOperator::Eq,
        "!=" | "<>" => BinaryOperator::NotEq,
        "<" => BinaryOperator::Lt,
        "<=" => BinaryOperator::LtEq,
        ">" => BinaryOperator::Gt,
        ">=" => BinaryOperator::GtEq,
        _ => return col, // unknown op → no pruning (return just column, conservative)
    };

    Expr::BinaryOp {
        left: Box::new(col),
        op,
        right: Box::new(val),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::{ResolvedExpr, ResolvedFilter, ResolvedStatement};
    use arrow::datatypes::{Field, Schema};
    use nova_common::{ColumnStats, Compression, MicroPartitionMeta};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_mp(mp_id: u64, min: i64, max: i64) -> MicroPartitionMeta {
        let mut column_stats = HashMap::new();
        column_stats.insert(
            0,
            ColumnStats {
                min_value: Some(min.to_le_bytes().to_vec()),
                max_value: Some(max.to_le_bytes().to_vec()),
                null_count: 0,
                distinct_count: 0,
                byte_size: 0,
            },
        );
        MicroPartitionMeta {
            mp_id,
            table_id: 1,
            partition_id: None,
            version: 1,
            s3_path: format!("s3://nova/test/mp-{}.parquet", mp_id),
            s3_temp_path: None,
            row_count: 100,
            byte_size: 1024,
            compression: Compression::Snappy,
            column_stats,
            commit_ts: 0,
            txn_id: 0,
            supersedes: None,
            superseded_by: None,
            active: true,
        }
    }

    #[test]
    fn test_optimize_select_no_filter() {
        let optimizer = NovaOptimizer::new();
        let stmt = ResolvedStatement::Select {
            db: "test".to_string(),
            schema: "public".to_string(),
            table: "t".to_string(),
            projection: vec!["*".to_string()],
            filter: None,
            at_timestamp: None,
            raw_sql: None,
        };
        let mps = vec![make_mp(1, 0, 100), make_mp(2, 100, 200)];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let result = optimizer.optimize_select(&stmt, &mps, &schema).unwrap();
        assert_eq!(result.len(), 2); // no filter → all MPs
    }

    #[test]
    fn test_optimize_select_with_filter_prunes_mps() {
        let optimizer = NovaOptimizer::new();
        let stmt = ResolvedStatement::Select {
            db: "test".to_string(),
            schema: "public".to_string(),
            table: "t".to_string(),
            projection: vec!["*".to_string()],
            filter: Some(ResolvedFilter {
                column: "id".to_string(),
                op: ">".to_string(),
                value: ResolvedExpr::Int64(150),
            }),
            at_timestamp: None,
            raw_sql: None,
        };
        // MP1: min=0, max=100 → pruned (can't have id > 150)
        // MP2: min=100, max=200 → kept (can have id > 150)
        let mps = vec![make_mp(1, 0, 100), make_mp(2, 100, 200)];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let result = optimizer.optimize_select(&stmt, &mps, &schema).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].mp_id, 2);
    }

    #[test]
    fn test_resolved_filter_to_expr() {
        let filter = ResolvedFilter {
            column: "age".to_string(),
            op: "=".to_string(),
            value: ResolvedExpr::Int64(25),
        };
        let expr = resolved_filter_to_expr(&filter);
        assert!(matches!(expr, Expr::BinaryOp { .. }));
    }
}
