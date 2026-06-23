// MP Pruning — skip micro-partitions based on column statistics (zone maps).
//
// Before reading MPs from S3, check min/max/null_count stats stored in FDB.
// If a predicate can't possibly match (e.g. WHERE dt > '2026-06-01' but MP max(dt) = '2026-05-15'),
// skip that MP entirely. This is the #1 performance optimization for OLAP queries.
//
// How it works:
// 1. Coordinator receives query with filter predicate
// 2. Pruner reads MP column stats from metadata
// 3. For each MP, check if predicate COULD match using min/max bounds
// 4. Return only MPs that might contain matching rows
// 5. MicroPartitionScanExec only reads the pruned set

use arrow::datatypes::SchemaRef;
use nova_common::types::ColumnStats;
use nova_common::{ColumnId, MicroPartitionMeta};
use sqlparser::ast::{BinaryOperator, Expr, Value};

/// Prune micro-partitions based on column statistics and filter predicates.
///
/// Returns only MPs whose column stats indicate they MIGHT contain matching rows.
/// Conservative: if unsure, include the MP (never skip a valid MP).
pub fn prune_mps(
    mps: &[MicroPartitionMeta],
    predicate: &Expr,
    schema: &SchemaRef,
) -> Vec<MicroPartitionMeta> {
    mps.iter()
        .filter(|mp| mp_might_match(mp, predicate, schema))
        .cloned()
        .collect()
}

/// Check if a single MP might contain rows matching the predicate.
/// Returns true if the MP SHOULD be scanned (conservative: include on doubt).
fn mp_might_match(mp: &MicroPartitionMeta, predicate: &Expr, schema: &SchemaRef) -> bool {
    match predicate {
        // AND: both sides must potentially match
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => mp_might_match(mp, left, schema) && mp_might_match(mp, right, schema),
        // OR: either side could match
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => mp_might_match(mp, left, schema) || mp_might_match(mp, right, schema),
        // Simple comparison: col > value, col < value, col = value
        Expr::BinaryOp { left, op, right } => {
            // Try to extract column name and literal value
            if let (Some(col_name), Some(lit_val)) =
                (extract_column_name(left), extract_literal_value(right))
                && let Some(col_idx) = schema.fields().iter().position(|f| f.name() == &col_name)
                && let Some(stats) = mp.column_stats.get(&(col_idx as ColumnId))
            {
                return evaluate_predicate_on_stats(stats, op, &lit_val);
            }
            // Can't evaluate → include MP (conservative)
            true
        }
        // NOT: can't easily prune
        Expr::UnaryOp { .. } => true,
        // Everything else: include MP
        _ => true,
    }
}

/// Evaluate a comparison against column stats (min/max bounds).
fn evaluate_predicate_on_stats(
    stats: &ColumnStats,
    op: &BinaryOperator,
    value: &LiteralValue,
) -> bool {
    let min = stats.min_value.as_ref();
    let max = stats.max_value.as_ref();

    match (min, max) {
        (Some(min_bytes), Some(max_bytes)) => {
            let min_val = decode_literal(min_bytes, value);
            let max_val = decode_literal(max_bytes, value);

            match (min_val, max_val) {
                (Some(min_v), Some(max_v)) => match op {
                    // col > value: skip if max(col) <= value
                    BinaryOperator::Gt => max_v > value.to_f64(),
                    // col >= value: skip if max(col) < value
                    BinaryOperator::GtEq => max_v >= value.to_f64(),
                    // col < value: skip if min(col) >= value
                    BinaryOperator::Lt => min_v < value.to_f64(),
                    // col <= value: skip if min(col) > value
                    BinaryOperator::LtEq => min_v <= value.to_f64(),
                    // col = value: skip if value < min or value > max
                    BinaryOperator::Eq => {
                        let v = value.to_f64();
                        v >= min_v && v <= max_v
                    }
                    // col != value: can't easily prune
                    BinaryOperator::NotEq => true,
                    _ => true,
                },
                // Can't decode → include MP
                _ => true,
            }
        }
        // No stats → include MP
        _ => true,
    }
}

/// Literal value extracted from predicate.
#[derive(Debug, Clone)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(String),
}

impl LiteralValue {
    fn to_f64(&self) -> f64 {
        match self {
            LiteralValue::Int(v) => *v as f64,
            LiteralValue::Float(v) => *v,
            LiteralValue::String(_) => 0.0, // can't compare strings numerically
        }
    }
}

/// Extract column name from expression (left side of comparison).
fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        _ => None,
    }
}

/// Extract literal value from expression (right side of comparison).
fn extract_literal_value(expr: &Expr) -> Option<LiteralValue> {
    match expr {
        Expr::Value(v) => match v {
            Value::Number(n, _) => {
                if n.contains('.') {
                    Some(LiteralValue::Float(n.parse().ok()?))
                } else {
                    Some(LiteralValue::Int(n.parse().ok()?))
                }
            }
            Value::SingleQuotedString(s) => Some(LiteralValue::String(s.clone())),
            _ => None,
        },
        _ => None,
    }
}

/// Decode a stored min/max value (bytes) to f64 for comparison.
fn decode_literal(bytes: &[u8], reference: &LiteralValue) -> Option<f64> {
    match bytes.len() {
        1 => Some(bytes[0] as f64),
        2 => Some(i16::from_le_bytes(bytes.try_into().ok()?) as f64),
        4 => match reference {
            LiteralValue::Float(_) => Some(f32::from_le_bytes(bytes.try_into().ok()?) as f64),
            _ => Some(i32::from_le_bytes(bytes.try_into().ok()?) as f64),
        },
        8 => match reference {
            LiteralValue::Float(_) => Some(f64::from_le_bytes(bytes.try_into().ok()?)),
            _ => Some(i64::from_le_bytes(bytes.try_into().ok()?) as f64),
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("dt", DataType::Date32, false),
        ]))
    }

    fn mp_with_stats(
        id_min: i64,
        id_max: i64,
        amount_min: f64,
        amount_max: f64,
    ) -> MicroPartitionMeta {
        let mut column_stats = HashMap::new();
        column_stats.insert(
            0,
            ColumnStats {
                min_value: Some(id_min.to_le_bytes().to_vec()),
                max_value: Some(id_max.to_le_bytes().to_vec()),
                null_count: 0,
                distinct_count: 0,
                byte_size: 0,
            },
        );
        column_stats.insert(
            1,
            ColumnStats {
                min_value: Some(amount_min.to_le_bytes().to_vec()),
                max_value: Some(amount_max.to_le_bytes().to_vec()),
                null_count: 0,
                distinct_count: 0,
                byte_size: 0,
            },
        );
        MicroPartitionMeta {
            mp_id: id_min as u64,
            table_id: 1,
            partition_id: None,
            version: 1,
            s3_path: format!("s3://test/mp-{}.parquet", id_min),
            s3_temp_path: None,
            row_count: 1000,
            byte_size: 1024 * 1024,
            compression: nova_common::Compression::Snappy,
            column_stats,
            commit_ts: 0,
            txn_id: 0,
            supersedes: None,
            superseded_by: None,
            active: true,
        }
    }

    #[test]
    fn test_prune_gt_skips_low_mps() {
        let schema = test_schema();
        let mps = vec![
            mp_with_stats(1, 100, 0.0, 500.0),
            mp_with_stats(101, 200, 500.0, 1000.0),
            mp_with_stats(201, 300, 1000.0, 2000.0),
        ];

        // WHERE id > 150 → should skip MP 1 (max=100 <= 150)
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(Value::Number("150".to_string(), false))),
        };

        let pruned = prune_mps(&mps, &pred, &schema);
        assert_eq!(pruned.len(), 2); // MP 1 skipped, MP 2 and 3 kept
        assert_eq!(pruned[0].mp_id, 101);
        assert_eq!(pruned[1].mp_id, 201);
    }

    #[test]
    fn test_prune_lt_skips_high_mps() {
        let schema = test_schema();
        let mps = vec![
            mp_with_stats(1, 100, 0.0, 500.0),
            mp_with_stats(101, 200, 500.0, 1000.0),
            mp_with_stats(201, 300, 1000.0, 2000.0),
        ];

        // WHERE id < 150 → should skip MP 3 (min=201 > 150)
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Value(Value::Number("150".to_string(), false))),
        };

        let pruned = prune_mps(&mps, &pred, &schema);
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0].mp_id, 1);
        assert_eq!(pruned[1].mp_id, 101);
    }

    #[test]
    fn test_prune_eq_skips_out_of_range() {
        let schema = test_schema();
        let mps = vec![
            mp_with_stats(1, 100, 0.0, 500.0),
            mp_with_stats(101, 200, 500.0, 1000.0),
        ];

        // WHERE id = 150 → should skip MP 1 (150 < min=101? no, 150 > max=100? yes → skip)
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number("150".to_string(), false))),
        };

        let pruned = prune_mps(&mps, &pred, &schema);
        assert_eq!(pruned.len(), 1);
        assert_eq!(pruned[0].mp_id, 101);
    }

    #[test]
    fn test_prune_and_both_must_match() {
        let schema = test_schema();
        let mps = vec![
            mp_with_stats(1, 100, 0.0, 500.0),
            mp_with_stats(101, 200, 500.0, 1000.0),
            mp_with_stats(201, 300, 1000.0, 2000.0),
        ];

        // WHERE id > 50 AND id < 250
        let pred = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Value(Value::Number("50".to_string(), false))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
                op: BinaryOperator::Lt,
                right: Box::new(Expr::Value(Value::Number("250".to_string(), false))),
            }),
        };

        let pruned = prune_mps(&mps, &pred, &schema);
        // All 3 MPs: id>50 keeps all, id<250 skips MP 3 (min=201<250, keep)
        // Actually: MP1 max=100 > 50 ✓, MP1 min=1 < 250 ✓ → keep
        //           MP2 max=200 > 50 ✓, MP2 min=101 < 250 ✓ → keep
        //           MP3 max=300 > 50 ✓, MP3 min=201 < 250 ✓ → keep
        assert_eq!(pruned.len(), 3);
    }

    #[test]
    fn test_prune_no_stats_keeps_all() {
        let schema = test_schema();
        let mp_no_stats = MicroPartitionMeta {
            mp_id: 1,
            table_id: 1,
            partition_id: None,
            version: 1,
            s3_path: "s3://test/mp-1.parquet".to_string(),
            s3_temp_path: None,
            row_count: 1000,
            byte_size: 1024 * 1024,
            compression: nova_common::Compression::Snappy,
            column_stats: HashMap::new(), // no stats
            commit_ts: 0,
            txn_id: 0,
            supersedes: None,
            superseded_by: None,
            active: true,
        };

        let pred = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id"))),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Value(Value::Number("999999".to_string(), false))),
        };

        let pruned = prune_mps(&[mp_no_stats], &pred, &schema);
        assert_eq!(pruned.len(), 1); // conservative: include when no stats
    }
}
