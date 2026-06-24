// Statistics — collect and provide table/column statistics for the CBO.
//
// DataFusion's optimizer needs statistics to make cost-based decisions:
// - Row count: how many rows in each table/partition
// - Column stats: min, max, null_count, distinct_count per column
// - Byte size: total data size (affects I/O cost estimation)
//
// Nova collects stats from micro-partition metadata (already stored in FDB)
// and feeds them into DataFusion's Statistics struct.

use datafusion::common::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics as TableStatistics};
use nova_common::{MicroPartitionMeta, TableMeta};

/// Collect table-level statistics from micro-partition metadata.
///
/// Aggregates column stats across all active MPs to produce
/// table-level statistics for DataFusion's optimizer.
pub fn collect_table_stats(
    mps: &[MicroPartitionMeta],
    table: &TableMeta,
    schema: &arrow::datatypes::Schema,
) -> TableStatistics {
    if mps.is_empty() {
        return TableStatistics::new_unknown(schema);
    }

    // Aggregate row count and byte size
    let total_rows: u64 = mps.iter().map(|mp| mp.row_count).sum();
    let total_bytes: u64 = mps.iter().map(|mp| mp.byte_size).sum();

    // Aggregate column statistics
    let num_cols = table.columns.len();
    let mut column_stats = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let stats = aggregate_column_stats(mps, col_idx as u32, &table.columns[col_idx].data_type);
        column_stats.push(stats);
    }

    TableStatistics {
        num_rows: Precision::Exact(total_rows as usize),
        total_byte_size: Precision::Exact(total_bytes as usize),
        column_statistics: column_stats,
    }
}

/// Aggregate column statistics across all MPs.
fn aggregate_column_stats(
    mps: &[MicroPartitionMeta],
    col_id: u32,
    data_type: &nova_common::NovaType,
) -> ColumnStatistics {
    let mut total_nulls: u64 = 0;
    let mut min_val: Option<Vec<u8>> = None;
    let mut max_val: Option<Vec<u8>> = None;
    let mut has_stats = false;

    for mp in mps {
        if let Some(stats) = mp.column_stats.get(&col_id) {
            has_stats = true;
            total_nulls += stats.null_count;

            // Track global min (across all MPs)
            if let Some(ref min) = stats.min_value {
                min_val = Some(match min_val {
                    Some(current) => min_bytes(&current, min, data_type),
                    None => min.clone(),
                });
            }

            // Track global max (across all MPs)
            if let Some(ref max) = stats.max_value {
                max_val = Some(match max_val {
                    Some(current) => max_bytes(&current, max, data_type),
                    None => max.clone(),
                });
            }
        }
    }

    if !has_stats {
        return ColumnStatistics::new_unknown();
    }

    let min_scalar = min_val.map(|b| bytes_to_scalar(&b, data_type));
    let max_scalar = max_val.map(|b| bytes_to_scalar(&b, data_type));

    ColumnStatistics {
        null_count: Precision::Exact(total_nulls as usize),
        distinct_count: Precision::Absent,
        min_value: match min_scalar {
            Some(v) => Precision::Exact(v),
            None => Precision::Absent,
        },
        max_value: match max_scalar {
            Some(v) => Precision::Exact(v),
            None => Precision::Absent,
        },
        sum_value: Precision::Absent,
    }
}

/// Return the smaller of two byte-encoded values.
fn min_bytes(a: &[u8], b: &[u8], data_type: &nova_common::NovaType) -> Vec<u8> {
    match data_type {
        nova_common::NovaType::Int64
        | nova_common::NovaType::Int32
        | nova_common::NovaType::Int16
        | nova_common::NovaType::Int8 => {
            let va = bytes_to_i64(a);
            let vb = bytes_to_i64(b);
            if va <= vb { a.to_vec() } else { b.to_vec() }
        }
        nova_common::NovaType::Float64 | nova_common::NovaType::Decimal { .. } => {
            let va = bytes_to_f64(a);
            let vb = bytes_to_f64(b);
            if va <= vb { a.to_vec() } else { b.to_vec() }
        }
        nova_common::NovaType::Utf8 => {
            if a <= b {
                a.to_vec()
            } else {
                b.to_vec()
            }
        }
        _ => a.to_vec(),
    }
}

/// Return the larger of two byte-encoded values.
fn max_bytes(a: &[u8], b: &[u8], data_type: &nova_common::NovaType) -> Vec<u8> {
    match data_type {
        nova_common::NovaType::Int64
        | nova_common::NovaType::Int32
        | nova_common::NovaType::Int16
        | nova_common::NovaType::Int8 => {
            let va = bytes_to_i64(a);
            let vb = bytes_to_i64(b);
            if va >= vb { a.to_vec() } else { b.to_vec() }
        }
        nova_common::NovaType::Float64 | nova_common::NovaType::Decimal { .. } => {
            let va = bytes_to_f64(a);
            let vb = bytes_to_f64(b);
            if va >= vb { a.to_vec() } else { b.to_vec() }
        }
        nova_common::NovaType::Utf8 => {
            if a >= b {
                a.to_vec()
            } else {
                b.to_vec()
            }
        }
        _ => a.to_vec(),
    }
}

fn bytes_to_i64(bytes: &[u8]) -> i64 {
    match bytes.len() {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes.try_into().unwrap_or([0; 2])) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])),
        _ => 0,
    }
}

fn bytes_to_f64(bytes: &[u8]) -> f64 {
    match bytes.len() {
        4 => f32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])) as f64,
        8 => f64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])),
        _ => 0.0,
    }
}

/// Convert byte-encoded min/max to DataFusion's ScalarValue for statistics.
fn bytes_to_scalar(
    bytes: &[u8],
    data_type: &nova_common::NovaType,
) -> datafusion::common::ScalarValue {
    match data_type {
        nova_common::NovaType::Int8 => ScalarValue::Int8(Some(bytes_to_i64(bytes) as i8)),
        nova_common::NovaType::Int16 => ScalarValue::Int16(Some(bytes_to_i64(bytes) as i16)),
        nova_common::NovaType::Int32 => ScalarValue::Int32(Some(bytes_to_i64(bytes) as i32)),
        nova_common::NovaType::Int64 => ScalarValue::Int64(Some(bytes_to_i64(bytes))),
        nova_common::NovaType::Float32 => ScalarValue::Float32(Some(bytes_to_f64(bytes) as f32)),
        nova_common::NovaType::Float64 | nova_common::NovaType::Decimal { .. } => {
            ScalarValue::Float64(Some(bytes_to_f64(bytes)))
        }
        nova_common::NovaType::Utf8 => {
            ScalarValue::Utf8(Some(String::from_utf8_lossy(bytes).to_string()))
        }
        _ => ScalarValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_common::ColumnStats;
    use std::collections::HashMap;

    fn mp_with_col_stats(
        mp_id: u64,
        row_count: u64,
        id_min: i64,
        id_max: i64,
        nulls: u64,
    ) -> MicroPartitionMeta {
        let mut column_stats = HashMap::new();
        column_stats.insert(
            0,
            ColumnStats {
                min_value: Some(id_min.to_le_bytes().to_vec()),
                max_value: Some(id_max.to_le_bytes().to_vec()),
                null_count: nulls,
                distinct_count: 0,
                byte_size: row_count * 8,
            },
        );
        MicroPartitionMeta {
            mp_id,
            table_id: 1,
            partition_id: None,
            version: 1,
            s3_path: format!("s3://test/mp-{}.parquet", mp_id),
            s3_temp_path: None,
            row_count,
            byte_size: row_count * 8,
            compression: nova_common::Compression::Snappy,
            column_stats,
            commit_ts: 0,
            txn_id: 0,
            supersedes: None,
            superseded_by: None,
            active: true,
        }
    }

    fn test_table() -> TableMeta {
        TableMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "test".to_string(),
            columns: vec![nova_common::ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: nova_common::NovaType::Int64,
                nullable: false,
                default_value: None,
                comment: None,
            }],
            created_at: 0,
            owner: 1,
            comment: None,
            version: 0,
            properties: Default::default(),
        }
    }

    #[test]
    fn test_collect_table_stats_basic() {
        let mps = vec![
            mp_with_col_stats(1, 1000, 1, 1000, 5),
            mp_with_col_stats(2, 2000, 1001, 3000, 10),
        ];
        let table = test_table();

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let stats = collect_table_stats(&mps, &table, &schema);

        assert_eq!(stats.num_rows, Precision::Exact(3000));
        assert!(stats.total_byte_size.get_value().unwrap() > &0);
        assert!(!stats.column_statistics.is_empty());
    }

    #[test]
    fn test_collect_table_stats_aggregates_min_max() {
        let mps = vec![
            mp_with_col_stats(1, 1000, 50, 200, 0),
            mp_with_col_stats(2, 1000, 10, 300, 0),
        ];
        let table = test_table();

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let stats = collect_table_stats(&mps, &table, &schema);
        let col_stats = &stats.column_statistics[0];

        // Global min should be 10 (across both MPs)
        if let Precision::Exact(datafusion::common::ScalarValue::Int64(Some(min))) =
            &col_stats.min_value
        {
            assert_eq!(min, &10);
        } else {
            panic!("expected Int64 min, got {:?}", col_stats.min_value);
        }

        // Global max should be 300
        if let Precision::Exact(datafusion::common::ScalarValue::Int64(Some(max))) =
            &col_stats.max_value
        {
            assert_eq!(max, &300);
        } else {
            panic!("expected Int64 max, got {:?}", col_stats.max_value);
        }
    }

    #[test]
    fn test_collect_table_stats_aggregates_nulls() {
        let mps = vec![
            mp_with_col_stats(1, 1000, 1, 1000, 5),
            mp_with_col_stats(2, 1000, 1001, 2000, 10),
        ];
        let table = test_table();

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let stats = collect_table_stats(&mps, &table, &schema);
        let col_stats = &stats.column_statistics[0];

        assert_eq!(col_stats.null_count, Precision::Exact(15)); // 5 + 10
    }

    #[test]
    fn test_collect_table_stats_empty_mps() {
        let mps = vec![];
        let table = test_table();

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let stats = collect_table_stats(&mps, &table, &schema);
        assert!(matches!(stats.num_rows, Precision::Absent)); // unknown
    }
}
