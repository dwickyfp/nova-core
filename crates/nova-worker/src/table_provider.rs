//! NovaTableProvider — DataFusion TableProvider backed by Nova micro-partitions.
//!
//! Bridges Nova's storage layer (immutable Parquet MPs on S3) with DataFusion's
//! query engine. DataFusion uses this to scan Nova tables with:
//! - Column pruning (projection pushdown)
//! - MP pruning (via statistics)
//! - Parallel scan (1 partition per MP)

use arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use nova_common::{MicroPartitionMeta, TableMeta};
use nova_storage::MpReader;
use std::any::Any;
use std::sync::Arc;

use crate::operators::mp_scan_exec::MicroPartitionScanExec;

/// DataFusion TableProvider that reads from Nova micro-partitions.
#[derive(Debug)]
pub struct NovaTableProvider {
    #[allow(dead_code)]
    table_meta: TableMeta,
    mps: Vec<MicroPartitionMeta>,
    reader: Arc<MpReader>,
    schema: SchemaRef,
}

impl NovaTableProvider {
    /// Create a new NovaTableProvider.
    pub fn new(table_meta: TableMeta, mps: Vec<MicroPartitionMeta>, reader: Arc<MpReader>) -> Self {
        let schema = build_schema(&table_meta);
        Self {
            table_meta,
            mps,
            reader,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for NovaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Log filters for future MP pruning integration
        if !filters.is_empty() {
            tracing::debug!(
                filters = ?filters.len(),
                "NovaTableProvider::scan received filters (MP pruning via filters is future work)"
            );
        }
        Ok(Arc::new(MicroPartitionScanExec::new(
            self.mps.clone(),
            self.schema.clone(),
            projection.cloned(),
            self.reader.clone(),
        )))
    }

    /// Return table statistics for DataFusion's CBO.
    /// Aggregates row count and byte size from all micro-partitions.
    fn statistics(&self) -> Option<datafusion::common::stats::Statistics> {
        if self.mps.is_empty() {
            return None;
        }
        let total_rows: u64 = self.mps.iter().map(|mp| mp.row_count).sum();
        let total_bytes: u64 = self.mps.iter().map(|mp| mp.byte_size).sum();
        use datafusion::common::stats::Precision;
        Some(datafusion::common::stats::Statistics {
            num_rows: Precision::Exact(total_rows as usize),
            total_byte_size: Precision::Exact(total_bytes as usize),
            column_statistics: vec![],
        })
    }
}

/// Build Arrow schema from Nova table metadata.
fn build_schema(table_meta: &TableMeta) -> SchemaRef {
    use nova_common::NovaType;
    let fields: Vec<arrow::datatypes::Field> = table_meta
        .columns
        .iter()
        .map(|c| {
            let dt = match &c.data_type {
                NovaType::Boolean => arrow::datatypes::DataType::Boolean,
                NovaType::Int8 => arrow::datatypes::DataType::Int8,
                NovaType::Int16 => arrow::datatypes::DataType::Int16,
                NovaType::Int32 => arrow::datatypes::DataType::Int32,
                NovaType::Int64 => arrow::datatypes::DataType::Int64,
                NovaType::Float32 => arrow::datatypes::DataType::Float32,
                NovaType::Float64 => arrow::datatypes::DataType::Float64,
                NovaType::Utf8 => arrow::datatypes::DataType::Utf8,
                NovaType::Date32 => arrow::datatypes::DataType::Date32,
                NovaType::Timestamp => arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    None,
                ),
                NovaType::Binary => arrow::datatypes::DataType::Binary,
                NovaType::Decimal { precision, scale } => {
                    arrow::datatypes::DataType::Decimal128(*precision, *scale)
                }
                NovaType::List(inner) => arrow::datatypes::DataType::List(Arc::new(
                    arrow::datatypes::Field::new("item", nova_type_to_arrow(inner), true),
                )),
            };
            arrow::datatypes::Field::new(&c.name, dt, c.nullable)
        })
        .collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

fn nova_type_to_arrow(t: &nova_common::NovaType) -> arrow::datatypes::DataType {
    use nova_common::NovaType;
    match t {
        NovaType::Boolean => arrow::datatypes::DataType::Boolean,
        NovaType::Int8 => arrow::datatypes::DataType::Int8,
        NovaType::Int16 => arrow::datatypes::DataType::Int16,
        NovaType::Int32 => arrow::datatypes::DataType::Int32,
        NovaType::Int64 => arrow::datatypes::DataType::Int64,
        NovaType::Float32 => arrow::datatypes::DataType::Float32,
        NovaType::Float64 => arrow::datatypes::DataType::Float64,
        NovaType::Utf8 => arrow::datatypes::DataType::Utf8,
        NovaType::Date32 => arrow::datatypes::DataType::Date32,
        NovaType::Timestamp => {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        NovaType::Binary => arrow::datatypes::DataType::Binary,
        NovaType::Decimal { precision, scale } => {
            arrow::datatypes::DataType::Decimal128(*precision, *scale)
        }
        NovaType::List(inner) => arrow::datatypes::DataType::List(Arc::new(
            arrow::datatypes::Field::new("item", nova_type_to_arrow(inner), true),
        )),
    }
}
