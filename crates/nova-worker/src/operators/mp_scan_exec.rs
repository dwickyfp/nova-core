// MicroPartitionScanExec — custom DataFusion operator for reading micro-partitions.
//
// This is the core operator that makes nova-core fast. It reads Parquet MPs
// from S3 (or cache) and produces Arrow RecordBatch streams.
// Integrates with DataFusion's push-based execution pipeline.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use nova_common::MicroPartitionMeta;
use nova_storage::MpReader;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Custom DataFusion execution plan for reading micro-partitions.
///
/// Each partition corresponds to one micro-partition file.
/// Supports column pruning (projection) via DataFusion's push-down mechanism.
pub struct MicroPartitionScanExec {
    /// Micro-partitions to read (one per partition).
    mp_list: Vec<MicroPartitionMeta>,
    /// Output schema (after projection).
    schema: SchemaRef,
    /// Requested logical column order (None = all columns).
    projection: Option<Vec<usize>>,
    /// Sorted unique physical columns to read from Parquet.
    read_projection: Option<Vec<usize>>,
    /// True when DataFusion requested a zero-column projection such as COUNT(*).
    empty_projection: bool,
    /// MP reader (handles S3 + Parquet deserialization).
    reader: Arc<MpReader>,
    /// Plan properties (cached).
    properties: PlanProperties,
}

impl MicroPartitionScanExec {
    /// Create a new MicroPartitionScanExec.
    pub fn new(
        mp_list: Vec<MicroPartitionMeta>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        reader: Arc<MpReader>,
    ) -> DFResult<Self> {
        let n_partitions = mp_list.len().max(1);
        let field_count = schema.fields().len();
        if let Some(indices) = &projection {
            for &index in indices {
                if index >= field_count {
                    return Err(DataFusionError::Plan(format!(
                        "projection index {index} out of bounds for schema with {field_count} fields"
                    )));
                }
            }
        }

        // Compute projected schema if projection is provided.
        let empty_projection = matches!(&projection, Some(indices) if indices.is_empty());
        let output_schema = match &projection {
            Some(indices) => {
                let projected_fields: Vec<_> = indices
                    .iter()
                    .map(|&index| schema.fields()[index].clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(projected_fields))
            }
            None => schema.clone(),
        };
        // For COUNT(*), DataFusion passes Some([]). Read all columns to preserve
        // row counts, then emit zero-column batches with the same row counts.
        let read_projection = match &projection {
            Some(indices) if !indices.is_empty() => {
                let mut physical_indices = indices.clone();
                physical_indices.sort_unstable();
                physical_indices.dedup();
                Some(physical_indices)
            }
            _ => None,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(n_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            mp_list,
            schema: output_schema,
            projection,
            read_projection,
            empty_projection,
            reader,
            properties,
        })
    }
}

impl fmt::Debug for MicroPartitionScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MicroPartitionScanExec")
            .field("mps", &self.mp_list.len())
            .finish()
    }
}

impl DisplayAs for MicroPartitionScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MicroPartitionScan: mps={}, cols={}",
            self.mp_list.len(),
            self.projection
                .as_ref()
                .map(|p| p.len())
                .unwrap_or(self.schema.fields().len())
        )
    }
}

impl ExecutionPlan for MicroPartitionScanExec {
    fn name(&self) -> &'static str {
        "MicroPartitionScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node, no children
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Leaf node — return self unchanged. Required by EnforceDistribution optimizer rule.
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::physical_plan::SendableRecordBatchStream> {
        if self.mp_list.is_empty() && partition == 0 {
            let stream = futures::stream::empty::<DFResult<RecordBatch>>();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )));
        }

        if partition >= self.mp_list.len() {
            return Err(DataFusionError::Internal(format!(
                "partition {} out of range ({} MPs)",
                partition,
                self.mp_list.len()
            )));
        }

        let mp = self.mp_list[partition].clone();
        let reader = self.reader.clone();
        let logical_projection = self.projection.clone();
        let read_projection = self.read_projection.clone();
        let output_schema = self.schema.clone();
        let empty_projection = self.empty_projection;
        let stream = async_stream::try_stream! {
            let batches = reader
                .read(&mp, read_projection.as_deref())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // MpReader uses Parquet ProjectionMask roots, which behave like a
            // physical column mask. Rebuild logical projection order here.
            for batch in batches {
                if empty_projection {
                    yield RecordBatch::try_new_with_options(
                        output_schema.clone(),
                        Vec::new(),
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                    )
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                } else if let Some(indices) = &logical_projection {
                    let read_indices = read_projection.as_deref().ok_or_else(|| {
                        DataFusionError::Internal("missing read projection".to_string())
                    })?;
                    let columns = indices
                        .iter()
                        .map(|index| {
                            let column_pos = read_indices.binary_search(index).map_err(|_| {
                                DataFusionError::Internal(format!(
                                    "projection index {index} missing from read projection"
                                ))
                            })?;
                            Ok(batch.column(column_pos).clone())
                        })
                        .collect::<DFResult<Vec<_>>>()?;
                    yield RecordBatch::try_new(output_schema.clone(), columns)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                } else {
                    yield batch;
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use nova_storage::{MpReader, MpWriter};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])),
            ],
        )
        .unwrap()
    }

    async fn write_test_mp() -> (Vec<MicroPartitionMeta>, TempDir) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let writer = MpWriter::new(store.clone(), "test".to_string());
        let batch = test_batch();
        let mp = writer.write(1, 1, 1, &[batch], 1).await.unwrap();
        (vec![mp], dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_basic() {
        let (mps, _dir) = write_test_mp().await;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(_dir.path()).unwrap());
        let reader = Arc::new(MpReader::new(store));
        let schema = test_schema();

        let exec = MicroPartitionScanExec::new(mps, schema.clone(), None, reader)
            .expect("valid scan exec");

        assert_eq!(exec.schema(), schema);
        assert_eq!(exec.properties().eq_properties.schema(), &schema);

        // Execute partition 0
        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();

        let mut total_rows = 0;
        while let Some(batch) = futures::StreamExt::next(&mut stream).await {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
            assert_eq!(batch.num_columns(), 3);
        }
        assert_eq!(total_rows, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_with_projection() {
        let (mps, _dir) = write_test_mp().await;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(_dir.path()).unwrap());
        let reader = Arc::new(MpReader::new(store));
        let schema = test_schema();

        let exec = MicroPartitionScanExec::new(mps, schema, Some(vec![0, 2]), reader)
            .expect("valid projection");

        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();

        let batch = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "amount");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_preserves_projection_order() {
        let (mps, _dir) = write_test_mp().await;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(_dir.path()).unwrap());
        let reader = Arc::new(MpReader::new(store));

        let exec = MicroPartitionScanExec::new(mps, test_schema(), Some(vec![2, 0]), reader)
            .expect("valid projection");

        assert_eq!(exec.schema().field(0).name(), "amount");
        assert_eq!(exec.schema().field(1).name(), "id");

        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();

        let batch = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "amount");
        assert_eq!(batch.schema().field(1).name(), "id");

        let amounts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amounts.value(0), 100.0);
        assert_eq!(ids.value(0), 1);
        assert_eq!(amounts.value(2), 300.0);
        assert_eq!(ids.value(2), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_preserves_duplicate_projection_columns() {
        let (mps, _dir) = write_test_mp().await;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(_dir.path()).unwrap());
        let reader = Arc::new(MpReader::new(store));

        let exec = MicroPartitionScanExec::new(mps, test_schema(), Some(vec![2, 2, 0]), reader)
            .expect("valid duplicate projection");

        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();
        let batch = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "amount");
        assert_eq!(batch.schema().field(1).name(), "amount");
        assert_eq!(batch.schema().field(2).name(), "id");

        let first_amount = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let second_amount = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(first_amount.value(1), 200.0);
        assert_eq!(second_amount.value(1), 200.0);
        assert_eq!(ids.value(1), 2);
    }

    #[test]
    fn test_scan_exec_rejects_projection_out_of_bounds() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
        let reader = Arc::new(MpReader::new(store));
        let err = MicroPartitionScanExec::new(Vec::new(), test_schema(), Some(vec![0, 3]), reader)
            .expect_err("projection index beyond schema should fail");

        assert!(
            err.to_string().contains("projection index 3 out of bounds"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_empty_mps_returns_empty_stream() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
        let reader = Arc::new(MpReader::new(store));
        let exec = MicroPartitionScanExec::new(Vec::new(), test_schema(), None, reader)
            .expect("empty MP list should create deterministic scan");

        assert_eq!(exec.properties().partitioning.partition_count(), 1);

        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut stream = exec.execute(0, ctx).expect("empty scan should stream");

        assert!(futures::StreamExt::next(&mut stream).await.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_empty_projection_preserves_rows_with_zero_columns() {
        let (mps, _dir) = write_test_mp().await;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(_dir.path()).unwrap());
        let reader = Arc::new(MpReader::new(store));
        let exec = MicroPartitionScanExec::new(mps, test_schema(), Some(vec![]), reader)
            .expect("COUNT(*) empty projection should be valid");

        assert_eq!(exec.schema().fields().len(), 0);

        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut stream = exec
            .execute(0, ctx)
            .expect("empty projection should stream");
        let batch = futures::StreamExt::next(&mut stream)
            .await
            .expect("scan should produce one batch")
            .expect("scan batch should be successful");

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 0);
        assert!(futures::StreamExt::next(&mut stream).await.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_partition_count() {
        let (mps, _dir) = write_test_mp().await;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(_dir.path()).unwrap());
        let reader = Arc::new(MpReader::new(store));
        let schema = test_schema();

        let exec = MicroPartitionScanExec::new(mps, schema, None, reader).expect("valid scan exec");

        // 1 MP = 1 partition
        let n_partitions = exec.properties().partitioning.partition_count();
        assert_eq!(n_partitions, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_parallel_multiple_mps() {
        // Write 5 separate MPs (each with 3 rows = 15 total)
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let writer = MpWriter::new(store.clone(), "test".to_string());
        let schema = test_schema();

        let mut mps = Vec::new();
        for i in 0..5u64 {
            let batch = test_batch(); // 3 rows each
            let mp = writer.write(1, i + 1, i + 1, &[batch], 1).await.unwrap();
            mps.push(mp);
        }

        let reader = Arc::new(MpReader::new(store));

        // Verify: 5 MPs = 5 partitions
        let exec = MicroPartitionScanExec::new(mps.clone(), schema.clone(), None, reader.clone())
            .expect("valid scan exec");
        assert_eq!(exec.properties().partitioning.partition_count(), 5);

        // Execute all 5 partitions in parallel and collect results
        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut handles = Vec::new();
        for part in 0..5 {
            let exec_clone =
                MicroPartitionScanExec::new(mps.clone(), schema.clone(), None, reader.clone())
                    .expect("valid scan exec");
            let ctx_clone = ctx.clone();
            handles.push(tokio::task::spawn(async move {
                let mut stream = exec_clone.execute(part, ctx_clone).unwrap();
                let mut rows = 0;
                while let Some(batch) = futures::StreamExt::next(&mut stream).await {
                    rows += batch.unwrap().num_rows();
                }
                rows
            }));
        }

        // All 5 partitions should return 3 rows each = 15 total
        let mut total = 0;
        for h in handles {
            total += h.await.unwrap();
        }
        assert_eq!(total, 15);
    }
}
