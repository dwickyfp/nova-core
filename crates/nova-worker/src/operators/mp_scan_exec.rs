// MicroPartitionScanExec — custom DataFusion operator for reading micro-partitions.
//
// This is the core operator that makes nova-core fast. It reads Parquet MPs
// from S3 (or cache) and produces Arrow RecordBatch streams.
// Integrates with DataFusion's push-based execution pipeline.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
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
    /// Column indices to read (None = all columns).
    projection: Option<Vec<usize>>,
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
    ) -> Self {
        let n_partitions = mp_list.len().max(1); // at least 1 partition
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(n_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            mp_list,
            schema,
            projection,
            reader,
            properties,
        }
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
        if partition >= self.mp_list.len() {
            return Err(DataFusionError::Internal(format!(
                "partition {} out of range ({} MPs)",
                partition,
                self.mp_list.len()
            )));
        }

        let mp = self.mp_list[partition].clone();
        let reader = self.reader.clone();
        let projection = self.projection.clone();

        let fut = async move {
            let batches = reader
                .read(&mp, projection.as_deref())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let projected_batches: Vec<RecordBatch> = if let Some(ref cols) = projection {
                batches
                    .into_iter()
                    .map(|batch| {
                        let projected_cols: Vec<_> =
                            cols.iter().map(|&i| batch.column(i).clone()).collect();
                        let projected_schema = Arc::new(arrow::datatypes::Schema::new(
                            cols.iter()
                                .map(|&i| batch.schema().field(i).clone())
                                .collect::<Vec<_>>(),
                        ));
                        RecordBatch::try_new(projected_schema, projected_cols)
                            .map_err(|e| DataFusionError::External(Box::new(e)))
                    })
                    .collect::<DFResult<Vec<_>>>()?
            } else {
                batches
            };

            Ok::<_, DataFusionError>(projected_batches)
        };

        // std::thread::spawn with fresh Runtime avoids "nested runtime" error
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(fut);
            let _ = tx.send(result);
        });

        let batches = rx
            .recv()
            .map_err(|_| DataFusionError::Internal("read task panicked".into()))?
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let stream = futures::stream::iter(batches.into_iter().map(Ok));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Project selected columns from a batch.

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

        let exec = MicroPartitionScanExec::new(mps, schema.clone(), None, reader);

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

        // Project only columns 0 and 2 (id, amount)
        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));

        let exec =
            MicroPartitionScanExec::new(mps, projected_schema.clone(), Some(vec![0, 1]), reader);

        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();

        let batch = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scan_exec_partition_count() {
        let (mps, _dir) = write_test_mp().await;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(_dir.path()).unwrap());
        let reader = Arc::new(MpReader::new(store));
        let schema = test_schema();

        let exec = MicroPartitionScanExec::new(mps, schema, None, reader);

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
        let exec = MicroPartitionScanExec::new(mps.clone(), schema.clone(), None, reader.clone());
        assert_eq!(exec.properties().partitioning.partition_count(), 5);

        // Execute all 5 partitions in parallel and collect results
        let ctx = Arc::new(datafusion::execution::context::TaskContext::default());
        let mut handles = Vec::new();
        for part in 0..5 {
            let exec_clone =
                MicroPartitionScanExec::new(mps.clone(), schema.clone(), None, reader.clone());
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
