// ClickBench-style benchmark for nova-core micro-partition scan.
//
// Measures: MpWriter write → MpReader read → MicroPartitionScanExec execute
//
// Run: cargo bench -p nova-worker --bench clickbench

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use nova_storage::{MpReader, MpWriter};
use nova_worker::operators::mp_scan_exec::MicroPartitionScanExec;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tempfile::TempDir;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn bench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]))
}

fn make_batch(row_count: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..row_count as i64).collect();
    let names: Vec<String> = (0..row_count).map(|i| format!("user_{i}")).collect();
    let amounts: Vec<f64> = (0..row_count).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        bench_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(
                names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap()
}

fn bench_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Pre-setup: write 5 MPs with 1000 rows each
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let (mps, store, schema) = rt.block_on(async {
        let writer = MpWriter::new(store.clone(), "bench".to_string());
        let batch = make_batch(1000);
        let mut mps = Vec::with_capacity(5);
        for i in 0..5u64 {
            let mp = writer
                .write(1, i + 1, i + 1, &[batch.clone()], 1)
                .await
                .unwrap();
            mps.push(mp);
        }
        (mps, store, bench_schema())
    });

    c.bench_function("scan_5mps_5k_rows", |b| {
        b.iter(|| {
            let reader = Arc::new(MpReader::new(store.clone()));
            let exec = MicroPartitionScanExec::new(mps.clone(), schema.clone(), None, reader);
            let ctx = Arc::new(TaskContext::default());

            rt.block_on(async {
                let mut total_rows = 0usize;
                for part in 0..5 {
                    let mut stream = exec.execute(part, ctx.clone()).unwrap();
                    while let Some(batch) = stream.next().await {
                        total_rows += batch.unwrap().num_rows();
                    }
                }
                total_rows
            })
        })
    });
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
