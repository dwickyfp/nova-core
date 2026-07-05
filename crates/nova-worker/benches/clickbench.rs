// ClickBench-style benchmark for nova-core micro-partition scan.
//
// Measures: MpWriter write → MpReader read → MicroPartitionScanExec execute
//           + DataFusion SQL path (SELECT, AGG, filter)
//
// Run: cargo bench -p nova-worker --bench clickbench

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use nova_storage::{MpReader, MpWriter};
use nova_worker::NovaTableProvider;
use nova_worker::operators::mp_scan_exec::MicroPartitionScanExec;
use object_store::local::LocalFileSystem;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
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

/// Write N MPs of `rows_per_mp` rows each. Returns (mps, store, schema).
fn write_mps(
    rt: &tokio::runtime::Runtime,
    n_mps: u64,
    rows_per_mp: usize,
) -> (
    Vec<nova_common::MicroPartitionMeta>,
    Arc<dyn object_store::ObjectStore>,
    Arc<Schema>,
    TempDir,
) {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema = bench_schema();
    let mps = rt.block_on(async {
        let writer = MpWriter::new(store.clone(), "bench".to_string());
        let batch = make_batch(rows_per_mp);
        let mut mps = Vec::with_capacity(n_mps as usize);
        for i in 0..n_mps {
            let mp = writer
                .write(1, i + 1, i + 1, std::slice::from_ref(&batch), 1)
                .await
                .unwrap();
            mps.push(mp);
        }
        mps
    });
    (mps, store, schema, dir)
}

// --- Low-level scan benchmark (MicroPartitionScanExec) ---

fn bench_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("mp_scan");
    group.measurement_time(Duration::from_secs(10));

    for (label, n_mps, rows_per_mp) in [
        ("5mp_1k", 5u64, 1_000usize),
        ("5mp_10k", 5, 10_000),
        ("10mp_10k", 10, 10_000),
        ("5mp_100k", 5, 100_000),
    ] {
        let (mps, store, schema, _dir) = write_mps(&rt, n_mps, rows_per_mp);
        let total_rows = n_mps as usize * rows_per_mp;
        group.bench_function(BenchmarkId::new("scan", label), |b| {
            b.iter(|| {
                let reader = Arc::new(MpReader::new(store.clone()));
                let exec = MicroPartitionScanExec::new(mps.clone(), schema.clone(), None, reader);
                let ctx = Arc::new(TaskContext::default());
                rt.block_on(async {
                    let mut rows = 0usize;
                    for part in 0..n_mps as usize {
                        let mut stream = exec.execute(part, ctx.clone()).unwrap();
                        while let Some(batch) = stream.next().await {
                            rows += batch.unwrap().num_rows();
                        }
                    }
                    black_box(rows == total_rows)
                })
            })
        });
    }
    group.finish();
}

// --- DataFusion SQL path benchmark ---

fn bench_datafusion_sql(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("datafusion_sql");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    // Build a TableMeta for bench table
    let table_meta = {
        use nova_common::{ColumnDef, NovaType, TableMeta};
        use std::collections::HashMap;
        TableMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "bench".to_string(),
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".to_string(),
                    data_type: NovaType::Int64,
                    nullable: false,
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    id: 2,
                    name: "name".to_string(),
                    data_type: NovaType::Utf8,
                    nullable: false,
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    id: 3,
                    name: "amount".to_string(),
                    data_type: NovaType::Float64,
                    nullable: false,
                    default_value: None,
                    comment: None,
                },
            ],
            created_at: 0,
            owner: 0,
            comment: None,
            version: 0,
            properties: HashMap::new(),
        }
    };

    for (label, n_mps, rows_per_mp, sql) in [
        ("select_star_5k", 5u64, 1_000usize, "SELECT * FROM bench"),
        ("select_star_50k", 5, 10_000, "SELECT * FROM bench"),
        ("count_star_50k", 5, 10_000, "SELECT COUNT(*) FROM bench"),
        ("sum_agg_50k", 5, 10_000, "SELECT SUM(amount) FROM bench"),
        (
            "filter_50k",
            5,
            10_000,
            "SELECT * FROM bench WHERE id > 25000",
        ),
        (
            "group_by_50k",
            5,
            10_000,
            "SELECT COUNT(*), SUM(amount) FROM bench GROUP BY name",
        ),
    ] {
        let (mps, store, _schema, _dir) = write_mps(&rt, n_mps, rows_per_mp);
        let tm = table_meta.clone();
        let mps_c = mps.clone();
        let store_c = store.clone();
        group.bench_function(BenchmarkId::new("sql", label), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut config =
                        datafusion::prelude::SessionConfig::new().with_target_partitions(1);
                    config.options_mut().optimizer.skip_failed_rules = true;
                    let ctx = datafusion::prelude::SessionContext::new_with_config(config);
                    let reader = Arc::new(MpReader::new(store_c.clone()));
                    let provider = NovaTableProvider::new(tm.clone(), mps_c.clone(), reader);
                    ctx.register_table("bench", Arc::new(provider)).unwrap();
                    let df = ctx.sql(sql).await.unwrap();
                    let batches = df.collect().await.unwrap_or_default();
                    black_box(batches.len())
                })
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    targets = bench_scan, bench_datafusion_sql
}
criterion_main!(benches);
