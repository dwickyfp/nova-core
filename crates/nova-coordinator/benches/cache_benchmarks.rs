// Cache benchmarks — QueryResultCache throughput & latency.
// Run: cargo bench -p nova-coordinator --bench cache_benchmarks

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nova_coordinator::cache::QueryResultCache;
use std::collections::HashMap;
use std::time::Duration;

fn bench_cache_put(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cache_put");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for size in [10, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("entries", size), &size, |b, &s| {
            let cache = QueryResultCache::new(100000);
            let cols = vec!["id".to_string(), "val".to_string()];
            let rows: Vec<Vec<String>> = (0..s)
                .map(|i| vec![i.to_string(), (i * 2).to_string()])
                .collect();
            let versions = HashMap::new();
            let mut idx = 0u64;
            b.iter(|| {
                rt.block_on(cache.put(idx, cols.clone(), rows.clone(), versions.clone()));
                idx += 1;
            });
        });
    }
    group.finish();
}

fn bench_cache_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cache_get");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    let cache = QueryResultCache::new(100000);
    let cols = vec!["id".to_string()];
    let rows = vec![vec!["1".to_string()]];
    let versions = HashMap::new();
    for i in 0..1000u64 {
        rt.block_on(cache.put(i, cols.clone(), rows.clone(), versions.clone()));
    }

    group.bench_function("hit", |b| {
        let mut idx = 0u64;
        b.iter(|| {
            let r = rt.block_on(cache.get(idx % 1000));
            black_box(r);
            idx += 1;
        });
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            let r = rt.block_on(cache.get(99999));
            black_box(r);
        });
    });

    group.finish();
}

fn bench_cache_invalidation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cache_invalidation");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    let cache = QueryResultCache::new(100000);
    let cols = vec!["id".to_string()];
    let rows = vec![vec!["1".to_string()]];
    for i in 0..100u64 {
        let versions = HashMap::from([(i, 1u64)]);
        rt.block_on(cache.put(i, cols.clone(), rows.clone(), versions));
    }

    group.bench_function("invalidate_table", |b| {
        b.iter(|| {
            rt.block_on(cache.invalidate_table(50));
        });
    });

    group.finish();
}

fn bench_cache_key_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_key_gen");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));

    let queries = vec![
        "SELECT * FROM orders WHERE id = 1",
        "SELECT id, name, total FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.region = 'APAC' GROUP BY c.country",
        "SELECT COUNT(*), AVG(total) FROM orders WHERE created_at > '2024-01-01' AND status = 'completed' HAVING COUNT(*) > 10 ORDER BY AVG(total) DESC LIMIT 100",
    ];

    for (i, q) in queries.iter().enumerate() {
        let versions = HashMap::from([(1u64, 1u64), (2u64, 3u64), (3u64, 5u64)]);
        group.bench_with_input(BenchmarkId::new("sql", i), q, |b, sql| {
            b.iter(|| {
                black_box(QueryResultCache::key(sql, &versions));
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(5));
    targets = bench_cache_put, bench_cache_get, bench_cache_invalidation, bench_cache_key_generation
}
criterion_main!(benches);
