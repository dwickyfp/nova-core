// Query benchmarks — SQL parsing + CBO join optimization.
// Run: cargo bench -p nova-coordinator --bench query_benchmarks

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nova_coordinator::cbo::{CboOptimizer, JoinEdge, TableRef};
use nova_coordinator::parser::SqlParser;
use std::time::Duration;

fn make_table(id: u64, name: &str, rows: u64) -> TableRef {
    TableRef {
        table_id: id,
        name: name.to_string(),
        row_count: rows,
        byte_size: rows * 100,
        columns: vec!["id".to_string(), "val".to_string()],
    }
}

fn bench_sql_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_parsing");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));

    let queries = vec![
        ("simple_select", "SELECT * FROM orders WHERE id = 1"),
        (
            "multi_col",
            "SELECT id, value FROM orders WHERE id > 100 AND value < 1000",
        ),
        (
            "in_clause",
            "SELECT * FROM orders WHERE id IN (1, 2, 3, 4, 5)",
        ),
        ("like", "SELECT * FROM orders WHERE name LIKE '%test%'"),
        (
            "join",
            "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.region = 'APAC'",
        ),
        (
            "complex",
            "SELECT COUNT(*), AVG(total) FROM orders WHERE created_at > '2024-01-01' GROUP BY customer_id HAVING COUNT(*) > 10 ORDER BY AVG(total) DESC LIMIT 100",
        ),
    ];

    let parser = SqlParser::new();
    for (name, sql) in queries {
        group.bench_with_input(BenchmarkId::new("parse", name), sql, |b, q| {
            b.iter(|| {
                black_box(parser.parse(q).ok());
            });
        });
    }
    group.finish();
}

fn bench_cbo_join_reorder(c: &mut Criterion) {
    let mut group = c.benchmark_group("cbo_join_reorder");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    // 2-table join
    {
        let tables = vec![
            make_table(1, "orders", 1_000_000),
            make_table(2, "customers", 100_000),
        ];
        let edges = vec![JoinEdge {
            left_table: "orders".to_string(),
            right_table: "customers".to_string(),
            left_col: "customer_id".to_string(),
            right_col: "id".to_string(),
        }];
        let opt = CboOptimizer::new();
        group.bench_function("2_table", |b| {
            b.iter(|| black_box(opt.optimize(&tables, &edges)));
        });
    }

    // 5-table star
    {
        let tables: Vec<TableRef> = vec![
            make_table(1, "fact", 1_000_000),
            make_table(2, "dim1", 500_000),
            make_table(3, "dim2", 200_000),
            make_table(4, "dim3", 100_000),
            make_table(5, "dim4", 50_000),
        ];
        let edges: Vec<JoinEdge> = (1..5)
            .map(|i| JoinEdge {
                left_table: "fact".to_string(),
                right_table: format!("dim{}", i),
                left_col: format!("dim{}_id", i),
                right_col: "id".to_string(),
            })
            .collect();
        let opt = CboOptimizer::new();
        group.bench_function("5_table_star", |b| {
            b.iter(|| black_box(opt.optimize(&tables, &edges)));
        });
    }

    // 8-table
    {
        let tables: Vec<TableRef> = (0..8)
            .map(|i| {
                make_table(
                    i as u64,
                    &format!("t{}", i),
                    [
                        1_000_000, 500_000, 200_000, 100_000, 50_000, 25_000, 10_000, 5_000,
                    ][i],
                )
            })
            .collect();
        let edges: Vec<JoinEdge> = (1..8)
            .map(|i| JoinEdge {
                left_table: format!("t{}", if i % 2 == 0 { 0 } else { i - 1 }),
                right_table: format!("t{}", i),
                left_col: "id".to_string(),
                right_col: "id".to_string(),
            })
            .collect();
        let opt = CboOptimizer::new();
        group.bench_function("8_table", |b| {
            b.iter(|| black_box(opt.optimize(&tables, &edges)));
        });
    }

    group.finish();
}

fn bench_cbo_with_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("cbo_with_filter");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for n_tables in [2, 4, 8] {
        let tables: Vec<TableRef> = (0..n_tables)
            .map(|i| make_table(i as u64, &format!("t{}", i), 100_000 * (i as u64 + 1)))
            .collect();
        let edges: Vec<JoinEdge> = (1..n_tables)
            .map(|i| JoinEdge {
                left_table: "t0".to_string(),
                right_table: format!("t{}", i),
                left_col: "id".to_string(),
                right_col: "id".to_string(),
            })
            .collect();
        let mut opt = CboOptimizer::new();
        for t in &tables {
            opt.set_filter(&t.name, 0.1);
        }
        group.bench_with_input(BenchmarkId::new("filtered", n_tables), &n_tables, |b, _| {
            b.iter(|| black_box(opt.optimize(&tables, &edges)));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(5));
    targets = bench_sql_parsing, bench_cbo_join_reorder, bench_cbo_with_filter
}
criterion_main!(benches);
