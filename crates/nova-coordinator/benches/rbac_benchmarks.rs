// RBAC benchmarks — user/role/privilege operations.
// Run: cargo bench -p nova-coordinator --bench rbac_benchmarks

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nova_coordinator::rbac::{Privilege, RbacManager};
use std::time::Duration;

fn bench_rbac_user_creation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("rbac_user_creation");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for n in [10, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("users", n), &n, |b, &count| {
            let rbac = RbacManager::new();
            let mut idx = 0;
            b.iter(|| {
                rt.block_on(rbac.create_user(&format!("user_{}", idx)))
                    .unwrap();
                idx += 1;
                if idx >= count {
                    idx = 0;
                }
            });
        });
    }
    group.finish();
}

fn bench_rbac_privilege_check(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("rbac_privilege_check");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    let rbac = RbacManager::new();
    let user_id = rt.block_on(rbac.create_user("test_user")).unwrap();
    let role_id = rt.block_on(rbac.create_role("test_role")).unwrap();
    rt.block_on(rbac.grant_role(user_id, role_id)).unwrap();

    let tables = ["orders", "customers", "products", "inventory", "logs"];
    for table in &tables {
        rt.block_on(rbac.grant_privilege(role_id, table, Privilege::Select))
            .unwrap();
    }

    group.bench_function("allowed", |b| {
        let mut idx = 0;
        b.iter(|| {
            let result = rt.block_on(rbac.check_privilege(
                user_id,
                tables[idx % tables.len()],
                Privilege::Select,
            ));
            black_box(result);
            idx += 1;
        });
    });

    group.bench_function("denied", |b| {
        b.iter(|| {
            let result =
                rt.block_on(rbac.check_privilege(user_id, "nonexistent_table", Privilege::Delete));
            black_box(result);
        });
    });

    group.finish();
}

fn bench_rbac_role_grant(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("rbac_role_grant");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for n in [5, 20, 50] {
        group.bench_with_input(BenchmarkId::new("roles", n), &n, |b, &count| {
            let rbac = RbacManager::new();
            let user_id = rt.block_on(rbac.create_user("bench_user")).unwrap();
            let role_ids: Vec<u64> = (0..count)
                .map(|i| {
                    rt.block_on(rbac.create_role(&format!("role_{}", i)))
                        .unwrap()
                })
                .collect();
            let mut idx = 0;
            b.iter(|| {
                rt.block_on(rbac.grant_role(user_id, role_ids[idx % count]))
                    .unwrap();
                idx += 1;
            });
        });
    }
    group.finish();
}

fn bench_rbac_multi_role_check(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("rbac_multi_role");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for n_roles in [1, 3, 5, 10] {
        let rbac = RbacManager::new();
        let user_id = rt.block_on(rbac.create_user("multi_user")).unwrap();
        let table = "orders";

        for i in 0..n_roles {
            let role_id = rt
                .block_on(rbac.create_role(&format!("role_{}", i)))
                .unwrap();
            rt.block_on(rbac.grant_role(user_id, role_id)).unwrap();
            let privs = match i % 4 {
                0 => Privilege::Select,
                1 => Privilege::Insert,
                2 => Privilege::Update,
                _ => Privilege::Delete,
            };
            rt.block_on(rbac.grant_privilege(role_id, table, privs))
                .unwrap();
        }

        group.bench_with_input(BenchmarkId::new("roles", n_roles), &n_roles, |b, _| {
            b.iter(|| {
                let result = rt.block_on(rbac.check_privilege(user_id, table, Privilege::Select));
                black_box(result);
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
    targets = bench_rbac_user_creation, bench_rbac_privilege_check,
              bench_rbac_role_grant, bench_rbac_multi_role_check
}
criterion_main!(benches);
