// Comprehensive Demo — Showcases all Nova features using real APIs
//
// Run: cargo run --example comprehensive_demo

use nova_common::{ColumnDef, MicroPartitionMeta, NovaType, TableMeta};
use nova_coordinator::cache::QueryResultCache;
use nova_coordinator::ha::{HealthChecker, NodeStatus};
use nova_coordinator::monitoring::MetricsRegistry;
use nova_coordinator::rbac::{Privilege, RbacManager};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║         Nova-Core Comprehensive Feature Demo               ║");
    println!("║         Snowflake-inspired Analytical Engine               ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // ─────────────────────────────────────────────────────────────
    // 1. RBAC (Role-Based Access Control)
    // ─────────────────────────────────────────────────────────────
    println!("🔐 1. RBAC (Role-Based Access Control)");
    println!("   Creating users and roles...\n");

    let rbac = RbacManager::new();

    // Create roles
    let admin_role_id = rbac.create_role("admin").await.unwrap();
    let analyst_role_id = rbac.create_role("analyst").await.unwrap();
    let engineer_role_id = rbac.create_role("data_engineer").await.unwrap();

    println!("   ✓ Created role: admin (id={})", admin_role_id);
    println!("   ✓ Created role: analyst (id={})", analyst_role_id);
    println!(
        "   ✓ Created role: data_engineer (id={})\n",
        engineer_role_id
    );

    // Create users
    let alice_id = rbac.create_user("alice").await.unwrap();
    let bob_id = rbac.create_user("bob").await.unwrap();
    let charlie_id = rbac.create_user("charlie").await.unwrap();

    println!("   ✓ Created user: alice (id={})", alice_id);
    println!("   ✓ Created user: bob (id={})", bob_id);
    println!("   ✓ Created user: charlie (id={})\n", charlie_id);

    // Assign roles
    rbac.grant_role(alice_id, admin_role_id).await.unwrap();
    rbac.grant_role(bob_id, analyst_role_id).await.unwrap();
    rbac.grant_role(charlie_id, engineer_role_id).await.unwrap();

    println!("   ✓ alice → admin");
    println!("   ✓ bob → analyst");
    println!("   ✓ charlie → data_engineer\n");

    // Grant privileges
    rbac.grant_privilege(admin_role_id, "orders", Privilege::Select)
        .await
        .unwrap();
    rbac.grant_privilege(admin_role_id, "orders", Privilege::Insert)
        .await
        .unwrap();
    rbac.grant_privilege(admin_role_id, "orders", Privilege::Update)
        .await
        .unwrap();
    rbac.grant_privilege(admin_role_id, "orders", Privilege::Delete)
        .await
        .unwrap();

    rbac.grant_privilege(analyst_role_id, "orders", Privilege::Select)
        .await
        .unwrap();

    rbac.grant_privilege(engineer_role_id, "orders", Privilege::Select)
        .await
        .unwrap();
    rbac.grant_privilege(engineer_role_id, "orders", Privilege::Insert)
        .await
        .unwrap();
    rbac.grant_privilege(engineer_role_id, "orders", Privilege::Update)
        .await
        .unwrap();

    // Test authorization
    println!("   Authorization checks:");
    let can_alice_delete = rbac
        .check_privilege(alice_id, "orders", Privilege::Delete)
        .await;
    let can_bob_insert = rbac
        .check_privilege(bob_id, "orders", Privilege::Insert)
        .await;
    let can_charlie_update = rbac
        .check_privilege(charlie_id, "orders", Privilege::Update)
        .await;

    println!(
        "   alice (admin) DELETE table: {}",
        if can_alice_delete {
            "✓ ALLOWED"
        } else {
            "✗ DENIED"
        }
    );
    println!(
        "   bob (analyst) INSERT table: {}",
        if can_bob_insert {
            "✓ ALLOWED"
        } else {
            "✗ DENIED"
        }
    );
    println!(
        "   charlie (data_engineer) UPDATE table: {}\n",
        if can_charlie_update {
            "✓ ALLOWED"
        } else {
            "✗ DENIED"
        }
    );

    // ─────────────────────────────────────────────────────────────
    // 2. Query Result Cache
    // ─────────────────────────────────────────────────────────────
    println!("💾 2. Query Result Cache");
    println!("   Testing query result caching...\n");

    let cache = QueryResultCache::new(1000);

    // Simulate query results
    let query_key = 1001u64;
    let columns = vec!["id".to_string(), "name".to_string(), "amount".to_string()];
    let rows = vec![
        vec!["1".to_string(), "Alice".to_string(), "1500".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "2300".to_string()],
    ];
    let table_versions = HashMap::from([(100u64, 1u64)]);

    // First query - cache miss
    let cached = cache.get(query_key).await;
    println!("   Query 1 (first time): cache hit = {}", cached.is_some());

    // Store result
    cache
        .put(
            query_key,
            columns.clone(),
            rows.clone(),
            table_versions.clone(),
        )
        .await;
    println!("   ✓ Cached result for query 1");

    // Second query - cache hit
    let cached = cache.get(query_key).await;
    println!("   Query 1 (second time): cache hit = {}", cached.is_some());

    // Different query - cache miss
    let cached = cache.get(1002u64).await;
    println!(
        "   Query 2 (different key): cache hit = {}\n",
        cached.is_some()
    );

    // ─────────────────────────────────────────────────────────────
    // 3. Monitoring Metrics (Prometheus)
    // ─────────────────────────────────────────────────────────────
    println!("📊 3. Monitoring Metrics (Prometheus)");
    println!("   Collecting system metrics...\n");

    let metrics = MetricsRegistry::new();

    // Simulate queries
    for i in 0..100 {
        let start = metrics.queries.record_query_start();
        std::thread::sleep(std::time::Duration::from_micros(10));
        let success = i % 10 != 0; // 10% failure rate
        metrics.queries.record_query_end(start, success);
    }

    // Record cache stats
    metrics.cache.l1_hits.add(850);
    metrics.cache.l1_misses.add(150);
    metrics.cache.bytes_cached.set(512 * 1024 * 1024); // 512MB

    // Record storage stats
    metrics.storage.mps_created.add(50);
    metrics.storage.bytes_written.add(1024 * 1024 * 500); // 500MB
    metrics.storage.active_mps.set(50);

    println!("   Query Metrics:");
    println!(
        "   - Total queries: {}",
        metrics.queries.total_queries.get()
    );
    println!(
        "   - Successful: {}",
        metrics.queries.successful_queries.get()
    );
    println!("   - Failed: {}", metrics.queries.failed_queries.get());
    println!(
        "   - Avg latency: {:.2}ms\n",
        metrics.queries.query_latency_ms.sum_ms() as f64
            / metrics.queries.query_latency_ms.count() as f64
    );

    println!("   Cache Metrics:");
    println!("   - L1 hits: {}", metrics.cache.l1_hits.get());
    println!("   - L1 misses: {}", metrics.cache.l1_misses.get());
    println!("   - Hit rate: {:.1}%", metrics.cache.l1_hit_rate() * 100.0);
    println!(
        "   - Bytes cached: {} MB\n",
        metrics.cache.bytes_cached.get() / 1024 / 1024
    );

    println!("   Storage Metrics:");
    println!("   - MPs created: {}", metrics.storage.mps_created.get());
    println!(
        "   - Bytes written: {} MB",
        metrics.storage.bytes_written.get() / 1024 / 1024
    );
    println!("   - Active MPs: {}\n", metrics.storage.active_mps.get());

    // Prometheus export
    let prom = metrics.export_prometheus();
    println!("   Prometheus output (first 8 lines):");
    for line in prom.lines().take(8) {
        println!("   {}", line);
    }
    println!("   ... ({} lines total)\n", prom.lines().count());

    // ─────────────────────────────────────────────────────────────
    // 4. Health Check & HA
    // ─────────────────────────────────────────────────────────────
    println!("🏥 4. Health Check & HA");
    println!("   Testing health probes...\n");

    let hc = HealthChecker::new("node-1");

    // Starting state
    let health = hc.health().await;
    println!("   Initial state:");
    println!("   - Liveness: {}", hc.liveness().await);
    println!("   - Readiness: {}", hc.readiness().await);
    println!("   - Status: {:?}\n", health.status);

    // Ready state
    hc.set_status(NodeStatus::Ready).await;
    hc.set_storage_healthy(true);
    hc.set_raft_healthy(true);

    let health = hc.health().await;
    println!("   After startup:");
    println!("   - Liveness: {}", hc.liveness().await);
    println!("   - Readiness: {}", hc.readiness().await);
    println!(
        "   - Status: {:?}, Message: {}\n",
        health.status, health.message
    );

    // Become leader
    hc.set_status(NodeStatus::Leader).await;
    hc.inc_queries();
    hc.inc_queries();
    hc.inc_connections();

    let health = hc.health().await;
    println!("   As leader:");
    println!("   - Status: {:?}", health.status);
    println!("   - Active queries: {}", health.active_queries);
    println!("   - Active connections: {}", health.active_connections);
    println!("   - Uptime: {}s\n", health.uptime_secs);

    // ─────────────────────────────────────────────────────────────
    // 5. Backup & Restore
    // ─────────────────────────────────────────────────────────────
    println!("💿 5. Backup & Restore");
    println!("   Testing backup creation...\n");

    let backup_mgr = nova_storage::backup::BackupManager::new();

    // Create mock tables
    let users_table = TableMeta {
        id: 100,
        db_id: 1,
        schema_id: 1,
        name: "users".to_string(),
        columns: vec![
            ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: NovaType::Int64,
                nullable: false,
                default_value: None,
                comment: None,
            },
            ColumnDef {
                id: 1,
                name: "name".to_string(),
                data_type: NovaType::Utf8,
                nullable: false,
                default_value: None,
                comment: None,
            },
        ],
        created_at: 0,
        owner: 0,
        comment: None,
        version: 1,
        properties: HashMap::new(),
    };

    let orders_table = TableMeta {
        id: 101,
        db_id: 1,
        schema_id: 1,
        name: "orders".to_string(),
        columns: vec![
            ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: NovaType::Int64,
                nullable: false,
                default_value: None,
                comment: None,
            },
            ColumnDef {
                id: 1,
                name: "amount".to_string(),
                data_type: NovaType::Int64,
                nullable: false,
                default_value: None,
                comment: None,
            },
        ],
        created_at: 0,
        owner: 0,
        comment: None,
        version: 1,
        properties: HashMap::new(),
    };

    // Create mock micro-partitions
    let mut mps = HashMap::new();
    mps.insert(
        100u64,
        vec![MicroPartitionMeta {
            mp_id: 1000,
            table_id: 100,
            partition_id: None,
            version: 1,
            s3_path: "s3://nova/tables/100/mp-1000-v1.parquet".to_string(),
            s3_temp_path: None,
            row_count: 10000,
            byte_size: 1024 * 1024,
            compression: Default::default(),
            column_stats: HashMap::new(),
            commit_ts: 1,
            txn_id: 1,
            supersedes: None,
            superseded_by: None,
            active: true,
        }],
    );
    mps.insert(
        101u64,
        vec![MicroPartitionMeta {
            mp_id: 1001,
            table_id: 101,
            partition_id: None,
            version: 1,
            s3_path: "s3://nova/tables/101/mp-1001-v1.parquet".to_string(),
            s3_temp_path: None,
            row_count: 50000,
            byte_size: 5 * 1024 * 1024,
            compression: Default::default(),
            column_stats: HashMap::new(),
            commit_ts: 1,
            txn_id: 1,
            supersedes: None,
            superseded_by: None,
            active: true,
        }],
    );

    // Create backup
    let tables = vec![users_table, orders_table];
    let manifest = backup_mgr.create_backup(&tables, &mps).await;

    println!("   ✓ Created full backup: {}", manifest.backup_id);
    println!("   - Tables: {}", manifest.tables.len());
    println!("   - Total MPs: {}", manifest.total_mps);
    println!(
        "   - Total bytes: {} MB\n",
        manifest.total_bytes / 1024 / 1024
    );

    let backups = backup_mgr.list_backups().await;
    println!("   Available backups: {}", backups.len());
    for b in &backups {
        println!(
            "   - {} ({} tables, {} MPs, {} MB)",
            b.backup_id,
            b.tables.len(),
            b.total_mps,
            b.total_bytes / 1024 / 1024
        );
    }
    println!();

    // ─────────────────────────────────────────────────────────────
    // Summary
    // ─────────────────────────────────────────────────────────────
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    Demo Complete! 🎉                       ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Features Demonstrated:                                    ║");
    println!("║  ✓ RBAC (users, roles, privileges, authorization)          ║");
    println!("║  ✓ Query Result Cache (hit/miss, version tracking)         ║");
    println!("║  ✓ Prometheus Metrics (queries, cache, storage)            ║");
    println!("║  ✓ Health Check & HA (liveness, readiness, leader)         ║");
    println!("║  ✓ Backup & Restore (full backup, listing)                 ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Full Architecture (203 tests, 35 commits):                ║");
    println!("║  Phase 1: Foundation (Sled/FDB, S3, MVCC, SQL Parser)     ║");
    println!("║  Phase 2: Query Engine (DataFusion, Zone-Map Pruning)      ║");
    println!("║  Phase 3: Snowflake (Time Travel, Clone, Streams/CDC)      ║");
    println!("║  Phase 4: Distributed (Raft, Worker Pool, Auto-Scaling)    ║");
    println!("║  Phase 5: CBO (Join Reorder, Runtime Filter, Stats)        ║");
    println!("║  Phase 6: Polish (Cache, RBAC, Monitoring, Backup, HA)     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}
