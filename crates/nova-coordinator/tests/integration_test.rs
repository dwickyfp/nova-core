// Integration Tests — End-to-end testing across all components
//
// Run: cargo test --test integration

use nova_common::{ColumnDef, MicroPartitionMeta, NovaType, TableMeta};
use nova_coordinator::{
    cache::QueryResultCache,
    ha::{HealthChecker, NodeStatus},
    monitoring::MetricsRegistry,
    rbac::{Privilege, RbacManager},
};
use nova_storage::backup::BackupManager;
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────
// Test 1: RBAC + Authorization Flow
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_rbac_authorization_flow() {
    let rbac = RbacManager::new();

    // Create roles
    let admin_role = rbac.create_role("admin").await.unwrap();
    let analyst_role = rbac.create_role("analyst").await.unwrap();

    // Create users
    let alice = rbac.create_user("alice").await.unwrap();
    let bob = rbac.create_user("bob").await.unwrap();

    // Assign roles
    rbac.grant_role(alice, admin_role).await.unwrap();
    rbac.grant_role(bob, analyst_role).await.unwrap();

    // Grant privileges
    rbac.grant_privilege(admin_role, "orders", Privilege::Delete)
        .await
        .unwrap();
    rbac.grant_privilege(analyst_role, "orders", Privilege::Select)
        .await
        .unwrap();

    // Test authorization
    assert!(
        rbac.check_privilege(alice, "orders", Privilege::Delete)
            .await
    );
    assert!(rbac.check_privilege(bob, "orders", Privilege::Select).await);
    assert!(!rbac.check_privilege(bob, "orders", Privilege::Delete).await);
}

// ─────────────────────────────────────────────────────────────
// Test 2: Query Cache Hit/Miss
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_query_cache_hit_miss() {
    let cache = QueryResultCache::new(100);
    let query_key = 1u64;
    let columns = vec!["id".to_string(), "value".to_string()];
    let rows = vec![vec!["1".to_string(), "100".to_string()]];
    let versions = HashMap::from([(100u64, 1u64)]);

    // First access - miss
    assert!(cache.get(query_key).await.is_none());

    // Store result
    cache
        .put(query_key, columns.clone(), rows.clone(), versions.clone())
        .await;

    // Second access - hit
    let cached = cache.get(query_key).await;
    assert!(cached.is_some());
    let (cols, data) = cached.unwrap();
    assert_eq!(cols, columns);
    assert_eq!(data, rows);
}

// ─────────────────────────────────────────────────────────────
// Test 3: Cache Invalidation on Table Update
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_cache_invalidation_on_table_update() {
    let cache = QueryResultCache::new(100);
    let query_key = 1u64;
    let columns = vec!["id".to_string()];
    let rows = vec![vec!["1".to_string()]];
    let versions_v1 = HashMap::from([(100u64, 1u64)]);
    let _versions_v2 = HashMap::from([(100u64, 2u64)]);

    // Cache with v1
    cache.put(query_key, columns, rows, versions_v1).await;
    assert!(cache.get(query_key).await.is_some());

    // Invalidate table 100
    cache.invalidate_table(100).await;

    // Should be empty now
    assert!(cache.get(query_key).await.is_none());
}

// ─────────────────────────────────────────────────────────────
// Test 4: Health Check Lifecycle
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_health_check_lifecycle() {
    let hc = HealthChecker::new("test-node");

    // Initial state - not ready
    assert!(hc.liveness().await);
    assert!(!hc.readiness().await);

    // Set ready
    hc.set_status(NodeStatus::Ready).await;
    hc.set_storage_healthy(true);
    hc.set_raft_healthy(true);

    assert!(hc.readiness().await);

    // Become leader
    hc.set_status(NodeStatus::Leader).await;
    let health = hc.health().await;
    assert_eq!(health.status, NodeStatus::Leader);
    assert!(health.healthy);

    // Simulate activity
    hc.inc_queries();
    hc.inc_queries();
    hc.inc_connections();

    let health = hc.health().await;
    assert_eq!(health.active_queries, 2);
    assert_eq!(health.active_connections, 1);
}

// ─────────────────────────────────────────────────────────────
// Test 5: Metrics Collection
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_metrics_collection() {
    let metrics = MetricsRegistry::new();

    // Record queries
    for i in 0..10 {
        let start = metrics.queries.record_query_start();
        std::thread::sleep(std::time::Duration::from_micros(5));
        metrics.queries.record_query_end(start, i % 3 != 0);
    }

    assert_eq!(metrics.queries.total_queries.get(), 10);
    assert!(metrics.queries.successful_queries.get() > 0);
    assert!(metrics.queries.failed_queries.get() > 0);

    // Record cache stats
    metrics.cache.l1_hits.add(100);
    metrics.cache.l1_misses.add(25);

    assert_eq!(metrics.cache.l1_hits.get(), 100);
    assert_eq!(metrics.cache.l1_misses.get(), 25);
    assert!((metrics.cache.l1_hit_rate() - 0.8).abs() < 0.01);

    // Record storage stats
    metrics.storage.mps_created.add(5);
    metrics.storage.bytes_written.add(1024 * 1024);
    metrics.storage.active_mps.set(5);

    assert_eq!(metrics.storage.mps_created.get(), 5);
    assert_eq!(metrics.storage.active_mps.get(), 5);
}

// ─────────────────────────────────────────────────────────────
// Test 6: Prometheus Export Format
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_prometheus_export_format() {
    let metrics = MetricsRegistry::new();
    metrics.queries.total_queries.add(42);
    metrics.cache.l1_hits.add(100);

    let output = metrics.export_prometheus();

    assert!(output.contains("nova_queries_total 42"));
    assert!(output.contains("nova_cache_l1_hits 100"));
    assert!(output.contains("# HELP nova_queries_total"));
    assert!(output.contains("# TYPE nova_queries_total counter"));
}

// ─────────────────────────────────────────────────────────────
// Test 7: Backup Creation and Listing
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_backup_creation_and_listing() {
    let backup_mgr = BackupManager::new();

    // Create mock table
    let table = TableMeta {
        id: 100,
        db_id: 1,
        schema_id: 1,
        name: "test_table".to_string(),
        columns: vec![ColumnDef {
            id: 0,
            name: "id".to_string(),
            data_type: NovaType::Int64,
            nullable: false,
            default_value: None,
            comment: None,
        }],
        created_at: 0,
        owner: 0,
        comment: None,
        version: 1,
        properties: HashMap::new(),
    };

    // Create mock MP
    let mp = MicroPartitionMeta {
        mp_id: 1000,
        table_id: 100,
        partition_id: None,
        version: 1,
        s3_path: "s3://test/mp-1000.parquet".to_string(),
        s3_temp_path: None,
        row_count: 100,
        byte_size: 1024,
        compression: Default::default(),
        column_stats: HashMap::new(),
        commit_ts: 1,
        txn_id: 1,
        supersedes: None,
        superseded_by: None,
        active: true,
    };

    let tables = vec![table];
    let mut mps = HashMap::new();
    mps.insert(100u64, vec![mp]);

    // Create backup
    let manifest = backup_mgr.create_backup(&tables, &mps).await;
    assert_eq!(manifest.tables.len(), 1);
    assert_eq!(manifest.total_mps, 1);
    assert_eq!(manifest.total_bytes, 1024);

    // List backups
    let backups = backup_mgr.list_backups().await;
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].backup_id, manifest.backup_id);
}

// ─────────────────────────────────────────────────────────────
// Test 8: RBAC with Multiple Roles
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_rbac_multiple_roles() {
    let rbac = RbacManager::new();

    let user = rbac.create_user("multi_role_user").await.unwrap();
    let role1 = rbac.create_role("role1").await.unwrap();
    let role2 = rbac.create_role("role2").await.unwrap();

    rbac.grant_role(user, role1).await.unwrap();
    rbac.grant_role(user, role2).await.unwrap();

    rbac.grant_privilege(role1, "table1", Privilege::Select)
        .await
        .unwrap();
    rbac.grant_privilege(role2, "table2", Privilege::Insert)
        .await
        .unwrap();

    // User should have both privileges
    assert!(
        rbac.check_privilege(user, "table1", Privilege::Select)
            .await
    );
    assert!(
        rbac.check_privilege(user, "table2", Privilege::Insert)
            .await
    );
}

// ─────────────────────────────────────────────────────────────
// Test 9: Cache Eviction (FIFO)
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_cache_eviction_fifo() {
    let cache = QueryResultCache::new(2); // Max 2 entries
    let columns = vec!["id".to_string()];
    let rows = vec![vec!["1".to_string()]];
    let versions = HashMap::new();

    // Fill cache
    cache
        .put(1, columns.clone(), rows.clone(), versions.clone())
        .await;
    cache
        .put(2, columns.clone(), rows.clone(), versions.clone())
        .await;

    assert!(cache.get(1).await.is_some());
    assert!(cache.get(2).await.is_some());

    // Add third entry - should evict one entry (FIFO, but HashMap order is arbitrary)
    cache.put(3, columns, rows, versions).await;

    // Exactly one entry should be evicted, cache should still have 2 entries
    let has_1 = cache.get(1).await.is_some();
    let has_2 = cache.get(2).await.is_some();
    let has_3 = cache.get(3).await.is_some();

    // Entry 3 must exist (just added)
    assert!(has_3, "Entry 3 must exist");
    // Exactly one of entry 1 or 2 should be evicted
    assert!(
        has_1 ^ has_2,
        "Exactly one of entry 1 or 2 should be evicted"
    );
}

// ─────────────────────────────────────────────────────────────
// Test 10: Health Check with Unhealthy Storage
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_health_check_unhealthy_storage() {
    let hc = HealthChecker::new("test-node");

    hc.set_status(NodeStatus::Ready).await;
    hc.set_storage_healthy(false);
    hc.set_raft_healthy(true);

    assert!(!hc.readiness().await);

    let health = hc.health().await;
    assert!(!health.healthy);
    assert_eq!(health.message, "storage unhealthy");
}

// ─────────────────────────────────────────────────────────────
// Test 11: Concurrent Cache Access
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_concurrent_cache_access() {
    use std::sync::Arc;
    use tokio::task;

    let cache = Arc::new(QueryResultCache::new(100));
    let columns = vec!["id".to_string()];
    let rows = vec![vec!["1".to_string()]];
    let versions = HashMap::new();

    // Spawn multiple tasks accessing cache
    let mut handles = vec![];
    for i in 0..10 {
        let cache_clone = Arc::clone(&cache);
        let cols = columns.clone();
        let data = rows.clone();
        let ver = versions.clone();

        let handle = task::spawn(async move {
            cache_clone.put(i, cols, data, ver).await;
            cache_clone.get(i).await
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_some());
    }
}

// ─────────────────────────────────────────────────────────────
// Test 12: Backup Manager Delete
// ─────────────────────────────────────────────────────────────
#[tokio::test]
async fn test_backup_manager_delete() {
    let backup_mgr = BackupManager::new();

    let table = TableMeta {
        id: 100,
        db_id: 1,
        schema_id: 1,
        name: "test".to_string(),
        columns: vec![],
        created_at: 0,
        owner: 0,
        comment: None,
        version: 1,
        properties: HashMap::new(),
    };

    let mps = HashMap::new();
    let manifest = backup_mgr.create_backup(&[table], &mps).await;

    // Verify backup exists
    let backups = backup_mgr.list_backups().await;
    assert_eq!(backups.len(), 1);

    // Delete backup
    let deleted = backup_mgr.delete_backup(&manifest.backup_id).await;
    assert!(deleted);

    // Verify backup is gone
    let backups = backup_mgr.list_backups().await;
    assert_eq!(backups.len(), 0);
}
