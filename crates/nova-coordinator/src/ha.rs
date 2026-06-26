// Health Check & HA — Production readiness.
//
// Phase 6.6: High Availability components.
// - Health check endpoint (liveness + readiness)
// - Leader election (wraps Raft)
// - Graceful shutdown
// - Node status tracking

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;

/// Node status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Starting,
    Ready,
    Leader,
    Follower,
    Draining,
    Stopped,
}

/// Health check result.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub status: NodeStatus,
    pub uptime_secs: u64,
    pub active_queries: u64,
    pub active_connections: u64,
    pub storage_healthy: bool,
    pub raft_healthy: bool,
    pub message: String,
}

/// Health checker — tracks system health.
pub struct HealthChecker {
    status: Arc<RwLock<NodeStatus>>,
    storage_healthy: AtomicBool,
    raft_healthy: AtomicBool,
    active_queries: AtomicU64,
    active_connections: AtomicU64,
    start_time: Instant,
    node_id: String,
}

impl HealthChecker {
    pub fn new(node_id: &str) -> Self {
        Self {
            status: Arc::new(RwLock::new(NodeStatus::Starting)),
            storage_healthy: AtomicBool::new(false),
            raft_healthy: AtomicBool::new(false),
            active_queries: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            start_time: Instant::now(),
            node_id: node_id.to_string(),
        }
    }

    pub async fn set_status(&self, status: NodeStatus) {
        *self.status.write().await = status;
    }

    pub fn set_storage_healthy(&self, healthy: bool) {
        self.storage_healthy.store(healthy, Ordering::Relaxed);
    }

    pub fn set_raft_healthy(&self, healthy: bool) {
        self.raft_healthy.store(healthy, Ordering::Relaxed);
    }

    pub fn inc_queries(&self) {
        self.active_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_queries(&self) {
        self.active_queries.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Liveness probe — is the process alive?
    pub async fn liveness(&self) -> bool {
        let status = *self.status.read().await;
        !matches!(status, NodeStatus::Stopped)
    }

    /// Readiness probe — can we accept traffic?
    pub async fn readiness(&self) -> bool {
        let status = *self.status.read().await;
        matches!(
            status,
            NodeStatus::Ready | NodeStatus::Leader | NodeStatus::Follower
        ) && self.storage_healthy.load(Ordering::Relaxed)
    }

    /// Full health status.
    pub async fn health(&self) -> HealthStatus {
        let status = *self.status.read().await;
        let storage_ok = self.storage_healthy.load(Ordering::Relaxed);
        let raft_ok = self.raft_healthy.load(Ordering::Relaxed);

        let healthy = matches!(
            status,
            NodeStatus::Ready | NodeStatus::Leader | NodeStatus::Follower
        ) && storage_ok;

        let message = if !storage_ok {
            "storage unhealthy".to_string()
        } else if !raft_ok && matches!(status, NodeStatus::Leader | NodeStatus::Follower) {
            "raft unhealthy".to_string()
        } else if healthy {
            "ok".to_string()
        } else {
            format!("status: {:?}", status)
        };

        HealthStatus {
            healthy,
            status,
            uptime_secs: self.start_time.elapsed().as_secs(),
            active_queries: self.active_queries.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            storage_healthy: storage_ok,
            raft_healthy: raft_ok,
            message,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Graceful shutdown coordinator.
pub struct ShutdownCoordinator {
    shutdown_requested: AtomicBool,
    draining: AtomicBool,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            shutdown_requested: AtomicBool::new(false),
            draining: AtomicBool::new(false),
        }
    }

    pub fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Relaxed);
        self.draining.store(true, Ordering::Relaxed);
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Relaxed)
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    pub fn complete_drain(&self) {
        self.draining.store(false, Ordering::Relaxed);
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Cluster node info.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub node_id: String,
    pub address: String,
    pub status: NodeStatus,
    pub last_heartbeat: Instant,
}

/// Cluster membership tracker.
pub struct ClusterTracker {
    nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    leader_id: Arc<RwLock<Option<String>>>,
}

impl ClusterTracker {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            leader_id: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn register_node(&self, node_id: &str, address: &str) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(
            node_id.to_string(),
            ClusterNode {
                node_id: node_id.to_string(),
                address: address.to_string(),
                status: NodeStatus::Starting,
                last_heartbeat: Instant::now(),
            },
        );
    }

    pub async fn remove_node(&self, node_id: &str) {
        self.nodes.write().await.remove(node_id);
    }

    pub async fn update_heartbeat(&self, node_id: &str) {
        if let Some(node) = self.nodes.write().await.get_mut(node_id) {
            node.last_heartbeat = Instant::now();
        }
    }

    pub async fn set_leader(&self, node_id: Option<&str>) {
        *self.leader_id.write().await = node_id.map(|s| s.to_string());
    }

    pub async fn get_leader(&self) -> Option<String> {
        self.leader_id.read().await.clone()
    }

    pub async fn node_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    pub async fn list_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.read().await.values().cloned().collect()
    }

    /// Detect stale nodes (no heartbeat for >30s).
    pub async fn stale_nodes(&self, timeout_secs: u64) -> Vec<String> {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .filter(|n| n.last_heartbeat.elapsed().as_secs() > timeout_secs)
            .map(|n| n.node_id.clone())
            .collect()
    }
}

impl Default for ClusterTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_checker_starting() {
        let hc = HealthChecker::new("node-1");
        assert!(hc.liveness().await);
        assert!(!hc.readiness().await); // not ready yet
    }

    #[tokio::test]
    async fn test_health_checker_ready() {
        let hc = HealthChecker::new("node-1");
        hc.set_status(NodeStatus::Ready).await;
        hc.set_storage_healthy(true);

        assert!(hc.liveness().await);
        assert!(hc.readiness().await);
    }

    #[tokio::test]
    async fn test_health_checker_stopped() {
        let hc = HealthChecker::new("node-1");
        hc.set_status(NodeStatus::Stopped).await;

        assert!(!hc.liveness().await);
    }

    #[tokio::test]
    async fn test_health_status() {
        let hc = HealthChecker::new("node-1");
        hc.set_status(NodeStatus::Leader).await;
        hc.set_storage_healthy(true);
        hc.set_raft_healthy(true);
        hc.inc_queries();
        hc.inc_connections();

        let status = hc.health().await;
        assert!(status.healthy);
        assert_eq!(status.status, NodeStatus::Leader);
        assert_eq!(status.active_queries, 1);
        assert_eq!(status.active_connections, 1);
        assert_eq!(status.message, "ok");
    }

    #[tokio::test]
    async fn test_health_unhealthy_storage() {
        let hc = HealthChecker::new("node-1");
        hc.set_status(NodeStatus::Ready).await;
        hc.set_storage_healthy(false);

        let status = hc.health().await;
        assert!(!status.healthy);
        assert_eq!(status.message, "storage unhealthy");
    }

    #[test]
    fn test_shutdown_coordinator() {
        let sc = ShutdownCoordinator::new();
        assert!(!sc.is_shutdown_requested());
        assert!(!sc.is_draining());

        sc.request_shutdown();
        assert!(sc.is_shutdown_requested());
        assert!(sc.is_draining());

        sc.complete_drain();
        assert!(!sc.is_draining());
    }

    #[tokio::test]
    async fn test_cluster_tracker() {
        let ct = ClusterTracker::new();
        ct.register_node("node-1", "10.0.0.1:9000").await;
        ct.register_node("node-2", "10.0.0.2:9000").await;

        assert_eq!(ct.node_count().await, 2);

        ct.set_leader(Some("node-1")).await;
        assert_eq!(ct.get_leader().await.as_deref(), Some("node-1"));

        ct.remove_node("node-2").await;
        assert_eq!(ct.node_count().await, 1);
    }

    #[tokio::test]
    async fn test_cluster_heartbeat() {
        let ct = ClusterTracker::new();
        ct.register_node("node-1", "10.0.0.1:9000").await;

        // Just registered — not stale
        let stale = ct.stale_nodes(30).await;
        assert!(stale.is_empty());

        ct.update_heartbeat("node-1").await;
        let stale = ct.stale_nodes(30).await;
        assert!(stale.is_empty());
    }
}
