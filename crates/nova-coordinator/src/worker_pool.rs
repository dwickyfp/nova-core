// Worker Pool — coordinator-side management of worker nodes.
//
// Architecture:
// - Workers register with coordinator on startup (gRPC Register RPC)
// - Workers send heartbeats every 5s (gRPC Heartbeat RPC)
// - Coordinator tracks worker health, assigns work, collects results
// - If worker misses 3 heartbeats → marked dead → work re-scheduled
//
// Phase 4.2: In-memory worker registry (no gRPC yet — uses direct function calls)
// Phase 10: Full gRPC with tonic wired via WorkerClientPool

use nova_common::{NovaError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Worker status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Worker is registered and sending heartbeats.
    Active,
    /// Worker missed heartbeats, may be dead.
    Suspect,
    /// Worker is confirmed dead.
    Dead,
}

/// Worker metadata tracked by the coordinator.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: u64,
    pub address: String,
    pub status: WorkerStatus,
    pub last_heartbeat: Instant,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub active_queries: u32,
}

/// Worker pool — manages all registered workers.
pub struct WorkerPool {
    workers: Arc<RwLock<HashMap<u64, WorkerInfo>>>,
    heartbeat_timeout: Duration,
    max_missed_heartbeats: u32,
}

impl WorkerPool {
    pub fn new() -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout: Duration::from_secs(5),
            max_missed_heartbeats: 3,
        }
    }

    /// Register a new worker.
    pub async fn register(&self, address: String) -> Result<u64> {
        let mut workers = self.workers.write().await;

        // Assign next worker ID
        let worker_id = workers.keys().max().copied().unwrap_or(0) + 1;

        let worker = WorkerInfo {
            worker_id,
            address,
            status: WorkerStatus::Active,
            last_heartbeat: Instant::now(),
            cpu_usage: 0.0,
            memory_usage: 0.0,
            active_queries: 0,
        };

        workers.insert(worker_id, worker);
        Ok(worker_id)
    }

    /// Process a heartbeat from a worker.
    pub async fn heartbeat(
        &self,
        worker_id: u64,
        cpu_usage: f64,
        memory_usage: f64,
        active_queries: u32,
    ) -> Result<()> {
        let mut workers = self.workers.write().await;
        let worker = workers.get_mut(&worker_id).ok_or(NovaError::Internal {
            message: format!("worker {} not registered", worker_id),
        })?;

        worker.last_heartbeat = Instant::now();
        worker.status = WorkerStatus::Active;
        worker.cpu_usage = cpu_usage;
        worker.memory_usage = memory_usage;
        worker.active_queries = active_queries;
        Ok(())
    }

    /// Deregister a worker.
    pub async fn deregister(&self, worker_id: u64) -> Result<()> {
        let mut workers = self.workers.write().await;
        workers.remove(&worker_id).ok_or(NovaError::Internal {
            message: format!("worker {} not registered", worker_id),
        })?;
        Ok(())
    }

    /// Get all active workers.
    pub async fn active_workers(&self) -> Vec<WorkerInfo> {
        let workers = self.workers.read().await;
        workers
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .cloned()
            .collect()
    }

    /// Get a specific worker.
    pub async fn get_worker(&self, worker_id: u64) -> Option<WorkerInfo> {
        let workers = self.workers.read().await;
        workers.get(&worker_id).cloned()
    }

    /// Check for dead workers (missed heartbeats).
    /// Marks workers as Suspect or Dead based on heartbeat timeout.
    pub async fn check_health(&self) -> Vec<u64> {
        let mut workers = self.workers.write().await;
        let now = Instant::now();
        let timeout = self.heartbeat_timeout * self.max_missed_heartbeats;
        let suspect_timeout = self.heartbeat_timeout;

        let mut dead_workers = Vec::new();

        for worker in workers.values_mut() {
            let elapsed = now.duration_since(worker.last_heartbeat);
            if elapsed > timeout {
                worker.status = WorkerStatus::Dead;
                dead_workers.push(worker.worker_id);
            } else if elapsed > suspect_timeout {
                worker.status = WorkerStatus::Suspect;
            }
        }

        dead_workers
    }

    /// Select the best worker for a new query (least loaded).
    pub async fn select_worker(&self) -> Option<WorkerInfo> {
        let workers = self.workers.read().await;
        workers
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .min_by(|a, b| {
                a.active_queries.cmp(&b.active_queries).then_with(|| {
                    a.cpu_usage
                        .partial_cmp(&b.cpu_usage)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
            })
            .cloned()
    }

    /// Get worker count by status.
    pub async fn worker_count(&self) -> (usize, usize, usize) {
        let workers = self.workers.read().await;
        let active = workers
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .count();
        let suspect = workers
            .values()
            .filter(|w| w.status == WorkerStatus::Suspect)
            .count();
        let dead = workers
            .values()
            .filter(|w| w.status == WorkerStatus::Dead)
            .count();
        (active, suspect, dead)
    }
}

impl Default for WorkerPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_register_worker() {
        let pool = WorkerPool::new();
        let id = pool.register("localhost:50051".to_string()).await.unwrap();
        assert_eq!(id, 1);

        let id2 = pool.register("localhost:50052".to_string()).await.unwrap();
        assert_eq!(id2, 2);

        let active = pool.active_workers().await;
        assert_eq!(active.len(), 2);
    }

    #[tokio::test]
    async fn test_heartbeat_updates_worker() {
        let pool = WorkerPool::new();
        let id = pool.register("localhost:50051".to_string()).await.unwrap();

        pool.heartbeat(id, 45.5, 60.0, 3).await.unwrap();

        let worker = pool.get_worker(id).await.unwrap();
        assert_eq!(worker.cpu_usage, 45.5);
        assert_eq!(worker.active_queries, 3);
        assert_eq!(worker.status, WorkerStatus::Active);
    }

    #[tokio::test]
    async fn test_heartbeat_unknown_worker() {
        let pool = WorkerPool::new();
        assert!(pool.heartbeat(999, 0.0, 0.0, 0).await.is_err());
    }

    #[tokio::test]
    async fn test_deregister_worker() {
        let pool = WorkerPool::new();
        let id = pool.register("localhost:50051".to_string()).await.unwrap();

        pool.deregister(id).await.unwrap();
        assert!(pool.get_worker(id).await.is_none());
    }

    #[tokio::test]
    async fn test_select_worker_least_loaded() {
        let pool = WorkerPool::new();
        let id1 = pool.register("w1".to_string()).await.unwrap();
        let id2 = pool.register("w2".to_string()).await.unwrap();

        pool.heartbeat(id1, 80.0, 50.0, 10).await.unwrap();
        pool.heartbeat(id2, 20.0, 30.0, 2).await.unwrap();

        let selected = pool.select_worker().await.unwrap();
        assert_eq!(selected.worker_id, id2); // less loaded
    }

    #[tokio::test]
    async fn test_select_worker_none_when_empty() {
        let pool = WorkerPool::new();
        assert!(pool.select_worker().await.is_none());
    }

    #[tokio::test]
    async fn test_health_check_marks_dead() {
        let mut pool = WorkerPool::new();
        // Override timeout for fast testing
        pool.heartbeat_timeout = Duration::from_millis(10);
        pool.max_missed_heartbeats = 1;

        let id = pool.register("w1".to_string()).await.unwrap();

        // Wait for heartbeat to expire
        sleep(Duration::from_millis(50)).await;

        let dead = pool.check_health().await;
        assert!(dead.contains(&id));

        let worker = pool.get_worker(id).await.unwrap();
        assert_eq!(worker.status, WorkerStatus::Dead);
    }

    #[tokio::test]
    async fn test_worker_count() {
        let pool = WorkerPool::new();
        pool.register("w1".to_string()).await.unwrap();
        pool.register("w2".to_string()).await.unwrap();

        let (active, suspect, dead) = pool.worker_count().await;
        assert_eq!(active, 2);
        assert_eq!(suspect, 0);
        assert_eq!(dead, 0);
    }
}
