// Auto-Scaling — elastic worker management.
//
// Architecture:
// - Monitor: track CPU, memory, queue depth across all workers
// - Scale-Up: when CPU > threshold or queue depth > limit → add workers
// - Scale-Down: when workers idle for N seconds → suspend (terminate)
// - Auto-Resume: when query arrives and no active workers → provision
//
// Warehouse concept (Snowflake-style):
// - Virtual compute clusters with configurable size
// - T-shirt sizing: X-Small (1), Small (2), Medium (4), Large (8), XLarge (16)

use crate::worker_pool::{WorkerInfo, WorkerStatus};
use std::time::Duration;

/// Warehouse size (T-shirt sizing like Snowflake).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarehouseSize {
    XSmall, // 1 worker
    Small,  // 2 workers
    Medium, // 4 workers
    Large,  // 8 workers
    XLarge, // 16 workers
}

impl WarehouseSize {
    pub fn worker_count(&self) -> usize {
        match self {
            WarehouseSize::XSmall => 1,
            WarehouseSize::Small => 2,
            WarehouseSize::Medium => 4,
            WarehouseSize::Large => 8,
            WarehouseSize::XLarge => 16,
        }
    }
}

/// Auto-scaling policy.
#[derive(Debug, Clone)]
pub struct ScalingPolicy {
    /// CPU usage threshold to trigger scale-up (0.0–1.0).
    pub cpu_scale_up_threshold: f64,
    /// CPU usage threshold to trigger scale-down (0.0–1.0).
    pub cpu_scale_down_threshold: f64,
    /// Max workers before refusing to scale up.
    pub max_workers: usize,
    /// Min workers (never scale below this).
    pub min_workers: usize,
    /// Idle duration before worker is suspended.
    pub idle_suspend_timeout: Duration,
    /// Queued queries that trigger scale-up.
    pub queue_depth_scale_up_threshold: u32,
}

impl Default for ScalingPolicy {
    fn default() -> Self {
        Self {
            cpu_scale_up_threshold: 0.80,   // 80% CPU → scale up
            cpu_scale_down_threshold: 0.30, // < 30% CPU → scale down
            max_workers: 16,
            min_workers: 1,
            idle_suspend_timeout: Duration::from_secs(60),
            queue_depth_scale_up_threshold: 10,
        }
    }
}

/// Scaling decision from the auto-scaler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingDecision {
    /// Add N workers.
    ScaleUp { count: usize, reason: String },
    /// Remove N workers.
    ScaleDown {
        worker_ids: Vec<u64>,
        reason: String,
    },
    /// No change needed.
    NoAction,
}

/// Auto-scaler — evaluates worker metrics and decides scale actions.
pub struct AutoScaler {
    policy: ScalingPolicy,
    warehouse_size: WarehouseSize,
}

impl AutoScaler {
    pub fn new(policy: ScalingPolicy, warehouse_size: WarehouseSize) -> Self {
        Self {
            policy,
            warehouse_size,
        }
    }

    /// Evaluate current worker metrics and produce a scaling decision.
    pub fn evaluate(&self, workers: &[WorkerInfo]) -> ScalingDecision {
        self.evaluate_with_queue_depth(workers, 0)
    }

    /// Evaluate current worker metrics and queue depth.
    pub fn evaluate_with_queue_depth(
        &self,
        workers: &[WorkerInfo],
        queue_depth: u32,
    ) -> ScalingDecision {
        let active: Vec<&WorkerInfo> = workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Active)
            .collect();

        if active.is_empty() {
            // No active workers — need to resume
            return ScalingDecision::ScaleUp {
                count: self.warehouse_size.worker_count(),
                reason: "no active workers, resuming warehouse".to_string(),
            };
        }

        // Calculate average CPU
        let avg_cpu: f64 =
            active.iter().map(|w| w.cpu_usage).sum::<f64>() / active.len() as f64 / 100.0;

        if queue_depth > self.policy.queue_depth_scale_up_threshold
            && active.len() < self.policy.max_workers
        {
            return ScalingDecision::ScaleUp {
                count: 1,
                reason: format!(
                    "queue depth {} > threshold {}",
                    queue_depth, self.policy.queue_depth_scale_up_threshold
                ),
            };
        }

        // Scale up if average CPU > threshold
        if avg_cpu > self.policy.cpu_scale_up_threshold && active.len() < self.policy.max_workers {
            let needed = (active.len() + 1).min(self.policy.max_workers) - active.len();
            return ScalingDecision::ScaleUp {
                count: needed,
                reason: format!(
                    "avg CPU {:.0}% > threshold {:.0}%",
                    avg_cpu * 100.0,
                    self.policy.cpu_scale_up_threshold * 100.0
                ),
            };
        }

        // Scale down if average CPU < threshold AND more than min_workers
        if avg_cpu < self.policy.cpu_scale_down_threshold && active.len() > self.policy.min_workers
        {
            // Find idle workers (0 active queries)
            let idle: Vec<u64> = active
                .iter()
                .filter(|w| w.active_queries == 0)
                .map(|w| w.worker_id)
                .collect();

            if !idle.is_empty() {
                let to_remove = idle.len().min(active.len() - self.policy.min_workers);
                return ScalingDecision::ScaleDown {
                    worker_ids: idle.into_iter().take(to_remove).collect(),
                    reason: format!(
                        "avg CPU {:.0}% < threshold {:.0}%, {} idle workers",
                        avg_cpu * 100.0,
                        self.policy.cpu_scale_down_threshold * 100.0,
                        to_remove
                    ),
                };
            }
        }

        ScalingDecision::NoAction
    }

    /// Check if a worker should be suspended due to idle timeout.
    pub fn should_suspend(&self, worker: &WorkerInfo) -> bool {
        worker.status == WorkerStatus::Active
            && worker.active_queries == 0
            && worker.last_heartbeat.elapsed() > self.policy.idle_suspend_timeout
    }

    /// Get warehouse size.
    pub fn warehouse_size(&self) -> WarehouseSize {
        self.warehouse_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn mock_worker(id: u64, cpu: f64, queries: u32) -> WorkerInfo {
        WorkerInfo {
            worker_id: id,
            address: format!("localhost:{}", id),
            status: WorkerStatus::Active,
            last_heartbeat: Instant::now(),
            cpu_usage: cpu,
            memory_usage: 0.0,
            active_queries: queries,
        }
    }

    #[test]
    fn test_warehouse_sizes() {
        assert_eq!(WarehouseSize::XSmall.worker_count(), 1);
        assert_eq!(WarehouseSize::Small.worker_count(), 2);
        assert_eq!(WarehouseSize::Medium.worker_count(), 4);
        assert_eq!(WarehouseSize::Large.worker_count(), 8);
        assert_eq!(WarehouseSize::XLarge.worker_count(), 16);
    }

    #[test]
    fn test_scale_up_on_high_cpu() {
        let scaler = AutoScaler::new(ScalingPolicy::default(), WarehouseSize::Medium);
        let workers = vec![mock_worker(1, 90.0, 5), mock_worker(2, 85.0, 4)];

        let decision = scaler.evaluate(&workers);
        assert!(matches!(decision, ScalingDecision::ScaleUp { .. }));
    }

    #[test]
    fn test_scale_up_on_queue_depth() {
        let scaler = AutoScaler::new(ScalingPolicy::default(), WarehouseSize::Medium);
        let workers = vec![mock_worker(1, 10.0, 0)];

        let decision = scaler.evaluate_with_queue_depth(&workers, 11);
        assert!(matches!(decision, ScalingDecision::ScaleUp { .. }));
    }

    #[test]
    fn test_scale_down_on_low_cpu() {
        let scaler = AutoScaler::new(ScalingPolicy::default(), WarehouseSize::Medium);
        let workers = vec![
            mock_worker(1, 10.0, 0),
            mock_worker(2, 15.0, 0),
            mock_worker(3, 20.0, 0),
            mock_worker(4, 5.0, 0),
        ];

        let decision = scaler.evaluate(&workers);
        assert!(matches!(decision, ScalingDecision::ScaleDown { .. }));
    }

    #[test]
    fn test_no_action_on_normal_cpu() {
        let scaler = AutoScaler::new(ScalingPolicy::default(), WarehouseSize::Small);
        let workers = vec![mock_worker(1, 50.0, 2), mock_worker(2, 55.0, 1)];

        let decision = scaler.evaluate(&workers);
        assert_eq!(decision, ScalingDecision::NoAction);
    }

    #[test]
    fn test_scale_up_when_no_workers() {
        let scaler = AutoScaler::new(ScalingPolicy::default(), WarehouseSize::Small);
        let decision = scaler.evaluate(&[]);
        assert!(matches!(decision, ScalingDecision::ScaleUp { .. }));
    }

    #[test]
    fn test_no_scale_down_below_min() {
        let policy = ScalingPolicy {
            min_workers: 2,
            ..Default::default()
        };
        let scaler = AutoScaler::new(policy, WarehouseSize::Small);
        let workers = vec![mock_worker(1, 5.0, 0), mock_worker(2, 10.0, 0)];

        let decision = scaler.evaluate(&workers);
        // Already at min_workers → no scale down
        assert_eq!(decision, ScalingDecision::NoAction);
    }

    #[test]
    fn test_no_scale_up_above_max() {
        let policy = ScalingPolicy {
            max_workers: 2,
            ..Default::default()
        };
        let scaler = AutoScaler::new(policy, WarehouseSize::Small);
        let workers = vec![mock_worker(1, 95.0, 10), mock_worker(2, 90.0, 8)];

        let decision = scaler.evaluate(&workers);
        // Already at max_workers → no scale up
        assert_eq!(decision, ScalingDecision::NoAction);
    }

    #[test]
    fn test_should_suspend_idle_worker() {
        let policy = ScalingPolicy {
            idle_suspend_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let scaler = AutoScaler::new(policy, WarehouseSize::Small);

        let mut worker = mock_worker(1, 0.0, 0);
        worker.last_heartbeat = Instant::now() - Duration::from_millis(10);

        assert!(scaler.should_suspend(&worker));
    }

    #[test]
    fn test_should_not_suspend_active_worker() {
        let scaler = AutoScaler::new(ScalingPolicy::default(), WarehouseSize::Small);
        let worker = mock_worker(1, 50.0, 3);
        assert!(!scaler.should_suspend(&worker));
    }

    #[test]
    fn test_warehouse_size_getter() {
        let scaler = AutoScaler::new(ScalingPolicy::default(), WarehouseSize::Large);
        assert_eq!(scaler.warehouse_size(), WarehouseSize::Large);
    }
}
