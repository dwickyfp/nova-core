// Distributed Execution — scan partitioning + join strategies.
//
// Architecture:
// - Coordinator splits a query plan into fragments
// - Each fragment is dispatched to a worker
// - Workers execute fragments in parallel
// - Results are collected and merged by coordinator
//
// Join strategies (selected by CBO based on table statistics):
// - Shuffle Join: both tables partitioned by join key → local join per partition
// - Broadcast Join: small table copied to all workers → local join
// - Colocated Join: both tables already share distribution key → no shuffle needed

use crate::worker_pool::WorkerInfo;
use nova_common::MicroPartitionMeta;
use std::collections::HashMap;

/// Join strategy selected by the optimizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Partition both tables by join key, then local join per partition.
    Shuffle,
    /// Broadcast small table to all workers, then local join.
    Broadcast,
    /// Both tables share distribution key — no shuffle needed.
    Colocated,
}

/// Query fragment — a unit of work dispatched to a single worker.
#[derive(Debug, Clone)]
pub struct QueryFragment {
    pub fragment_id: u64,
    pub worker_id: u64,
    pub mp_ids: Vec<u64>,
    pub sql: String,
}

/// Fragment dispatcher — splits work across workers.
pub struct FragmentDispatcher {
    workers: Vec<WorkerInfo>,
    next_fragment_id: u64,
}

impl FragmentDispatcher {
    pub fn new(workers: Vec<WorkerInfo>) -> Self {
        Self {
            workers,
            next_fragment_id: 1,
        }
    }

    /// Partition MPs across workers for distributed scan.
    /// Round-robin distribution: each worker gets ~equal MPs.
    pub fn distribute_scan(&mut self, mps: &[MicroPartitionMeta]) -> Vec<QueryFragment> {
        if self.workers.is_empty() || mps.is_empty() {
            return vec![];
        }

        let n_workers = self.workers.len();
        let mut fragments = Vec::with_capacity(n_workers);

        // Round-robin MP assignment
        let mut worker_mps: HashMap<u64, Vec<u64>> = HashMap::new();
        for (i, mp) in mps.iter().enumerate() {
            let worker = &self.workers[i % n_workers];
            worker_mps
                .entry(worker.worker_id)
                .or_default()
                .push(mp.mp_id);
        }

        for (worker_id, mp_ids) in worker_mps {
            let fragment = QueryFragment {
                fragment_id: self.next_fragment_id,
                worker_id,
                mp_ids,
                sql: String::new(), // filled by planner
            };
            self.next_fragment_id += 1;
            fragments.push(fragment);
        }

        fragments
    }

    /// Dispatch fragments to workers via gRPC and collect results.
    /// Returns merged Arrow RecordBatches from all workers.
    ///
    /// ponytail: currently sequential dispatch — add parallel join_all when
    /// multi-worker throughput matters.
    pub async fn dispatch_via_grpc(
        &mut self,
        sql: &str,
        mps: &[nova_common::MicroPartitionMeta],
        worker_pool: &mut crate::grpc_client::WorkerClientPool,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        if worker_pool.is_empty() {
            return Err("no workers available".to_string());
        }

        let fragments = self.distribute_scan(mps);
        if fragments.is_empty() {
            return Ok(vec![]);
        }

        let mut all_batches = Vec::new();
        for (i, fragment) in fragments.iter().enumerate() {
            let worker_idx = i % worker_pool.len();
            let batches = worker_pool
                .execute_on(worker_idx, fragment.fragment_id, sql, vec![])
                .await
                .map_err(|e| format!("worker {} error: {}", worker_idx, e))?;
            all_batches.extend(batches);
        }
        Ok(all_batches)
    }

    /// Select join strategy based on table statistics.
    ///
    /// Rules:
    /// - If one table fits in memory (< 100MB) → Broadcast
    /// - If both tables share distribution key → Colocated
    /// - Otherwise → Shuffle (most general)
    pub fn select_join_strategy(
        _left_rows: u64,
        left_bytes: u64,
        _right_rows: u64,
        right_bytes: u64,
        colocated: bool,
    ) -> JoinStrategy {
        // Broadcast if small table < 100MB
        let broadcast_threshold = 100 * 1024 * 1024; // 100MB
        if left_bytes < broadcast_threshold || right_bytes < broadcast_threshold {
            return JoinStrategy::Broadcast;
        }

        if colocated {
            return JoinStrategy::Colocated;
        }

        JoinStrategy::Shuffle
    }

    /// Create broadcast fragments — send small table to all workers.
    pub fn distribute_broadcast(
        &mut self,
        small_table_mps: &[MicroPartitionMeta],
    ) -> Vec<QueryFragment> {
        if self.workers.is_empty() {
            return vec![];
        }

        // Each worker gets ALL MPs of the small table
        let all_mp_ids: Vec<u64> = small_table_mps.iter().map(|mp| mp.mp_id).collect();

        self.workers
            .iter()
            .map(|worker| {
                let fragment = QueryFragment {
                    fragment_id: self.next_fragment_id,
                    worker_id: worker.worker_id,
                    mp_ids: all_mp_ids.clone(),
                    sql: String::new(),
                };
                self.next_fragment_id += 1;
                fragment
            })
            .collect()
    }

    /// Create shuffle fragments — partition by join key hash.
    pub fn distribute_shuffle(
        &mut self,
        mps: &[MicroPartitionMeta],
        _join_key_col: &str,
    ) -> Vec<QueryFragment> {
        // ponytail: real shuffle would hash join_key values to determine partition.
        // For now, round-robin by MP (same as distribute_scan).
        // Upgrade: hash first row of join_key per MP to determine target worker.
        self.distribute_scan(mps)
    }

    /// Get number of available workers.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_pool::{WorkerInfo, WorkerStatus};
    use nova_common::Compression;
    use std::collections::HashMap;
    use std::time::Instant;

    fn mock_worker(id: u64) -> WorkerInfo {
        WorkerInfo {
            worker_id: id,
            address: format!("localhost:{}", 50050 + id),
            status: WorkerStatus::Active,
            last_heartbeat: Instant::now(),
            cpu_usage: 0.0,
            memory_usage: 0.0,
            active_queries: 0,
        }
    }

    fn mock_mp(id: u64) -> MicroPartitionMeta {
        MicroPartitionMeta {
            mp_id: id,
            table_id: 1,
            partition_id: None,
            version: 1,
            s3_path: format!("s3://test/mp-{}.parquet", id),
            s3_temp_path: None,
            row_count: 1000,
            byte_size: 1024 * 1024,
            compression: Compression::Snappy,
            column_stats: HashMap::new(),
            commit_ts: 0,
            txn_id: 0,
            supersedes: None,
            superseded_by: None,
            active: true,
        }
    }

    #[test]
    fn test_distribute_scan_round_robin() {
        let workers = vec![mock_worker(1), mock_worker(2), mock_worker(3)];
        let mut dispatcher = FragmentDispatcher::new(workers);
        let mps = vec![
            mock_mp(1),
            mock_mp(2),
            mock_mp(3),
            mock_mp(4),
            mock_mp(5),
            mock_mp(6),
        ];

        let fragments = dispatcher.distribute_scan(&mps);

        assert_eq!(fragments.len(), 3); // 3 workers
        let total_mps: usize = fragments.iter().map(|f| f.mp_ids.len()).sum();
        assert_eq!(total_mps, 6); // all MPs distributed
    }

    #[test]
    fn test_distribute_scan_empty_mps() {
        let workers = vec![mock_worker(1)];
        let mut dispatcher = FragmentDispatcher::new(workers);
        let fragments = dispatcher.distribute_scan(&[]);
        assert!(fragments.is_empty());
    }

    #[test]
    fn test_distribute_scan_no_workers() {
        let mut dispatcher = FragmentDispatcher::new(vec![]);
        let mps = vec![mock_mp(1)];
        let fragments = dispatcher.distribute_scan(&mps);
        assert!(fragments.is_empty());
    }

    #[test]
    fn test_select_join_strategy_broadcast() {
        // Small table (50MB) → Broadcast
        let strategy = FragmentDispatcher::select_join_strategy(
            1_000_000,
            50 * 1024 * 1024, // 50MB
            100_000_000,
            5 * 1024 * 1024 * 1024, // 5GB
            false,
        );
        assert_eq!(strategy, JoinStrategy::Broadcast);
    }

    #[test]
    fn test_select_join_strategy_colocated() {
        // Both large, colocated → Colocated
        let strategy = FragmentDispatcher::select_join_strategy(
            100_000_000,
            1024 * 1024 * 1024, // 1GB
            200_000_000,
            2 * 1024 * 1024 * 1024, // 2GB
            true,
        );
        assert_eq!(strategy, JoinStrategy::Colocated);
    }

    #[test]
    fn test_select_join_strategy_shuffle() {
        // Both large, not colocated → Shuffle
        let strategy = FragmentDispatcher::select_join_strategy(
            100_000_000,
            1024 * 1024 * 1024, // 1GB
            200_000_000,
            2 * 1024 * 1024 * 1024, // 2GB
            false,
        );
        assert_eq!(strategy, JoinStrategy::Shuffle);
    }

    #[test]
    fn test_distribute_broadcast() {
        let workers = vec![mock_worker(1), mock_worker(2)];
        let mut dispatcher = FragmentDispatcher::new(workers);
        let mps = vec![mock_mp(1), mock_mp(2), mock_mp(3)];

        let fragments = dispatcher.distribute_broadcast(&mps);

        assert_eq!(fragments.len(), 2); // 2 workers
        // Each worker gets ALL 3 MPs
        for fragment in &fragments {
            assert_eq!(fragment.mp_ids.len(), 3);
        }
    }

    #[test]
    fn test_distribute_shuffle() {
        let workers = vec![mock_worker(1), mock_worker(2)];
        let mut dispatcher = FragmentDispatcher::new(workers);
        let mps = vec![mock_mp(1), mock_mp(2), mock_mp(3), mock_mp(4)];

        let fragments = dispatcher.distribute_shuffle(&mps, "user_id");

        assert_eq!(fragments.len(), 2);
        let total: usize = fragments.iter().map(|f| f.mp_ids.len()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_fragment_ids_increment() {
        let workers = vec![mock_worker(1), mock_worker(2)];
        let mut dispatcher = FragmentDispatcher::new(workers);
        let mps = vec![mock_mp(1), mock_mp(2)];

        let f1 = dispatcher.distribute_scan(&mps);
        let f2 = dispatcher.distribute_scan(&mps);

        // Fragment IDs should be unique and incrementing
        let id1 = f1[0].fragment_id;
        let id2 = f2[0].fragment_id;
        assert!(id2 > id1);
    }

    #[test]
    fn test_worker_count() {
        let workers = vec![mock_worker(1), mock_worker(2), mock_worker(3)];
        let dispatcher = FragmentDispatcher::new(workers);
        assert_eq!(dispatcher.worker_count(), 3);
    }
}
