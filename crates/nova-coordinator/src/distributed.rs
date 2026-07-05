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

use crate::grpc_client::TableSnapshot;
use crate::worker_pool::WorkerInfo;
use nova_common::MicroPartitionMeta;
use std::collections::{BTreeMap, HashSet};

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

/// Runtime join metrics used for adaptive strategy switching.
#[derive(Debug, Clone, Copy)]
pub struct JoinRuntimeStats {
    pub left_bytes: u64,
    pub right_bytes: u64,
    pub left_rows: u64,
    pub right_rows: u64,
    pub colocated: bool,
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
        let mut worker_mps: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
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

    /// Dispatch fragments to workers via gRPC in parallel and collect results.
    ///
    /// `partitioned_table` is split per fragment. Other table snapshots are sent
    /// unchanged so broadcast-side JOIN inputs remain available on every worker.
    pub async fn dispatch_via_grpc(
        &mut self,
        sql: &str,
        partitioned_table: &str,
        mps: &[nova_common::MicroPartitionMeta],
        tables: &[TableSnapshot],
        worker_pool: &crate::grpc_client::WorkerClientPool,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        if worker_pool.is_empty() {
            return Err("no workers available".to_string());
        }
        if tables.is_empty() {
            return Err("no table snapshots provided".to_string());
        }
        if !tables.iter().any(|t| t.table_name == partitioned_table) {
            return Err(format!(
                "partitioned table snapshot not found: {partitioned_table}"
            ));
        }

        let fragments = self.distribute_scan(mps);
        if fragments.is_empty() {
            return Ok(vec![]);
        }

        let jobs = fragments.into_iter().enumerate().map(|(i, fragment)| {
            let tables = Self::tables_for_fragment(partitioned_table, tables, &fragment);
            async move {
                let worker_idx = worker_pool
                    .index_of_worker_id(fragment.worker_id)
                    .await
                    .unwrap_or(i % worker_pool.len());
                worker_pool
                    .execute_on(worker_idx, fragment.fragment_id, sql, tables)
                    .await
                    .map_err(|e| format!("worker {} error: {}", worker_idx, e))
            }
        });

        let mut all_batches = Vec::new();
        for result in futures::future::join_all(jobs).await {
            all_batches.extend(result?);
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
        if self.workers.is_empty() || mps.is_empty() {
            return vec![];
        }

        let mut buckets: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for mp in mps {
            // ponytail: MP-level shuffle uses partition_id/mp_id metadata, not row-level
            // exchange; add worker-side RecordBatch repartition when distributed joins
            // need exact row hash partitioning.
            let key = mp.partition_id.unwrap_or(mp.mp_id);
            let worker =
                &self.workers[(stable_hash_u64(_join_key_col, key) as usize) % self.workers.len()];
            buckets.entry(worker.worker_id).or_default().push(mp.mp_id);
        }
        self.fragments_from_buckets(buckets)
    }

    /// Create colocated fragments — keep equal partition IDs on the same worker.
    pub fn distribute_colocated(&mut self, mps: &[MicroPartitionMeta]) -> Vec<QueryFragment> {
        if self.workers.is_empty() || mps.is_empty() {
            return vec![];
        }

        let mut buckets: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for mp in mps {
            let key = mp.partition_id.unwrap_or(mp.mp_id) as usize;
            let worker = &self.workers[key % self.workers.len()];
            buckets.entry(worker.worker_id).or_default().push(mp.mp_id);
        }
        self.fragments_from_buckets(buckets)
    }

    /// Re-evaluate join strategy from runtime stats.
    pub fn select_adaptive_join_strategy(stats: JoinRuntimeStats) -> JoinStrategy {
        Self::select_join_strategy(
            stats.left_rows,
            stats.left_bytes,
            stats.right_rows,
            stats.right_bytes,
            stats.colocated,
        )
    }

    /// Reassign fragments from dead workers to active workers.
    pub fn reschedule_failed_fragments(
        &mut self,
        fragments: &[QueryFragment],
        dead_worker_ids: &[u64],
    ) -> Vec<QueryFragment> {
        let mut active: Vec<u64> = self
            .workers
            .iter()
            .map(|w| w.worker_id)
            .filter(|id| !dead_worker_ids.contains(id))
            .collect();
        active.sort_unstable();
        if active.is_empty() {
            return vec![];
        }

        fragments
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let mut next = f.clone();
                if dead_worker_ids.contains(&next.worker_id) {
                    next.fragment_id = self.next_fragment_id;
                    self.next_fragment_id += 1;
                    next.worker_id = active[i % active.len()];
                }
                next
            })
            .collect()
    }

    fn fragments_from_buckets(&mut self, buckets: BTreeMap<u64, Vec<u64>>) -> Vec<QueryFragment> {
        let mut fragments = Vec::with_capacity(buckets.len());
        for (worker_id, mp_ids) in buckets {
            fragments.push(QueryFragment {
                fragment_id: self.next_fragment_id,
                worker_id,
                mp_ids,
                sql: String::new(),
            });
            self.next_fragment_id += 1;
        }
        fragments
    }

    fn tables_for_fragment(
        partitioned_table: &str,
        tables: &[TableSnapshot],
        fragment: &QueryFragment,
    ) -> Vec<TableSnapshot> {
        let mp_ids: HashSet<u64> = fragment.mp_ids.iter().copied().collect();
        tables
            .iter()
            .cloned()
            .map(|mut table| {
                if table.table_name == partitioned_table {
                    table.mps.retain(|mp| mp_ids.contains(&mp.mp_id));
                }
                table
            })
            .collect()
    }

    /// Get number of available workers.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

fn stable_hash_u64(seed: &str, value: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for b in seed.as_bytes().iter().chain(value.to_le_bytes().iter()) {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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
    fn test_distribute_colocated_groups_same_partition() {
        let workers = vec![mock_worker(1), mock_worker(2)];
        let mut dispatcher = FragmentDispatcher::new(workers);
        let mut mps = vec![mock_mp(1), mock_mp(2), mock_mp(3), mock_mp(4)];
        mps[0].partition_id = Some(7);
        mps[1].partition_id = Some(7);
        mps[2].partition_id = Some(8);
        mps[3].partition_id = Some(8);

        let fragments = dispatcher.distribute_colocated(&mps);

        assert_eq!(fragments.len(), 2);
        assert!(fragments.iter().any(|f| f.mp_ids == vec![1, 2]));
        assert!(fragments.iter().any(|f| f.mp_ids == vec![3, 4]));
    }

    #[test]
    fn test_adaptive_join_strategy() {
        let strategy = FragmentDispatcher::select_adaptive_join_strategy(JoinRuntimeStats {
            left_rows: 1_000_000,
            left_bytes: 2 * 1024 * 1024 * 1024,
            right_rows: 10_000,
            right_bytes: 10 * 1024 * 1024,
            colocated: false,
        });
        assert_eq!(strategy, JoinStrategy::Broadcast);
    }

    #[test]
    fn test_reschedule_failed_fragments() {
        let workers = vec![mock_worker(1), mock_worker(2), mock_worker(3)];
        let mut dispatcher = FragmentDispatcher::new(workers);
        let fragments = vec![
            QueryFragment {
                fragment_id: 1,
                worker_id: 1,
                mp_ids: vec![1],
                sql: String::new(),
            },
            QueryFragment {
                fragment_id: 2,
                worker_id: 2,
                mp_ids: vec![2],
                sql: String::new(),
            },
        ];

        let rescheduled = dispatcher.reschedule_failed_fragments(&fragments, &[1]);

        assert_ne!(rescheduled[0].worker_id, 1);
        assert_eq!(rescheduled[1].worker_id, 2);
        assert_eq!(rescheduled.iter().map(|f| f.mp_ids.len()).sum::<usize>(), 2);
    }

    #[test]
    fn test_tables_for_fragment_filters_only_partitioned_table() {
        let fragment = QueryFragment {
            fragment_id: 1,
            worker_id: 1,
            mp_ids: vec![1, 3],
            sql: String::new(),
        };
        let tables = vec![
            TableSnapshot {
                table_name: "fact".to_string(),
                schema: vec![],
                mps: vec![
                    crate::grpc_client::MicroPartitionInfo {
                        mp_id: 1,
                        s3_path: "a".to_string(),
                        row_count: 1,
                        byte_size: 1,
                    },
                    crate::grpc_client::MicroPartitionInfo {
                        mp_id: 2,
                        s3_path: "b".to_string(),
                        row_count: 1,
                        byte_size: 1,
                    },
                    crate::grpc_client::MicroPartitionInfo {
                        mp_id: 3,
                        s3_path: "c".to_string(),
                        row_count: 1,
                        byte_size: 1,
                    },
                ],
            },
            TableSnapshot {
                table_name: "dim".to_string(),
                schema: vec![],
                mps: vec![crate::grpc_client::MicroPartitionInfo {
                    mp_id: 9,
                    s3_path: "d".to_string(),
                    row_count: 1,
                    byte_size: 1,
                }],
            },
        ];

        let out = FragmentDispatcher::tables_for_fragment("fact", &tables, &fragment);

        assert_eq!(
            out[0].mps.iter().map(|mp| mp.mp_id).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            out[1].mps.iter().map(|mp| mp.mp_id).collect::<Vec<_>>(),
            vec![9]
        );
    }

    #[test]
    fn test_worker_count() {
        let workers = vec![mock_worker(1), mock_worker(2), mock_worker(3)];
        let dispatcher = FragmentDispatcher::new(workers);
        assert_eq!(dispatcher.worker_count(), 3);
    }
}
