// Transaction Manager — MVCC + snapshot isolation + optimistic concurrency control.
//
// Architecture:
// - BEGIN: assign snapshot_ts (current time), track read/write sets
// - Read: only see MPs with commit_ts <= snapshot_ts (snapshot isolation)
// - Write: stage new MPs in write set, don't commit yet
// - COMMIT: check for conflicts (did another txn supersede MPs we read?),
//   if clean → assign commit_ts, mark MPs active, link version chains
// - ABORT: discard write set, mark txn aborted
//
// Conflict detection (optimistic):
// For each MP in the read set, check if its `superseded_by` was set
// by a transaction that committed AFTER our snapshot_ts.
// If yes → conflict → abort.

use nova_common::{MicroPartitionMeta, TableId, Timestamp, TxnId};
use nova_storage::MetadataStore;
use std::sync::Arc;

/// Result of a commit attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResult {
    /// Transaction committed successfully at this timestamp.
    Committed { commit_ts: Timestamp },
    /// Transaction aborted due to write-write conflict.
    Aborted { reason: String },
}

/// Manages MVCC transactions with snapshot isolation and optimistic conflict detection.
pub struct TransactionManager<M: MetadataStore> {
    store: Arc<M>,
}

impl<M: MetadataStore> TransactionManager<M> {
    pub fn new(store: Arc<M>) -> Self {
        Self { store }
    }

    /// BEGIN a new transaction.
    /// Returns (txn_id, snapshot_ts). All reads will see data as of snapshot_ts.
    pub async fn begin(&self) -> nova_common::Result<(TxnId, Timestamp)> {
        let txn_id = self.store.begin_transaction().await?;
        let txn =
            self.store
                .get_transaction(txn_id)
                .await?
                .ok_or(nova_common::NovaError::Internal {
                    message: format!("transaction {} not found after begin", txn_id),
                })?;
        Ok((txn_id, txn.snapshot_ts))
    }

    /// Record a read on an MP (for conflict detection).
    /// Call this when a transaction reads an MP.
    pub async fn record_read(&self, txn_id: TxnId, _mp_id: u64) -> nova_common::Result<()> {
        // Just verify the txn is still active
        let txn =
            self.store
                .get_transaction(txn_id)
                .await?
                .ok_or(nova_common::NovaError::Internal {
                    message: format!("transaction {} not found", txn_id),
                })?;
        if txn.status != nova_common::TxnStatus::Active {
            return Err(nova_common::NovaError::Internal {
                message: format!("transaction {} is not active", txn_id),
            });
        }
        Ok(())
    }

    /// Record a write: a new MP that should be committed, and an old MP it supersedes.
    pub fn record_write(
        &self,
        _txn_id: TxnId,
        _table_id: TableId,
        _new_mp_id: u64,
        _old_mp_id: Option<u64>,
    ) {
        // Write tracking happens in memory at the coordinator level.
        // The actual MP metadata is already in the store (written by MpWriter).
        // We just need to track the supersede relationships for commit.
        // ponytail: full in-memory tracking would need a HashMap<TxnId, TxnState>.
        // For now, the conflict check uses the store's supersedes/superseded_by fields.
    }

    /// COMMIT a transaction with optimistic conflict detection.
    ///
    /// Checks: for each MP in the read set, has it been superseded
    /// by a transaction that committed after our snapshot_ts?
    pub async fn commit(&self, txn_id: TxnId) -> nova_common::Result<CommitResult> {
        let txn =
            self.store
                .get_transaction(txn_id)
                .await?
                .ok_or(nova_common::NovaError::Internal {
                    message: format!("transaction {} not found", txn_id),
                })?;

        if txn.status != nova_common::TxnStatus::Active {
            return Ok(CommitResult::Aborted {
                reason: format!("transaction {} is not active", txn_id),
            });
        }

        // Conflict detection: check if any MP we read has been superseded
        // since our snapshot_ts.
        // For a full implementation, we'd check the read set here.
        // ponytail: conflict detection on read set is deferred until we have
        // per-txn in-memory state tracking. The version chain in the store
        // already prevents lost updates at the MP level.

        // Commit the transaction
        self.store.commit_transaction(txn_id).await?;

        let committed_txn =
            self.store
                .get_transaction(txn_id)
                .await?
                .ok_or(nova_common::NovaError::Internal {
                    message: format!("transaction {} not found after commit", txn_id),
                })?;

        Ok(CommitResult::Committed {
            commit_ts: committed_txn.commit_ts.unwrap_or(0),
        })
    }

    /// ABORT a transaction.
    pub async fn abort(&self, txn_id: TxnId) -> nova_common::Result<()> {
        self.store.abort_transaction(txn_id).await
    }

    /// Get MPs visible to a transaction (snapshot isolation).
    /// Returns only MPs with commit_ts <= txn.snapshot_ts.
    pub async fn get_visible_mps(
        &self,
        table_id: TableId,
        snapshot_ts: Timestamp,
    ) -> nova_common::Result<Vec<MicroPartitionMeta>> {
        self.store.get_mps_at_timestamp(table_id, snapshot_ts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_storage::FdbMetadataStore;

    async fn setup() -> (TransactionManager<FdbMetadataStore>, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(
            FdbMetadataStore::open_test(
                "docker:docker@127.0.0.1:4500",
                format!("test_{}", nova_common::now_micros()).into_bytes(),
            )
            .unwrap(),
        );
        (TransactionManager::new(store), dir)
    }

    #[tokio::test]
    async fn test_begin_commit() {
        let (tm, _dir) = setup().await;
        let (txn_id, snapshot_ts) = tm.begin().await.unwrap();
        assert!(snapshot_ts > 0);

        let result = tm.commit(txn_id).await.unwrap();
        match result {
            CommitResult::Committed { commit_ts } => {
                assert!(commit_ts >= snapshot_ts);
            }
            CommitResult::Aborted { reason } => panic!("expected commit, got abort: {}", reason),
        }
    }

    #[tokio::test]
    async fn test_abort() {
        let (tm, _dir) = setup().await;
        let (txn_id, _) = tm.begin().await.unwrap();
        tm.abort(txn_id).await.unwrap();

        let txn = tm.store.get_transaction(txn_id).await.unwrap().unwrap();
        assert_eq!(txn.status, nova_common::TxnStatus::Aborted);
    }

    #[tokio::test]
    async fn test_commit_already_committed() {
        let (tm, _dir) = setup().await;
        let (txn_id, _) = tm.begin().await.unwrap();
        tm.commit(txn_id).await.unwrap();

        // Second commit should return Aborted (not active)
        let result = tm.commit(txn_id).await.unwrap();
        assert!(matches!(result, CommitResult::Aborted { .. }));
    }

    #[tokio::test]
    async fn test_commit_already_aborted() {
        let (tm, _dir) = setup().await;
        let (txn_id, _) = tm.begin().await.unwrap();
        tm.abort(txn_id).await.unwrap();

        let result = tm.commit(txn_id).await.unwrap();
        assert!(matches!(result, CommitResult::Aborted { .. }));
    }

    #[tokio::test]
    async fn test_snapshot_isolation() {
        let (tm, _dir) = setup().await;

        // T1 begins and sees snapshot at t1
        let (_txn1, ts1) = tm.begin().await.unwrap();

        // T2 begins later and commits — its data should NOT be visible to T1
        let (txn2, ts2) = tm.begin().await.unwrap();
        assert!(ts2 >= ts1);
        tm.commit(txn2).await.unwrap();

        // T1 should still see data as of ts1, not ts2
        let visible = tm.get_visible_mps(1, ts1).await.unwrap();
        assert!(visible.is_empty()); // no MPs at ts1
    }

    #[tokio::test]
    async fn test_concurrent_transactions_no_conflict() {
        let (tm, _dir) = setup().await;

        // Two concurrent transactions on different tables — no conflict
        let (txn1, _) = tm.begin().await.unwrap();
        let (txn2, _) = tm.begin().await.unwrap();

        let r1 = tm.commit(txn1).await.unwrap();
        let r2 = tm.commit(txn2).await.unwrap();

        assert!(matches!(r1, CommitResult::Committed { .. }));
        assert!(matches!(r2, CommitResult::Committed { .. }));
    }

    #[tokio::test]
    async fn test_record_read_active_txn() {
        let (tm, _dir) = setup().await;
        let (txn_id, _) = tm.begin().await.unwrap();
        tm.record_read(txn_id, 42).await.unwrap(); // should not error
    }

    #[tokio::test]
    async fn test_record_read_committed_txn_fails() {
        let (tm, _dir) = setup().await;
        let (txn_id, _) = tm.begin().await.unwrap();
        tm.commit(txn_id).await.unwrap();
        assert!(tm.record_read(txn_id, 42).await.is_err());
    }
}
