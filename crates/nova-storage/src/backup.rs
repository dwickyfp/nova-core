// Backup & Restore — snapshot metadata + MP manifest.
//
// Phase 6.4: Create point-in-time backups of the database.
// - Backup: export all metadata (tables, MPs, schemas) + MP manifest
// - Restore: import metadata from backup, verify MP files in S3
// - Incremental: only backup MPs created since last backup

use nova_common::{MicroPartitionMeta, TableMeta, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Backup manifest — describes a complete database snapshot.
#[derive(Debug, Clone)]
pub struct BackupManifest {
    pub backup_id: String,
    pub created_at: Timestamp,
    pub tables: Vec<TableBackup>,
    pub total_mps: u64,
    pub total_bytes: u64,
    pub incremental_from: Option<String>, // previous backup_id for incremental
}

/// Table-level backup data.
#[derive(Debug, Clone)]
pub struct TableBackup {
    pub table: TableMeta,
    pub mp_ids: Vec<u64>,
    pub mp_count: usize,
}

/// Restore result.
#[derive(Debug, Clone)]
pub struct RestoreResult {
    pub tables_restored: usize,
    pub mps_verified: usize,
    pub mps_missing: Vec<u64>,
}

/// Backup manager — in-memory backup catalog.
pub struct BackupManager {
    backups: Arc<RwLock<HashMap<String, BackupManifest>>>,
    next_id: Arc<RwLock<u64>>,
}

impl BackupManager {
    pub fn new() -> Self {
        Self {
            backups: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Create a full backup from current metadata.
    pub async fn create_backup(
        &self,
        tables: &[TableMeta],
        mps: &HashMap<u64, Vec<MicroPartitionMeta>>,
    ) -> BackupManifest {
        let mut next = self.next_id.write().await;
        let backup_id = format!("backup_{:06}", *next);
        *next += 1;

        let mut total_mps = 0u64;
        let mut total_bytes = 0u64;
        let mut table_backups = Vec::new();

        for table in tables {
            let mp_list = mps.get(&table.id).cloned().unwrap_or_default();
            let mp_ids: Vec<u64> = mp_list.iter().map(|m| m.mp_id).collect();
            let mp_count = mp_ids.len();
            total_mps += mp_count as u64;
            total_bytes += mp_list.iter().map(|m| m.byte_size).sum::<u64>();

            table_backups.push(TableBackup {
                table: table.clone(),
                mp_ids,
                mp_count,
            });
        }

        let manifest = BackupManifest {
            backup_id: backup_id.clone(),
            created_at: 0,
            tables: table_backups,
            total_mps,
            total_bytes,
            incremental_from: None,
        };

        self.backups
            .write()
            .await
            .insert(backup_id, manifest.clone());
        manifest
    }

    /// Create an incremental backup (only new MPs since last backup).
    pub async fn create_incremental_backup(
        &self,
        from_backup_id: &str,
        tables: &[TableMeta],
        mps: &HashMap<u64, Vec<MicroPartitionMeta>>,
        cutoff_version: u64,
    ) -> Option<BackupManifest> {
        let backups = self.backups.read().await;
        let _from = backups.get(from_backup_id)?;

        let mut next = self.next_id.write().await;
        let backup_id = format!("backup_{:06}", *next);
        *next += 1;

        let mut total_mps = 0u64;
        let mut total_bytes = 0u64;
        let mut table_backups = Vec::new();

        for table in tables {
            let mp_list = mps.get(&table.id).cloned().unwrap_or_default();
            // Only include MPs newer than cutoff_version
            let new_mps: Vec<_> = mp_list
                .iter()
                .filter(|m| m.version > cutoff_version)
                .collect();
            let mp_ids: Vec<u64> = new_mps.iter().map(|m| m.mp_id).collect();
            let mp_count = mp_ids.len();
            total_mps += mp_count as u64;
            total_bytes += new_mps.iter().map(|m| m.byte_size).sum::<u64>();

            table_backups.push(TableBackup {
                table: table.clone(),
                mp_ids,
                mp_count,
            });
        }

        let manifest = BackupManifest {
            backup_id: backup_id.clone(),
            created_at: 0,
            tables: table_backups,
            total_mps,
            total_bytes,
            incremental_from: Some(from_backup_id.to_string()),
        };

        self.backups
            .write()
            .await
            .insert(backup_id, manifest.clone());
        Some(manifest)
    }

    /// Restore from a backup — verify MPs exist in storage.
    pub async fn restore(
        &self,
        backup_id: &str,
        existing_mp_ids: &std::collections::HashSet<u64>,
    ) -> Option<RestoreResult> {
        let backups = self.backups.read().await;
        let manifest = backups.get(backup_id)?;

        let mut mps_verified = 0usize;
        let mut mps_missing = Vec::new();

        for table_backup in &manifest.tables {
            for &mp_id in &table_backup.mp_ids {
                if existing_mp_ids.contains(&mp_id) {
                    mps_verified += 1;
                } else {
                    mps_missing.push(mp_id);
                }
            }
        }

        Some(RestoreResult {
            tables_restored: manifest.tables.len(),
            mps_verified,
            mps_missing,
        })
    }

    /// List all backups.
    pub async fn list_backups(&self) -> Vec<BackupManifest> {
        self.backups.read().await.values().cloned().collect()
    }

    /// Get a specific backup.
    pub async fn get_backup(&self, backup_id: &str) -> Option<BackupManifest> {
        self.backups.read().await.get(backup_id).cloned()
    }

    /// Delete a backup.
    pub async fn delete_backup(&self, backup_id: &str) -> bool {
        self.backups.write().await.remove(backup_id).is_some()
    }
}

impl Default for BackupManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_common::{ColumnDef, NovaType};

    fn make_table(id: u64, name: &str) -> TableMeta {
        TableMeta {
            id,
            db_id: 1,
            schema_id: 1,
            name: name.to_string(),
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
            version: 0,
            properties: Default::default(),
        }
    }

    fn make_mp(mp_id: u64, table_id: u64, version: u64, byte_size: u64) -> MicroPartitionMeta {
        MicroPartitionMeta {
            mp_id,
            table_id,
            partition_id: None,
            version,
            s3_path: format!("s3://bucket/mp_{}.parquet", mp_id),
            s3_temp_path: None,
            row_count: 100,
            byte_size,
            compression: Default::default(),
            column_stats: HashMap::new(),
            commit_ts: 0,
            txn_id: 0,
            supersedes: None,
            superseded_by: None,
            active: true,
        }
    }

    #[tokio::test]
    async fn test_create_full_backup() {
        let mgr = BackupManager::new();
        let tables = vec![make_table(1, "users"), make_table(2, "orders")];
        let mut mps = HashMap::new();
        mps.insert(1, vec![make_mp(100, 1, 1, 1024), make_mp(101, 1, 2, 2048)]);
        mps.insert(2, vec![make_mp(200, 2, 1, 4096)]);

        let manifest = mgr.create_backup(&tables, &mps).await;
        assert_eq!(manifest.tables.len(), 2);
        assert_eq!(manifest.total_mps, 3);
        assert_eq!(manifest.total_bytes, 1024 + 2048 + 4096);
        assert!(manifest.incremental_from.is_none());
    }

    #[tokio::test]
    async fn test_create_incremental_backup() {
        let mgr = BackupManager::new();
        let tables = vec![make_table(1, "users")];
        let mut mps = HashMap::new();
        mps.insert(
            1,
            vec![
                make_mp(100, 1, 1, 1024),
                make_mp(101, 1, 2, 2048),
                make_mp(102, 1, 3, 512),
            ],
        );

        let full = mgr.create_backup(&tables, &mps).await;

        // Incremental: only MPs with version > 2
        let incr = mgr
            .create_incremental_backup(&full.backup_id, &tables, &mps, 2)
            .await
            .unwrap();

        assert_eq!(incr.total_mps, 1); // only mp 102 (version 3)
        assert_eq!(
            incr.incremental_from.as_deref(),
            Some(full.backup_id.as_str())
        );
    }

    #[tokio::test]
    async fn test_restore_success() {
        let mgr = BackupManager::new();
        let tables = vec![make_table(1, "users")];
        let mut mps = HashMap::new();
        mps.insert(1, vec![make_mp(100, 1, 1, 1024), make_mp(101, 1, 2, 2048)]);

        let manifest = mgr.create_backup(&tables, &mps).await;

        let existing: std::collections::HashSet<u64> = [100, 101].into_iter().collect();
        let result = mgr.restore(&manifest.backup_id, &existing).await.unwrap();
        assert_eq!(result.tables_restored, 1);
        assert_eq!(result.mps_verified, 2);
        assert!(result.mps_missing.is_empty());
    }

    #[tokio::test]
    async fn test_restore_missing_mps() {
        let mgr = BackupManager::new();
        let tables = vec![make_table(1, "users")];
        let mut mps = HashMap::new();
        mps.insert(1, vec![make_mp(100, 1, 1, 1024), make_mp(101, 1, 2, 2048)]);

        let manifest = mgr.create_backup(&tables, &mps).await;

        let existing: std::collections::HashSet<u64> = [100].into_iter().collect();
        let result = mgr.restore(&manifest.backup_id, &existing).await.unwrap();
        assert_eq!(result.mps_verified, 1);
        assert_eq!(result.mps_missing, vec![101]);
    }

    #[tokio::test]
    async fn test_list_backups() {
        let mgr = BackupManager::new();
        let tables = vec![make_table(1, "users")];
        let mps = HashMap::new();

        mgr.create_backup(&tables, &mps).await;
        mgr.create_backup(&tables, &mps).await;

        let backups = mgr.list_backups().await;
        assert_eq!(backups.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_backup() {
        let mgr = BackupManager::new();
        let tables = vec![make_table(1, "users")];
        let mps = HashMap::new();

        let manifest = mgr.create_backup(&tables, &mps).await;
        assert!(mgr.delete_backup(&manifest.backup_id).await);
        assert!(!mgr.delete_backup(&manifest.backup_id).await);
    }

    #[tokio::test]
    async fn test_restore_nonexistent_backup() {
        let mgr = BackupManager::new();
        let existing: std::collections::HashSet<u64> = std::collections::HashSet::new();
        assert!(mgr.restore("nonexistent", &existing).await.is_none());
    }
}
