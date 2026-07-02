//! Auto-compaction service — background GC + MP merge.
//!
//! Runs two tokio tasks:
//! - GC sweep: purge superseded MPs older than retention window (periodic)
//! - Merge sweep: compact small MPs per table into larger ones (periodic)
//!
//! Started once at server startup, no SQL control needed.

use std::sync::Arc;
use std::time::Duration;

use nova_common::now_micros;
use nova_storage::{MetadataStore, MpReader, MpWriter};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::executor::Executor;

/// Compaction configuration. Loaded from `[compaction]` in config.toml.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CompactionConfig {
    /// Enable background compaction (GC + merge). Default: true.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// How often to run GC sweep (seconds). Default: 3600 (1 hour).
    #[serde(default = "default_gc_interval")]
    pub gc_interval_secs: u64,

    /// Purge superseded MPs older than this many days. Default: 7.
    #[serde(default = "default_retention")]
    pub gc_retention_days: u32,

    /// Enable MP merge/rewrite. Default: true.
    #[serde(default = "default_true")]
    pub merge_enabled: bool,

    /// How often to run merge sweep (seconds). Default: 7200 (2 hours).
    #[serde(default = "default_merge_interval")]
    pub merge_interval_secs: u64,

    /// Minimum active MPs per table before merge is attempted. Default: 8.
    #[serde(default = "default_min_mps")]
    pub merge_min_mps: usize,

    /// Target row count per merged MP. MPs smaller than half this are candidates. Default: 500_000.
    #[serde(default = "default_target_rows")]
    pub merge_target_rows: u64,
}

fn default_true() -> bool {
    true
}
fn default_gc_interval() -> u64 {
    3600
}
fn default_retention() -> u32 {
    7
}
fn default_merge_interval() -> u64 {
    7200
}
fn default_min_mps() -> usize {
    8
}
fn default_target_rows() -> u64 {
    500_000
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            gc_interval_secs: 3600,
            gc_retention_days: 7,
            merge_enabled: true,
            merge_interval_secs: 7200,
            merge_min_mps: 8,
            merge_target_rows: 500_000,
        }
    }
}

/// Start background compaction tasks. Spawns and forgets — returns immediately.
pub fn start(executor: Arc<Executor>, cfg: CompactionConfig) {
    if !cfg.enabled {
        info!("Compaction disabled via config");
        return;
    }

    // Task 1: auto-GC
    {
        let ex = executor.clone();
        let retention = cfg.gc_retention_days;
        let interval = Duration::from_secs(cfg.gc_interval_secs);
        tokio::spawn(async move {
            info!(
                interval_secs = interval.as_secs(),
                retention_days = retention,
                "Auto-GC task started"
            );
            loop {
                sleep(interval).await;
                match ex.gc_internal(retention).await {
                    Ok(deleted) => info!(deleted, "Auto-GC sweep done"),
                    Err(e) => warn!(error = %e, "Auto-GC sweep failed"),
                }
            }
        });
    }

    // Task 2: MP merge
    if cfg.merge_enabled {
        let ex = executor.clone();
        let interval = Duration::from_secs(cfg.merge_interval_secs);
        let min_mps = cfg.merge_min_mps;
        let target_rows = cfg.merge_target_rows;
        tokio::spawn(async move {
            info!(
                interval_secs = interval.as_secs(),
                min_mps, target_rows, "Auto-merge task started"
            );
            loop {
                sleep(interval).await;
                if let Err(e) = run_merge_sweep(&ex, min_mps, target_rows).await {
                    warn!(error = %e, "Auto-merge sweep failed");
                }
            }
        });
    }
}

/// Scan all tables, merge small MPs per table.
async fn run_merge_sweep(
    executor: &Executor,
    min_mps: usize,
    target_rows: u64,
) -> nova_common::Result<()> {
    let meta = executor.meta();
    let dbs = meta.list_databases().await?;
    let mut tables_merged = 0u64;
    let mut mps_merged = 0u64;

    for db in &dbs {
        let schemas = meta.list_schemas(db.id).await?;
        for schema in &schemas {
            let tables = meta.list_tables(db.id, schema.id).await?;
            for table in &tables {
                let active_mps = meta.get_active_mps(table.id).await?;
                if active_mps.len() < min_mps {
                    continue;
                }
                // Group MPs that are small enough to be merge candidates
                let candidates: Vec<_> = active_mps
                    .iter()
                    .filter(|mp| mp.row_count < target_rows / 2)
                    .collect();
                if candidates.len() < 2 {
                    continue;
                }
                // Merge candidates in pairs/groups up to target_rows
                let merged = merge_mp_group(executor, &candidates, target_rows).await?;
                if merged > 0 {
                    tables_merged += 1;
                    mps_merged += merged;
                }
            }
        }
    }

    if tables_merged > 0 {
        info!(tables_merged, mps_merged, "Auto-merge sweep done");
    }
    Ok(())
}

/// Merge a group of small MPs into larger ones.
/// Returns number of old MPs superseded.
async fn merge_mp_group(
    executor: &Executor,
    candidates: &[&nova_common::MicroPartitionMeta],
    target_rows: u64,
) -> nova_common::Result<u64> {
    use arrow::compute::concat_batches;
    use nova_common::MicroPartitionMeta;

    let meta = executor.meta();
    let reader = executor.mp_reader();
    let writer = executor.mp_writer();

    let mut superseded = 0u64;

    // Greedy bin-packing: accumulate MPs until target_rows reached, then write+supersede
    let mut batch_mps: Vec<&nova_common::MicroPartitionMeta> = Vec::new();
    let mut batch_rows: u64 = 0;

    for mp in candidates {
        batch_mps.push(mp);
        batch_rows += mp.row_count;
        if batch_rows >= target_rows {
            superseded += flush_batch(&batch_mps, meta, &reader, writer).await?;
            batch_mps.clear();
            batch_rows = 0;
        }
    }
    // Flush remainder if at least 2
    if batch_mps.len() >= 2 {
        superseded += flush_batch(&batch_mps, meta, &reader, writer).await?;
    }

    Ok(superseded)
}

/// Write merged MP and supersede old ones.
async fn flush_batch(
    batch: &[&nova_common::MicroPartitionMeta],
    meta: &Arc<dyn nova_storage::MetadataStore>,
    reader: &Arc<nova_storage::MpReader>,
    writer: &nova_storage::MpWriter,
) -> nova_common::Result<u64> {
    use arrow::compute::concat_batches;

    if batch.len() < 2 {
        return Ok(0);
    }
    let mut all_batches = Vec::new();
    for mp in batch {
        let batches = reader.read(mp, None).await?;
        all_batches.extend(batches);
    }
    if all_batches.is_empty() {
        return Ok(0);
    }
    let schema = all_batches[0].schema();
    let merged =
        concat_batches(&schema, &all_batches).map_err(|e| nova_common::NovaError::Internal {
            message: e.to_string(),
        })?;

    let table_id = batch[0].table_id;
    let new_mp = writer
        .write(table_id, 0, 0, &[merged], batch[0].txn_id)
        .await?;

    for old_mp in batch {
        meta.mark_superseded(old_mp.mp_id, new_mp.mp_id).await?;
    }
    Ok(batch.len() as u64)
}
