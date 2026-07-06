//! Dynamic Table Scheduler — background task that refreshes DTs based on TARGET_LAG.
//!
//! Spawned once at server startup. Polls every 10 seconds.
//! For each DT: if now() - last_refresh_ts > target_lag_seconds → refresh.

use std::sync::Arc;
use std::time::Duration;

pub(crate) fn dynamic_table_due_for_refresh(
    dt: &nova_common::DynamicTableMeta,
    now: u64,
    source_backlog_watermark: Option<u64>,
    uses_source_watermark: bool,
) -> bool {
    let lag_micros = dt.target_lag_seconds.saturating_mul(1_000_000);
    if uses_source_watermark {
        return match (dt.last_refresh_ts, source_backlog_watermark) {
            (None, _) => true,
            (Some(last), Some(watermark)) if watermark > last => {
                now.saturating_sub(watermark) >= lag_micros
            }
            _ => false,
        };
    }
    let last = dt.last_refresh_ts.unwrap_or(0);
    now.saturating_sub(last) >= lag_micros
}

pub struct DynamicTableScheduler {
    executor: Arc<crate::executor::Executor>,
    poll_interval: Duration,
}

impl DynamicTableScheduler {
    pub fn new(executor: Arc<crate::executor::Executor>) -> Self {
        Self {
            executor,
            poll_interval: Duration::from_secs(10),
        }
    }

    /// Spawn the scheduler as a background tokio task.
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                self.tick().await;
                tokio::time::sleep(self.poll_interval).await;
            }
        })
    }

    async fn tick(&self) {
        let Ok(dbs) = self.executor.meta().list_databases().await else {
            return;
        };
        for db in dbs {
            let Ok(dts) = self.executor.meta().list_dynamic_tables(db.id).await else {
                continue;
            };
            for dt in dts {
                if !dt.scheduler_enabled {
                    continue;
                }
                if dt.refresh_status == nova_common::DtRefreshStatus::Running {
                    continue;
                }
                let now = nova_common::now_micros();
                let uses_source_watermark =
                    crate::executor::Executor::dynamic_table_uses_source_watermark(&dt)
                        .unwrap_or(false);
                let source_backlog_watermark = if uses_source_watermark {
                    self.executor
                        .dynamic_table_source_backlog_watermark(&dt)
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                };
                if dynamic_table_due_for_refresh(
                    &dt,
                    now,
                    source_backlog_watermark,
                    uses_source_watermark,
                ) {
                    let dt_name = dt.name.clone();
                    if let Err(err) = self.executor.exec_refresh_dynamic_table_meta(dt).await {
                        tracing::warn!(
                            dynamic_table = %dt_name,
                            error = %err,
                            "dynamic table refresh failed"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_common::{DtRefreshMode, DtRefreshStatus, DynamicTableMeta};

    fn dt(last_refresh_ts: Option<u64>, target_lag_seconds: u64) -> DynamicTableMeta {
        DynamicTableMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "dt".to_string(),
            query_definition: "SELECT id FROM source".to_string(),
            target_lag_seconds,
            refresh_mode: DtRefreshMode::Incremental,
            initialize_on_create: false,
            output_table_id: 2,
            last_refresh_ts,
            refresh_status: DtRefreshStatus::Success,
            comment: None,
            created_at: 1,
            scheduler_enabled: true,
        }
    }

    #[test]
    fn source_watermark_dynamic_table_is_not_due_without_new_source_mps() {
        let dt = dt(Some(100), 60);

        assert!(!dynamic_table_due_for_refresh(
            &dt,
            10_000_000_000,
            Some(100),
            true
        ));
    }

    #[test]
    fn source_watermark_dynamic_table_is_due_after_new_source_lag_expires() {
        let dt = dt(Some(100), 60);

        assert!(dynamic_table_due_for_refresh(
            &dt,
            70_000_200,
            Some(200),
            true
        ));
    }

    #[test]
    fn wall_clock_dynamic_table_uses_last_refresh_time() {
        let dt = dt(Some(100), 60);

        assert!(dynamic_table_due_for_refresh(&dt, 60_000_100, None, false));
    }
}
