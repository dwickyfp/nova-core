//! Dynamic Table Scheduler — background task that refreshes DTs based on TARGET_LAG.
//!
//! Spawned once at server startup. Polls every 10 seconds.
//! For each DT: if now() - last_refresh_ts > target_lag_seconds → refresh.

use std::sync::Arc;
use std::time::Duration;

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
                let lag_micros = dt.target_lag_seconds * 1_000_000;
                let last = dt.last_refresh_ts.unwrap_or(0);
                if now.saturating_sub(last) >= lag_micros {
                    let db_name = db.name.clone();
                    let dt_name = dt.name.clone();
                    let _ = self
                        .executor
                        .exec_refresh_dynamic_table(&db_name, "public", &dt_name)
                        .await;
                }
            }
        }
    }
}
