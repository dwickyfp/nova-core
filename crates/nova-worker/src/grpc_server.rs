//! gRPC server for WorkerService — receives commands from coordinator.
//!
//! Implements: RegisterWorker, Heartbeat, ExecuteFragment, DeregisterWorker.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::ipc::writer::StreamWriter;
use futures::Stream;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use nova_common::{Compression, MicroPartitionMeta, TableMeta};
use nova_storage::MetadataStore;

use crate::executor::Executor;

// Include generated gRPC stubs.
tonic::include_proto!("nova.rpc");

// Bring the trait into scope.
use worker_service_server::WorkerService;

/// Worker state shared across gRPC handlers.
pub struct WorkerState {
    pub worker_id: AtomicU64,
    pub registered: std::sync::atomic::AtomicBool,
    pub executor: Executor,
    pub meta: Arc<dyn MetadataStore>,
    pub stats: RwLock<WorkerStats>,
}

#[derive(Debug, Default, Clone)]
pub struct WorkerStats {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub active_queries: u32,
    pub cache_hit_count: u64,
    pub cache_miss_count: u64,
}

/// gRPC server implementing WorkerService.
pub struct WorkerGrpcServer {
    state: Arc<WorkerState>,
}

impl WorkerGrpcServer {
    pub fn new(state: Arc<WorkerState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerGrpcServer {
    async fn register_worker(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        tracing::info!(
            address = %req.address,
            grpc_port = req.grpc_port,
            cpu_count = req.cpu_count,
            "Worker registering with coordinator"
        );

        // Assign worker_id (in real distributed mode, coordinator assigns this.
        // For now, self-assign since worker may register before coordinator knows about it.)
        let worker_id = self.state.worker_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.state.registered.store(true, Ordering::SeqCst);

        let resp = RegisterResponse {
            worker_id,
            session_token: format!("nova-worker-{}", worker_id),
        };
        Ok(Response::new(resp))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let mut stats = self.state.stats.write().await;
        stats.cpu_usage = req.cpu_usage;
        stats.memory_usage = req.memory_usage;
        stats.active_queries = req.active_queries;
        stats.cache_hit_count = req.cache_hit_count;
        stats.cache_miss_count = req.cache_miss_count;
        drop(stats);

        tracing::trace!(
            worker_id = req.worker_id,
            cpu = req.cpu_usage,
            mem = req.memory_usage,
            queries = req.active_queries,
            "Heartbeat received"
        );

        Ok(Response::new(HeartbeatResponse {
            healthy: true,
            message: "ok".to_string(),
        }))
    }

    type ExecuteFragmentStream =
        Pin<Box<dyn Stream<Item = Result<FragmentResult, Status>> + Send + 'static>>;

    async fn execute_fragment(
        &self,
        request: Request<FragmentRequest>,
    ) -> Result<Response<Self::ExecuteFragmentStream>, Status> {
        let req = request.into_inner();
        tracing::info!(
            fragment_id = req.fragment_id,
            sql = %req.sql,
            tables = req.tables.len(),
            "Executing fragment"
        );
        validate_fragment_request(&req)?;

        // For each table snapshot in the request, find metadata + MPs and execute SQL.
        // ponytail: for now, execute against the first table. Multi-table JOIN support
        // requires coordinator to send all table snapshots — add when needed.
        let state = self.state.clone();

        let stream = async_stream::try_stream! {
            let result = execute_fragment_inner(&state, &req).await;
            match result {
                Ok(batches) => {
                    for batch in &batches {
                        let mut buf = Vec::new();
                        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                            .map_err(|e| Status::internal(format!("Arrow IPC writer: {e}")))?;
                        writer.write(batch)
                            .map_err(|e| Status::internal(format!("Arrow IPC write: {e}")))?;
                        writer.finish()
                            .map_err(|e| Status::internal(format!("Arrow IPC finish: {e}")))?;
                        yield FragmentResult {
                            batch: buf,
                            done: false,
                            error: String::new(),
                        };
                    }
                    yield FragmentResult {
                        batch: Vec::new(),
                        done: true,
                        error: String::new(),
                    };
                }
                Err(e) => {
                    yield FragmentResult {
                        batch: Vec::new(),
                        done: true,
                        error: format!("fragment {} failed: {e}", req.fragment_id),
                    };
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let req = request.into_inner();
        tracing::info!(worker_id = req.worker_id, "Worker deregistering");
        self.state.registered.store(false, Ordering::SeqCst);
        Ok(Response::new(DeregisterResponse { success: true }))
    }
}

fn validate_fragment_request(req: &FragmentRequest) -> Result<(), Status> {
    if req.sql.trim().is_empty() {
        return Err(Status::invalid_argument("fragment SQL must not be empty"));
    }
    if req.tables.is_empty() {
        return Err(Status::invalid_argument(
            "fragment request must include at least one table snapshot",
        ));
    }
    if req.batch_size == 0 {
        return Err(Status::invalid_argument(
            "fragment batch_size must be greater than zero",
        ));
    }

    for (table_index, table) in req.tables.iter().enumerate() {
        if table.table_name.trim().is_empty() {
            return Err(Status::invalid_argument(format!(
                "table snapshot {table_index} must include table_name"
            )));
        }
        for (mp_index, mp) in table.mps.iter().enumerate() {
            if mp.s3_path.trim().is_empty() {
                return Err(Status::invalid_argument(format!(
                    "table '{}' micro-partition {mp_index} must include s3_path",
                    table.table_name
                )));
            }
        }
    }

    Ok(())
}

fn register_table_snapshot(
    ctx: &datafusion::prelude::SessionContext,
    meta: &TableMeta,
    mps: Vec<MicroPartitionMeta>,
    reader: Arc<nova_storage::MpReader>,
    fragment_id: u64,
) -> Result<(), String> {
    let provider = crate::NovaTableProvider::new(meta.clone(), mps, reader);
    ctx.register_table(&meta.name, Arc::new(provider))
        .map_err(|e| {
            format!(
                "register table '{}' for fragment {}: {e}",
                meta.name, fragment_id
            )
        })?;
    Ok(())
}

/// Execute a fragment: resolve tables from metadata, run SQL via DataFusion.
async fn execute_fragment_inner(
    state: &WorkerState,
    req: &FragmentRequest,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if req.tables.is_empty() {
        return Err("no tables in fragment request".to_string());
    }

    let config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
    let ctx = datafusion::prelude::SessionContext::new_with_config(config);

    // Register each table snapshot
    for table_snap in &req.tables {
        // Find table metadata from local metadata store
        let dbs = state
            .meta
            .list_databases()
            .await
            .map_err(|e| e.to_string())?;
        let mut found = false;
        for db in &dbs {
            let schemas = state
                .meta
                .list_schemas(db.id)
                .await
                .map_err(|e| e.to_string())?;
            for schema in &schemas {
                let tables = state
                    .meta
                    .list_tables(db.id, schema.id)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(meta) = tables.iter().find(|t| t.name == table_snap.table_name) {
                    let mps = if table_snap.mps.is_empty() {
                        state
                            .meta
                            .get_active_mps(meta.id)
                            .await
                            .map_err(|e| e.to_string())?
                    } else {
                        table_snap
                            .mps
                            .iter()
                            .map(|mp| snapshot_mp_to_meta(meta.id, mp))
                            .collect()
                    };
                    register_table_snapshot(
                        &ctx,
                        meta,
                        mps,
                        state.executor.reader_clone(),
                        req.fragment_id,
                    )?;
                    found = true;
                    break;
                }
            }
            if found {
                break;
            }
        }
        if !found {
            return Err(format!(
                "table '{}' not found in metadata",
                table_snap.table_name
            ));
        }
    }

    let df = ctx
        .sql(&req.sql)
        .await
        .map_err(|e| format!("DataFusion SQL: {e}"))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| format!("DataFusion collect: {e}"))?;
    Ok(batches)
}

fn snapshot_mp_to_meta(table_id: u64, mp: &MicroPartitionInfo) -> MicroPartitionMeta {
    MicroPartitionMeta {
        mp_id: mp.mp_id,
        table_id,
        partition_id: None,
        version: 0,
        s3_path: mp.s3_path.clone(),
        s3_temp_path: None,
        row_count: mp.row_count,
        byte_size: mp.byte_size,
        compression: Compression::Snappy,
        column_stats: std::collections::HashMap::new(),
        commit_ts: 0,
        txn_id: 0,
        supersedes: None,
        superseded_by: None,
        active: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_common::{ColumnDef, NovaType};
    use std::collections::HashMap;
    use tonic::Code;

    fn test_table_meta() -> TableMeta {
        TableMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                id: 1,
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
            properties: HashMap::new(),
        }
    }

    fn valid_request() -> FragmentRequest {
        FragmentRequest {
            fragment_id: 42,
            sql: "SELECT * FROM orders".to_string(),
            tables: vec![TableSnapshot {
                table_name: "orders".to_string(),
                schema: Vec::new(),
                mps: vec![MicroPartitionInfo {
                    mp_id: 7,
                    s3_path: "tables/1/mp-7.parquet".to_string(),
                    row_count: 3,
                    byte_size: 128,
                }],
            }],
            batch_size: 8192,
        }
    }

    #[test]
    fn validate_fragment_request_rejects_empty_sql() {
        let mut req = valid_request();
        req.sql = "  ".to_string();

        let err = validate_fragment_request(&req).expect_err("empty SQL should fail");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("SQL"));
    }

    #[test]
    fn validate_fragment_request_rejects_missing_tables() {
        let mut req = valid_request();
        req.tables.clear();

        let err = validate_fragment_request(&req).expect_err("missing tables should fail");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("table snapshot"));
    }

    #[test]
    fn validate_fragment_request_rejects_empty_mp_path() {
        let mut req = valid_request();
        req.tables[0].mps[0].s3_path.clear();

        let err = validate_fragment_request(&req).expect_err("empty MP path should fail");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("s3_path"));
    }

    #[test]
    fn register_table_snapshot_returns_datafusion_registration_errors() {
        let ctx = datafusion::prelude::SessionContext::new();
        let meta = test_table_meta();
        let reader = Arc::new(nova_storage::MpReader::new(Arc::new(
            object_store::local::LocalFileSystem::new(),
        )));
        register_table_snapshot(&ctx, &meta, Vec::new(), reader.clone(), 42)
            .expect("first registration should succeed");

        let err = register_table_snapshot(&ctx, &meta, Vec::new(), reader, 42)
            .expect_err("duplicate table registration should surface DataFusion errors");

        assert!(
            err.contains("register table 'orders' for fragment 42"),
            "unexpected error: {err}"
        );
    }
}
