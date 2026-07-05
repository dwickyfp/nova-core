//! gRPC client for Coordinator → Worker communication.
//!
//! Connects to worker gRPC servers, dispatches query fragments,
//! and collects streaming Arrow IPC results.

use std::sync::Arc;
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use tonic::transport::Channel;

// Include generated gRPC stubs (coordinator builds with client+server).
tonic::include_proto!("nova.rpc");
use worker_service_client::WorkerServiceClient;

/// Client for communicating with a single worker node.
pub struct WorkerClient {
    client: WorkerServiceClient<Channel>,
    worker_id: u64,
}

impl WorkerClient {
    /// Connect to a worker at the given address (e.g. "127.0.0.1:50051").
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let channel = Channel::builder(addr.parse()?)
            .timeout(Duration::from_secs(30))
            .connect()
            .await?;

        let client = WorkerServiceClient::new(channel);
        Ok(Self {
            client,
            worker_id: 0,
        })
    }

    /// Register this coordinator with the worker.
    pub async fn register(
        &mut self,
        address: &str,
        grpc_port: u32,
        cpu_count: u32,
    ) -> Result<u64, tonic::Status> {
        let resp = self
            .client
            .register_worker(RegisterRequest {
                address: address.to_string(),
                grpc_port,
                memory_limit_bytes: 0,
                cpu_count,
            })
            .await?;
        let worker_id = resp.into_inner().worker_id;
        self.worker_id = worker_id;
        Ok(worker_id)
    }

    /// Send a heartbeat to the worker.
    pub async fn heartbeat(
        &mut self,
        cpu_usage: f64,
        memory_usage: f64,
        active_queries: u32,
    ) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .heartbeat(HeartbeatRequest {
                worker_id: self.worker_id,
                cpu_usage,
                memory_usage,
                active_queries,
                cache_hit_count: 0,
                cache_miss_count: 0,
            })
            .await?;
        Ok(resp.into_inner().healthy)
    }

    /// Execute a fragment on this worker. Returns Arrow RecordBatches.
    pub async fn execute_fragment(
        &mut self,
        fragment_id: u64,
        sql: &str,
        tables: Vec<TableSnapshot>,
    ) -> Result<Vec<RecordBatch>, tonic::Status> {
        let resp = self
            .client
            .execute_fragment(FragmentRequest {
                fragment_id,
                sql: sql.to_string(),
                tables,
                batch_size: 8192,
            })
            .await?;

        let mut stream = resp.into_inner();
        let mut batches = Vec::new();

        while let Some(result) = stream.message().await? {
            if !result.error.is_empty() {
                return Err(tonic::Status::internal(result.error));
            }
            if result.done {
                break;
            }
            if !result.batch.is_empty() {
                let reader = StreamReader::try_new(&result.batch[..], None)
                    .map_err(|e| tonic::Status::internal(format!("Arrow IPC reader: {e}")))?;
                for batch in reader {
                    let batch = batch
                        .map_err(|e| tonic::Status::internal(format!("Arrow IPC decode: {e}")))?;
                    batches.push(batch);
                }
            }
        }

        Ok(batches)
    }

    /// Deregister from the worker.
    pub async fn deregister(&mut self) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .deregister_worker(DeregisterRequest {
                worker_id: self.worker_id,
            })
            .await?;
        Ok(resp.into_inner().success)
    }

    /// Get the worker ID assigned during registration.
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }
}

/// Pool of worker clients, keyed by worker_id.
pub struct WorkerClientPool {
    clients: Vec<Arc<tokio::sync::Mutex<WorkerClient>>>,
}

impl WorkerClientPool {
    pub fn new() -> Self {
        Self {
            clients: Vec::new(),
        }
    }

    /// Add a worker to the pool by connecting to it.
    pub async fn add_worker(&mut self, addr: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let mut client = WorkerClient::connect(addr).await?;
        let worker_id = client.register("coordinator", 0, num_cpus()).await?;
        self.clients.push(Arc::new(tokio::sync::Mutex::new(client)));
        Ok(worker_id)
    }

    /// Get the number of connected workers.
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// Check if pool is empty.
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// Get a worker client by index (round-robin style).
    pub fn get(&self, index: usize) -> Option<&Arc<tokio::sync::Mutex<WorkerClient>>> {
        self.clients.get(index)
    }

    /// Find the client index for a registered worker id.
    pub async fn index_of_worker_id(&self, worker_id: u64) -> Option<usize> {
        for (idx, client) in self.clients.iter().enumerate() {
            if client.lock().await.worker_id() == worker_id {
                return Some(idx);
            }
        }
        None
    }

    /// Execute a fragment on a specific worker by index.
    pub async fn execute_on(
        &self,
        worker_index: usize,
        fragment_id: u64,
        sql: &str,
        tables: Vec<TableSnapshot>,
    ) -> Result<Vec<RecordBatch>, tonic::Status> {
        let client = self
            .clients
            .get(worker_index)
            .ok_or_else(|| tonic::Status::not_found("worker not found"))?;
        let mut guard = client.lock().await;
        guard.execute_fragment(fragment_id, sql, tables).await
    }
}

impl Default for WorkerClientPool {
    fn default() -> Self {
        Self::new()
    }
}

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}
