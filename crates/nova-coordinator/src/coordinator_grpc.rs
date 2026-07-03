//! Coordinator-side gRPC service for worker registration and heartbeats.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::worker_pool::WorkerPool;

tonic::include_proto!("nova.rpc");
pub use coordinator_service_client::CoordinatorServiceClient;
use coordinator_service_server::CoordinatorService;
pub use coordinator_service_server::CoordinatorServiceServer;

pub struct CoordinatorGrpcServer {
    worker_pool: Arc<WorkerPool>,
}

impl CoordinatorGrpcServer {
    pub fn new(worker_pool: Arc<WorkerPool>) -> Self {
        Self { worker_pool }
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorGrpcServer {
    async fn register_worker(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let addr = format!("{}:{}", req.address, req.grpc_port);
        let worker_id = self
            .worker_pool
            .register(addr.clone())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        tracing::info!(worker_id, addr, "Worker registered");
        Ok(Response::new(RegisterResponse {
            worker_id,
            session_token: format!("nova-worker-{worker_id}"),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        self.worker_pool
            .heartbeat(
                req.worker_id,
                req.cpu_usage,
                req.memory_usage,
                req.active_queries,
            )
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;
        Ok(Response::new(HeartbeatResponse {
            healthy: true,
            message: "ok".to_string(),
        }))
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let worker_id = request.into_inner().worker_id;
        self.worker_pool
            .deregister(worker_id)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;
        tracing::info!(worker_id, "Worker deregistered");
        Ok(Response::new(DeregisterResponse { success: true }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_heartbeat() {
        let pool = Arc::new(WorkerPool::new());
        let svc = CoordinatorGrpcServer::new(pool.clone());
        let resp = svc
            .register_worker(Request::new(RegisterRequest {
                address: "127.0.0.1".to_string(),
                grpc_port: 50051,
                memory_limit_bytes: 0,
                cpu_count: 8,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.worker_id, 1);

        svc.heartbeat(Request::new(HeartbeatRequest {
            worker_id: resp.worker_id,
            cpu_usage: 12.0,
            memory_usage: 34.0,
            active_queries: 2,
            cache_hit_count: 0,
            cache_miss_count: 0,
        }))
        .await
        .unwrap();

        let worker = pool.get_worker(resp.worker_id).await.unwrap();
        assert_eq!(worker.active_queries, 2);
    }
}
