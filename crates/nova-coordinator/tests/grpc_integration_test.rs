//! Integration test: real gRPC worker server + coordinator client.
//!
//! Spins up a WorkerGrpcServer on a random port, connects WorkerClient,
//! verifies register → heartbeat → execute_fragment → deregister lifecycle.

#[cfg(test)]
mod tests {
    use nova_coordinator::grpc_client::WorkerClient;
    use nova_storage::{FdbMetadataStore, MetadataStore, MpReader};
    use nova_worker::{WorkerGrpcServer, WorkerState, WorkerStats};
    use object_store::local::LocalFileSystem;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    };
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    /// Bind to OS port 0, return actual bound address.
    async fn find_free_port() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    }

    fn make_worker_state(dir: &TempDir) -> Arc<WorkerState> {
        let data_path = dir.path().join("data");
        std::fs::create_dir_all(&data_path).unwrap();
        let meta: Arc<dyn MetadataStore> = Arc::new(
            FdbMetadataStore::open_test(
                "docker:docker@127.0.0.1:4500",
                format!(
                    "test_{}_{}",
                    nova_common::now_micros(),
                    nova_common::generate_id()
                )
                .into_bytes(),
            )
            .unwrap(),
        );
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&data_path).unwrap());
        let reader = MpReader::new(store);
        let executor = nova_worker::Executor::new(reader);
        Arc::new(WorkerState {
            worker_id: AtomicU64::new(0),
            registered: AtomicBool::new(false),
            executor,
            meta,
            stats: RwLock::new(WorkerStats::default()),
        })
    }

    #[tokio::test]
    async fn test_grpc_register_and_heartbeat() {
        let dir = TempDir::new().unwrap();
        let port = find_free_port().await;
        let addr = format!("127.0.0.1:{}", port);

        let state = make_worker_state(&dir);
        let server = WorkerGrpcServer::new(state);

        // Spawn server in background
        let server_addr = addr.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    nova_worker::grpc_server::worker_service_server::WorkerServiceServer::new(
                        server,
                    ),
                )
                .serve(server_addr.parse().unwrap())
                .await
                .unwrap();
        });

        // Give server a tick to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect client
        let endpoint = format!("http://{}", addr);
        let mut client = WorkerClient::connect(&endpoint).await.unwrap();

        // Register
        let worker_id = client.register("coordinator", 50051, 4).await.unwrap();
        assert!(worker_id > 0, "worker_id should be assigned");

        // Heartbeat
        let healthy = client.heartbeat(12.5, 40.0, 0).await.unwrap();
        assert!(healthy, "heartbeat should return healthy=true");

        // Deregister
        let ok = client.deregister().await.unwrap();
        assert!(ok, "deregister should succeed");
    }

    #[tokio::test]
    async fn test_grpc_execute_fragment_empty_table() {
        let dir = TempDir::new().unwrap();
        let port = find_free_port().await;
        let addr = format!("127.0.0.1:{}", port);

        let state = make_worker_state(&dir);
        let server = WorkerGrpcServer::new(state);

        let server_addr = addr.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    nova_worker::grpc_server::worker_service_server::WorkerServiceServer::new(
                        server,
                    ),
                )
                .serve(server_addr.parse().unwrap())
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let endpoint = format!("http://{}", addr);
        let mut client = WorkerClient::connect(&endpoint).await.unwrap();
        client.register("coordinator", 50051, 4).await.unwrap();

        // Execute fragment — table doesn't exist, should return an error result
        // (not a panic). The error propagates cleanly via the stream.
        let result = client.execute_fragment(1, "SELECT 1", vec![]).await;
        // Empty tables vec → should return error or empty results
        // Both are acceptable for this edge case
        match result {
            Ok(batches) => assert!(batches.is_empty(), "no tables = no batches"),
            Err(e) => {
                let msg = e.message();
                assert!(
                    msg.contains("no tables") || msg.contains("not found") || msg.contains("table"),
                    "unexpected error: {}",
                    msg
                );
            }
        }
    }

    #[tokio::test]
    async fn test_worker_client_pool_add() {
        use nova_coordinator::grpc_client::WorkerClientPool;

        let dir = TempDir::new().unwrap();
        let port = find_free_port().await;
        let addr = format!("127.0.0.1:{}", port);

        let state = make_worker_state(&dir);
        let server = WorkerGrpcServer::new(state);

        let server_addr = addr.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    nova_worker::grpc_server::worker_service_server::WorkerServiceServer::new(
                        server,
                    ),
                )
                .serve(server_addr.parse().unwrap())
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut pool = WorkerClientPool::new();
        let endpoint = format!("http://{}", addr);
        let worker_id = pool.add_worker(&endpoint).await.unwrap();
        assert!(worker_id > 0);
        assert_eq!(pool.len(), 1);
    }
}
