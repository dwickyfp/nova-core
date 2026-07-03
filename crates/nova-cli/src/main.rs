//! nova — CLI entry point for nova-core coordinator.

use clap::{Parser, Subcommand};
use figment::providers::Format;
use nova_coordinator::auth::AuthManager;
use nova_coordinator::executor::Executor;
use nova_coordinator::ha::HealthChecker;
use nova_coordinator::monitoring::QueryMetrics;
use nova_coordinator::mysql_protocol::MySqlServer;
use nova_coordinator::mysql_protocol::nova_engine::NovaEngine;
use nova_storage::{FdbMetadataStore, MetadataStore, MpReader, MpWriter};
use object_store::ObjectStore;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "nova",
    about = "Nova Engine — Rust-native analytical query engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the coordinator (MySQL server + query engine).
    Server {
        #[arg(short, long, default_value = "config.toml")]
        config: String,
        /// This node's Raft ID (1, 2, or 3). Defaults to 1 (single-node).
        #[arg(long, default_value = "1")]
        node_id: u64,
        /// Comma-separated peer list: "2=host:port,3=host:port".
        /// Empty = single-node mode (no Raft cluster).
        #[arg(long, default_value = "")]
        raft_peers: String,
    },
    /// Start a worker node (connects to coordinator via gRPC).
    Worker {
        #[arg(short, long, default_value = "config.toml")]
        config: String,
        /// Address this worker listens on for gRPC (e.g. 0.0.0.0:50051).
        #[arg(long, default_value = "0.0.0.0:50051")]
        grpc_addr: String,
        /// Coordinator gRPC address to register with (e.g. coordinator:50060).
        #[arg(long, default_value = "127.0.0.1:50060")]
        coordinator_addr: String,
    },
    /// Show version info.
    Version,
}

#[derive(serde::Deserialize)]
struct Config {
    server: ServerConfig,
    storage: StorageConfig,
    metadata: MetadataConfig,
    #[serde(default)]
    auth: AuthConfig,
    #[serde(default)]
    compaction: nova_coordinator::compaction::CompactionConfig,
}

#[derive(serde::Deserialize)]
struct ServerConfig {
    host: String,
    port: u16,
}

#[derive(serde::Deserialize)]
struct StorageConfig {
    s3_endpoint: String,
    s3_bucket: String,
    s3_access_key: String,
    s3_secret_key: String,
    #[serde(default = "default_region")]
    s3_region: String,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(serde::Deserialize, Default)]
struct AuthConfig {
    #[serde(default = "default_auth_enabled")]
    enabled: bool,
    #[serde(default = "default_username")]
    default_username: String,
    #[serde(default)]
    default_password_hash: String,
}

fn default_auth_enabled() -> bool {
    false // dev mode: auth disabled by default
}

fn default_username() -> String {
    "root".to_string()
}

#[derive(serde::Deserialize)]
struct MetadataConfig {
    #[allow(dead_code)]
    fdb_cluster_file: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("nova=info".parse()?),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            config,
            node_id,
            raft_peers,
        } => {
            tracing::info!(config = %config, "Starting Nova coordinator");

            let cfg: Config = figment::Figment::new()
                .merge(figment::providers::Toml::file(config.clone()))
                .merge(figment::providers::Env::prefixed("NOVA_"))
                .extract()?;

            let cluster_file = cfg
                .metadata
                .fdb_cluster_file
                .as_deref()
                .unwrap_or("docker:docker@127.0.0.1:4500");
            tracing::info!(cluster = %cluster_file, "Using FoundationDB metadata store");
            let fdb_store = Arc::new(FdbMetadataStore::open(cluster_file)?);
            let meta: Arc<dyn MetadataStore> = fdb_store.clone();

            // Setup MinIO object store
            let store = object_store::aws::AmazonS3Builder::new()
                .with_endpoint(&cfg.storage.s3_endpoint)
                .with_access_key_id(&cfg.storage.s3_access_key)
                .with_secret_access_key(&cfg.storage.s3_secret_key)
                .with_bucket_name(&cfg.storage.s3_bucket)
                .with_region(&cfg.storage.s3_region)
                .with_allow_http(true)
                .build()?;

            let store: Arc<dyn ObjectStore> = Arc::new(store);

            let writer = MpWriter::new(store.clone(), cfg.storage.s3_bucket);
            let reader = MpReader::new(store);

            let executor = Arc::new(Executor::new(meta, writer, reader));

            // Phase 6: Initialize Auth, Health, Monitoring
            let auth = if cfg.auth.enabled {
                Some(Arc::new(AuthManager::with_default_user(
                    &cfg.auth.default_username,
                    &cfg.auth.default_password_hash,
                )))
            } else {
                None
            };
            tracing::info!(
                enabled = cfg.auth.enabled,
                "Auth manager initialized (default user: root)"
            );

            let _health = HealthChecker::new("nova-coordinator-1");
            tracing::info!("Health checker initialized");

            let _monitoring = QueryMetrics::default();
            tracing::info!("Monitoring initialized (query metrics ready)");

            // Phase 4: Initialize WorkerPool (single-node mode — self-registered)
            let worker_pool = Arc::new(nova_coordinator::worker_pool::WorkerPool::new());
            let _self_worker_id = worker_pool
                .register("127.0.0.1:0".to_string())
                .await
                .map_err(|e| anyhow::anyhow!("failed to register self as worker: {}", e))?;
            tracing::info!("WorkerPool initialized (single-node mode, self-registered)");

            // Phase 15: Start Raft node (single-node by default, cluster if --raft-peers set)
            {
                let peers: std::collections::HashMap<u64, String> = if raft_peers.is_empty() {
                    Default::default()
                } else {
                    raft_peers
                        .split(',')
                        .filter_map(|s| {
                            let (id, addr) = s.split_once('=')?;
                            Some((id.parse::<u64>().ok()?, addr.to_string()))
                        })
                        .collect()
                };
                let raft_node = nova_coordinator::raft_transport::NovaRaftNode::start_durable(
                    node_id,
                    peers,
                    fdb_store.clone(),
                    cluster_file,
                )
                .await
                .map_err(|e| anyhow::anyhow!("raft start failed: {e:?}"))?;
                if raft_node.leader_id().is_none() {
                    raft_node.initialize_single().await.ok();
                }
                // Start Raft gRPC server on port cfg.server.port + 10000
                let raft_grpc_addr: std::net::SocketAddr =
                    format!("{}:{}", cfg.server.host, cfg.server.port as u32 + 10000)
                        .parse()
                        .unwrap_or_else(|_| "0.0.0.0:13306".parse().unwrap());
                let raft_svc = raft_node.grpc_server();
                tokio::spawn(async move {
                    use nova_coordinator::raft_transport::raft_service_server::RaftServiceServer;
                    tracing::info!(addr = %raft_grpc_addr, "Raft gRPC server listening");
                    tonic::transport::Server::builder()
                        .add_service(RaftServiceServer::new(raft_svc))
                        .serve(raft_grpc_addr)
                        .await
                        .expect("Raft gRPC server error");
                });
                tracing::info!(node_id, "Raft node started");
                std::mem::forget(raft_node);
            }

            // Worker registry gRPC service (worker -> coordinator)
            {
                let coordinator_grpc_addr: std::net::SocketAddr = "0.0.0.0:50060".parse().unwrap();
                let coordinator_svc =
                    nova_coordinator::coordinator_grpc::CoordinatorGrpcServer::new(
                        worker_pool.clone(),
                    );
                tokio::spawn(async move {
                    tracing::info!(addr = %coordinator_grpc_addr, "Coordinator gRPC registry listening");
                    tonic::transport::Server::builder()
                        .add_service(
                            nova_coordinator::coordinator_grpc::CoordinatorServiceServer::new(
                                coordinator_svc,
                            ),
                        )
                        .serve(coordinator_grpc_addr)
                        .await
                        .expect("Coordinator gRPC registry error");
                });
            }

            // Phase 15: Start AutoScaler background loop
            {
                let worker_pool_for_scaler = worker_pool.clone();
                tokio::spawn(async move {
                    let scaler = nova_coordinator::auto_scaling::AutoScaler::new(
                        nova_coordinator::auto_scaling::ScalingPolicy::default(),
                        nova_coordinator::auto_scaling::WarehouseSize::Small,
                    );
                    loop {
                        let workers: Vec<_> = worker_pool_for_scaler.active_workers().await;
                        let decision = scaler.evaluate(&workers);
                        if !matches!(
                            decision,
                            nova_coordinator::auto_scaling::ScalingDecision::NoAction
                        ) {
                            tracing::info!(?decision, "AutoScaler decision");
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    }
                });
                tracing::info!("AutoScaler background loop started (30s interval)");
            }

            let engine = Arc::new(NovaEngine::new(executor.clone()));

            // Phase 12: Start background compaction (auto-GC + MP merge)
            nova_coordinator::compaction::start(executor, cfg.compaction.clone());

            // Phase 6: Start HTTP server for /health and /metrics
            let http_addr = format!("{}:{}", cfg.server.host, 9090);
            let health_router = axum::Router::new()
                .route("/health", axum::routing::get(|| async { "ok" }))
                .route(
                    "/metrics",
                    axum::routing::get(|| async {
                        "# Nova Engine Metrics\n# (Prometheus format — counters not yet wired)\n"
                    }),
                );
            let http_addr_clone = http_addr.clone();
            tokio::spawn(async move {
                let listener = tokio::net::TcpListener::bind(&http_addr_clone)
                    .await
                    .expect("failed to bind HTTP port");
                tracing::info!(addr = %http_addr_clone, "HTTP server listening (/health, /metrics)");
                axum::serve(listener, health_router)
                    .await
                    .expect("HTTP server error");
            });
            let addr = format!("{}:{}", cfg.server.host, cfg.server.port);
            let server = MySqlServer::bind(&addr, engine, auth).await?;

            tracing::info!(addr = %addr, "MySQL server listening");

            server.run().await?;
        }
        Commands::Version => {
            println!("nova-core {}", env!("CARGO_PKG_VERSION"));
        }
        Commands::Worker {
            config,
            grpc_addr,
            coordinator_addr,
        } => {
            tracing::info!(
                coordinator = %coordinator_addr,
                "Starting Nova worker — gRPC server + coordinator registration"
            );

            let cfg: Config = figment::Figment::new()
                .merge(figment::providers::Toml::file(config.clone()))
                .merge(figment::providers::Env::prefixed("NOVA_"))
                .extract()?;
            let cluster_file = cfg
                .metadata
                .fdb_cluster_file
                .as_deref()
                .unwrap_or("docker:docker@127.0.0.1:4500");
            let meta: Arc<dyn nova_storage::MetadataStore> =
                Arc::new(FdbMetadataStore::open(cluster_file)?);

            // Setup object store (local for dev)
            let data_dir = "./data/nova-worker-data";
            std::fs::create_dir_all(data_dir)?;
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(data_dir)?,
            );

            let reader = nova_storage::MpReader::new(store);
            let executor = nova_worker::Executor::new(reader);

            // Create worker state
            let state = Arc::new(nova_worker::WorkerState {
                worker_id: std::sync::atomic::AtomicU64::new(0),
                registered: std::sync::atomic::AtomicBool::new(false),
                executor,
                meta,
                stats: tokio::sync::RwLock::new(nova_worker::WorkerStats::default()),
            });

            // Register with coordinator and start heartbeat loop.
            {
                let state_for_hb = state.clone();
                let coordinator = coordinator_addr.clone();
                let advertise_addr = std::env::var("NOVA_WORKER_ADDRESS").unwrap_or_else(|_| {
                    grpc_addr
                        .split_once(':')
                        .map(|(host, _)| if host == "0.0.0.0" { "127.0.0.1" } else { host })
                        .unwrap_or("127.0.0.1")
                        .to_string()
                });
                let grpc_port = grpc_addr
                    .rsplit_once(':')
                    .and_then(|(_, p)| p.parse::<u32>().ok())
                    .unwrap_or(50051);
                tokio::spawn(async move {
                    loop {
                        match nova_coordinator::coordinator_grpc::CoordinatorServiceClient::connect(
                            format!("http://{coordinator}"),
                        )
                        .await
                        {
                            Ok(mut client) => match client
                                .register_worker(
                                    nova_coordinator::coordinator_grpc::RegisterRequest {
                                        address: advertise_addr.clone(),
                                        grpc_port,
                                        memory_limit_bytes: 0,
                                        cpu_count: std::thread::available_parallelism()
                                            .map(|n| n.get() as u32)
                                            .unwrap_or(1),
                                    },
                                )
                                .await
                            {
                                Ok(resp) => {
                                    let worker_id = resp.into_inner().worker_id;
                                    state_for_hb
                                        .worker_id
                                        .store(worker_id, std::sync::atomic::Ordering::SeqCst);
                                    state_for_hb
                                        .registered
                                        .store(true, std::sync::atomic::Ordering::SeqCst);
                                    tracing::info!(worker_id, "Registered with coordinator");
                                    loop {
                                        let stats = state_for_hb.stats.read().await.clone();
                                        if client
                                            .heartbeat(nova_coordinator::coordinator_grpc::HeartbeatRequest {
                                                worker_id,
                                                cpu_usage: stats.cpu_usage,
                                                memory_usage: stats.memory_usage,
                                                active_queries: stats.active_queries,
                                                cache_hit_count: stats.cache_hit_count,
                                                cache_miss_count: stats.cache_miss_count,
                                            })
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                    }
                                }
                                Err(e) => tracing::warn!(error = %e, "Worker registration failed"),
                            },
                            Err(e) => tracing::warn!(error = %e, "Coordinator connect failed"),
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                });
            }

            // Start gRPC server on grpc_addr
            let grpc_socket = grpc_addr
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid gRPC address: {e}"))?;
            let grpc_server = nova_worker::WorkerGrpcServer::new(state);

            tracing::info!(addr = %grpc_addr, "Worker gRPC server listening");
            tonic::transport::Server::builder()
                .add_service(
                    nova_worker::grpc_server::worker_service_server::WorkerServiceServer::new(
                        grpc_server,
                    ),
                )
                .serve(grpc_socket)
                .await?;
            tracing::info!("Worker shutting down.");
        }
    }

    Ok(())
}
