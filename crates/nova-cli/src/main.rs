//! nova — CLI entry point for nova-core coordinator.

use anyhow::{Context, bail};
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
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
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

#[derive(serde::Deserialize)]
struct AuthConfig {
    #[serde(default = "default_auth_enabled")]
    enabled: bool,
    #[serde(default = "default_username")]
    default_username: String,
    #[serde(default)]
    default_password_hash: String,
    #[serde(default)]
    allow_insecure: bool,
}

fn default_auth_enabled() -> bool {
    false // dev mode: auth disabled by default
}

fn default_username() -> String {
    "root".to_string()
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: default_auth_enabled(),
            default_username: default_username(),
            default_password_hash: String::new(),
            allow_insecure: false,
        }
    }
}

#[derive(serde::Deserialize)]
struct MetadataConfig {
    #[allow(dead_code)]
    fdb_cluster_file: Option<String>,
}

fn parse_config_str(config: &str) -> anyhow::Result<Config> {
    figment::Figment::new()
        .merge(figment::providers::Toml::string(config))
        .extract()
        .context("invalid Nova config")
}

fn apply_env_overrides(
    config: &mut Config,
    vars: impl IntoIterator<Item = (String, String)>,
) -> anyhow::Result<()> {
    for (key, value) in vars {
        match key.as_str() {
            "NOVA_SERVER_HOST" => config.server.host = value,
            "NOVA_SERVER_PORT" => {
                config.server.port = parse_env(&key, &value)?;
            }
            "NOVA_STORAGE_S3_ENDPOINT" => config.storage.s3_endpoint = value,
            "NOVA_STORAGE_S3_BUCKET" => config.storage.s3_bucket = value,
            "NOVA_STORAGE_S3_ACCESS_KEY" => config.storage.s3_access_key = value,
            "NOVA_STORAGE_S3_SECRET_KEY" => config.storage.s3_secret_key = value,
            "NOVA_STORAGE_S3_REGION" => config.storage.s3_region = value,
            "NOVA_METADATA_FDB_CLUSTER_FILE" => config.metadata.fdb_cluster_file = Some(value),
            "NOVA_AUTH_ENABLED" => {
                config.auth.enabled = parse_env(&key, &value)?;
            }
            "NOVA_AUTH_DEFAULT_USERNAME" => config.auth.default_username = value,
            "NOVA_AUTH_DEFAULT_PASSWORD_HASH" => config.auth.default_password_hash = value,
            "NOVA_AUTH_ALLOW_INSECURE" => {
                config.auth.allow_insecure = parse_env(&key, &value)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_env<T>(key: &str, value: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid {key}={value:?}: {error}"))
}

fn validate_config(config: &Config) -> anyhow::Result<()> {
    if !config.auth.enabled && is_wildcard_host(&config.server.host) && !config.auth.allow_insecure
    {
        bail!(
            "refusing auth.enabled=false on wildcard host {}; set auth.allow_insecure=true for explicit dev mode",
            config.server.host
        );
    }

    if !config.auth.allow_insecure && uses_non_loopback_http(&config.storage.s3_endpoint) {
        bail!(
            "storage.s3_endpoint uses HTTP for non-loopback host {}; set auth.allow_insecure=true for explicit dev mode",
            config.storage.s3_endpoint
        );
    }

    Ok(())
}

fn is_wildcard_host(host: &str) -> bool {
    matches!(host, "0.0.0.0" | "::")
}

fn uses_non_loopback_http(endpoint: &str) -> bool {
    let Some(rest) = endpoint
        .get(..7)
        .filter(|scheme| scheme.eq_ignore_ascii_case("http://"))
        .and_then(|_| endpoint.get(7..))
    else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or(bracketed)
    } else {
        authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host)
            .split(':')
            .next()
            .unwrap_or(authority)
    };

    !is_loopback_host(host)
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|ip_addr| ip_addr.is_loopback())
}

fn load_config(path: impl AsRef<Path>) -> anyhow::Result<Config> {
    let path = path.as_ref();
    if !path.exists() {
        bail!("config file not found: {}", path.display());
    }

    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file: {}", path.display()))?;
    let mut config = parse_config_str(&contents)?;
    apply_env_overrides(&mut config, std::env::vars())?;
    validate_config(&config)?;
    Ok(config)
}

fn object_store_from_config(config: &StorageConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&config.s3_endpoint)
        .with_access_key_id(&config.s3_access_key)
        .with_secret_access_key(&config.s3_secret_key)
        .with_bucket_name(&config.s3_bucket)
        .with_region(&config.s3_region)
        .with_allow_http(true)
        .build()?;
    Ok(Arc::new(store))
}

fn raft_cluster_file_path(cluster_file: &str) -> anyhow::Result<String> {
    if Path::new(cluster_file).exists() || !cluster_file.contains('@') {
        return Ok(cluster_file.to_string());
    }
    let mut hasher = DefaultHasher::new();
    cluster_file.hash(&mut hasher);
    let hash = hasher.finish();
    let path = std::env::temp_dir().join(format!("nova-core-fdb-{hash:016x}.cluster"));
    std::fs::write(&path, cluster_file)
        .with_context(|| format!("FDB cluster file write failed: {}", path.display()))?;
    Ok(path.to_string_lossy().into_owned())
}

fn raft_grpc_addr(host: &str, mysql_port: u16) -> anyhow::Result<SocketAddr> {
    let port = mysql_port
        .checked_add(10_000)
        .with_context(|| format!("Raft gRPC port overflow for MySQL port {mysql_port}"))?;
    format!("{host}:{port}")
        .parse()
        .with_context(|| format!("invalid Raft gRPC address: {host}:{port}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("nova=info".parse()?),
        )
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing subscriber: {error}"))?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            config,
            node_id,
            raft_peers,
        } => {
            tracing::info!(config = %config, "Starting Nova coordinator");

            let cfg = load_config(&config)?;

            let cluster_file = cfg
                .metadata
                .fdb_cluster_file
                .as_deref()
                .unwrap_or("docker:docker@127.0.0.1:4500");
            tracing::info!(cluster = %cluster_file, "Using FoundationDB metadata store");
            let fdb_store = Arc::new(FdbMetadataStore::open(cluster_file)?);
            let meta: Arc<dyn MetadataStore> = fdb_store.clone();

            // Setup configured object store
            let store = object_store_from_config(&cfg.storage)?;

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
                default_username = %cfg.auth.default_username,
                "Auth manager initialized"
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
                let raft_cluster_file = raft_cluster_file_path(cluster_file)?;
                let raft_node = nova_coordinator::raft_transport::NovaRaftNode::start_durable(
                    node_id,
                    peers,
                    fdb_store.clone(),
                    &raft_cluster_file,
                )
                .await
                .map_err(|e| anyhow::anyhow!("raft start failed: {e:?}"))?;
                if raft_node.leader_id().is_none() {
                    raft_node.initialize_single().await.ok();
                }
                // Start Raft gRPC server on port cfg.server.port + 10000
                let raft_grpc_addr = raft_grpc_addr(&cfg.server.host, cfg.server.port)?;
                let raft_svc = raft_node.grpc_server();
                tokio::spawn(async move {
                    use nova_coordinator::raft_transport::raft_service_server::RaftServiceServer;
                    tracing::info!(addr = %raft_grpc_addr, "Raft gRPC server listening");
                    if let Err(error) = tonic::transport::Server::builder()
                        .add_service(RaftServiceServer::new(raft_svc))
                        .serve(raft_grpc_addr)
                        .await
                    {
                        tracing::error!(%error, "Raft gRPC server stopped");
                    }
                });
                tracing::info!(node_id, "Raft node started");
                std::mem::forget(raft_node);
            }

            // Worker registry gRPC service (worker -> coordinator)
            {
                let coordinator_grpc_addr: SocketAddr = "0.0.0.0:50060"
                    .parse()
                    .context("invalid coordinator gRPC registry address")?;
                let coordinator_svc =
                    nova_coordinator::coordinator_grpc::CoordinatorGrpcServer::new(
                        worker_pool.clone(),
                    );
                tokio::spawn(async move {
                    tracing::info!(addr = %coordinator_grpc_addr, "Coordinator gRPC registry listening");
                    if let Err(error) = tonic::transport::Server::builder()
                        .add_service(
                            nova_coordinator::coordinator_grpc::CoordinatorServiceServer::new(
                                coordinator_svc,
                            ),
                        )
                        .serve(coordinator_grpc_addr)
                        .await
                    {
                        tracing::error!(%error, "Coordinator gRPC registry stopped");
                    }
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
                match tokio::net::TcpListener::bind(&http_addr_clone).await {
                    Ok(listener) => {
                        tracing::info!(addr = %http_addr_clone, "HTTP server listening (/health, /metrics)");
                        if let Err(error) = axum::serve(listener, health_router).await {
                            tracing::error!(%error, "HTTP server stopped");
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, addr = %http_addr_clone, "failed to bind HTTP server")
                    }
                }
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

            let cfg = load_config(&config)?;
            let cluster_file = cfg
                .metadata
                .fdb_cluster_file
                .as_deref()
                .unwrap_or("docker:docker@127.0.0.1:4500");
            let meta: Arc<dyn nova_storage::MetadataStore> =
                Arc::new(FdbMetadataStore::open(cluster_file)?);

            // Setup configured object store; workers stay stateless and read coordinator-written MPs.
            let store = object_store_from_config(&cfg.storage)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_auth(host: &str, auth: &str) -> String {
        format!(
            r#"
[server]
host = "{host}"
port = 3306

[storage]
s3_endpoint = "http://localhost:9000"
s3_bucket = "nova"
s3_access_key = "placeholder-access-key"
s3_secret_key = "placeholder-secret-key"
s3_region = "us-east-1"

[metadata]
fdb_cluster_file = "docker:docker@127.0.0.1:4500"

{auth}
"#
        )
    }

    #[test]
    fn config_example_matches_cli_schema() {
        let cfg = parse_config_str(include_str!("../../../config.toml.example")).unwrap();

        assert_eq!(cfg.storage.s3_bucket, "nova");
        validate_config(&cfg).unwrap();
    }

    #[test]
    fn missing_auth_section_uses_root_dev_default() {
        let cfg = parse_config_str(&config_with_auth("127.0.0.1", "")).unwrap();

        assert!(!cfg.auth.enabled);
        assert_eq!(cfg.auth.default_username, "root");
        assert!(!cfg.auth.allow_insecure);
    }

    #[test]
    fn docker_config_declares_insecure_dev_mode_explicitly() {
        let cfg = parse_config_str(include_str!("../../../docker/config-fdb.toml")).unwrap();

        assert!(!cfg.auth.enabled);
        assert!(cfg.auth.allow_insecure);
        validate_config(&cfg).unwrap();
    }

    #[test]
    fn validation_rejects_auth_disabled_on_wildcard_host_without_override() {
        let cfg = parse_config_str(&config_with_auth(
            "0.0.0.0",
            r#"[auth]
enabled = false
"#,
        ))
        .unwrap();

        let err = validate_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("auth.enabled=false"));
        assert!(err.contains("auth.allow_insecure=true"));
    }

    #[test]
    fn validation_rejects_non_loopback_http_storage_without_override() {
        let mut cfg = parse_config_str(&config_with_auth(
            "127.0.0.1",
            r#"[auth]
enabled = false
"#,
        ))
        .unwrap();
        cfg.storage.s3_endpoint = "http://minio:9000".to_string();

        let err = validate_config(&cfg).unwrap_err().to_string();
        assert!(err.contains("storage.s3_endpoint"));
        assert!(err.contains("auth.allow_insecure=true"));
    }

    #[test]
    fn validation_allows_enabled_auth_without_unused_default_password() {
        let cfg = parse_config_str(&config_with_auth(
            "127.0.0.1",
            r#"[auth]
enabled = true
default_username = "root"
default_password_hash = ""
"#,
        ))
        .unwrap();

        validate_config(&cfg).unwrap();
    }

    #[test]
    fn missing_config_file_reports_path() {
        let path = std::env::temp_dir().join("nova-cli-missing-config-for-test.toml");

        let err = match load_config(&path) {
            Ok(_) => panic!("missing config unexpectedly loaded"),
            Err(error) => error.to_string(),
        };

        assert!(err.contains("config file not found"));
        assert!(err.contains("nova-cli-missing-config-for-test.toml"));
    }

    #[test]
    fn explicit_env_overrides_replace_docker_secret_placeholders() {
        let mut cfg = parse_config_str(&config_with_auth(
            "127.0.0.1",
            r#"[auth]
enabled = false
"#,
        ))
        .unwrap();

        apply_env_overrides(
            &mut cfg,
            vec![
                (
                    "NOVA_STORAGE_S3_ACCESS_KEY".to_string(),
                    "dev-access-key".to_string(),
                ),
                (
                    "NOVA_STORAGE_S3_SECRET_KEY".to_string(),
                    "dev-secret-key".to_string(),
                ),
            ],
        )
        .unwrap();

        assert_eq!(cfg.storage.s3_access_key, "dev-access-key");
        assert_eq!(cfg.storage.s3_secret_key, "dev-secret-key");
    }

    #[test]
    fn raft_grpc_addr_is_derived_without_silent_fallback() {
        let addr = raft_grpc_addr("127.0.0.1", 3306).unwrap();
        assert_eq!(addr.to_string(), "127.0.0.1:13306");

        let err = raft_grpc_addr("127.0.0.1", 60000).unwrap_err().to_string();
        assert!(err.contains("Raft gRPC port overflow"));
    }

    #[test]
    fn review_fix_raw_fdb_cluster_contents_are_written_to_cluster_file_for_raft() {
        let raw_cluster = "docker:docker@nova-fdb:4500";

        let path = raft_cluster_file_path(raw_cluster).unwrap();

        assert_ne!(path, raw_cluster);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), raw_cluster);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn review_fix_existing_fdb_cluster_file_path_is_reused_for_raft() {
        let path = std::env::temp_dir().join(format!(
            "nova-cli-existing-cluster-{}-{}.cluster",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "docker:docker@127.0.0.1:4500").unwrap();

        let resolved = raft_cluster_file_path(path.to_str().unwrap()).unwrap();

        assert_eq!(resolved, path.to_string_lossy());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn review_fix_configured_object_store_accepts_docker_storage_config() {
        let mut cfg = parse_config_str(include_str!("../../../docker/config-fdb.toml")).unwrap();
        apply_env_overrides(
            &mut cfg,
            vec![
                (
                    "NOVA_STORAGE_S3_ACCESS_KEY".to_string(),
                    "dev-access-key".to_string(),
                ),
                (
                    "NOVA_STORAGE_S3_SECRET_KEY".to_string(),
                    "dev-secret-key".to_string(),
                ),
            ],
        )
        .unwrap();

        let _store = object_store_from_config(&cfg.storage).unwrap();
    }

    #[test]
    fn review_fix_docker_workers_do_not_mount_persistent_data_volumes() {
        let compose = include_str!("../../../docker/docker-compose.yml");

        assert!(!compose.contains("worker1_cache"));
        assert!(!compose.contains("worker2_cache"));
        assert!(!compose.contains("nova-worker-data"));
    }

    #[test]
    fn review_fix_invalid_server_port_env_returns_clear_error() {
        let mut cfg = parse_config_str(&config_with_auth("127.0.0.1", "")).unwrap();

        let err = apply_env_overrides(
            &mut cfg,
            vec![("NOVA_SERVER_PORT".to_string(), "not-a-port".to_string())],
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("NOVA_SERVER_PORT"));
        assert!(err.contains("not-a-port"));
    }

    #[test]
    fn review_fix_invalid_auth_enabled_env_returns_clear_error() {
        let mut cfg = parse_config_str(&config_with_auth("127.0.0.1", "")).unwrap();

        let err = apply_env_overrides(
            &mut cfg,
            vec![("NOVA_AUTH_ENABLED".to_string(), "maybe".to_string())],
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("NOVA_AUTH_ENABLED"));
        assert!(err.contains("maybe"));
    }

    #[test]
    fn review_fix_invalid_auth_allow_insecure_env_returns_clear_error() {
        let mut cfg = parse_config_str(&config_with_auth("127.0.0.1", "")).unwrap();

        let err = apply_env_overrides(
            &mut cfg,
            vec![("NOVA_AUTH_ALLOW_INSECURE".to_string(), "maybe".to_string())],
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("NOVA_AUTH_ALLOW_INSECURE"));
        assert!(err.contains("maybe"));
    }

    #[test]
    fn review_fix_validation_rejects_uppercase_http_non_loopback_storage() {
        let mut cfg = parse_config_str(&config_with_auth("127.0.0.1", "")).unwrap();
        cfg.storage.s3_endpoint = "HTTP://minio:9000".to_string();

        let err = validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("storage.s3_endpoint"));
        assert!(err.contains("auth.allow_insecure=true"));
    }

    #[test]
    fn review_fix_validation_rejects_127_prefix_hostname_storage() {
        let mut cfg = parse_config_str(&config_with_auth("127.0.0.1", "")).unwrap();
        cfg.storage.s3_endpoint = "http://127.evil.example:9000".to_string();

        let err = validate_config(&cfg).unwrap_err().to_string();

        assert!(err.contains("storage.s3_endpoint"));
        assert!(err.contains("auth.allow_insecure=true"));
    }

    #[test]
    fn review_fix_validation_allows_loopback_http_storage() {
        let mut cfg = parse_config_str(&config_with_auth("127.0.0.1", "")).unwrap();
        cfg.storage.s3_endpoint = "HTTP://LOCALHOST:9000".to_string();
        validate_config(&cfg).unwrap();

        cfg.storage.s3_endpoint = "http://127.12.34.56:9000".to_string();
        validate_config(&cfg).unwrap();

        cfg.storage.s3_endpoint = "http://[::1]:9000".to_string();
        validate_config(&cfg).unwrap();
    }
}
