//! nova — CLI entry point for nova-core coordinator.

use clap::{Parser, Subcommand};
use figment::providers::Format;
use nova_coordinator::executor::Executor;
use nova_coordinator::mysql_protocol::MySqlServer;
use nova_coordinator::mysql_protocol::nova_engine::NovaEngine;
use nova_storage::{MetadataStore, MpReader, MpWriter, SledMetadataStore};
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
    },
    /// Show version info.
    Version,
}

#[derive(serde::Deserialize)]
struct Config {
    server: ServerConfig,
    storage: StorageConfig,
    metadata: MetadataConfig,
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
struct MetadataConfig {
    #[serde(default = "default_backend")]
    backend: String,
    sled_path: Option<String>,
    fdb_cluster_file: Option<String>,
}

fn default_backend() -> String {
    "sled".to_string()
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
        Commands::Server { config } => {
            tracing::info!(config = %config, "Starting Nova coordinator");

            let cfg: Config = figment::Figment::new()
                .merge(figment::providers::Toml::file(config.clone()))
                .merge(figment::providers::Env::prefixed("NOVA_"))
                .extract()?;

            // Setup metadata store (sled or fdb based on config)
            let meta: Arc<dyn MetadataStore> = match cfg.metadata.backend.as_str() {
                "fdb" => {
                    #[cfg(feature = "fdb-backend")]
                    {
                        let cluster_file = cfg.metadata.fdb_cluster_file
                            .as_deref()
                            .unwrap_or("docker:docker@127.0.0.1:4500");
                        tracing::info!(cluster = %cluster_file, "Using FoundationDB metadata store");
                        Arc::new(nova_storage::FdbMetadataStore::open(cluster_file)?)
                    }
                    #[cfg(not(feature = "fdb-backend"))]
                    {
                        anyhow::bail!("FDB backend not compiled. Rebuild with: cargo build --features nova-storage/fdb-backend");
                    }
                }
                _ => {
                    let sled_path = cfg.metadata.sled_path
                        .as_deref()
                        .unwrap_or("./data/nova-meta");
                    tracing::info!(path = %sled_path, "Using sled metadata store");
                    std::fs::create_dir_all(sled_path)?;
                    Arc::new(SledMetadataStore::open(sled_path)?)
                }
            };

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
            let engine = Arc::new(NovaEngine::new(executor));

            // Start MySQL server
            let addr = format!("{}:{}", cfg.server.host, cfg.server.port);
            let server = MySqlServer::bind(&addr, engine).await?;

            tracing::info!(addr = %addr, "MySQL server listening");

            server.run().await?;
        }
        Commands::Version => {
            println!("nova-core {}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}
