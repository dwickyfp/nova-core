//! nova — CLI entry point for nova-core coordinator and worker.

use clap::{Parser, Subcommand};

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
    /// Start the coordinator (SQL parsing, optimization, scheduling).
    Server {
        /// Path to config file.
        #[arg(short, long, default_value = "config.toml")]
        config: String,
    },
    /// Start a worker (execution engine, cache, storage I/O).
    Worker {
        /// Path to config file.
        #[arg(short, long, default_value = "config.toml")]
        config: String,
    },
    /// Show version info.
    Version,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("nova=info".parse()?),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config } => {
            tracing::info!(config = %config, "Starting Nova coordinator");
            // TODO: Phase 1 Milestone 1.6 — start coordinator
            println!("Nova coordinator starting... (not yet implemented)");
        }
        Commands::Worker { config } => {
            tracing::info!(config = %config, "Starting Nova worker");
            // TODO: Phase 4 Milestone 4.2 — start worker
            println!("Nova worker starting... (not yet implemented)");
        }
        Commands::Version => {
            println!("nova-core {}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}
