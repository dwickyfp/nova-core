//! nova-coordinator — SQL parsing, optimization, planning, scheduling.

pub mod advanced_stats;
pub mod analyzer;
pub mod auth;
pub mod auto_scaling;
pub mod cache;
pub mod cbo;
pub mod compaction;
pub mod distributed;
pub mod executor;
pub mod grpc_client;
pub mod ha;
pub mod monitoring;
pub mod mp_pruning;
pub mod mysql_protocol;
pub mod mysql_server;
pub mod optimizer;
pub mod optimizer_rules;
pub mod parser;
pub mod planner;
pub mod raft;
pub mod raft_transport;
pub mod rbac;
pub mod result_cache;
pub mod runtime_filter;
pub mod scheduler;
pub mod statistics;
pub mod txn_manager;
pub mod worker_pool;

// Re-exports
pub use executor::Executor;
pub use mysql_server::MySqlServer;
pub use optimizer::NovaOptimizer;
pub use parser::SqlParser;
pub use planner::QueryPlanner;
pub use scheduler::QueryScheduler;
