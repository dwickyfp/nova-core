//! nova-coordinator — SQL parsing, optimization, planning, scheduling.

pub mod analyzer;
pub mod auth;
pub mod cache;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod scheduler;

// Re-exports
pub use optimizer::NovaOptimizer;
pub use parser::SqlParser;
pub use planner::QueryPlanner;
pub use scheduler::QueryScheduler;
