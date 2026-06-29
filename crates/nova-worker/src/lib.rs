//! nova-worker — Execution engine, cache, storage I/O.

pub mod executor;
pub mod operators;
pub mod table_provider;

// Re-exports
pub use executor::Executor;
pub use table_provider::NovaTableProvider;
