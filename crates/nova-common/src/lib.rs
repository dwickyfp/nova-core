//! nova-common — Shared types, errors, and utilities for nova-core.

pub mod error;
pub mod types;

// Re-exports
pub use error::NovaError;
pub use types::*;
