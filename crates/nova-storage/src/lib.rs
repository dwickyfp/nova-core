//! nova-storage — Micro-partition read/write, metadata operations, cache.

pub mod backup;
pub mod cache;
pub mod metadata;
pub mod mp_reader;
pub mod mp_writer;

// Re-exports
pub use cache::NovaCache;
pub use mp_reader::MpReader;
pub use mp_writer::MpWriter;

#[cfg(feature = "fdb-backend")]
pub use metadata::fdb_store::FdbMetadataStore;

pub use metadata::{MetadataStore, SecurityStore};
