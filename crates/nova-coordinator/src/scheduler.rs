//! Scheduler — dispatches query plans for execution.
//!
//! For single-node (Phase 2), scheduling is local: the executor
//! runs the plan directly on the coordinator.
//! When distributed execution (Phase 4) is wired, this will
//! dispatch fragments to workers via gRPC.

use nova_common::{Result, SecurityContext};

use crate::analyzer::ResolvedStatement;
use crate::executor::{Executor, QueryResult};

/// Query scheduler: executes plans locally (single-node) or dispatches to workers.
pub struct QueryScheduler {
    executor: std::sync::Arc<Executor>,
}

impl QueryScheduler {
    pub fn new(executor: std::sync::Arc<Executor>) -> Self {
        Self { executor }
    }

    /// Execute a planned statement locally.
    pub async fn execute(
        &self,
        stmt: ResolvedStatement,
        security: &SecurityContext,
    ) -> Result<QueryResult> {
        self.executor.execute_with_context(stmt, security).await
    }

    /// Get a reference to the executor (for metadata queries).
    pub fn executor(&self) -> &std::sync::Arc<Executor> {
        &self.executor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use nova_storage::{FdbMetadataStore, MetadataStore};
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_scheduler_create_database() {
        let _dir = TempDir::new().unwrap();
        let meta = Arc::new(
            FdbMetadataStore::open_test(
                "docker:docker@127.0.0.1:4500",
                format!("test_{}", nova_common::now_micros()).into_bytes(),
            )
            .unwrap(),
        ) as Arc<dyn MetadataStore>;
        let store = Arc::new(LocalFileSystem::new()) as Arc<dyn object_store::ObjectStore>;
        let writer = nova_storage::MpWriter::new(store.clone(), "test".to_string());
        let reader = nova_storage::MpReader::new(store);
        let executor = Arc::new(Executor::new(meta, writer, reader));
        let scheduler = QueryScheduler::new(executor);

        let stmt = ResolvedStatement::CreateDatabase {
            name: "testdb".to_string(),
        };
        let security = SecurityContext::root();
        let result = scheduler.execute(stmt, &security).await.unwrap();
        assert!(matches!(result, QueryResult::Success { .. }));
    }
}
