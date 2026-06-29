//! Executor — DataFusion-based query execution with custom operators.
//!
//! Phase 8: Real implementation using DataFusion SessionContext.
//! The worker executor creates a SessionContext, registers NovaTableProvider,
//! and executes SQL queries using DataFusion's push-based execution engine.

use datafusion::prelude::SessionContext;
use nova_common::{MicroPartitionMeta, TableMeta};
use nova_storage::MpReader;
use std::sync::Arc;

use crate::table_provider::NovaTableProvider;

/// Worker executor — runs SQL queries via DataFusion.
pub struct Executor {
    reader: Arc<MpReader>,
}

impl Executor {
    pub fn new(reader: MpReader) -> Self {
        Self {
            reader: Arc::new(reader),
        }
    }

    /// Execute a SQL query against a registered table.
    /// Returns Arrow RecordBatches.
    pub async fn execute(
        &self,
        sql: &str,
        table_meta: &TableMeta,
        mps: &[MicroPartitionMeta],
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, datafusion::common::DataFusionError> {
        let ctx = SessionContext::new();
        let provider =
            NovaTableProvider::new(table_meta.clone(), mps.to_vec(), self.reader.clone());
        ctx.register_table(&table_meta.name, Arc::new(provider))?;
        let df = ctx.sql(sql).await?;
        df.collect().await
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new(MpReader::new(Arc::new(
            object_store::local::LocalFileSystem::new(),
        )))
    }
}
