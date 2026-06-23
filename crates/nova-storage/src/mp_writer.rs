//! MpWriter — Write Arrow RecordBatches as immutable Parquet micro-partitions to S3.

// TODO: Phase 1 Milestone 1.3 — implement Arrow → Parquet → S3 writer

/// Writes micro-partitions as immutable Parquet files to S3.
pub struct MpWriter {
    // TODO: Phase 1 — add object_store + config references
}

impl MpWriter {
    /// Creates a new MpWriter.
    pub fn new() -> Self {
        Self {}
    }

    // TODO: Phase 1 Milestone 1.3
    // pub async fn write(
    //     &self,
    //     table_id: u64,
    //     mp_id: u64,
    //     version: u64,
    //     batches: Vec<RecordBatch>,
    //     txn_id: u64,
    // ) -> Result<MicroPartitionMeta> { ... }
}

impl Default for MpWriter {
    fn default() -> Self {
        Self::new()
    }
}
