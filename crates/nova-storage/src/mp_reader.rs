//! MpReader — Read micro-partitions from S3 with caching and pruning.

// TODO: Phase 1 Milestone 1.4 — implement S3 → Parquet → Arrow streaming read

/// Reads micro-partitions from S3 (or cache) as Arrow RecordBatch streams.
pub struct MpReader {
    // TODO: Phase 1 — add object_store + cache references
}

impl MpReader {
    /// Creates a new MpReader.
    pub fn new() -> Self {
        Self {}
    }

    // TODO: Phase 1 Milestone 1.4
    // pub async fn read(
    //     &self,
    //     mp: &MicroPartitionMeta,
    //     projection: Option<&[usize]>,
    //     predicate: Option<&Expr>,
    // ) -> Result<SendableRecordBatchStream> { ... }
}

impl Default for MpReader {
    fn default() -> Self {
        Self::new()
    }
}
