//! Executor — DataFusion-based query execution with custom operators.

// TODO: Phase 2 Milestone 2.1 — integrate DataFusion execution

pub struct Executor {
    // TODO: Phase 2 — add DataFusion SessionContext + custom operators
}

impl Executor {
    pub fn new() -> Self {
        Self {}
    }
    // TODO: Phase 2
    // pub async fn execute(&self, plan: PhysicalPlan) -> Result<SendableRecordBatchStream> { ... }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}
