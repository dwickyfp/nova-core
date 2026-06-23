//! Scheduler — dispatch query fragments to workers, collect results.

// TODO: Phase 4 Milestone 4.2

pub struct QueryScheduler;

impl QueryScheduler {
    pub fn new() -> Self {
        Self
    }
    // TODO: Phase 4
    // pub async fn execute(&self, plan: PhysicalPlan) -> Result<QueryResult> { ... }
}

impl Default for QueryScheduler {
    fn default() -> Self {
        Self::new()
    }
}
