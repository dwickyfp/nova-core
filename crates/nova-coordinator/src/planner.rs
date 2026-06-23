//! Planner — convert optimized logical plan to physical execution plan.

// TODO: Phase 2 Milestone 2.1

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }
    // TODO: Phase 2
    // pub fn plan(&self, logical: LogicalPlan) -> Result<PhysicalPlan> { ... }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}
