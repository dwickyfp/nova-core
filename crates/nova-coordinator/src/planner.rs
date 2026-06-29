//! Planner — converts optimized logical plan to physical execution plan.
//!
//! For single-node (Phase 2), planning is a pass-through: the executor
//! handles physical execution directly. When distributed execution (Phase 4)
//! is wired, this will convert to DataFusion physical plans with custom operators.

use nova_common::Result;

use crate::analyzer::ResolvedStatement;

/// Query planner: converts resolved statements to executable plans.
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Plan a resolved statement for local execution.
    /// Returns the statement unchanged (single-node pass-through).
    /// When DataFusion integration is complete, this will return a PhysicalPlan.
    pub fn plan(&self, stmt: ResolvedStatement) -> Result<ResolvedStatement> {
        Ok(stmt)
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::ResolvedStatement;

    #[test]
    fn test_plan_pass_through() {
        let planner = QueryPlanner::new();
        let stmt = ResolvedStatement::CreateDatabase {
            name: "test".to_string(),
        };
        let planned = planner.plan(stmt).unwrap();
        assert!(matches!(planned, ResolvedStatement::CreateDatabase { .. }));
    }
}
