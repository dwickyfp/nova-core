//! Optimizer — Cost-Based Optimizer (CBO) with custom Nova rules.

// TODO: Phase 2 Milestone 2.4 — integrate DataFusion optimizer + custom rules

/// Nova optimizer: DataFusion base + custom rules (MP pruning, runtime filter, etc.)
pub struct NovaOptimizer;

impl NovaOptimizer {
    pub fn new() -> Self {
        Self
    }
    // TODO: Phase 2
    // pub fn optimize(&self, plan: LogicalPlan, stats: &TableStats) -> Result<LogicalPlan> { ... }
}

impl Default for NovaOptimizer {
    fn default() -> Self {
        Self::new()
    }
}
