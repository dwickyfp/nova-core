//! Analyzer — name resolution, type checking, privilege checking.

// TODO: Phase 1 Milestone 1.5

pub struct Analyzer;

impl Analyzer {
    pub fn new() -> Self {
        Self
    }
    // TODO: resolve table/column names, check types, check RBAC
}

impl Default for Analyzer {
    fn default() -> Self {
        Self::new()
    }
}
