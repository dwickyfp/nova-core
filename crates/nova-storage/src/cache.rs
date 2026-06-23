//! NovaCache — 3-layer cache using Foyer (RAM + SSD).

// TODO: Phase 6 — implement with foyer
// L1: Query Result Cache (2GB RAM + 50GB SSD)
// L2: Metadata Cache (512MB RAM)
// L3: MP Data Cache (4GB RAM + 100GB SSD)

/// 3-layer cache stack for nova-core.
pub struct NovaCache {
    // TODO: Phase 6 — add Foyer hybrid caches
}

impl NovaCache {
    /// Creates a new NovaCache with default settings.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NovaCache {
    fn default() -> Self {
        Self::new()
    }
}
