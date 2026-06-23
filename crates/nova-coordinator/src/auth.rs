//! Authentication & RBAC.

// TODO: Phase 6

pub struct AuthManager;

impl AuthManager {
    pub fn new() -> Self {
        Self
    }
    // TODO: Phase 6
    // pub fn authenticate(&self, user: &str, password: &str) -> Result<UserMeta> { ... }
    // pub fn check_privilege(&self, user: &UserId, object: &ObjectId, action: &Action) -> Result<()> { ... }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}
