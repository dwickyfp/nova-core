//! Authentication & RBAC — user authentication and privilege checking.
//!
//! Phase 6: Wire auth manager with password hashing (argon2) and
//! integrate with RBAC for privilege checking.

use nova_common::{NovaError, Result, UserId};
use std::collections::HashMap;

use crate::rbac::RbacManager;

/// Authentication manager — verifies user credentials.
pub struct AuthManager {
    rbac: RbacManager,
    /// In-memory user → password_hash map (production: store in FDB/sled).
    users: HashMap<String, UserInfo>,
}

/// User authentication info.
#[derive(Debug, Clone)]
pub struct UserInfo {
    pub user_id: UserId,
    pub username: String,
    pub password_hash: String,
    pub is_admin: bool,
}

impl AuthManager {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        // Default admin user (root, no password — for dev mode)
        users.insert(
            "root".to_string(),
            UserInfo {
                user_id: 1,
                username: "root".to_string(),
                password_hash: String::new(), // empty = no password required
                is_admin: true,
            },
        );
        Self {
            rbac: RbacManager::new(),
            users,
        }
    }

    /// Create auth manager with a custom default user.
    pub fn with_default_user(username: &str, password: &str) -> Self {
        let mut auth = Self::new();
        if !password.is_empty() {
            let salt = argon2::password_hash::SaltString::generate(
                &mut argon2::password_hash::rand_core::OsRng,
            );
            let hash = argon2::PasswordHasher::hash_password(
                &argon2::Argon2::default(),
                password.as_bytes(),
                &salt,
            )
            .map(|h| h.to_string())
            .unwrap_or_default();
            auth.users.insert(
                username.to_string(),
                UserInfo {
                    user_id: 1,
                    username: username.to_string(),
                    password_hash: hash,
                    is_admin: true,
                },
            );
        }
        auth
    }

    /// Authenticate a user by username and password.
    /// Returns Ok(user_id) on success, Err on failure.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<UserId> {
        let user = self
            .users
            .get(username)
            .ok_or_else(|| NovaError::Internal {
                message: format!("user '{}' not found", username),
            })?;

        // Empty hash = no password required (dev mode)
        if user.password_hash.is_empty() {
            return Ok(user.user_id);
        }

        // Verify password with argon2
        use argon2::PasswordVerifier;
        let parsed_hash =
            argon2::PasswordHash::new(&user.password_hash).map_err(|e| NovaError::Internal {
                message: format!("password hash parse failed: {}", e),
            })?;
        argon2::Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| NovaError::Internal {
                message: "invalid password".to_string(),
            })?;
        Ok(user.user_id)
    }

    /// Check if user has admin privileges.
    pub fn is_admin(&self, username: &str) -> bool {
        self.users
            .get(username)
            .map(|u| u.is_admin)
            .unwrap_or(false)
    }

    /// Get the RBAC manager for privilege checking.
    pub fn rbac(&self) -> &RbacManager {
        &self.rbac
    }

    /// Add a new user.
    pub fn add_user(&mut self, username: &str, password: &str, is_admin: bool) -> Result<UserId> {
        let user_id = (self.users.len() as u64) + 1;
        let hash = if password.is_empty() {
            String::new()
        } else {
            let salt = argon2::password_hash::SaltString::generate(
                &mut argon2::password_hash::rand_core::OsRng,
            );
            argon2::PasswordHasher::hash_password(
                &argon2::Argon2::default(),
                password.as_bytes(),
                &salt,
            )
            .map(|h| h.to_string())
            .map_err(|e| NovaError::Internal {
                message: format!("password hash failed: {}", e),
            })?
        };
        self.users.insert(
            username.to_string(),
            UserInfo {
                user_id,
                username: username.to_string(),
                password_hash: hash,
                is_admin,
            },
        );
        Ok(user_id)
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_root_no_password() {
        let auth = AuthManager::new();
        let result = auth.authenticate("root", "");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_auth_unknown_user() {
        let auth = AuthManager::new();
        let result = auth.authenticate("nonexistent", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_is_admin() {
        let auth = AuthManager::new();
        assert!(auth.is_admin("root"));
        assert!(!auth.is_admin("nonexistent"));
    }

    #[test]
    fn test_auth_add_user() {
        let mut auth = AuthManager::new();
        let uid = auth.add_user("alice", "secret123", false).unwrap();
        assert_eq!(uid, 2);
        assert!(auth.authenticate("alice", "secret123").is_ok());
        assert!(auth.authenticate("alice", "wrong").is_err());
        assert!(!auth.is_admin("alice"));
    }
}
