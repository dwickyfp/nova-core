// RBAC — Role-Based Access Control.
//
// Phase 6.3: Users, roles, privileges.
// - Users: create, drop, list
// - Roles: create, drop, grant, revoke
// - Privileges: SELECT, INSERT, CREATE, DROP, ADMIN
// - Access control: check privileges before execution

use nova_common::{NovaError, Result, Timestamp};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Database privilege.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Admin,
}

/// User account.
#[derive(Debug, Clone)]
pub struct User {
    pub user_id: u64,
    pub name: String,
    pub roles: HashSet<u64>, // role IDs
    pub created_at: Timestamp,
}

/// Role.
#[derive(Debug, Clone)]
pub struct Role {
    pub role_id: u64,
    pub name: String,
    pub privileges: HashMap<String, HashSet<Privilege>>, // table_name → privileges
    pub created_at: Timestamp,
}

/// RBAC manager — in-memory user/role management.
pub struct RbacManager {
    users: Arc<RwLock<HashMap<u64, User>>>,
    roles: Arc<RwLock<HashMap<u64, Role>>>,
    next_user_id: Arc<RwLock<u64>>,
    next_role_id: Arc<RwLock<u64>>,
}

impl RbacManager {
    pub fn new() -> Self {
        let mut roles = HashMap::new();
        // Built-in admin role
        let mut admin_privs = HashMap::new();
        let mut admin_set = HashSet::new();
        admin_set.insert(Privilege::Admin);
        admin_privs.insert("*".to_string(), admin_set);
        roles.insert(
            1,
            Role {
                role_id: 1,
                name: "admin".to_string(),
                privileges: admin_privs,
                created_at: 0,
            },
        );

        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(roles)),
            next_user_id: Arc::new(RwLock::new(1)),
            next_role_id: Arc::new(RwLock::new(2)),
        }
    }

    pub async fn create_user(&self, name: &str) -> Result<u64> {
        let mut next = self.next_user_id.write().await;
        let id = *next;
        *next += 1;

        let user = User {
            user_id: id,
            name: name.to_string(),
            roles: HashSet::new(),
            created_at: 0,
        };

        self.users.write().await.insert(id, user);
        Ok(id)
    }

    pub async fn drop_user(&self, user_id: u64) -> Result<()> {
        self.users
            .write()
            .await
            .remove(&user_id)
            .ok_or(NovaError::Internal {
                message: format!("user {} not found", user_id),
            })?;
        Ok(())
    }

    pub async fn create_role(&self, name: &str) -> Result<u64> {
        let mut next = self.next_role_id.write().await;
        let id = *next;
        *next += 1;

        let role = Role {
            role_id: id,
            name: name.to_string(),
            privileges: HashMap::new(),
            created_at: 0,
        };

        self.roles.write().await.insert(id, role);
        Ok(id)
    }

    pub async fn grant_role(&self, user_id: u64, role_id: u64) -> Result<()> {
        let mut users = self.users.write().await;
        let user = users.get_mut(&user_id).ok_or(NovaError::Internal {
            message: format!("user {} not found", user_id),
        })?;
        user.roles.insert(role_id);
        Ok(())
    }

    pub async fn revoke_role(&self, user_id: u64, role_id: u64) -> Result<()> {
        let mut users = self.users.write().await;
        let user = users.get_mut(&user_id).ok_or(NovaError::Internal {
            message: format!("user {} not found", user_id),
        })?;
        user.roles.remove(&role_id);
        Ok(())
    }

    pub async fn grant_privilege(
        &self,
        role_id: u64,
        table_name: &str,
        privilege: Privilege,
    ) -> Result<()> {
        let mut roles = self.roles.write().await;
        let role = roles.get_mut(&role_id).ok_or(NovaError::Internal {
            message: format!("role {} not found", role_id),
        })?;
        role.privileges
            .entry(table_name.to_string())
            .or_default()
            .insert(privilege);
        Ok(())
    }

    pub async fn revoke_privilege(
        &self,
        role_id: u64,
        table_name: &str,
        privilege: Privilege,
    ) -> Result<()> {
        let mut roles = self.roles.write().await;
        let role = roles.get_mut(&role_id).ok_or(NovaError::Internal {
            message: format!("role {} not found", role_id),
        })?;
        if let Some(privs) = role.privileges.get_mut(table_name) {
            privs.remove(&privilege);
        }
        Ok(())
    }

    /// Check if user has a specific privilege on a table.
    #[allow(clippy::collapsible_if)]
    pub async fn check_privilege(
        &self,
        user_id: u64,
        table_name: &str,
        privilege: Privilege,
    ) -> bool {
        let users = self.users.read().await;
        let roles = self.roles.read().await;

        let Some(user) = users.get(&user_id) else {
            return false;
        };

        for &role_id in &user.roles {
            if let Some(role) = roles.get(&role_id) {
                // Check for wildcard privilege (admin)
                if let Some(wildcard) = role.privileges.get("*") {
                    if wildcard.contains(&Privilege::Admin) || wildcard.contains(&privilege) {
                        return true;
                    }
                }
                // Check specific table privilege
                if let Some(privs) = role.privileges.get(table_name) {
                    if privs.contains(&privilege) {
                        return true;
                    }
                }
            }
        }

        false
    }

    pub async fn get_user(&self, user_id: u64) -> Option<User> {
        self.users.read().await.get(&user_id).cloned()
    }

    pub async fn list_users(&self) -> Vec<User> {
        self.users.read().await.values().cloned().collect()
    }

    pub async fn list_roles(&self) -> Vec<Role> {
        self.roles.read().await.values().cloned().collect()
    }
}

impl Default for RbacManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_user() {
        let mgr = RbacManager::new();
        let id = mgr.create_user("alice").await.unwrap();
        assert_eq!(id, 1);

        let user = mgr.get_user(id).await.unwrap();
        assert_eq!(user.name, "alice");
    }

    #[tokio::test]
    async fn test_drop_user() {
        let mgr = RbacManager::new();
        let id = mgr.create_user("bob").await.unwrap();
        mgr.drop_user(id).await.unwrap();
        assert!(mgr.get_user(id).await.is_none());
    }

    #[tokio::test]
    async fn test_create_role() {
        let mgr = RbacManager::new();
        let id = mgr.create_role("analyst").await.unwrap();
        assert!(id >= 2); // 1 is reserved for admin
    }

    #[tokio::test]
    async fn test_grant_revoke_role() {
        let mgr = RbacManager::new();
        let uid = mgr.create_user("carol").await.unwrap();
        let rid = mgr.create_role("reader").await.unwrap();

        mgr.grant_role(uid, rid).await.unwrap();
        assert!(mgr.get_user(uid).await.unwrap().roles.contains(&rid));

        mgr.revoke_role(uid, rid).await.unwrap();
        assert!(!mgr.get_user(uid).await.unwrap().roles.contains(&rid));
    }

    #[tokio::test]
    async fn test_grant_privilege() {
        let mgr = RbacManager::new();
        let uid = mgr.create_user("dave").await.unwrap();
        let rid = mgr.create_role("writer").await.unwrap();

        mgr.grant_role(uid, rid).await.unwrap();
        mgr.grant_privilege(rid, "orders", Privilege::Insert)
            .await
            .unwrap();

        assert!(mgr.check_privilege(uid, "orders", Privilege::Insert).await);
        assert!(!mgr.check_privilege(uid, "orders", Privilege::Delete).await);
    }

    #[tokio::test]
    async fn test_revoke_privilege() {
        let mgr = RbacManager::new();
        let uid = mgr.create_user("eve").await.unwrap();
        let rid = mgr.create_role("editor").await.unwrap();

        mgr.grant_role(uid, rid).await.unwrap();
        mgr.grant_privilege(rid, "users", Privilege::Select)
            .await
            .unwrap();
        mgr.grant_privilege(rid, "users", Privilege::Update)
            .await
            .unwrap();

        assert!(mgr.check_privilege(uid, "users", Privilege::Select).await);
        assert!(mgr.check_privilege(uid, "users", Privilege::Update).await);

        mgr.revoke_privilege(rid, "users", Privilege::Update)
            .await
            .unwrap();

        assert!(mgr.check_privilege(uid, "users", Privilege::Select).await);
        assert!(!mgr.check_privilege(uid, "users", Privilege::Update).await);
    }

    #[tokio::test]
    async fn test_admin_has_all_privileges() {
        let mgr = RbacManager::new();
        let uid = mgr.create_user("admin_user").await.unwrap();

        // Grant built-in admin role (id=1)
        mgr.grant_role(uid, 1).await.unwrap();

        // Admin should have all privileges on all tables
        assert!(
            mgr.check_privilege(uid, "any_table", Privilege::Select)
                .await
        );
        assert!(
            mgr.check_privilege(uid, "any_table", Privilege::Insert)
                .await
        );
        assert!(
            mgr.check_privilege(uid, "any_table", Privilege::Delete)
                .await
        );
    }

    #[tokio::test]
    async fn test_no_privilege_for_new_user() {
        let mgr = RbacManager::new();
        let uid = mgr.create_user("guest").await.unwrap();

        assert!(!mgr.check_privilege(uid, "orders", Privilege::Select).await);
    }

    #[tokio::test]
    async fn test_list_users_and_roles() {
        let mgr = RbacManager::new();
        mgr.create_user("u1").await.unwrap();
        mgr.create_user("u2").await.unwrap();
        mgr.create_role("r1").await.unwrap();

        assert_eq!(mgr.list_users().await.len(), 2);
        assert!(mgr.list_roles().await.len() >= 2); // admin + r1
    }

    #[tokio::test]
    async fn test_drop_nonexistent_user() {
        let mgr = RbacManager::new();
        assert!(mgr.drop_user(999).await.is_err());
    }

    #[tokio::test]
    async fn test_grant_role_nonexistent() {
        let mgr = RbacManager::new();
        assert!(mgr.grant_role(999, 1).await.is_err());
    }
}
