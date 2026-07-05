use async_trait::async_trait;
use foundationdb as fdb;
use nova_common::{
    ACCOUNT_OBJECT_ID, ACCOUNTADMIN_ROLE_ID, GrantSetMeta, NovaError, ObjectOwnerMeta, ObjectRef,
    ObjectType, PUBLIC_ROLE_ID, PrivilegeSet, ROOT_USER_ID, Result, RoleGrantMeta, RoleId,
    RoleMeta, SecurityPrivilege, UserId, UserMeta, normalize_ident, now_micros,
};

use super::SecurityStore;
use super::fdb_store::FdbMetadataStore;

impl FdbMetadataStore {
    pub(crate) fn object_type_key(object_type: ObjectType) -> u64 {
        object_type as u64
    }

    async fn security_get<T: serde::de::DeserializeOwned>(
        &self,
        tuple: &impl foundationdb::tuple::TuplePack,
    ) -> Result<Option<T>> {
        self.fdb_get(self.pack(tuple))
            .await?
            .map(|v| Self::deserialize(&v))
            .transpose()
    }

    fn security_kv<T: serde::Serialize>(
        &self,
        tuple: &impl foundationdb::tuple::TuplePack,
        value: &T,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        Ok((self.pack(tuple), Self::serialize(value)?))
    }

    fn security_id_from_sequence(seq: u64, reserved_floor: u64) -> u64 {
        reserved_floor + seq - 1
    }

    async fn security_next_id(&self, name: &str, reserved_floor: u64) -> Result<u64> {
        let seq = self.fdb_atomic_inc(self.pack(&("next_id", name))).await?;
        Ok(Self::security_id_from_sequence(seq, reserved_floor))
    }

    async fn atomic_grant_role_to_user(
        &self,
        user_id: UserId,
        role_id: RoleId,
        granted_by: RoleId,
    ) -> Result<()> {
        let user_key = self.pack(&("user", user_id));
        let role_key = self.pack(&("role", role_id));
        let grantor_key = self.pack(&("role", granted_by));
        let user_role_key = self.pack(&("user_role", user_id, role_id));
        let role_user_key = self.pack(&("role_user", role_id, user_id));
        let epoch_key = self.pack(&("security_epoch",));
        self.db
            .run(|trx, _maybe_committed| {
                let user_key = user_key.clone();
                let role_key = role_key.clone();
                let grantor_key = grantor_key.clone();
                let user_role_key = user_role_key.clone();
                let role_user_key = role_user_key.clone();
                let epoch_key = epoch_key.clone();
                async move {
                    for key in [&user_key, &role_key, &grantor_key] {
                        if trx
                            .get(key, false)
                            .await
                            .map_err(fdb::FdbBindingError::from)?
                            .is_none()
                        {
                            return Err(fdb::FdbBindingError::CustomError(Box::new(
                                std::io::Error::new(
                                    std::io::ErrorKind::NotFound,
                                    "role grant precondition failed",
                                ),
                            )));
                        }
                    }
                    if trx
                        .get(&user_role_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .is_some()
                    {
                        return Ok::<(), fdb::FdbBindingError>(());
                    }
                    let meta = RoleGrantMeta {
                        granted_by_role_id: granted_by,
                        created_at: now_micros(),
                    };
                    let encoded = bincode::serialize(&meta)
                        .map_err(|e| fdb::FdbBindingError::CustomError(Box::new(e)))?;
                    trx.set(&user_role_key, &encoded);
                    trx.set(&role_user_key, b"");
                    let current = trx
                        .get(&epoch_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0);
                    trx.set(&epoch_key, &(current + 1).to_be_bytes()[..]);
                    Ok::<(), fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB atomic role grant failed: {}", e),
            })
    }

    async fn atomic_revoke_role_from_user(&self, user_id: UserId, role_id: RoleId) -> Result<()> {
        let user_role_key = self.pack(&("user_role", user_id, role_id));
        let role_user_key = self.pack(&("role_user", role_id, user_id));
        let epoch_key = self.pack(&("security_epoch",));
        self.db
            .run(|trx, _maybe_committed| {
                let user_role_key = user_role_key.clone();
                let role_user_key = role_user_key.clone();
                let epoch_key = epoch_key.clone();
                async move {
                    if trx
                        .get(&user_role_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .is_none()
                    {
                        return Ok::<(), fdb::FdbBindingError>(());
                    }
                    trx.clear(&user_role_key);
                    trx.clear(&role_user_key);
                    let current = trx
                        .get(&epoch_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0);
                    trx.set(&epoch_key, &(current + 1).to_be_bytes()[..]);
                    Ok::<(), fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB atomic role revoke failed: {}", e),
            })
    }

    async fn atomic_grant_privileges(&self, grant: GrantSetMeta) -> Result<()> {
        let object_type = Self::object_type_key(grant.object.object_type);
        let grant_key = self.pack(&("grant", grant.role_id, object_type, grant.object.object_id));
        let reverse_key = self.pack(&(
            "grant_by_object",
            object_type,
            grant.object.object_id,
            grant.role_id,
        ));
        let epoch_key = self.pack(&("security_epoch",));
        self.db
            .run(|trx, _maybe_committed| {
                let grant = grant.clone();
                let grant_key = grant_key.clone();
                let reverse_key = reverse_key.clone();
                let epoch_key = epoch_key.clone();
                async move {
                    let mut merged = match trx
                        .get(&grant_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                    {
                        Some(bytes) => bincode::deserialize::<GrantSetMeta>(bytes.as_ref())
                            .map_err(|e| fdb::FdbBindingError::CustomError(Box::new(e)))?,
                        None => GrantSetMeta {
                            role_id: grant.role_id,
                            object: grant.object,
                            privileges: PrivilegeSet::empty(),
                            grant_options: PrivilegeSet::empty(),
                            granted_by_role_id: grant.granted_by_role_id,
                            updated_at: grant.updated_at,
                        },
                    };
                    let old_privileges = merged.privileges.bits;
                    let old_grant_options = merged.grant_options.bits;
                    merged.privileges.bits |= grant.privileges.bits;
                    merged.grant_options.bits |= grant.grant_options.bits;
                    if merged.privileges.bits == old_privileges
                        && merged.grant_options.bits == old_grant_options
                    {
                        return Ok::<(), fdb::FdbBindingError>(());
                    }
                    merged.updated_at = now_micros();
                    merged.granted_by_role_id = grant.granted_by_role_id;
                    let encoded = bincode::serialize(&merged)
                        .map_err(|e| fdb::FdbBindingError::CustomError(Box::new(e)))?;
                    trx.set(&grant_key, &encoded);
                    trx.set(&reverse_key, &merged.privileges.bits.to_be_bytes()[..]);
                    let current = trx
                        .get(&epoch_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0);
                    trx.set(&epoch_key, &(current + 1).to_be_bytes()[..]);
                    Ok::<(), fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB atomic grant failed: {}", e),
            })
    }

    async fn atomic_revoke_privileges(
        &self,
        role_id: RoleId,
        object: ObjectRef,
        privileges: PrivilegeSet,
    ) -> Result<()> {
        let object_type = Self::object_type_key(object.object_type);
        let grant_key = self.pack(&("grant", role_id, object_type, object.object_id));
        let reverse_key = self.pack(&("grant_by_object", object_type, object.object_id, role_id));
        let epoch_key = self.pack(&("security_epoch",));
        self.db
            .run(|trx, _maybe_committed| {
                let grant_key = grant_key.clone();
                let reverse_key = reverse_key.clone();
                let epoch_key = epoch_key.clone();
                async move {
                    let Some(bytes) = trx
                        .get(&grant_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                    else {
                        return Ok::<(), fdb::FdbBindingError>(());
                    };
                    let mut grant = bincode::deserialize::<GrantSetMeta>(bytes.as_ref())
                        .map_err(|e| fdb::FdbBindingError::CustomError(Box::new(e)))?;
                    let old_privileges = grant.privileges.bits;
                    let old_grant_options = grant.grant_options.bits;
                    grant.privileges.bits &= !privileges.bits;
                    grant.grant_options.bits &= !privileges.bits;
                    if grant.privileges.bits == old_privileges
                        && grant.grant_options.bits == old_grant_options
                    {
                        return Ok::<(), fdb::FdbBindingError>(());
                    }
                    if grant.privileges.is_empty() {
                        trx.clear(&grant_key);
                        trx.clear(&reverse_key);
                    } else {
                        let encoded = bincode::serialize(&grant)
                            .map_err(|e| fdb::FdbBindingError::CustomError(Box::new(e)))?;
                        trx.set(&grant_key, &encoded);
                        trx.set(&reverse_key, &grant.privileges.bits.to_be_bytes()[..]);
                    }
                    let current = trx
                        .get(&epoch_key, false)
                        .await
                        .map_err(fdb::FdbBindingError::from)?
                        .map(|v| {
                            let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0; 8]);
                            u64::from_be_bytes(bytes)
                        })
                        .unwrap_or(0);
                    trx.set(&epoch_key, &(current + 1).to_be_bytes()[..]);
                    Ok::<(), fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB atomic revoke failed: {}", e),
            })
    }

    pub(crate) async fn rbac_clear_keys_for_object(
        &self,
        object: ObjectRef,
    ) -> Result<Vec<Vec<u8>>> {
        let object_type = Self::object_type_key(object.object_type);
        let mut clears = vec![self.pack(&("object_owner", object_type, object.object_id))];
        let (start, end) = self.category_range(&("grant_by_object", object_type, object.object_id));
        for (key, _) in self.fdb_get_range(start, end).await? {
            let unpacked: (String, u64, u64, RoleId) =
                self.subspace
                    .unpack(&key)
                    .map_err(|e| NovaError::Internal {
                        message: format!("FDB tuple unpack failed: {}", e),
                    })?;
            clears.push(self.pack(&("grant", unpacked.3, object_type, object.object_id)));
            clears.push(key);
        }
        Ok(clears)
    }
}

#[async_trait]
impl SecurityStore for FdbMetadataStore {
    async fn bootstrap_security(&self) -> Result<()> {
        let now = now_micros();
        if self.get_role(ACCOUNTADMIN_ROLE_ID).await?.is_none() {
            self.create_role(RoleMeta {
                id: ACCOUNTADMIN_ROLE_ID,
                name: "ACCOUNTADMIN".to_string(),
                owner_role_id: ACCOUNTADMIN_ROLE_ID,
                system: true,
                created_at: now,
                created_by_user_id: ROOT_USER_ID,
                comment: Some("Built-in account administrator role".to_string()),
            })
            .await?;
        }
        if self.get_role(PUBLIC_ROLE_ID).await?.is_none() {
            self.create_role(RoleMeta {
                id: PUBLIC_ROLE_ID,
                name: "PUBLIC".to_string(),
                owner_role_id: ACCOUNTADMIN_ROLE_ID,
                system: true,
                created_at: now,
                created_by_user_id: ROOT_USER_ID,
                comment: Some("Built-in public role".to_string()),
            })
            .await?;
        }
        if self.get_user(ROOT_USER_ID).await?.is_none() {
            self.create_user(UserMeta {
                id: ROOT_USER_ID,
                name: "root".to_string(),
                password_hash: String::new(),
                mysql_native_hash: Vec::new(),
                default_role_id: ACCOUNTADMIN_ROLE_ID,
                disabled: false,
                created_at: now,
                created_by_user_id: ROOT_USER_ID,
                created_by_role_id: ACCOUNTADMIN_ROLE_ID,
                comment: Some("Built-in root user".to_string()),
            })
            .await?;
        }
        self.grant_role_to_user(ROOT_USER_ID, ACCOUNTADMIN_ROLE_ID, ACCOUNTADMIN_ROLE_ID)
            .await?;
        self.grant_role_to_user(ROOT_USER_ID, PUBLIC_ROLE_ID, ACCOUNTADMIN_ROLE_ID)
            .await?;
        let account = ObjectRef::new(ObjectType::Account, ACCOUNT_OBJECT_ID);
        if self.get_object_owner(account).await?.is_none() {
            self.set_object_owner(ObjectOwnerMeta {
                object: account,
                owner_role_id: ACCOUNTADMIN_ROLE_ID,
                created_by_user_id: ROOT_USER_ID,
                created_at: now,
                transferred_at: None,
            })
            .await?;
        }
        Ok(())
    }

    async fn create_user(&self, mut user: UserMeta) -> Result<UserId> {
        let name = normalize_ident(&user.name);
        if self.get_user_by_name(&name).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("user '{}' already exists", user.name),
            });
        }
        if self.get_role(user.default_role_id).await?.is_none() {
            return Err(NovaError::Internal {
                message: format!("default role '{}' not found", user.default_role_id),
            });
        }
        if user.id == 0 {
            user.id = self.security_next_id("user", ROOT_USER_ID + 1).await?;
        } else if self.get_user(user.id).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("user id '{}' already exists", user.id),
            });
        }
        let now = now_micros();
        let public_grant = RoleGrantMeta {
            granted_by_role_id: ACCOUNTADMIN_ROLE_ID,
            created_at: now,
        };
        let mut sets = vec![
            self.security_kv(&("user", user.id), &user)?,
            (
                self.pack(&("user_by_name", name.clone())),
                user.id.to_be_bytes().to_vec(),
            ),
            self.security_kv(&("user_role", user.id, PUBLIC_ROLE_ID), &public_grant)?,
            (
                self.pack(&("role_user", PUBLIC_ROLE_ID, user.id)),
                b"".to_vec(),
            ),
        ];
        if user.default_role_id != PUBLIC_ROLE_ID {
            let default_grant = RoleGrantMeta {
                granted_by_role_id: ACCOUNTADMIN_ROLE_ID,
                created_at: now,
            };
            sets.push(self.security_kv(
                &("user_role", user.id, user.default_role_id),
                &default_grant,
            )?);
            sets.push((
                self.pack(&("role_user", user.default_role_id, user.id)),
                b"".to_vec(),
            ));
        }
        self.fdb_checked_write_batch(
            vec![
                self.pack(&("user", user.id)),
                self.pack(&("user_by_name", name.clone())),
            ],
            sets,
            vec![],
            true,
        )
        .await?;
        Ok(user.id)
    }

    async fn get_user(&self, user_id: UserId) -> Result<Option<UserMeta>> {
        self.security_get(&("user", user_id)).await
    }

    async fn get_user_by_name(&self, name: &str) -> Result<Option<UserMeta>> {
        let key = self.pack(&("user_by_name", normalize_ident(name)));
        let Some(bytes) = self.fdb_get(key).await? else {
            return Ok(None);
        };
        let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
        self.get_user(u64::from_be_bytes(arr)).await
    }

    async fn create_role(&self, mut role: RoleMeta) -> Result<RoleId> {
        let name = normalize_ident(&role.name);
        if self.get_role_by_name(&name).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("role '{}' already exists", role.name),
            });
        }
        if role.id != ACCOUNTADMIN_ROLE_ID && self.get_role(role.owner_role_id).await?.is_none() {
            return Err(NovaError::Internal {
                message: format!("owner role '{}' not found", role.owner_role_id),
            });
        }
        if role.id == 0 {
            role.id = self.security_next_id("role", PUBLIC_ROLE_ID + 1).await?;
        } else if self.get_role(role.id).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("role id '{}' already exists", role.id),
            });
        }
        let owner = ObjectOwnerMeta {
            object: ObjectRef::new(ObjectType::Role, role.id),
            owner_role_id: role.owner_role_id,
            created_by_user_id: role.created_by_user_id,
            created_at: role.created_at,
            transferred_at: None,
        };
        self.fdb_checked_write_batch(
            vec![
                self.pack(&("role", role.id)),
                self.pack(&("role_by_name", name.clone())),
            ],
            vec![
                self.security_kv(&("role", role.id), &role)?,
                (
                    self.pack(&("role_by_name", name)),
                    role.id.to_be_bytes().to_vec(),
                ),
                self.security_kv(
                    &(
                        "object_owner",
                        Self::object_type_key(owner.object.object_type),
                        owner.object.object_id,
                    ),
                    &owner,
                )?,
            ],
            vec![],
            true,
        )
        .await?;
        Ok(role.id)
    }

    async fn get_role(&self, role_id: RoleId) -> Result<Option<RoleMeta>> {
        self.security_get(&("role", role_id)).await
    }

    async fn get_role_by_name(&self, name: &str) -> Result<Option<RoleMeta>> {
        let key = self.pack(&("role_by_name", normalize_ident(name)));
        let Some(bytes) = self.fdb_get(key).await? else {
            return Ok(None);
        };
        let arr: [u8; 8] = bytes.as_slice().try_into().unwrap_or([0; 8]);
        self.get_role(u64::from_be_bytes(arr)).await
    }

    async fn grant_role_to_user(
        &self,
        user_id: UserId,
        role_id: RoleId,
        granted_by: RoleId,
    ) -> Result<()> {
        self.atomic_grant_role_to_user(user_id, role_id, granted_by)
            .await
    }

    async fn revoke_role_from_user(&self, user_id: UserId, role_id: RoleId) -> Result<()> {
        self.atomic_revoke_role_from_user(user_id, role_id).await
    }

    async fn list_user_roles(&self, user_id: UserId) -> Result<Vec<RoleId>> {
        let (start, end) = self.category_range(&("user_role", user_id));
        let mut roles = vec![];
        for (key, _) in self.fdb_get_range(start, end).await? {
            let unpacked: (String, UserId, RoleId) =
                self.subspace
                    .unpack(&key)
                    .map_err(|e| NovaError::Internal {
                        message: format!("FDB tuple unpack failed: {}", e),
                    })?;
            roles.push(unpacked.2);
        }
        Ok(roles)
    }

    async fn set_object_owner(&self, owner: ObjectOwnerMeta) -> Result<()> {
        if self.get_role(owner.owner_role_id).await?.is_none() {
            return Err(NovaError::Internal {
                message: format!("owner role '{}' not found", owner.owner_role_id),
            });
        }
        self.fdb_write_batch(
            vec![self.security_kv(
                &(
                    "object_owner",
                    Self::object_type_key(owner.object.object_type),
                    owner.object.object_id,
                ),
                &owner,
            )?],
            vec![],
            true,
        )
        .await
        .map(|_| ())
    }

    async fn get_object_owner(&self, object: ObjectRef) -> Result<Option<ObjectOwnerMeta>> {
        self.security_get(&(
            "object_owner",
            Self::object_type_key(object.object_type),
            object.object_id,
        ))
        .await
    }

    async fn grant_privileges(&self, grant: GrantSetMeta) -> Result<()> {
        if self.get_role(grant.role_id).await?.is_none() {
            return Err(NovaError::Internal {
                message: format!("role '{}' not found", grant.role_id),
            });
        }
        if self.get_role(grant.granted_by_role_id).await?.is_none() {
            return Err(NovaError::Internal {
                message: format!("grantor role '{}' not found", grant.granted_by_role_id),
            });
        }
        self.atomic_grant_privileges(grant).await
    }

    async fn revoke_privileges(
        &self,
        role_id: RoleId,
        object: ObjectRef,
        privileges: PrivilegeSet,
    ) -> Result<()> {
        self.atomic_revoke_privileges(role_id, object, privileges)
            .await
    }

    async fn get_grant(&self, role_id: RoleId, object: ObjectRef) -> Result<Option<GrantSetMeta>> {
        self.security_get(&(
            "grant",
            role_id,
            Self::object_type_key(object.object_type),
            object.object_id,
        ))
        .await
    }

    async fn list_grants_on_object(&self, object: ObjectRef) -> Result<Vec<GrantSetMeta>> {
        let (start, end) = self.category_range(&(
            "grant_by_object",
            Self::object_type_key(object.object_type),
            object.object_id,
        ));
        let mut grants = vec![];
        for (key, _) in self.fdb_get_range(start, end).await? {
            let unpacked: (String, u64, u64, RoleId) =
                self.subspace
                    .unpack(&key)
                    .map_err(|e| NovaError::Internal {
                        message: format!("FDB tuple unpack failed: {}", e),
                    })?;
            if let Some(grant) = self.get_grant(unpacked.3, object).await? {
                grants.push(grant);
            }
        }
        Ok(grants)
    }

    async fn list_grants_to_role(&self, role_id: RoleId) -> Result<Vec<GrantSetMeta>> {
        let (start, end) = self.category_range(&("grant", role_id));
        self.fdb_get_range(start, end)
            .await?
            .into_iter()
            .map(|(_, v)| Self::deserialize(&v))
            .collect()
    }

    async fn security_epoch(&self) -> Result<u64> {
        let key = self.pack(&("security_epoch",));
        Ok(self
            .fdb_get(key)
            .await?
            .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap_or([0; 8])))
            .unwrap_or(0))
    }

    async fn bump_security_epoch(&self) -> Result<u64> {
        self.fdb_atomic_inc(self.pack(&("security_epoch",))).await
    }
}

#[allow(dead_code)]
fn _privs(privileges: &[SecurityPrivilege]) -> PrivilegeSet {
    PrivilegeSet::from_privileges(privileges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_type_keys_are_stable() {
        assert_eq!(FdbMetadataStore::object_type_key(ObjectType::Account), 0);
        assert_eq!(FdbMetadataStore::object_type_key(ObjectType::Table), 3);
        assert_eq!(FdbMetadataStore::object_type_key(ObjectType::User), 7);
    }

    #[test]
    fn security_ids_start_after_reserved_ids_without_duplicates() {
        assert_eq!(
            FdbMetadataStore::security_id_from_sequence(1, ROOT_USER_ID + 1),
            2
        );
        assert_eq!(
            FdbMetadataStore::security_id_from_sequence(2, ROOT_USER_ID + 1),
            3
        );
        assert_eq!(
            FdbMetadataStore::security_id_from_sequence(1, PUBLIC_ROLE_ID + 1),
            3
        );
        assert_eq!(
            FdbMetadataStore::security_id_from_sequence(2, PUBLIC_ROLE_ID + 1),
            4
        );
    }

    #[tokio::test]
    async fn phase1_security_metadata_persists_when_fdb_configured() -> Result<()> {
        let Ok(cluster_file) = std::env::var("NOVA_FDB_CLUSTER_FILE") else {
            return Ok(());
        };
        let subspace = format!("nova_test_security_{}", now_micros()).into_bytes();
        let store = FdbMetadataStore::open_test(&cluster_file, subspace.clone())?;

        store.bootstrap_security().await?;
        let role_id = store
            .create_role(RoleMeta {
                id: 0,
                name: "analyst".to_string(),
                owner_role_id: ACCOUNTADMIN_ROLE_ID,
                system: false,
                created_at: now_micros(),
                created_by_user_id: ROOT_USER_ID,
                comment: None,
            })
            .await?;
        let user_id = store
            .create_user(UserMeta {
                id: 0,
                name: "alice".to_string(),
                password_hash: String::new(),
                mysql_native_hash: Vec::new(),
                default_role_id: role_id,
                disabled: false,
                created_at: now_micros(),
                created_by_user_id: ROOT_USER_ID,
                created_by_role_id: ACCOUNTADMIN_ROLE_ID,
                comment: None,
            })
            .await?;
        store
            .grant_role_to_user(user_id, role_id, ACCOUNTADMIN_ROLE_ID)
            .await?;
        let object = ObjectRef::new(ObjectType::Table, 42);
        store
            .set_object_owner(ObjectOwnerMeta {
                object,
                owner_role_id: role_id,
                created_by_user_id: ROOT_USER_ID,
                created_at: now_micros(),
                transferred_at: None,
            })
            .await?;
        store
            .grant_privileges(GrantSetMeta {
                role_id,
                object,
                privileges: PrivilegeSet::from_privileges(&[SecurityPrivilege::Select]),
                grant_options: PrivilegeSet::empty(),
                granted_by_role_id: ACCOUNTADMIN_ROLE_ID,
                updated_at: now_micros(),
            })
            .await?;

        let reopened = FdbMetadataStore::open_test(&cluster_file, subspace)?;
        assert_eq!(
            reopened.get_role_by_name("ANALYST").await?.unwrap().id,
            role_id
        );
        assert_eq!(
            reopened.get_user_by_name("alice").await?.unwrap().id,
            user_id
        );
        assert!(reopened.list_user_roles(user_id).await?.contains(&role_id));
        assert_eq!(
            reopened
                .get_object_owner(object)
                .await?
                .unwrap()
                .owner_role_id,
            role_id
        );
        assert!(
            reopened
                .get_grant(role_id, object)
                .await?
                .unwrap()
                .privileges
                .contains(SecurityPrivilege::Select)
        );
        assert!(reopened.security_epoch().await? > 0);
        Ok(())
    }

    #[tokio::test]
    async fn phase1_security_metadata_enforces_enterprise_invariants() -> Result<()> {
        let Ok(cluster_file) = std::env::var("NOVA_FDB_CLUSTER_FILE") else {
            return Ok(());
        };
        let subspace = format!("nova_test_security_invariants_{}", now_micros()).into_bytes();
        let store = FdbMetadataStore::open_test(&cluster_file, subspace)?;
        store.bootstrap_security().await?;
        let epoch_after_bootstrap = store.security_epoch().await?;
        store.bootstrap_security().await?;
        assert_eq!(store.security_epoch().await?, epoch_after_bootstrap);

        let before_role = store.security_epoch().await?;
        let analyst = store
            .create_role(RoleMeta {
                id: 0,
                name: "analyst".to_string(),
                owner_role_id: ACCOUNTADMIN_ROLE_ID,
                system: false,
                created_at: now_micros(),
                created_by_user_id: ROOT_USER_ID,
                comment: None,
            })
            .await?;
        assert_eq!(store.security_epoch().await?, before_role + 1);
        assert!(
            store
                .create_role(RoleMeta {
                    id: 0,
                    name: "ANALYST".to_string(),
                    owner_role_id: ACCOUNTADMIN_ROLE_ID,
                    system: false,
                    created_at: now_micros(),
                    created_by_user_id: ROOT_USER_ID,
                    comment: None,
                })
                .await
                .is_err()
        );
        assert!(
            store
                .create_role(RoleMeta {
                    id: analyst,
                    name: "duplicate_id".to_string(),
                    owner_role_id: ACCOUNTADMIN_ROLE_ID,
                    system: false,
                    created_at: now_micros(),
                    created_by_user_id: ROOT_USER_ID,
                    comment: None,
                })
                .await
                .is_err()
        );
        assert!(
            store
                .create_role(RoleMeta {
                    id: 0,
                    name: "bad_owner".to_string(),
                    owner_role_id: 999_999,
                    system: false,
                    created_at: now_micros(),
                    created_by_user_id: ROOT_USER_ID,
                    comment: None,
                })
                .await
                .is_err()
        );

        assert!(
            store
                .create_user(UserMeta {
                    id: 0,
                    name: "bad_default".to_string(),
                    password_hash: String::new(),
                    mysql_native_hash: Vec::new(),
                    default_role_id: 999_999,
                    disabled: false,
                    created_at: now_micros(),
                    created_by_user_id: ROOT_USER_ID,
                    created_by_role_id: ACCOUNTADMIN_ROLE_ID,
                    comment: None,
                })
                .await
                .is_err()
        );
        let before_user = store.security_epoch().await?;
        let bob = store
            .create_user(UserMeta {
                id: 0,
                name: "bob".to_string(),
                password_hash: String::new(),
                mysql_native_hash: Vec::new(),
                default_role_id: analyst,
                disabled: false,
                created_at: now_micros(),
                created_by_user_id: ROOT_USER_ID,
                created_by_role_id: ACCOUNTADMIN_ROLE_ID,
                comment: None,
            })
            .await?;
        assert_eq!(store.security_epoch().await?, before_user + 1);
        let roles = store.list_user_roles(bob).await?;
        assert!(roles.contains(&PUBLIC_ROLE_ID));
        assert!(roles.contains(&analyst));
        let before_idempotent_grant = store.security_epoch().await?;
        store
            .grant_role_to_user(bob, analyst, ACCOUNTADMIN_ROLE_ID)
            .await?;
        assert_eq!(store.security_epoch().await?, before_idempotent_grant);
        assert!(
            store
                .grant_role_to_user(999_999, analyst, ACCOUNTADMIN_ROLE_ID)
                .await
                .is_err()
        );
        assert!(
            store
                .grant_role_to_user(bob, 999_999, ACCOUNTADMIN_ROLE_ID)
                .await
                .is_err()
        );
        assert!(
            store
                .grant_role_to_user(bob, analyst, 999_999)
                .await
                .is_err()
        );

        let object = ObjectRef::new(ObjectType::Table, 7);
        assert!(
            store
                .set_object_owner(ObjectOwnerMeta {
                    object,
                    owner_role_id: 999_999,
                    created_by_user_id: ROOT_USER_ID,
                    created_at: now_micros(),
                    transferred_at: None,
                })
                .await
                .is_err()
        );
        assert!(
            store
                .grant_privileges(GrantSetMeta {
                    role_id: 999_999,
                    object,
                    privileges: PrivilegeSet::from_privileges(&[SecurityPrivilege::Select]),
                    grant_options: PrivilegeSet::empty(),
                    granted_by_role_id: ACCOUNTADMIN_ROLE_ID,
                    updated_at: now_micros(),
                })
                .await
                .is_err()
        );
        store
            .grant_privileges(GrantSetMeta {
                role_id: analyst,
                object,
                privileges: PrivilegeSet::from_privileges(&[
                    SecurityPrivilege::Select,
                    SecurityPrivilege::Insert,
                ]),
                grant_options: PrivilegeSet::empty(),
                granted_by_role_id: ACCOUNTADMIN_ROLE_ID,
                updated_at: now_micros(),
            })
            .await?;
        store
            .revoke_privileges(
                analyst,
                object,
                PrivilegeSet::from_privileges(&[SecurityPrivilege::Select]),
            )
            .await?;
        let grant = store.get_grant(analyst, object).await?.unwrap();
        assert!(!grant.privileges.contains(SecurityPrivilege::Select));
        assert!(grant.privileges.contains(SecurityPrivilege::Insert));
        Ok(())
    }
}
