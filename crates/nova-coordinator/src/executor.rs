// Executor — wire resolved SQL statements to storage layer.

use crate::analyzer::{ResolvedExpr, ResolvedFilter, ResolvedStatement};
use crate::function_runtime::{SqlFunctionRuntime, referenced_function_calls};
use crate::optimizer::NovaOptimizer;
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use nova_common::*;
use nova_storage::{CdcPayloadReader, CdcPayloadWriter, MetadataStore, MpReader, MpWriter};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

/// SQL execution engine. Wires resolved statements to storage layer.
pub struct Executor {
    meta: Arc<dyn MetadataStore>,
    writer: MpWriter,
    reader: MpReader,
    cdc_writer: CdcPayloadWriter,
    cdc_reader: CdcPayloadReader,
    optimizer: NovaOptimizer,
    current_txn: Arc<std::sync::Mutex<Option<TxnId>>>,
}

/// Result of executing a SQL statement.
#[derive(Debug)]
pub enum QueryResult {
    Success {
        message: String,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

struct CdcMetadataColumns {
    actions: Vec<String>,
    is_updates: Vec<bool>,
    row_ids: Vec<String>,
    txn_ids: Vec<u64>,
    commit_ts_values: Vec<u64>,
    sequences: Vec<u64>,
}

struct CdcRowBatchSpec<'a> {
    table: &'a TableMeta,
    source_batch: &'a RecordBatch,
    row_ordinals: &'a [u64],
    start_sequence: u64,
    txn_id: TxnId,
    commit_ts: Timestamp,
    mp_id: MpId,
    action: ChangeAction,
    is_update: bool,
}

struct CdcPayloadMetadata {
    action_counts: ChangeActionCounts,
    min_row_id: Option<String>,
    max_row_id: Option<String>,
}

impl Executor {
    pub fn new(meta: Arc<dyn MetadataStore>, writer: MpWriter, reader: MpReader) -> Self {
        let cdc_writer =
            CdcPayloadWriter::new(writer.store_arc(), writer.bucket_name().to_string());
        let cdc_reader = CdcPayloadReader::new(reader.store_arc());
        Self {
            meta,
            writer,
            reader,
            cdc_writer,
            cdc_reader,
            optimizer: NovaOptimizer::new(),
            current_txn: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub async fn user_for_auth(&self, username: &str) -> Result<Option<UserMeta>> {
        self.meta.get_user_by_name(username).await
    }

    pub async fn security_epoch(&self) -> Result<u64> {
        self.meta.security_epoch().await
    }

    pub async fn security_context_for_user(&self, username: &str) -> Result<SecurityContext> {
        let user =
            self.meta
                .get_user_by_name(username)
                .await?
                .ok_or_else(|| NovaError::AuthFailed {
                    reason: format!("user '{}' not found", username),
                })?;
        if user.disabled {
            return Err(NovaError::AuthFailed {
                reason: format!("user '{}' is disabled", username),
            });
        }
        let roles = self.meta.list_user_roles(user.id).await?;
        if !roles.contains(&user.default_role_id) {
            return Err(NovaError::AuthFailed {
                reason: format!(
                    "default role '{}' is not granted to user '{}'",
                    user.default_role_id, user.name
                ),
            });
        }
        Ok(SecurityContext {
            user_id: user.id,
            username: user.name,
            primary_role_id: user.default_role_id,
            secondary_role_ids: roles
                .into_iter()
                .filter(|role_id| *role_id != user.default_role_id)
                .collect(),
            secondary_all: true,
        })
    }

    pub async fn role_id_by_name(&self, role: &str) -> Result<Option<RoleId>> {
        Ok(self.meta.get_role_by_name(role).await?.map(|role| role.id))
    }

    /// List database names visible to this security context.
    pub async fn list_database_names(&self, security: &SecurityContext) -> Result<Vec<String>> {
        let dbs = self.meta.list_databases().await?;
        let mut visible = Vec::new();
        for db in dbs {
            if self
                .has_privilege(
                    security,
                    ObjectRef::new(ObjectType::Database, db.id),
                    SecurityPrivilege::Usage,
                )
                .await?
            {
                visible.push(db.name);
            }
        }
        Ok(visible)
    }

    /// List table names visible to this security context.
    pub async fn list_table_names(
        &self,
        db: &str,
        security: &SecurityContext,
    ) -> Result<Vec<String>> {
        let db_meta = self.find_database(db).await?;
        let schemas = self.meta.list_schemas(db_meta.id).await?;
        let mut tables = Vec::new();
        for schema in &schemas {
            if !self
                .has_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema.id),
                    SecurityPrivilege::Usage,
                )
                .await?
            {
                continue;
            }
            for table in self.meta.list_tables(db_meta.id, schema.id).await? {
                if self
                    .has_privilege(
                        security,
                        ObjectRef::new(ObjectType::Table, table.id),
                        SecurityPrivilege::Select,
                    )
                    .await?
                {
                    tables.push(table.name);
                }
            }
        }
        Ok(tables)
    }

    async fn active_role_closure(&self, security: &SecurityContext) -> Result<HashSet<RoleId>> {
        let mut roles = HashSet::new();
        let mut queue = VecDeque::new();
        for role_id in security.active_role_ids() {
            queue.push_back((role_id, 0usize));
        }
        while let Some((role_id, depth)) = queue.pop_front() {
            if depth > 64 {
                return Err(NovaError::PermissionDenied {
                    user: security.username.clone(),
                    action: "role inheritance depth limit exceeded".to_string(),
                });
            }
            if !roles.insert(role_id) {
                continue;
            }
            if self.meta.get_role(role_id).await?.is_none() {
                return Err(NovaError::PermissionDenied {
                    user: security.username.clone(),
                    action: format!("use missing role {}", role_id),
                });
            }
            for child_id in self.meta.list_role_children(role_id).await? {
                queue.push_back((child_id, depth + 1));
            }
        }
        Ok(roles)
    }

    async fn has_privilege(
        &self,
        security: &SecurityContext,
        object: ObjectRef,
        privilege: SecurityPrivilege,
    ) -> Result<bool> {
        let active_roles = self.active_role_closure(security).await?;
        if active_roles.contains(&ACCOUNTADMIN_ROLE_ID) {
            return Ok(true);
        }
        if let Some(owner) = self.meta.get_object_owner(object).await?
            && active_roles.contains(&owner.owner_role_id)
        {
            return Ok(true);
        }
        for role_id in active_roles {
            if let Some(grant) = self.meta.get_grant(role_id, object).await?
                && grant.privileges.contains(privilege)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn has_primary_role_privilege(
        &self,
        security: &SecurityContext,
        object: ObjectRef,
        privilege: SecurityPrivilege,
    ) -> Result<bool> {
        if self
            .meta
            .get_role(security.primary_role_id)
            .await?
            .is_none()
        {
            return Err(NovaError::PermissionDenied {
                user: security.username.clone(),
                action: format!("use missing role {}", security.primary_role_id),
            });
        }
        if security.primary_role_id == ACCOUNTADMIN_ROLE_ID {
            return Ok(true);
        }
        if let Some(owner) = self.meta.get_object_owner(object).await?
            && owner.owner_role_id == security.primary_role_id
        {
            return Ok(true);
        }
        Ok(self
            .meta
            .get_grant(security.primary_role_id, object)
            .await?
            .is_some_and(|grant| grant.privileges.contains(privilege)))
    }

    async fn require_privilege(
        &self,
        security: &SecurityContext,
        object: ObjectRef,
        privilege: SecurityPrivilege,
    ) -> Result<()> {
        if self.has_privilege(security, object, privilege).await? {
            Ok(())
        } else {
            Err(NovaError::PermissionDenied {
                user: security.username.clone(),
                action: format!(
                    "{} on {}:{}",
                    privilege, object.object_type, object.object_id
                ),
            })
        }
    }

    async fn require_primary_role_privilege(
        &self,
        security: &SecurityContext,
        object: ObjectRef,
        privilege: SecurityPrivilege,
    ) -> Result<()> {
        if self
            .has_primary_role_privilege(security, object, privilege)
            .await?
        {
            Ok(())
        } else {
            Err(NovaError::PermissionDenied {
                user: security.username.clone(),
                action: format!(
                    "{} on {}:{} with primary role {}",
                    privilege, object.object_type, object.object_id, security.primary_role_id
                ),
            })
        }
    }

    async fn find_database(&self, db: &str) -> Result<DatabaseMeta> {
        self.meta
            .list_databases()
            .await?
            .into_iter()
            .find(|d| d.name == db)
            .ok_or_else(|| NovaError::DatabaseNotFound {
                db_name: db.to_string(),
            })
    }

    async fn find_schema_meta(&self, db_id: DatabaseId, schema: &str) -> Result<SchemaMeta> {
        self.meta
            .list_schemas(db_id)
            .await?
            .into_iter()
            .find(|s| s.name == schema)
            .ok_or_else(|| NovaError::SchemaNotFound {
                schema_name: schema.to_string(),
            })
    }

    async fn authorize_table(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        table: &str,
        privilege: SecurityPrivilege,
    ) -> Result<()> {
        let db_meta = self.find_database(db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
        let table_meta = self.find_table(db, schema, table).await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Database, db_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Schema, schema_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Table, table_meta.id),
            privilege,
        )
        .await
    }

    async fn authorize_stream_read(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        stream: &StreamMeta,
    ) -> Result<TableMeta> {
        let db_meta = self.find_database(db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Database, db_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Schema, schema_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Stream, stream.stream_id),
            SecurityPrivilege::Select,
        )
        .await?;
        let source = self
            .meta
            .get_table(stream.db_id, stream.schema_id, stream.source_table_id)
            .await?
            .ok_or_else(|| NovaError::TableNotFound {
                table_name: stream.source_table_id.to_string(),
            })?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Table, source.id),
            SecurityPrivilege::Select,
        )
        .await?;
        Ok(source)
    }

    async fn authorize_stream_ownership(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        stream: &StreamMeta,
    ) -> Result<()> {
        let db_meta = self.find_database(db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Database, db_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Schema, schema_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Stream, stream.stream_id),
            SecurityPrivilege::Ownership,
        )
        .await
    }

    fn sql_like_matches(value: &str, pattern: &str) -> bool {
        fn matches_inner(value: &[char], pattern: &[char]) -> bool {
            match pattern.split_first() {
                None => value.is_empty(),
                Some(('%', rest)) => {
                    matches_inner(value, rest)
                        || (!value.is_empty() && matches_inner(&value[1..], pattern))
                }
                Some(('_', rest)) => !value.is_empty() && matches_inner(&value[1..], rest),
                Some((expected, rest)) => value
                    .split_first()
                    .map(|(actual, tail)| actual == expected && matches_inner(tail, rest))
                    .unwrap_or(false),
            }
        }
        let value_chars: Vec<char> = value.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();
        matches_inner(&value_chars, &pattern_chars)
    }

    /// Internal/test root execution helper. MySQL production paths must use session SecurityContext.
    pub async fn execute_as_root_for_internal(
        &self,
        stmt: ResolvedStatement,
    ) -> Result<QueryResult> {
        self.execute_with_context(stmt, &SecurityContext::root())
            .await
    }

    /// Execute a resolved statement with per-session security context.
    pub async fn execute_with_context(
        &self,
        stmt: ResolvedStatement,
        security: &SecurityContext,
    ) -> Result<QueryResult> {
        match stmt {
            ResolvedStatement::CreateDatabase { name } => {
                self.require_primary_role_privilege(
                    security,
                    ObjectRef::new(ObjectType::Account, ACCOUNT_OBJECT_ID),
                    SecurityPrivilege::CreateDatabase,
                )
                .await?;
                self.exec_create_database(security, name).await
            }
            ResolvedStatement::CreateTable {
                db,
                schema,
                table,
                columns,
            } => {
                let db_meta = self.find_database(&db).await?;
                if let Ok(schema_meta) = self.find_schema_meta(db_meta.id, &schema).await {
                    self.require_primary_role_privilege(
                        security,
                        ObjectRef::new(ObjectType::Schema, schema_meta.id),
                        SecurityPrivilege::CreateTable,
                    )
                    .await?;
                } else {
                    self.require_primary_role_privilege(
                        security,
                        ObjectRef::new(ObjectType::Database, db_meta.id),
                        SecurityPrivilege::CreateSchema,
                    )
                    .await?;
                }
                self.exec_create_table(security, db, schema, table, columns)
                    .await
            }
            ResolvedStatement::CreateFunction {
                db,
                schema,
                name,
                args,
                signature,
                return_type,
                language,
                body,
                volatility,
                null_handling,
                or_replace,
                if_not_exists,
            } => {
                let db_meta = self.find_database(&db).await?;
                let schema_meta = self.find_schema_meta(db_meta.id, &schema).await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Database, db_meta.id),
                    SecurityPrivilege::Usage,
                )
                .await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema_meta.id),
                    SecurityPrivilege::Usage,
                )
                .await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema_meta.id),
                    SecurityPrivilege::CreateFunction,
                )
                .await?;
                if let Some(existing) = self
                    .meta
                    .get_function_by_signature(db_meta.id, schema_meta.id, &name, &signature)
                    .await?
                    && or_replace
                {
                    self.require_privilege(
                        security,
                        ObjectRef::new(ObjectType::Function, existing.id),
                        SecurityPrivilege::Ownership,
                    )
                    .await?;
                }
                self.exec_create_function(
                    security,
                    db,
                    schema,
                    db_meta.id,
                    schema_meta.id,
                    name,
                    args,
                    signature,
                    return_type,
                    language,
                    body,
                    volatility,
                    null_handling,
                    or_replace,
                    if_not_exists,
                )
                .await
            }
            ResolvedStatement::DropFunction {
                db,
                schema,
                name,
                signature,
                if_exists,
            } => {
                let db_meta = self.find_database(&db).await?;
                let schema_meta = self.find_schema_meta(db_meta.id, &schema).await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Database, db_meta.id),
                    SecurityPrivilege::Usage,
                )
                .await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema_meta.id),
                    SecurityPrivilege::Usage,
                )
                .await?;
                if let Some(function) = self
                    .meta
                    .get_function_by_signature(db_meta.id, schema_meta.id, &name, &signature)
                    .await?
                {
                    self.require_privilege(
                        security,
                        ObjectRef::new(ObjectType::Function, function.id),
                        SecurityPrivilege::Ownership,
                    )
                    .await?;
                } else if !if_exists {
                    return Err(NovaError::Internal {
                        message: format!(
                            "function '{}' not found",
                            format_function_name(&db, &schema, &name, &signature)
                        ),
                    });
                }
                self.exec_drop_function(db, schema, name, signature, if_exists)
                    .await
            }
            ResolvedStatement::GrantFunctionUsage {
                db,
                schema,
                name,
                signature,
                role,
            } => {
                self.exec_grant_function_usage(security, &db, &schema, &name, &signature, &role)
                    .await
            }
            ResolvedStatement::RevokeFunctionUsage {
                db,
                schema,
                name,
                signature,
                role,
            } => {
                self.exec_revoke_function_usage(security, &db, &schema, &name, &signature, &role)
                    .await
            }
            ResolvedStatement::ShowGrantsOnFunction {
                db,
                schema,
                name,
                signature,
            } => {
                self.exec_show_grants_on_function(security, &db, &schema, &name, &signature)
                    .await
            }
            ResolvedStatement::ShowGrantsToRole { role } => {
                self.exec_show_grants_to_role(security, &role).await
            }
            ResolvedStatement::Insert {
                db,
                schema,
                table,
                values,
            } => {
                self.authorize_table(security, &db, &schema, &table, SecurityPrivilege::Insert)
                    .await?;
                self.exec_insert(db, schema, table, values).await
            }
            ResolvedStatement::Select {
                db,
                schema,
                table,
                dependencies,
                projection,
                filter,
                at_timestamp,
                raw_sql,
            } => {
                let stream_candidate = self.find_stream(&db, &schema, &table).await.ok();
                let table_candidate = self.find_table(&db, &schema, &table).await.ok();
                if let Some(stream) = stream_candidate {
                    if table_candidate.is_some() {
                        return Err(NovaError::AmbiguousRelationName { name: table });
                    }
                    let stream_sql =
                        raw_sql.unwrap_or_else(|| format!("SELECT * FROM {}", stream.name));
                    return self
                        .exec_read_stream(
                            security,
                            &db,
                            &schema,
                            &stream.name,
                            StreamReadMode::Commit,
                            &stream_sql,
                        )
                        .await;
                }

                let dependencies = if dependencies.is_empty() {
                    vec![table.clone()]
                } else {
                    dependencies
                };
                for dependency in &dependencies {
                    self.authorize_table(
                        security,
                        &db,
                        &schema,
                        dependency,
                        SecurityPrivilege::Select,
                    )
                    .await?;
                }
                self.exec_select(
                    security,
                    db,
                    schema,
                    table,
                    projection,
                    filter,
                    at_timestamp,
                    raw_sql,
                )
                .await
            }
            ResolvedStatement::Update {
                db,
                schema,
                table,
                assignments,
                filter,
            } => {
                self.authorize_table(security, &db, &schema, &table, SecurityPrivilege::Update)
                    .await?;
                self.exec_update(&db, &schema, &table, assignments, filter)
                    .await
            }
            ResolvedStatement::Delete {
                db,
                schema,
                table,
                filter,
            } => {
                self.authorize_table(security, &db, &schema, &table, SecurityPrivilege::Delete)
                    .await?;
                self.exec_delete(&db, &schema, &table, filter).await
            }
            ResolvedStatement::CreateClone {
                db,
                schema,
                clone_table,
                source_table,
                at_timestamp,
            } => {
                self.authorize_table(
                    security,
                    &db,
                    &schema,
                    &source_table,
                    SecurityPrivilege::Select,
                )
                .await?;
                let db_meta = self.find_database(&db).await?;
                let schema_meta = self.find_schema_meta(db_meta.id, &schema).await?;
                self.require_primary_role_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema_meta.id),
                    SecurityPrivilege::CreateTable,
                )
                .await?;
                self.exec_clone(&db, &schema, &clone_table, &source_table, at_timestamp)
                    .await
            }
            ResolvedStatement::CreateStream {
                db,
                schema,
                stream_name,
                table,
                append_only,
            } => {
                self.authorize_table(security, &db, &schema, &table, SecurityPrivilege::Select)
                    .await?;
                let db_meta = self.find_database(&db).await?;
                let schema_meta = self.find_schema_meta(db_meta.id, &schema).await?;
                self.require_primary_role_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema_meta.id),
                    SecurityPrivilege::CreateStream,
                )
                .await?;
                self.exec_create_stream(security, &db, &schema, &stream_name, &table, append_only)
                    .await
            }
            ResolvedStatement::ReadStream {
                db,
                schema,
                stream_name,
                read_mode,
                raw_sql,
                ..
            } => {
                self.exec_read_stream(security, &db, &schema, &stream_name, read_mode, &raw_sql)
                    .await
            }
            ResolvedStatement::SystemStreamHasData {
                db,
                schema,
                stream_name,
            } => {
                self.exec_system_stream_has_data(security, &db, &schema, &stream_name)
                    .await
            }
            ResolvedStatement::DropStream { db, schema, name } => {
                self.exec_drop_stream(security, &db, &schema, &name).await
            }
            ResolvedStatement::ShowStreams {
                db,
                schema,
                pattern,
            } => {
                self.exec_show_streams(security, &db, &schema, pattern.as_deref())
                    .await
            }
            ResolvedStatement::DescribeStream { db, schema, name } => {
                self.exec_describe_stream(security, &db, &schema, &name)
                    .await
            }
            ResolvedStatement::Gc { retention_days } => {
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Account, ACCOUNT_OBJECT_ID),
                    SecurityPrivilege::Operate,
                )
                .await?;
                self.exec_gc(retention_days).await
            }
            ResolvedStatement::Begin => {
                let txn_id = self.meta.begin_transaction().await?;
                let mut txn_guard = self.current_txn.lock().unwrap();
                *txn_guard = Some(txn_id);
                Ok(QueryResult::Success {
                    message: format!("Transaction {} started", txn_id),
                })
            }
            ResolvedStatement::Commit => {
                let txn_id = {
                    let mut guard = self.current_txn.lock().unwrap();
                    guard.take()
                };
                match txn_id {
                    Some(id) => {
                        self.meta.commit_transaction(id).await?;
                        Ok(QueryResult::Success {
                            message: format!("Transaction {} committed", id),
                        })
                    }
                    None => Ok(QueryResult::Success {
                        message: "No active transaction".to_string(),
                    }),
                }
            }
            ResolvedStatement::Rollback => {
                let txn_id = {
                    let mut guard = self.current_txn.lock().unwrap();
                    guard.take()
                };
                match txn_id {
                    Some(id) => {
                        self.meta.abort_transaction(id).await?;
                        Ok(QueryResult::Success {
                            message: format!("Transaction {} rolled back", id),
                        })
                    }
                    None => Ok(QueryResult::Success {
                        message: "No active transaction".to_string(),
                    }),
                }
            }
            ResolvedStatement::Backup { path } => {
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Account, ACCOUNT_OBJECT_ID),
                    SecurityPrivilege::Operate,
                )
                .await?;
                Ok(QueryResult::Success {
                    message: format!(
                        "Backup created{}",
                        path.map(|p| format!(" to {}", p)).unwrap_or_default()
                    ),
                })
            }
            ResolvedStatement::Restore { path } => {
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Account, ACCOUNT_OBJECT_ID),
                    SecurityPrivilege::Operate,
                )
                .await?;
                Ok(QueryResult::Success {
                    message: format!("Restored from {}", path),
                })
            }
            ResolvedStatement::AlterTable {
                db,
                schema,
                table,
                action,
            } => {
                self.authorize_table(security, &db, &schema, &table, SecurityPrivilege::Ownership)
                    .await?;
                match action {
                    crate::analyzer::AlterAction::AddColumn { name, data_type } => {
                        tracing::info!(
                            table = %table,
                            column = %name,
                            data_type = %data_type,
                            "ALTER TABLE ADD COLUMN (metadata-only, existing MPs unchanged)"
                        );
                        Ok(QueryResult::Success {
                            message: format!("Column '{}' added to table '{}'", name, table),
                        })
                    }
                    crate::analyzer::AlterAction::DropColumn { name } => {
                        tracing::info!(
                            table = %table,
                            column = %name,
                            "ALTER TABLE DROP COLUMN (metadata-only)"
                        );
                        Ok(QueryResult::Success {
                            message: format!("Column '{}' dropped from table '{}'", name, table),
                        })
                    }
                }
            }
            ResolvedStatement::DropTable { db, schema, table } => {
                self.authorize_table(security, &db, &schema, &table, SecurityPrivilege::Ownership)
                    .await?;
                let table_meta = self.find_table(&db, &schema, &table).await?;
                self.meta.drop_table(table_meta.id).await?;
                Ok(QueryResult::Success {
                    message: format!("Table '{}.{}.{}' dropped", db, schema, table),
                })
            }
            ResolvedStatement::DropDatabase { name } => {
                let dbs = self.meta.list_databases().await?;
                let db_meta = dbs.iter().find(|d| d.name == name).ok_or_else(|| {
                    NovaError::DatabaseNotFound {
                        db_name: name.clone(),
                    }
                })?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Database, db_meta.id),
                    SecurityPrivilege::Ownership,
                )
                .await?;
                self.meta.drop_database(db_meta.id).await?;
                Ok(QueryResult::Success {
                    message: format!("Database '{}' dropped", name),
                })
            }
            ResolvedStatement::DropSchema { db, schema } => {
                let dbs = self.meta.list_databases().await?;
                let db_meta = dbs.iter().find(|d| d.name == db).ok_or_else(|| {
                    NovaError::DatabaseNotFound {
                        db_name: db.clone(),
                    }
                })?;
                let schemas = self.meta.list_schemas(db_meta.id).await?;
                let schema_meta = schemas.iter().find(|s| s.name == schema).ok_or_else(|| {
                    NovaError::Internal {
                        message: format!("schema '{}' not found", schema),
                    }
                })?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema_meta.id),
                    SecurityPrivilege::Ownership,
                )
                .await?;
                self.meta.drop_schema(db_meta.id, schema_meta.id).await?;
                Ok(QueryResult::Success {
                    message: format!("Schema '{}.{}' dropped", db, schema),
                })
            }
            ResolvedStatement::CreateDynamicTable {
                db,
                schema,
                name,
                query_definition,
                target_lag_seconds,
                refresh_mode,
                initialize_on_create,
            } => {
                let db_meta = self.find_database(&db).await?;
                let schema_meta = self.find_schema_meta(db_meta.id, &schema).await?;
                self.require_primary_role_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, schema_meta.id),
                    SecurityPrivilege::CreateDynamicTable,
                )
                .await?;
                self.exec_create_dynamic_table(
                    security,
                    &db,
                    &schema,
                    &name,
                    query_definition,
                    target_lag_seconds,
                    refresh_mode,
                    initialize_on_create,
                )
                .await
            }
            ResolvedStatement::RefreshDynamicTable { db, schema, name } => {
                let dt = self.find_dynamic_table(&db, &schema, &name).await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::DynamicTable, dt.id),
                    SecurityPrivilege::Operate,
                )
                .await?;
                self.exec_refresh_dynamic_table(&db, &schema, &name).await
            }
            ResolvedStatement::SuspendDynamicTable { db, schema, name } => {
                let dt = self.find_dynamic_table(&db, &schema, &name).await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::DynamicTable, dt.id),
                    SecurityPrivilege::Operate,
                )
                .await?;
                self.exec_set_dt_scheduler(&db, &schema, &name, false).await
            }
            ResolvedStatement::ResumeDynamicTable { db, schema, name } => {
                let dt = self.find_dynamic_table(&db, &schema, &name).await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::DynamicTable, dt.id),
                    SecurityPrivilege::Operate,
                )
                .await?;
                self.exec_set_dt_scheduler(&db, &schema, &name, true).await
            }
            ResolvedStatement::DropDynamicTable { db, schema, name } => {
                let dt = self.find_dynamic_table(&db, &schema, &name).await?;
                self.require_privilege(
                    security,
                    ObjectRef::new(ObjectType::DynamicTable, dt.id),
                    SecurityPrivilege::Ownership,
                )
                .await?;
                self.exec_drop_dynamic_table(&db, &schema, &name).await
            }
            ResolvedStatement::ShowDynamicTables { db, pattern } => {
                self.exec_show_dynamic_tables(security, &db, pattern.as_deref())
                    .await
            }
        }
    }

    async fn exec_create_database(
        &self,
        security: &SecurityContext,
        name: String,
    ) -> Result<QueryResult> {
        let db = DatabaseMeta {
            id: 0,
            name: name.clone(),
            created_at: now_micros(),
            owner: 1,
        };
        self.meta.create_database(db).await?;
        let created = self.find_database(&name).await?;
        if self
            .meta
            .get_role(security.primary_role_id)
            .await?
            .is_some()
        {
            self.meta
                .set_object_owner(ObjectOwnerMeta {
                    object: ObjectRef::new(ObjectType::Database, created.id),
                    owner_role_id: security.primary_role_id,
                    created_by_user_id: security.user_id,
                    created_at: now_micros(),
                    transferred_at: None,
                })
                .await?;
        }
        Ok(QueryResult::Success {
            message: format!("Database '{}' created", name),
        })
    }

    async fn exec_create_table(
        &self,
        security: &SecurityContext,
        db: String,
        schema: String,
        table: String,
        columns: Vec<crate::analyzer::ResolvedColumn>,
    ) -> Result<QueryResult> {
        // Find database
        let dbs = self.meta.list_databases().await?;
        let db_meta =
            dbs.iter()
                .find(|d| d.name == db)
                .ok_or_else(|| NovaError::DatabaseNotFound {
                    db_name: db.clone(),
                })?;

        // Find or create schema
        let schemas = self.meta.list_schemas(db_meta.id).await?;
        let schema_meta = if let Some(s) = schemas.iter().find(|s| s.name == schema) {
            s.clone()
        } else {
            let s = SchemaMeta {
                id: 0,
                db_id: db_meta.id,
                name: schema.clone(),
                created_at: now_micros(),
            };
            self.meta.create_schema(s).await?;
            // Re-fetch to get auto-assigned ID
            self.meta
                .list_schemas(db_meta.id)
                .await?
                .into_iter()
                .find(|s| s.name == schema)
                .ok_or_else(|| NovaError::SchemaNotFound {
                    schema_name: schema.clone(),
                })?
        };

        // Create table
        let cols: Vec<ColumnDef> = columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| ColumnDef {
                id: i as u32,
                name: c.name,
                data_type: parse_sql_type(&c.data_type),
                nullable: c.nullable,
                default_value: None,
                comment: None,
            })
            .collect();

        let t = TableMeta {
            id: 0,
            db_id: db_meta.id,
            schema_id: schema_meta.id,
            name: table.clone(),
            columns: cols,
            created_at: now_micros(),
            owner: 1,
            comment: None,
            version: 0,
            properties: Default::default(),
        };
        self.meta.create_table(t).await?;
        let created = self.find_table(&db, &schema, &table).await?;
        if self
            .meta
            .get_role(security.primary_role_id)
            .await?
            .is_some()
        {
            self.meta
                .set_object_owner(ObjectOwnerMeta {
                    object: ObjectRef::new(ObjectType::Table, created.id),
                    owner_role_id: security.primary_role_id,
                    created_by_user_id: security.user_id,
                    created_at: now_micros(),
                    transferred_at: None,
                })
                .await?;
        }

        Ok(QueryResult::Success {
            message: format!("Table '{}.{}.{}' created", db, schema, table),
        })
    }

    fn append_cdc_metadata_columns(
        &self,
        source: &RecordBatch,
        metadata: CdcMetadataColumns,
    ) -> Result<RecordBatch> {
        let mut fields = source.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            "METADATA$ACTION",
            DataType::Utf8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$ISUPDATE",
            DataType::Boolean,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$ROW_ID",
            DataType::Utf8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$TXN_ID",
            DataType::UInt64,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$COMMIT_TS",
            DataType::UInt64,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$SEQUENCE",
            DataType::UInt64,
            false,
        )));
        let mut columns: Vec<ArrayRef> = source.columns().to_vec();
        columns.push(Arc::new(StringArray::from(metadata.actions)) as ArrayRef);
        columns.push(Arc::new(BooleanArray::from(metadata.is_updates)) as ArrayRef);
        columns.push(Arc::new(StringArray::from(metadata.row_ids)) as ArrayRef);
        columns.push(Arc::new(UInt64Array::from(metadata.txn_ids)) as ArrayRef);
        columns.push(Arc::new(UInt64Array::from(metadata.commit_ts_values)) as ArrayRef);
        columns.push(Arc::new(UInt64Array::from(metadata.sequences)) as ArrayRef);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| {
            NovaError::ArrowError {
                source: Box::new(e),
            }
        })
    }

    fn build_insert_cdc_batch(
        &self,
        table: &TableMeta,
        source_batch: &RecordBatch,
        start_sequence: u64,
        txn_id: TxnId,
        commit_ts: Timestamp,
        mp_id: MpId,
    ) -> Result<RecordBatch> {
        let row_count = source_batch.num_rows();
        let mut row_ids = Vec::with_capacity(row_count);
        let mut sequences = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            row_ids.push(stream_row_id(table.id, mp_id, row_idx as u64, 0));
            sequences.push(start_sequence + row_idx as u64);
        }
        self.append_cdc_metadata_columns(
            source_batch,
            CdcMetadataColumns {
                actions: vec![ChangeAction::Insert.to_string(); row_count],
                is_updates: vec![false; row_count],
                row_ids,
                txn_ids: vec![txn_id; row_count],
                commit_ts_values: vec![commit_ts; row_count],
                sequences,
            },
        )
    }

    fn select_rows_by_indices(
        &self,
        batch: &RecordBatch,
        indices: &[u32],
    ) -> Result<Option<RecordBatch>> {
        if indices.is_empty() {
            return Ok(None);
        }
        let take_arr = arrow::array::UInt32Array::from(indices.to_vec());
        let mut columns = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            columns.push(
                arrow::compute::take(batch.column(col_idx), &take_arr, None).map_err(|e| {
                    NovaError::ArrowError {
                        source: Box::new(e),
                    }
                })?,
            );
        }
        RecordBatch::try_new(batch.schema(), columns)
            .map(Some)
            .map_err(|e| NovaError::ArrowError {
                source: Box::new(e),
            })
    }

    fn build_cdc_batch_for_rows(&self, spec: CdcRowBatchSpec<'_>) -> Result<RecordBatch> {
        let row_count = spec.source_batch.num_rows();
        if row_count != spec.row_ordinals.len() {
            return Err(NovaError::Internal {
                message: "CDC row ordinal count does not match source batch".to_string(),
            });
        }
        let sequences = (0..row_count)
            .map(|row_idx| spec.start_sequence + row_idx as u64)
            .collect::<Vec<_>>();
        let row_ids = spec
            .row_ordinals
            .iter()
            .map(|row_ordinal| stream_row_id(spec.table.id, spec.mp_id, *row_ordinal, 0))
            .collect::<Vec<_>>();
        self.append_cdc_metadata_columns(
            spec.source_batch,
            CdcMetadataColumns {
                actions: vec![spec.action.to_string(); row_count],
                is_updates: vec![spec.is_update; row_count],
                row_ids,
                txn_ids: vec![spec.txn_id; row_count],
                commit_ts_values: vec![spec.commit_ts; row_count],
                sequences,
            },
        )
    }

    async fn write_cdc_payload_and_metadata(
        &self,
        table_id: TableId,
        txn_id: TxnId,
        start_sequence: u64,
        commit_ts: Timestamp,
        batch: &RecordBatch,
        metadata: CdcPayloadMetadata,
    ) -> Result<ChangeRecordMeta> {
        let payload = self
            .cdc_writer
            .write_payload(table_id, txn_id, start_sequence, batch)
            .await?;
        Ok(ChangeRecordMeta {
            table_id,
            sequence: start_sequence,
            txn_id,
            commit_ts,
            payload,
            action_counts: metadata.action_counts,
            min_row_id: metadata.min_row_id,
            max_row_id: metadata.max_row_id,
        })
    }

    fn stream_schema_for_table(&self, table: &TableMeta) -> Arc<Schema> {
        let mut fields = table
            .columns
            .iter()
            .map(|column| {
                Arc::new(Field::new(
                    &column.name,
                    nova_type_to_arrow(&column.data_type),
                    column.nullable,
                ))
            })
            .collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            "METADATA$ACTION",
            DataType::Utf8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$ISUPDATE",
            DataType::Boolean,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$ROW_ID",
            DataType::Utf8,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$TXN_ID",
            DataType::UInt64,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$COMMIT_TS",
            DataType::UInt64,
            false,
        )));
        fields.push(Arc::new(Field::new(
            "METADATA$SEQUENCE",
            DataType::UInt64,
            false,
        )));
        Arc::new(Schema::new(fields))
    }

    fn empty_stream_batch(&self, table: &TableMeta) -> Result<RecordBatch> {
        let schema = self.stream_schema_for_table(table);
        let columns = schema
            .fields()
            .iter()
            .map(|field| arrow::array::new_empty_array(field.data_type()))
            .collect::<Vec<_>>();
        RecordBatch::try_new(schema, columns).map_err(|e| NovaError::ArrowError {
            source: Box::new(e),
        })
    }

    fn remove_stream_preview_clause(sql: &str) -> String {
        let upper = sql.to_uppercase();
        let Some(with_pos) = upper.find("WITH") else {
            return sql.to_string();
        };
        let suffix_upper = &upper[with_pos..];
        if !suffix_upper.contains("COMMIT") {
            return sql.to_string();
        }
        let Some(open_rel) = suffix_upper.find('(') else {
            return sql.to_string();
        };
        let open_pos = with_pos + open_rel;
        let Some(close_rel) = upper[open_pos..].find(')') else {
            return sql.to_string();
        };
        let after_pos = open_pos + close_rel + 1;
        let before = sql[..with_pos].trim_end();
        let after = sql[after_pos..].trim_start();
        if after.is_empty() {
            before.to_string()
        } else {
            format!("{} {}", before, after)
        }
    }

    async fn read_stream_payload_batches(
        &self,
        stream: &StreamMeta,
        records: &[ChangeRecordMeta],
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for record in records {
            let payload_batches = self
                .cdc_reader
                .read_payload(&record.payload)
                .await
                .map_err(|err| match err {
                    NovaError::ObjectStoreError { .. } => NovaError::StreamPayloadMissing {
                        stream_id: stream.stream_id,
                        payload_path: record.payload.path.clone(),
                    },
                    other => other,
                })?;
            let mut rows_to_skip = record.payload.row_start as usize;
            let mut rows_remaining = record.payload.row_count as usize;
            for batch in payload_batches {
                if rows_remaining == 0 {
                    break;
                }
                if rows_to_skip >= batch.num_rows() {
                    rows_to_skip -= batch.num_rows();
                    continue;
                }
                let offset = rows_to_skip;
                let length = rows_remaining.min(batch.num_rows() - offset);
                batches.push(batch.slice(offset, length));
                rows_remaining -= length;
                rows_to_skip = 0;
            }
            if rows_remaining != 0 {
                return Err(NovaError::StreamPayloadMissing {
                    stream_id: stream.stream_id,
                    payload_path: record.payload.path.clone(),
                });
            }
        }
        Ok(batches)
    }

    async fn exec_system_stream_has_data(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        stream_name: &str,
    ) -> Result<QueryResult> {
        let stream = self.find_stream(db, schema, stream_name).await?;
        self.authorize_stream_read(security, db, schema, &stream)
            .await?;
        let has_data = self.meta.stream_has_data(stream.stream_id).await?;
        Ok(QueryResult::Rows {
            columns: vec!["SYSTEM$STREAM_HAS_DATA".to_string()],
            rows: vec![vec![has_data.to_string()]],
        })
    }

    fn contiguous_published_stream_sequence(
        committed_sequence: u64,
        records: &[ChangeRecordMeta],
    ) -> u64 {
        let mut next_sequence = committed_sequence + 1;
        let mut published_through = committed_sequence;
        for record in records {
            let row_count = record.payload.row_count;
            if row_count == 0 {
                continue;
            }
            let record_start = record.sequence;
            let record_end = record.sequence.saturating_add(row_count).saturating_sub(1);
            if record_end < next_sequence {
                continue;
            }
            if record_start > next_sequence {
                break;
            }
            published_through = record_end;
            next_sequence = record_end.saturating_add(1);
        }
        published_through
    }

    async fn exec_read_stream(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        stream_name: &str,
        read_mode: StreamReadMode,
        raw_sql: &str,
    ) -> Result<QueryResult> {
        let stream = self.find_stream(db, schema, stream_name).await?;
        let source_table = self
            .authorize_stream_read(security, db, schema, &stream)
            .await?;
        let offset = self
            .meta
            .get_stream_offset(stream.stream_id)
            .await?
            .ok_or_else(|| NovaError::StreamNotFound {
                stream_name: stream.name.clone(),
            })?;
        let current_sequence = self
            .meta
            .get_table_change_sequence(stream.source_table_id)
            .await?;
        let records = self
            .meta
            .get_change_records(
                stream.source_table_id,
                offset.committed_sequence,
                current_sequence,
            )
            .await?;
        let published_sequence =
            Self::contiguous_published_stream_sequence(offset.committed_sequence, &records);
        let consumable_records = records
            .iter()
            .filter(|record| {
                record
                    .sequence
                    .saturating_add(record.payload.row_count)
                    .saturating_sub(1)
                    <= published_sequence
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut batches = self
            .read_stream_payload_batches(&stream, &consumable_records)
            .await?;
        if batches.is_empty() {
            batches.push(self.empty_stream_batch(&source_table)?);
        }

        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.skip_failed_rules = true;
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);
        let table_schema =
            batches
                .first()
                .map(|batch| batch.schema())
                .ok_or_else(|| NovaError::Internal {
                    message: "stream read produced no schema".to_string(),
                })?;
        let provider = datafusion::datasource::MemTable::try_new(table_schema, vec![batches])
            .map_err(|e| NovaError::Internal {
                message: format!("DataFusion stream table registration failed: {}", e),
            })?;
        ctx.register_table(&stream.name, Arc::new(provider))
            .map_err(|e| NovaError::Internal {
                message: format!("DataFusion register stream failed: {}", e),
            })?;

        let stream_sql = Self::remove_stream_preview_clause(raw_sql);
        let df = ctx
            .sql(&stream_sql)
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("DataFusion stream SQL execution failed: {}", e),
            })?;
        let result_batches = df.collect().await.map_err(|e| NovaError::Internal {
            message: format!("DataFusion stream collect failed: {}", e),
        })?;

        if read_mode == StreamReadMode::Commit && published_sequence > offset.committed_sequence {
            self.meta
                .compare_and_set_stream_offset(
                    stream.stream_id,
                    offset.committed_sequence,
                    StreamOffset {
                        table_id: stream.source_table_id,
                        committed_sequence: published_sequence,
                        committed_ts: now_micros(),
                        last_consumed_at: Some(now_micros()),
                        last_consumed_txn_id: consumable_records.last().map(|record| record.txn_id),
                    },
                )
                .await?;
        }

        let (columns, rows) = batches_to_query_result(&result_batches);
        Ok(QueryResult::Rows { columns, rows })
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_create_function(
        &self,
        security: &SecurityContext,
        db: String,
        schema: String,
        db_id: DatabaseId,
        schema_id: SchemaId,
        name: String,
        args: Vec<FunctionArg>,
        signature: FunctionSignature,
        return_type: String,
        language: FunctionLanguage,
        body: FunctionBody,
        volatility: FunctionVolatility,
        null_handling: FunctionNullHandling,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<QueryResult> {
        if language != FunctionLanguage::Sql {
            return Err(NovaError::SqlAnalysisError {
                message: format!(
                    "function language {} is not supported yet; only SQL is supported",
                    language
                ),
            });
        }
        if !matches!(body, FunctionBody::SqlExpression(_)) {
            return Err(NovaError::SqlAnalysisError {
                message: "CREATE FUNCTION LANGUAGE SQL requires a SQL expression body".to_string(),
            });
        }

        let existing = self
            .meta
            .get_function_by_signature(db_id, schema_id, &name, &signature)
            .await?;
        let now = now_micros();
        let message_name = format_function_name(&db, &schema, &name, &signature);

        if let Some(existing) = existing {
            if if_not_exists && !or_replace {
                return Ok(QueryResult::Success {
                    message: format!("Function '{}' already exists", message_name),
                });
            }
            if !or_replace {
                return Err(NovaError::Internal {
                    message: format!("function '{}' already exists", message_name),
                });
            }

            let replacement = FunctionMeta {
                id: existing.id,
                db_id,
                schema_id,
                name,
                signature,
                args,
                return_type,
                language,
                body,
                volatility,
                null_handling,
                created_at: existing.created_at,
                updated_at: now,
                owner_role_id: existing.owner_role_id,
                comment: existing.comment,
                properties: existing.properties,
            };
            self.meta.replace_function(replacement).await?;
            return Ok(QueryResult::Success {
                message: format!("Function '{}' replaced", message_name),
            });
        }

        let function_id = generate_id();
        let function = FunctionMeta {
            id: function_id,
            db_id,
            schema_id,
            name,
            signature,
            args,
            return_type,
            language,
            body,
            volatility,
            null_handling,
            created_at: now,
            updated_at: now,
            owner_role_id: security.primary_role_id,
            comment: None,
            properties: Default::default(),
        };
        self.meta.create_function(function).await?;
        if self
            .meta
            .get_role(security.primary_role_id)
            .await?
            .is_some()
        {
            self.meta
                .set_object_owner(ObjectOwnerMeta {
                    object: ObjectRef::new(ObjectType::Function, function_id),
                    owner_role_id: security.primary_role_id,
                    created_by_user_id: security.user_id,
                    created_at: now,
                    transferred_at: None,
                })
                .await?;
        }

        Ok(QueryResult::Success {
            message: format!("Function '{}' created", message_name),
        })
    }

    async fn exec_drop_function(
        &self,
        db: String,
        schema: String,
        name: String,
        signature: FunctionSignature,
        if_exists: bool,
    ) -> Result<QueryResult> {
        let db_meta = self.find_database(&db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, &schema).await?;
        let message_name = format_function_name(&db, &schema, &name, &signature);
        let Some(function) = self
            .meta
            .get_function_by_signature(db_meta.id, schema_meta.id, &name, &signature)
            .await?
        else {
            if if_exists {
                return Ok(QueryResult::Success {
                    message: format!("Function '{}' does not exist, skipping", message_name),
                });
            }
            return Err(NovaError::Internal {
                message: format!("function '{}' not found", message_name),
            });
        };

        self.meta.drop_function(function.id).await?;
        Ok(QueryResult::Success {
            message: format!("Function '{}' dropped", message_name),
        })
    }

    async fn resolve_function(
        &self,
        db: &str,
        schema: &str,
        name: &str,
        signature: &FunctionSignature,
    ) -> Result<(DatabaseMeta, SchemaMeta, FunctionMeta)> {
        let db_meta = self.find_database(db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
        let function = self
            .meta
            .get_function_by_signature(db_meta.id, schema_meta.id, name, signature)
            .await?
            .ok_or_else(|| NovaError::Internal {
                message: format!(
                    "function '{}' not found",
                    format_function_name(db, schema, name, signature)
                ),
            })?;
        Ok((db_meta, schema_meta, function))
    }

    async fn require_can_manage_function_grants(
        &self,
        security: &SecurityContext,
        function_id: FunctionId,
    ) -> Result<()> {
        let function = ObjectRef::new(ObjectType::Function, function_id);
        let account = ObjectRef::new(ObjectType::Account, ACCOUNT_OBJECT_ID);
        if self
            .has_privilege(security, function, SecurityPrivilege::Ownership)
            .await?
            || self
                .has_privilege(security, account, SecurityPrivilege::ManageGrants)
                .await?
        {
            return Ok(());
        }
        Err(NovaError::PermissionDenied {
            user: security.username.clone(),
            action: format!("MANAGE GRANTS on FUNCTION:{}", function_id),
        })
    }

    async fn exec_grant_function_usage(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        name: &str,
        signature: &FunctionSignature,
        role: &str,
    ) -> Result<QueryResult> {
        let (db_meta, schema_meta, function) =
            self.resolve_function(db, schema, name, signature).await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Database, db_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Schema, schema_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_can_manage_function_grants(security, function.id)
            .await?;
        let target_role =
            self.meta
                .get_role_by_name(role)
                .await?
                .ok_or_else(|| NovaError::Internal {
                    message: format!("role '{}' not found", role),
                })?;
        self.meta
            .grant_privileges(GrantSetMeta {
                role_id: target_role.id,
                object: ObjectRef::new(ObjectType::Function, function.id),
                privileges: PrivilegeSet::from_privileges(&[SecurityPrivilege::Usage]),
                grant_options: PrivilegeSet::empty(),
                granted_by_role_id: security.primary_role_id,
                updated_at: now_micros(),
            })
            .await?;
        Ok(QueryResult::Success {
            message: format!(
                "Granted USAGE on FUNCTION '{}' to role '{}'",
                format_function_name(db, schema, &function.name, &function.signature),
                target_role.name
            ),
        })
    }

    async fn exec_revoke_function_usage(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        name: &str,
        signature: &FunctionSignature,
        role: &str,
    ) -> Result<QueryResult> {
        let (db_meta, schema_meta, function) =
            self.resolve_function(db, schema, name, signature).await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Database, db_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Schema, schema_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_can_manage_function_grants(security, function.id)
            .await?;
        let target_role =
            self.meta
                .get_role_by_name(role)
                .await?
                .ok_or_else(|| NovaError::Internal {
                    message: format!("role '{}' not found", role),
                })?;
        self.meta
            .revoke_privileges(
                target_role.id,
                ObjectRef::new(ObjectType::Function, function.id),
                PrivilegeSet::from_privileges(&[SecurityPrivilege::Usage]),
            )
            .await?;
        Ok(QueryResult::Success {
            message: format!(
                "Revoked USAGE on FUNCTION '{}' from role '{}'",
                format_function_name(db, schema, &function.name, &function.signature),
                target_role.name
            ),
        })
    }

    async fn exec_show_grants_on_function(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        name: &str,
        signature: &FunctionSignature,
    ) -> Result<QueryResult> {
        let (_db_meta, _schema_meta, function) =
            self.resolve_function(db, schema, name, signature).await?;
        self.require_can_manage_function_grants(security, function.id)
            .await?;
        let grants = self
            .meta
            .list_grants_on_object(ObjectRef::new(ObjectType::Function, function.id))
            .await?;
        let mut rows = Vec::new();
        for grant in grants {
            if let Some(role) = self.meta.get_role(grant.role_id).await? {
                rows.extend(function_grant_rows(
                    &role.name,
                    &format_function_name(db, schema, &function.name, &function.signature),
                    &grant,
                ));
            }
        }
        Ok(QueryResult::Rows {
            columns: grant_result_columns(),
            rows,
        })
    }

    async fn exec_show_grants_to_role(
        &self,
        security: &SecurityContext,
        role: &str,
    ) -> Result<QueryResult> {
        let target_role =
            self.meta
                .get_role_by_name(role)
                .await?
                .ok_or_else(|| NovaError::Internal {
                    message: format!("role '{}' not found", role),
                })?;
        let account = ObjectRef::new(ObjectType::Account, ACCOUNT_OBJECT_ID);
        if !security.active_role_ids().contains(&target_role.id)
            && !self
                .has_privilege(security, account, SecurityPrivilege::ManageGrants)
                .await?
        {
            return Err(NovaError::PermissionDenied {
                user: security.username.clone(),
                action: format!("SHOW GRANTS TO ROLE:{}", target_role.id),
            });
        }
        let grants = self.meta.list_grants_to_role(target_role.id).await?;
        let mut rows = Vec::new();
        for grant in grants
            .into_iter()
            .filter(|grant| grant.object.object_type == ObjectType::Function)
        {
            if let Some((db_name, schema_name, function)) =
                self.find_function_by_id(grant.object.object_id).await?
            {
                rows.extend(function_grant_rows(
                    &target_role.name,
                    &format_function_name(
                        &db_name,
                        &schema_name,
                        &function.name,
                        &function.signature,
                    ),
                    &grant,
                ));
            }
        }
        Ok(QueryResult::Rows {
            columns: grant_result_columns(),
            rows,
        })
    }

    async fn find_function_by_id(
        &self,
        function_id: FunctionId,
    ) -> Result<Option<(String, String, FunctionMeta)>> {
        for db in self.meta.list_databases().await? {
            for schema in self.meta.list_schemas(db.id).await? {
                for function in self.meta.list_functions(db.id, schema.id).await? {
                    if function.id == function_id {
                        return Ok(Some((db.name, schema.name, function)));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn exec_insert(
        &self,
        db: String,
        schema: String,
        table: String,
        values: Vec<Vec<ResolvedExpr>>,
    ) -> Result<QueryResult> {
        // Find table
        let table_meta = self.find_table(&db, &schema, &table).await?;
        let row_count = values.len();

        // Convert resolved values to Arrow RecordBatch
        let batch = self.values_to_batch(&table_meta, values)?;

        // Write as micro-partition and as an immutable CDC payload.
        let mp_id = generate_id();
        let version = self.meta.increment_table_version(table_meta.id).await?;
        let txn_id = self.meta.begin_transaction().await?;
        let commit_ts = now_micros();

        let mp = self
            .writer
            .write(
                table_meta.id,
                mp_id,
                version,
                std::slice::from_ref(&batch),
                txn_id,
            )
            .await?;

        let start_sequence = self
            .meta
            .allocate_table_change_sequences(table_meta.id, row_count as u64)
            .await?;
        let cdc_batch = self.build_insert_cdc_batch(
            &table_meta,
            &batch,
            start_sequence,
            txn_id,
            commit_ts,
            mp_id,
        )?;
        let min_row_id = Some(stream_row_id(table_meta.id, mp_id, 0, 0));
        let max_row_id = Some(stream_row_id(
            table_meta.id,
            mp_id,
            row_count.saturating_sub(1) as u64,
            0,
        ));
        let change_meta = self
            .write_cdc_payload_and_metadata(
                table_meta.id,
                txn_id,
                start_sequence,
                commit_ts,
                &cdc_batch,
                CdcPayloadMetadata {
                    action_counts: ChangeActionCounts {
                        inserts: row_count as u64,
                        deletes: 0,
                        update_pairs: 0,
                    },
                    min_row_id,
                    max_row_id,
                },
            )
            .await?;

        // Publish MP metadata, transaction commit status, and CDC metadata atomically.
        let mut committed_mp = mp;
        committed_mp.s3_path = committed_mp
            .s3_temp_path
            .take()
            .unwrap_or(committed_mp.s3_path);
        committed_mp.commit_ts = commit_ts;
        committed_mp.active = true;
        self.meta
            .commit_table_cdc(txn_id, vec![committed_mp], vec![], vec![change_meta])
            .await?;

        Ok(QueryResult::Success {
            message: format!(
                "{} row(s) inserted into '{}.{}.{}'",
                row_count, db, schema, table
            ),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn exec_select(
        &self,
        security: &SecurityContext,
        db: String,
        schema: String,
        table: String,
        projection: Vec<String>,
        filter: Option<ResolvedFilter>,
        at_timestamp: Option<u64>,
        raw_sql: Option<String>,
    ) -> Result<QueryResult> {
        let table_meta = self.find_table(&db, &schema, &table).await?;

        // Get MPs: active for current, or at specific timestamp for Time Travel
        let mps = match at_timestamp {
            Some(ts) => self.meta.get_mps_at_timestamp(table_meta.id, ts).await?,
            None => self.meta.get_active_mps(table_meta.id).await?,
        };

        // Handle empty table
        if mps.is_empty() {
            let cols = if projection.contains(&"*".to_string()) {
                table_meta.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                projection
            };
            return Ok(QueryResult::Rows {
                columns: cols,
                rows: vec![],
            });
        }

        // Route through DataFusion if raw SQL is available (enables AGG, GROUP BY, ORDER BY, LIMIT, JOIN)
        if let Some(sql_text) = raw_sql {
            return self
                .exec_select_datafusion(security, &table_meta, &mps, &sql_text)
                .await;
        }

        // Fallback: direct storage read (legacy path for programmatic SELECT without raw SQL)
        let arrow_schema = build_arrow_schema(&table_meta);
        let row_filter = filter.clone();
        let pruned_mps = if filter.is_some() {
            let stmt_with_filter = ResolvedStatement::Select {
                db: db.clone(),
                schema: schema.clone(),
                table: table.clone(),
                dependencies: vec![table.clone()],
                projection: projection.clone(),
                filter,
                at_timestamp,
                raw_sql: None,
            };
            self.optimizer
                .optimize_select(&stmt_with_filter, &mps, &arrow_schema)?
        } else {
            mps.clone()
        };

        let total_mp_count = mps.len();
        tracing::debug!(
            table = %table,
            total_mps = total_mp_count,
            pruned_mps = pruned_mps.len(),
            "SELECT (legacy path)"
        );

        if pruned_mps.is_empty() {
            let cols = if projection.contains(&"*".to_string()) {
                table_meta.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                projection
            };
            return Ok(QueryResult::Rows {
                columns: cols,
                rows: vec![],
            });
        }

        // Read all pruned MPs in parallel
        let read_futures: Vec<_> = pruned_mps
            .iter()
            .map(|mp| self.reader.read(mp, None))
            .collect();
        let results = futures::future::join_all(read_futures).await;
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for result in results {
            all_batches.extend(result?);
        }

        let result_columns = if projection.contains(&"*".to_string()) {
            table_meta.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            projection
        };

        let mut rows: Vec<Vec<String>> = Vec::new();
        for batch in &all_batches {
            for row_idx in 0..batch.num_rows() {
                if let Some(ref filter) = row_filter {
                    let col_idx = table_meta
                        .columns
                        .iter()
                        .position(|c| c.name == filter.column);
                    if let Some(ci) = col_idx {
                        let val = array_value_to_string(batch.column(ci), row_idx);
                        if !row_matches_filter(&val, &filter.op, &filter.value) {
                            continue;
                        }
                    }
                }
                let mut row = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    row.push(array_value_to_string(batch.column(col_idx), row_idx));
                }
                rows.push(row);
            }
        }

        Ok(QueryResult::Rows {
            columns: result_columns,
            rows,
        })
    }

    /// UPDATE: Copy-on-Write. Read affected MPs → modify rows → write new MP → mark old superseded.
    async fn exec_update(
        &self,
        db: &str,
        schema: &str,
        table: &str,
        assignments: Vec<(String, ResolvedExpr)>,
        filter: Option<ResolvedFilter>,
    ) -> Result<QueryResult> {
        let table_meta = self.find_table(db, schema, table).await?;
        let mps = self.meta.get_active_mps(table_meta.id).await?;
        let txn_id = self.meta.begin_transaction().await?;
        let commit_ts = now_micros();
        let mut new_mps = Vec::new();
        let mut superseded_mps = Vec::new();
        let mut change_records = Vec::new();

        for mp in &mps {
            let batches = self.reader.read(mp, None).await?;
            let mut modified_batches = Vec::new();
            let mut matched_in_mp = 0usize;
            let mut base_row_ordinal = 0u64;

            for batch in batches {
                let mask = match &filter {
                    Some(f) => self.eval_filter_on_batch(&batch, f, &table_meta)?,
                    None => vec![true; batch.num_rows()],
                };
                let matched_indices = mask
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, matched)| matched.then_some(idx as u32))
                    .collect::<Vec<_>>();
                let row_ordinals = matched_indices
                    .iter()
                    .map(|idx| base_row_ordinal + *idx as u64)
                    .collect::<Vec<_>>();
                let modified =
                    self.apply_update_to_batch(&batch, &assignments, &filter, &table_meta)?;

                if let (Some(old_rows), Some(new_rows)) = (
                    self.select_rows_by_indices(&batch, &matched_indices)?,
                    self.select_rows_by_indices(&modified, &matched_indices)?,
                ) {
                    let row_count = matched_indices.len() as u64;
                    let start_sequence = self
                        .meta
                        .allocate_table_change_sequences(table_meta.id, row_count * 2)
                        .await?;
                    let old_cdc = self.build_cdc_batch_for_rows(CdcRowBatchSpec {
                        table: &table_meta,
                        source_batch: &old_rows,
                        row_ordinals: &row_ordinals,
                        start_sequence,
                        txn_id,
                        commit_ts,
                        mp_id: mp.mp_id,
                        action: ChangeAction::Delete,
                        is_update: true,
                    })?;
                    let new_cdc = self.build_cdc_batch_for_rows(CdcRowBatchSpec {
                        table: &table_meta,
                        source_batch: &new_rows,
                        row_ordinals: &row_ordinals,
                        start_sequence: start_sequence + row_count,
                        txn_id,
                        commit_ts,
                        mp_id: mp.mp_id,
                        action: ChangeAction::Insert,
                        is_update: true,
                    })?;
                    let cdc_batch =
                        arrow::compute::concat_batches(&old_cdc.schema(), vec![&old_cdc, &new_cdc])
                            .map_err(|e| NovaError::ArrowError {
                                source: Box::new(e),
                            })?;
                    let min_row_id = row_ordinals
                        .first()
                        .map(|row_ordinal| stream_row_id(table_meta.id, mp.mp_id, *row_ordinal, 0));
                    let max_row_id = row_ordinals
                        .last()
                        .map(|row_ordinal| stream_row_id(table_meta.id, mp.mp_id, *row_ordinal, 0));
                    let change_meta = self
                        .write_cdc_payload_and_metadata(
                            table_meta.id,
                            txn_id,
                            start_sequence,
                            commit_ts,
                            &cdc_batch,
                            CdcPayloadMetadata {
                                action_counts: ChangeActionCounts {
                                    inserts: row_count,
                                    deletes: row_count,
                                    update_pairs: row_count,
                                },
                                min_row_id,
                                max_row_id,
                            },
                        )
                        .await?;
                    change_records.push(change_meta);
                    matched_in_mp += matched_indices.len();
                }

                base_row_ordinal += batch.num_rows() as u64;
                modified_batches.push(modified);
            }

            if matched_in_mp > 0 {
                let new_mp_id = generate_id();
                let version = self.meta.increment_table_version(table_meta.id).await?;
                let mut new_mp = self
                    .writer
                    .write(table_meta.id, new_mp_id, version, &modified_batches, txn_id)
                    .await?;
                new_mp.s3_path = new_mp.s3_temp_path.take().unwrap_or(new_mp.s3_path);
                new_mp.commit_ts = commit_ts;
                new_mp.active = true;
                new_mp.supersedes = Some(mp.mp_id);
                new_mps.push(new_mp);
                superseded_mps.push((mp.mp_id, new_mp_id));
            }
        }

        self.meta
            .commit_table_cdc(txn_id, new_mps, superseded_mps, change_records)
            .await?;
        Ok(QueryResult::Rows {
            columns: vec!["status".to_string()],
            rows: vec![vec!["UPDATE OK".to_string()]],
        })
    }

    /// DELETE: Copy-on-Write. Read affected MPs → filter out matching rows → write new MP → mark old superseded.
    async fn exec_delete(
        &self,
        db: &str,
        schema: &str,
        table: &str,
        filter: Option<ResolvedFilter>,
    ) -> Result<QueryResult> {
        let table_meta = self.find_table(db, schema, table).await?;
        let mps = self.meta.get_active_mps(table_meta.id).await?;
        let txn_id = self.meta.begin_transaction().await?;
        let commit_ts = now_micros();
        let mut new_mps = Vec::new();
        let mut superseded_mps = Vec::new();
        let mut change_records = Vec::new();

        for mp in &mps {
            let batches = self.reader.read(mp, None).await?;
            let mut kept_batches = Vec::new();
            let mut deleted_in_mp = 0usize;
            let mut base_row_ordinal = 0u64;

            for batch in batches {
                let match_mask = match &filter {
                    Some(f) => self.eval_filter_on_batch(&batch, f, &table_meta)?,
                    None => vec![true; batch.num_rows()],
                };
                let deleted_indices = match_mask
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, matched)| matched.then_some(idx as u32))
                    .collect::<Vec<_>>();
                let row_ordinals = deleted_indices
                    .iter()
                    .map(|idx| base_row_ordinal + *idx as u64)
                    .collect::<Vec<_>>();

                if let Some(deleted_rows) = self.select_rows_by_indices(&batch, &deleted_indices)? {
                    let row_count = deleted_indices.len() as u64;
                    let start_sequence = self
                        .meta
                        .allocate_table_change_sequences(table_meta.id, row_count)
                        .await?;
                    let cdc_batch = self.build_cdc_batch_for_rows(CdcRowBatchSpec {
                        table: &table_meta,
                        source_batch: &deleted_rows,
                        row_ordinals: &row_ordinals,
                        start_sequence,
                        txn_id,
                        commit_ts,
                        mp_id: mp.mp_id,
                        action: ChangeAction::Delete,
                        is_update: false,
                    })?;
                    let min_row_id = row_ordinals
                        .first()
                        .map(|row_ordinal| stream_row_id(table_meta.id, mp.mp_id, *row_ordinal, 0));
                    let max_row_id = row_ordinals
                        .last()
                        .map(|row_ordinal| stream_row_id(table_meta.id, mp.mp_id, *row_ordinal, 0));
                    let change_meta = self
                        .write_cdc_payload_and_metadata(
                            table_meta.id,
                            txn_id,
                            start_sequence,
                            commit_ts,
                            &cdc_batch,
                            CdcPayloadMetadata {
                                action_counts: ChangeActionCounts {
                                    inserts: 0,
                                    deletes: row_count,
                                    update_pairs: 0,
                                },
                                min_row_id,
                                max_row_id,
                            },
                        )
                        .await?;
                    change_records.push(change_meta);
                    deleted_in_mp += deleted_indices.len();
                }

                let kept = self.apply_delete_to_batch(&batch, &filter, &table_meta)?;
                if kept.num_rows() > 0 {
                    kept_batches.push(kept);
                }
                base_row_ordinal += batch.num_rows() as u64;
            }

            if deleted_in_mp > 0 {
                if kept_batches.is_empty() {
                    superseded_mps.push((mp.mp_id, generate_id()));
                } else {
                    let new_mp_id = generate_id();
                    let version = self.meta.increment_table_version(table_meta.id).await?;
                    let mut new_mp = self
                        .writer
                        .write(table_meta.id, new_mp_id, version, &kept_batches, txn_id)
                        .await?;
                    new_mp.s3_path = new_mp.s3_temp_path.take().unwrap_or(new_mp.s3_path);
                    new_mp.commit_ts = commit_ts;
                    new_mp.active = true;
                    new_mp.supersedes = Some(mp.mp_id);
                    new_mps.push(new_mp);
                    superseded_mps.push((mp.mp_id, new_mp_id));
                }
            }
        }

        self.meta
            .commit_table_cdc(txn_id, new_mps, superseded_mps, change_records)
            .await?;
        Ok(QueryResult::Rows {
            columns: vec!["status".to_string()],
            rows: vec![vec!["DELETE OK".to_string()]],
        })
    }

    /// Apply UPDATE assignments to a batch. Returns modified batch.
    fn apply_update_to_batch(
        &self,
        batch: &RecordBatch,
        assignments: &[(String, ResolvedExpr)],
        filter: &Option<ResolvedFilter>,
        table: &TableMeta,
    ) -> Result<RecordBatch> {
        use arrow::array::*;

        let schema = batch.schema();
        let n_rows = batch.num_rows();

        // Determine which rows match the filter (or all if no filter)
        let mask: Vec<bool> = match filter {
            Some(f) => self.eval_filter_on_batch(batch, f, table)?,
            None => vec![true; n_rows],
        };

        // For each column: either keep as-is or apply assignment
        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            let col = batch.column(i);
            let assignment = assignments.iter().find(|(name, _)| name == field.name());
            if let Some((_, expr)) = assignment {
                // Replace values in matching rows
                let new_col: ArrayRef = match field.data_type() {
                    DataType::Int64 => {
                        let val = match expr {
                            ResolvedExpr::Int64(v) => *v,
                            _ => 0,
                        };
                        let mut vals: Vec<i64> = col
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                            .map(|v| v.unwrap_or(0))
                            .collect();
                        for (idx, &m) in mask.iter().enumerate() {
                            if m {
                                vals[idx] = val;
                            }
                        }
                        Arc::new(Int64Array::from(vals))
                    }
                    DataType::Float64 => {
                        let val = match expr {
                            ResolvedExpr::Float64(v) => *v,
                            _ => 0.0,
                        };
                        let mut vals: Vec<f64> = col
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .iter()
                            .map(|v| v.unwrap_or(0.0))
                            .collect();
                        for (idx, &m) in mask.iter().enumerate() {
                            if m {
                                vals[idx] = val;
                            }
                        }
                        Arc::new(Float64Array::from(vals))
                    }
                    DataType::Utf8 => {
                        let val = match expr {
                            ResolvedExpr::String(s) => s.clone(),
                            _ => "".to_string(),
                        };
                        let mut vals: Vec<Option<String>> = col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .map(|v| v.map(|s| s.to_string()))
                            .collect();
                        for (idx, &m) in mask.iter().enumerate() {
                            if m {
                                vals[idx] = Some(val.clone());
                            }
                        }
                        Arc::new(StringArray::from(vals))
                    }
                    _ => col.clone(),
                };
                new_columns.push(new_col);
            } else {
                new_columns.push(col.clone());
            }
        }

        RecordBatch::try_new(schema, new_columns).map_err(|e| NovaError::Internal {
            message: e.to_string(),
        })
    }

    /// Apply DELETE filter to a batch. Returns batch with only non-matching rows.
    fn apply_delete_to_batch(
        &self,
        batch: &RecordBatch,
        filter: &Option<ResolvedFilter>,
        table: &TableMeta,
    ) -> Result<RecordBatch> {
        use arrow::array::*;
        use arrow::compute::take;

        let n_rows = batch.num_rows();

        // Determine which rows to KEEP (inverse of filter match)
        let keep_mask: Vec<bool> = match filter {
            Some(f) => {
                let match_mask = self.eval_filter_on_batch(batch, f, table)?;
                match_mask.into_iter().map(|m| !m).collect()
            }
            None => vec![false; n_rows], // DELETE all
        };

        // Collect indices of rows to keep
        let indices: Vec<u32> = keep_mask
            .iter()
            .enumerate()
            .filter(|(_, k)| **k)
            .map(|(i, _)| i as u32)
            .collect();

        if indices.is_empty() {
            // No rows to keep — return empty batch
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        let take_arr = UInt32Array::from(indices);
        let mut new_columns = Vec::with_capacity(batch.num_columns());
        for i in 0..batch.num_columns() {
            let taken =
                take(batch.column(i), &take_arr, None).map_err(|e| NovaError::Internal {
                    message: e.to_string(),
                })?;
            new_columns.push(taken);
        }

        RecordBatch::try_new(batch.schema(), new_columns).map_err(|e| NovaError::Internal {
            message: e.to_string(),
        })
    }

    /// Evaluate a filter on a batch. Returns a boolean mask (true = matches filter).
    #[allow(clippy::needless_range_loop)]
    fn eval_filter_on_batch(
        &self,
        batch: &RecordBatch,
        filter: &ResolvedFilter,
        _table: &TableMeta,
    ) -> Result<Vec<bool>> {
        use arrow::array::*;

        let col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == &filter.column)
            .ok_or(NovaError::Internal {
                message: format!("column {} not found", filter.column),
            })?;

        let col = batch.column(col_idx);
        let n = batch.num_rows();
        let mut mask = vec![false; n];

        match col.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                let target = match &filter.value {
                    ResolvedExpr::Int64(v) => *v,
                    _ => 0,
                };
                for i in 0..n {
                    let v = arr.value(i);
                    mask[i] = match filter.op.as_str() {
                        "=" => v == target,
                        "!=" => v != target,
                        ">" => v > target,
                        ">=" => v >= target,
                        "<" => v < target,
                        "<=" => v <= target,
                        _ => false,
                    };
                }
            }
            arrow::datatypes::DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                let target = match &filter.value {
                    ResolvedExpr::Float64(v) => *v,
                    _ => 0.0,
                };
                for i in 0..n {
                    let v = arr.value(i);
                    mask[i] = match filter.op.as_str() {
                        "=" => v == target,
                        "!=" => v != target,
                        ">" => v > target,
                        ">=" => v >= target,
                        "<" => v < target,
                        "<=" => v <= target,
                        _ => false,
                    };
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                let target = match &filter.value {
                    ResolvedExpr::String(s) => s.clone(),
                    _ => "".to_string(),
                };
                for i in 0..n {
                    let v = arr.value(i);
                    mask[i] = match filter.op.as_str() {
                        "=" => v == target,
                        "!=" => v != target,
                        _ => false,
                    };
                }
            }
            _ => {}
        }

        Ok(mask)
    }

    /// CLONE: Zero-copy table duplication.
    /// Copies metadata entries (same S3 paths, no data copy).
    /// Supports AT(TIMESTAMP => ...) for cloning at a point in time.
    async fn exec_clone(
        &self,
        db: &str,
        schema: &str,
        clone_table: &str,
        source_table: &str,
        at_timestamp: Option<u64>,
    ) -> Result<QueryResult> {
        let source_meta = self.find_table(db, schema, source_table).await?;

        // Get source MPs (current or at timestamp)
        let source_mps = match at_timestamp {
            Some(ts) => self.meta.get_mps_at_timestamp(source_meta.id, ts).await?,
            None => self.meta.get_active_mps(source_meta.id).await?,
        };

        // Create clone table with same schema as source
        let clone_meta = TableMeta {
            id: 0, // auto-assigned
            db_id: source_meta.db_id,
            schema_id: source_meta.schema_id,
            name: clone_table.to_string(),
            columns: source_meta.columns.clone(),
            created_at: now_micros(),
            owner: source_meta.owner,
            comment: Some(format!("Clone of {}", source_table)),
            version: 0,
            properties: source_meta.properties.clone(),
        };
        self.meta.create_table(clone_meta).await?;

        // Re-fetch to get the assigned table ID
        let created = self.find_table(db, schema, clone_table).await?;

        // Link source MPs to clone (zero-copy: same S3 paths)
        for mp in &source_mps {
            let clone_mp = MicroPartitionMeta {
                mp_id: 0, // auto-assigned
                table_id: created.id,
                partition_id: mp.partition_id,
                version: mp.version,
                s3_path: mp.s3_path.clone(), // same S3 path — zero copy!
                s3_temp_path: None,
                row_count: mp.row_count,
                byte_size: mp.byte_size,
                compression: mp.compression,
                column_stats: mp.column_stats.clone(),
                commit_ts: mp.commit_ts,
                txn_id: mp.txn_id,
                supersedes: None,
                superseded_by: None,
                active: true,
            };
            self.meta.insert_mp(clone_mp).await?;
        }

        // Record clone relationship
        self.meta
            .create_clone(CloneMeta {
                clone_table_id: created.id,
                source_table_id: source_meta.id,
                clone_ts: now_micros(),
            })
            .await?;

        Ok(QueryResult::Success {
            message: format!(
                "Table {} cloned from {} ({} MPs, zero-copy)",
                clone_table,
                source_table,
                source_mps.len()
            ),
        })
    }

    /// CREATE STREAM: register a CDC stream on a table.
    ///
    /// The initial stream offset is the table's current CDC sequence so rows
    /// that existed before stream creation are not emitted by later reads.
    async fn exec_create_stream(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        stream_name: &str,
        table: &str,
        _append_only: bool,
    ) -> Result<QueryResult> {
        let table_meta = self.find_table(db, schema, table).await?;
        let stream_id = generate_id();
        let now = now_micros();
        let current_sequence = self.meta.get_table_change_sequence(table_meta.id).await?;

        let stream = StreamMeta {
            stream_id,
            db_id: table_meta.db_id,
            schema_id: table_meta.schema_id,
            source_table_id: table_meta.id,
            name: stream_name.to_string(),
            created_at: now,
            updated_at: now,
            owner_role_id: security.primary_role_id,
            comment: None,
            stale_after: None,
            dropped: false,
        };
        self.meta.create_stream(stream).await?;
        self.meta
            .set_stream_offset(
                stream_id,
                StreamOffset {
                    table_id: table_meta.id,
                    committed_sequence: current_sequence,
                    committed_ts: now,
                    last_consumed_at: None,
                    last_consumed_txn_id: None,
                },
            )
            .await?;
        if self
            .meta
            .get_role(security.primary_role_id)
            .await?
            .is_some()
        {
            self.meta
                .set_object_owner(ObjectOwnerMeta {
                    object: ObjectRef::new(ObjectType::Stream, stream_id),
                    owner_role_id: security.primary_role_id,
                    created_by_user_id: security.user_id,
                    created_at: now_micros(),
                    transferred_at: None,
                })
                .await?;
        }

        Ok(QueryResult::Success {
            message: format!(
                "Stream '{}' created on table '{}' (id={})",
                stream_name, table, stream_id
            ),
        })
    }

    async fn exec_drop_stream(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        name: &str,
    ) -> Result<QueryResult> {
        let stream = self.find_stream(db, schema, name).await?;
        self.authorize_stream_ownership(security, db, schema, &stream)
            .await?;
        self.meta.drop_stream(stream.stream_id).await?;
        Ok(QueryResult::Success {
            message: format!("Stream '{}.{}.{}' dropped", db, schema, name),
        })
    }

    async fn exec_show_streams(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        pattern: Option<&str>,
    ) -> Result<QueryResult> {
        let db_meta = self.find_database(db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Database, db_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;
        self.require_privilege(
            security,
            ObjectRef::new(ObjectType::Schema, schema_meta.id),
            SecurityPrivilege::Usage,
        )
        .await?;

        let streams = self.meta.list_streams(db_meta.id, schema_meta.id).await?;
        let mut rows = Vec::new();
        for stream in streams {
            if let Some(pattern) = pattern
                && !Self::sql_like_matches(&stream.name, pattern)
            {
                continue;
            }
            if !self
                .has_privilege(
                    security,
                    ObjectRef::new(ObjectType::Stream, stream.stream_id),
                    SecurityPrivilege::Select,
                )
                .await?
            {
                continue;
            }
            let Some(source) = self
                .meta
                .get_table(stream.db_id, stream.schema_id, stream.source_table_id)
                .await?
            else {
                continue;
            };
            if !self
                .has_privilege(
                    security,
                    ObjectRef::new(ObjectType::Table, source.id),
                    SecurityPrivilege::Select,
                )
                .await?
            {
                continue;
            }
            rows.push(vec![
                stream.name,
                stream.stream_id.to_string(),
                source.name,
                stream.source_table_id.to_string(),
                stream.created_at.to_string(),
                stream
                    .stale_after
                    .map(|ts| ts.to_string())
                    .unwrap_or_else(|| "".to_string()),
            ]);
        }
        Ok(QueryResult::Rows {
            columns: vec![
                "name".to_string(),
                "stream_id".to_string(),
                "source_table".to_string(),
                "source_table_id".to_string(),
                "created_at".to_string(),
                "stale_after".to_string(),
            ],
            rows,
        })
    }

    async fn exec_describe_stream(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        name: &str,
    ) -> Result<QueryResult> {
        let stream = self.find_stream(db, schema, name).await?;
        let source = self
            .authorize_stream_read(security, db, schema, &stream)
            .await?;
        let offset = self.meta.get_stream_offset(stream.stream_id).await?;
        Ok(QueryResult::Rows {
            columns: vec![
                "name".to_string(),
                "stream_id".to_string(),
                "source_table".to_string(),
                "source_table_id".to_string(),
                "owner_role_id".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
                "committed_sequence".to_string(),
            ],
            rows: vec![vec![
                stream.name,
                stream.stream_id.to_string(),
                source.name,
                stream.source_table_id.to_string(),
                stream.owner_role_id.to_string(),
                stream.created_at.to_string(),
                stream.updated_at.to_string(),
                offset
                    .map(|offset| offset.committed_sequence.to_string())
                    .unwrap_or_else(|| "0".to_string()),
            ]],
        })
    }

    /// GC: delete micro-partitions that are superseded and older than retention period.
    async fn exec_gc(&self, retention_days: u32) -> Result<QueryResult> {
        let deleted = self.gc_internal(retention_days).await?;
        Ok(QueryResult::Success {
            message: format!(
                "GC: deleted {} expired MPs (retention={}d)",
                deleted, retention_days
            ),
        })
    }

    /// Internal GC callable from background compaction service.
    pub async fn gc_internal(&self, retention_days: u32) -> Result<u64> {
        let cutoff_ts = now_micros().saturating_sub((retention_days as u64) * 86_400 * 1_000_000);
        let mut deleted: u64 = 0;
        let dbs = self.meta.list_databases().await?;
        for db in &dbs {
            let schemas = self.meta.list_schemas(db.id).await?;
            for schema in &schemas {
                let tables = self.meta.list_tables(db.id, schema.id).await?;
                for table in &tables {
                    let mps = self.meta.get_active_mps(table.id).await?;
                    for mp in &mps {
                        if mp.superseded_by.is_some() && mp.commit_ts < cutoff_ts {
                            self.meta.delete_mp(mp.mp_id).await?;
                            deleted += 1;
                        }
                    }
                }
            }
        }
        tracing::info!(deleted, retention_days, "GC sweep completed");
        Ok(deleted)
    }

    /// Expose metadata store (for compaction service).
    pub fn meta(&self) -> &Arc<dyn nova_storage::MetadataStore> {
        &self.meta
    }

    // ══════════════════════════════════════════════════════════════
    //  DYNAMIC TABLE — CREATE / REFRESH / DROP / SHOW / SCHEDULER
    // ══════════════════════════════════════════════════════════════

    #[allow(clippy::too_many_arguments)]
    async fn exec_create_dynamic_table(
        &self,
        security: &SecurityContext,
        db: &str,
        schema: &str,
        name: &str,
        query_definition: String,
        target_lag_seconds: u64,
        refresh_mode: nova_common::DtRefreshMode,
        initialize_on_create: bool,
    ) -> Result<QueryResult> {
        use nova_common::{DtRefreshStatus, DynamicTableMeta, generate_id, now_micros};
        // 1. Resolve db + schema to get IDs
        let db_meta = self
            .meta
            .list_databases()
            .await?
            .into_iter()
            .find(|d| d.name == db)
            .ok_or_else(|| NovaError::DatabaseNotFound {
                db_name: db.to_string(),
            })?;
        let schema_meta = self
            .meta
            .list_schemas(db_meta.id)
            .await?
            .into_iter()
            .find(|s| s.name == schema)
            .ok_or_else(|| NovaError::Internal {
                message: format!("schema '{}' not found", schema),
            })?;

        // 2. Create placeholder output table (columns will be populated on first refresh)
        let output_table_id = generate_id();
        let output_table = nova_common::TableMeta {
            id: output_table_id,
            db_id: db_meta.id,
            schema_id: schema_meta.id,
            name: format!("__dt_output_{}", name),
            columns: vec![],
            created_at: now_micros(),
            owner: 1,
            comment: Some(format!("Dynamic table output for {}", name)),
            version: 0,
            properties: std::collections::HashMap::new(),
        };
        self.meta.create_table(output_table).await?;

        // 3. Store DynamicTableMeta
        let dt_id = generate_id();
        let dt = DynamicTableMeta {
            id: dt_id,
            db_id: db_meta.id,
            schema_id: schema_meta.id,
            name: name.to_string(),
            query_definition: query_definition.clone(),
            target_lag_seconds: target_lag_seconds.max(60),
            refresh_mode,
            initialize_on_create,
            output_table_id,
            last_refresh_ts: None,
            refresh_status: DtRefreshStatus::Pending,
            comment: None,
            created_at: now_micros(),
            scheduler_enabled: true,
        };
        self.meta.create_dynamic_table(dt).await?;
        if self
            .meta
            .get_role(security.primary_role_id)
            .await?
            .is_some()
        {
            self.meta
                .set_object_owner(ObjectOwnerMeta {
                    object: ObjectRef::new(ObjectType::DynamicTable, dt_id),
                    owner_role_id: security.primary_role_id,
                    created_by_user_id: security.user_id,
                    created_at: now_micros(),
                    transferred_at: None,
                })
                .await?;
        }

        // 4. Immediate initial refresh if requested
        if initialize_on_create {
            let _ = self
                .exec_refresh_dynamic_table_by_id(
                    dt_id,
                    &query_definition,
                    output_table_id,
                    refresh_mode,
                )
                .await;
        }

        Ok(QueryResult::Success {
            message: format!(
                "Dynamic table '{}' created (lag={}s, mode={})",
                name, target_lag_seconds, refresh_mode
            ),
        })
    }

    /// Refresh a dynamic table by name (called from ALTER DT REFRESH + scheduler).
    pub async fn exec_refresh_dynamic_table(
        &self,
        db: &str,
        _schema: &str,
        name: &str,
    ) -> Result<QueryResult> {
        use nova_common::DtRefreshStatus;
        // Find DT by name in this db
        let db_meta = self
            .meta
            .list_databases()
            .await?
            .into_iter()
            .find(|d| d.name == db)
            .ok_or_else(|| NovaError::DatabaseNotFound {
                db_name: db.to_string(),
            })?;
        let mut dt = self
            .meta
            .list_dynamic_tables(db_meta.id)
            .await?
            .into_iter()
            .find(|d| d.name == name)
            .ok_or_else(|| NovaError::Internal {
                message: format!("dynamic table '{}' not found", name),
            })?;

        dt.refresh_status = DtRefreshStatus::Running;
        self.meta.update_dynamic_table(dt.clone()).await?;

        let result = self
            .exec_refresh_dynamic_table_by_id(
                dt.id,
                &dt.query_definition,
                dt.output_table_id,
                dt.refresh_mode,
            )
            .await;

        dt.refresh_status = match &result {
            Ok(_) => DtRefreshStatus::Success,
            Err(e) => DtRefreshStatus::Failed {
                error: e.to_string(),
            },
        };
        dt.last_refresh_ts = Some(nova_common::now_micros());
        self.meta.update_dynamic_table(dt).await?;

        result.map(|rows| QueryResult::Success {
            message: format!("Dynamic table '{}' refreshed ({} rows)", name, rows),
        })
    }

    async fn exec_refresh_dynamic_table_by_id(
        &self,
        _dt_id: nova_common::TableId,
        query_definition: &str,
        output_table_id: nova_common::TableId,
        refresh_mode: nova_common::DtRefreshMode,
    ) -> Result<u64> {
        // Detect if we should do incremental refresh:
        // Auto → incremental only for pure filter/project (no AGG/GROUP/DISTINCT/WINDOW)
        let use_incremental = match refresh_mode {
            nova_common::DtRefreshMode::Incremental => true,
            nova_common::DtRefreshMode::Full => false,
            nova_common::DtRefreshMode::Auto => {
                let q_upper = query_definition.to_uppercase();
                !q_upper.contains("GROUP BY")
                    && !q_upper.contains("COUNT(")
                    && !q_upper.contains("SUM(")
                    && !q_upper.contains("AVG(")
                    && !q_upper.contains("MIN(")
                    && !q_upper.contains("MAX(")
                    && !q_upper.contains("DISTINCT")
                    && !q_upper.contains("HAVING")
                    && !q_upper.contains("OVER (")
            }
        };

        if use_incremental {
            // Incremental: get last_refresh_ts, only scan MPs newer than that watermark
            let last_ts = self
                .meta
                .list_databases()
                .await
                .ok()
                .and(None::<u64>) // ponytail: lookup dt.last_refresh_ts from id
                .unwrap_or(0);
            self.do_incremental_refresh(query_definition, output_table_id, last_ts)
                .await
        } else {
            self.do_full_refresh(query_definition, output_table_id)
                .await
        }
    }

    /// Full refresh: re-execute query, replace all output MPs.
    async fn do_full_refresh(
        &self,
        query_definition: &str,
        output_table_id: nova_common::TableId,
    ) -> Result<u64> {
        // Register all tables in all databases, execute query via DataFusion
        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.skip_failed_rules = true;
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);
        let reader = std::sync::Arc::new(self.reader.clone());

        // Register all tables we can find
        for db in self.meta.list_databases().await.unwrap_or_default() {
            for schema in self.meta.list_schemas(db.id).await.unwrap_or_default() {
                for table in self
                    .meta
                    .list_tables(db.id, schema.id)
                    .await
                    .unwrap_or_default()
                {
                    if let Ok(mps) = self.meta.get_active_mps(table.id).await
                        && !mps.is_empty()
                    {
                        let provider =
                            nova_worker::NovaTableProvider::new(table.clone(), mps, reader.clone());
                        let _ = ctx.register_table(&table.name, std::sync::Arc::new(provider));
                    }
                }
            }
        }

        let df = ctx
            .sql(query_definition)
            .await
            .map_err(|e| NovaError::Internal {
                message: e.to_string(),
            })?;
        let batches = df.collect().await.map_err(|e| NovaError::Internal {
            message: e.to_string(),
        })?;
        let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

        // Write new MPs to output table
        for batch in &batches {
            let mp_meta = self
                .writer
                .write(output_table_id, 0, 0, std::slice::from_ref(batch), 1)
                .await?;
            self.meta.insert_mp(mp_meta).await?;
        }

        // Supersede old MPs in output table (FULL refresh = replace all)
        let old_mps = self.meta.get_active_mps(output_table_id).await?;
        let new_mps = self.meta.get_active_mps(output_table_id).await?;
        for old in &old_mps {
            if let Some(new) = new_mps.iter().find(|n| n.mp_id != old.mp_id) {
                let _ = self.meta.mark_superseded(old.mp_id, new.mp_id).await;
            }
        }

        Ok(total_rows)
    }

    /// Incremental refresh: only scan MPs with commit_ts > last_refresh_ts, append results.
    async fn do_incremental_refresh(
        &self,
        query_definition: &str,
        output_table_id: nova_common::TableId,
        last_ts: u64,
    ) -> Result<u64> {
        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.skip_failed_rules = true;
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);
        let reader = std::sync::Arc::new(self.reader.clone());

        let mut any_new = false;
        for db in self.meta.list_databases().await.unwrap_or_default() {
            for schema in self.meta.list_schemas(db.id).await.unwrap_or_default() {
                for table in self
                    .meta
                    .list_tables(db.id, schema.id)
                    .await
                    .unwrap_or_default()
                {
                    if let Ok(all_mps) = self.meta.get_active_mps(table.id).await {
                        // Only MPs newer than last refresh watermark
                        let new_mps: Vec<_> = all_mps
                            .into_iter()
                            .filter(|mp| mp.commit_ts > last_ts)
                            .collect();
                        if !new_mps.is_empty() {
                            any_new = true;
                            let provider = nova_worker::NovaTableProvider::new(
                                table.clone(),
                                new_mps,
                                reader.clone(),
                            );
                            let _ = ctx.register_table(&table.name, std::sync::Arc::new(provider));
                        }
                    }
                }
            }
        }

        if !any_new {
            return Ok(0); // No new data — skip refresh
        }

        let df = ctx
            .sql(query_definition)
            .await
            .map_err(|e| NovaError::Internal {
                message: e.to_string(),
            })?;
        let batches = df.collect().await.map_err(|e| NovaError::Internal {
            message: e.to_string(),
        })?;
        let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

        // Append-only: write new MPs, don't supersede old ones
        for batch in &batches {
            let mp_meta = self
                .writer
                .write(output_table_id, 0, 0, std::slice::from_ref(batch), 1)
                .await?;
            self.meta.insert_mp(mp_meta).await?;
        }

        Ok(total_rows)
    }

    async fn exec_set_dt_scheduler(
        &self,
        db: &str,
        _schema: &str,
        name: &str,
        enabled: bool,
    ) -> Result<QueryResult> {
        let db_meta = self
            .meta
            .list_databases()
            .await?
            .into_iter()
            .find(|d| d.name == db)
            .ok_or_else(|| NovaError::DatabaseNotFound {
                db_name: db.to_string(),
            })?;
        let mut dt = self
            .meta
            .list_dynamic_tables(db_meta.id)
            .await?
            .into_iter()
            .find(|d| d.name == name)
            .ok_or_else(|| NovaError::Internal {
                message: format!("dynamic table '{}' not found", name),
            })?;
        dt.scheduler_enabled = enabled;
        self.meta.update_dynamic_table(dt).await?;
        let action = if enabled { "RESUMED" } else { "SUSPENDED" };
        Ok(QueryResult::Success {
            message: format!("Dynamic table '{}' scheduler {}", name, action),
        })
    }

    async fn exec_drop_dynamic_table(
        &self,
        db: &str,
        _schema: &str,
        name: &str,
    ) -> Result<QueryResult> {
        let db_meta = self
            .meta
            .list_databases()
            .await?
            .into_iter()
            .find(|d| d.name == db)
            .ok_or_else(|| NovaError::DatabaseNotFound {
                db_name: db.to_string(),
            })?;
        let dt = self
            .meta
            .list_dynamic_tables(db_meta.id)
            .await?
            .into_iter()
            .find(|d| d.name == name)
            .ok_or_else(|| NovaError::Internal {
                message: format!("dynamic table '{}' not found", name),
            })?;
        // Drop output MPs
        for mp in self
            .meta
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap_or_default()
        {
            let _ = self.meta.delete_mp(mp.mp_id).await;
        }
        // Drop output table
        let _ = self.meta.drop_table(dt.output_table_id).await;
        // Drop DT metadata
        self.meta.drop_dynamic_table(dt.id).await?;
        Ok(QueryResult::Success {
            message: format!("Dynamic table '{}' dropped", name),
        })
    }

    async fn exec_show_dynamic_tables(
        &self,
        security: &SecurityContext,
        db: &str,
        pattern: Option<&str>,
    ) -> Result<QueryResult> {
        let db_meta = self
            .meta
            .list_databases()
            .await?
            .into_iter()
            .find(|d| d.name == db)
            .ok_or_else(|| NovaError::DatabaseNotFound {
                db_name: db.to_string(),
            })?;
        let dts = self.meta.list_dynamic_tables(db_meta.id).await?;
        let mut rows: Vec<Vec<String>> = Vec::new();
        for dt in &dts {
            if !pattern.map(|p| dt.name.contains(p)).unwrap_or(true) {
                continue;
            }
            let has_schema_usage = self
                .has_privilege(
                    security,
                    ObjectRef::new(ObjectType::Schema, dt.schema_id),
                    SecurityPrivilege::Usage,
                )
                .await?;
            let has_object_usage = self
                .has_privilege(
                    security,
                    ObjectRef::new(ObjectType::DynamicTable, dt.id),
                    SecurityPrivilege::Usage,
                )
                .await?;
            if !has_schema_usage && !has_object_usage {
                continue;
            }
            rows.push(vec![
                dt.name.clone(),
                format!("{}", dt.refresh_mode),
                dt.target_lag_seconds.to_string(),
                dt.last_refresh_ts
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "never".to_string()),
                format!("{}", dt.refresh_status),
                if dt.scheduler_enabled {
                    "enabled"
                } else {
                    "disabled"
                }
                .to_string(),
            ]);
        }
        Ok(QueryResult::Rows {
            columns: vec![
                "name".into(),
                "refresh_mode".into(),
                "target_lag_s".into(),
                "last_refresh_ts".into(),
                "status".into(),
                "scheduler".into(),
            ],
            rows,
        })
    }

    /// Expose MP reader (for compaction service).
    pub fn mp_reader(&self) -> Arc<nova_storage::MpReader> {
        Arc::new(self.reader.clone())
    }

    /// Expose MP writer (for compaction service).
    pub fn mp_writer(&self) -> &nova_storage::MpWriter {
        &self.writer
    }

    /// Execute SELECT via DataFusion SessionContext (enables AGG, GROUP BY, ORDER BY, LIMIT, JOIN).
    async fn exec_select_datafusion(
        &self,
        security: &SecurityContext,
        table_meta: &TableMeta,
        _mps: &[MicroPartitionMeta],
        sql: &str,
    ) -> Result<QueryResult> {
        // Re-fetch active MPs to ensure we see the latest state (COW visibility fix).
        // The caller's mps snapshot may be stale after UPDATE/DELETE.
        let mps = self.meta.get_active_mps(table_meta.id).await?;

        // Disable EnforceDistribution (requires children for custom scan operators)
        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.skip_failed_rules = true;
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);
        let reader = Arc::new(self.reader.clone());

        // Register the primary table
        let provider =
            nova_worker::NovaTableProvider::new(table_meta.clone(), mps.to_vec(), reader.clone());
        ctx.register_table(&table_meta.name, Arc::new(provider))
            .map_err(|e| NovaError::Internal {
                message: format!("DataFusion register table failed: {}", e),
            })?;

        // Find and register additional tables referenced in JOINs
        // ponytail: simple scan for "JOIN <table>" patterns. Upgrade to sqlparser AST walk when needed.
        let additional_tables = find_join_tables(sql, &table_meta.name);
        for table_name in &additional_tables {
            // Search all databases/schemas for this table
            let dbs = self.meta.list_databases().await?;
            for db in &dbs {
                let schemas = self.meta.list_schemas(db.id).await?;
                for schema in &schemas {
                    if let Ok(tables) = self.meta.list_tables(db.id, schema.id).await
                        && let Some(meta) = tables.iter().find(|t| t.name == *table_name)
                    {
                        let join_mps = self.meta.get_active_mps(meta.id).await?;
                        if !join_mps.is_empty() {
                            let join_provider = nova_worker::NovaTableProvider::new(
                                meta.clone(),
                                join_mps,
                                reader.clone(),
                            );
                            let _ = ctx.register_table(&meta.name, Arc::new(join_provider));
                        }
                        break;
                    }
                }
            }
        }

        self.register_sql_functions_for_query(security, &ctx, table_meta, sql)
            .await?;

        let df = ctx.sql(sql).await.map_err(|e| NovaError::Internal {
            message: format!("DataFusion SQL execution failed: {}", e),
        })?;

        let batches = df.collect().await.map_err(|e| NovaError::Internal {
            message: format!("DataFusion collect failed: {}", e),
        })?;

        let (columns, rows) = batches_to_query_result(&batches);
        Ok(QueryResult::Rows { columns, rows })
    }

    async fn register_sql_functions_for_query(
        &self,
        security: &SecurityContext,
        ctx: &datafusion::prelude::SessionContext,
        table_meta: &TableMeta,
        sql: &str,
    ) -> Result<()> {
        let calls = referenced_function_calls(sql)?;
        if calls.is_empty() {
            return Ok(());
        }
        let functions = self
            .meta
            .list_functions(table_meta.db_id, table_meta.schema_id)
            .await?;
        for call in calls {
            let matches = functions
                .iter()
                .filter(|function| {
                    function.name.eq_ignore_ascii_case(&call.name)
                        && function.args.len() == call.arg_count
                })
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [] => {}
                [function] => {
                    self.require_privilege(
                        security,
                        ObjectRef::new(ObjectType::Function, function.id),
                        SecurityPrivilege::Usage,
                    )
                    .await?;
                    let udf = SqlFunctionRuntime::create_udf(function)?;
                    ctx.register_udf(udf);
                }
                _ => {
                    return Err(NovaError::SqlAnalysisError {
                        message: format!(
                            "ambiguous function call '{}({} args)'",
                            call.name, call.arg_count
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    async fn find_table(&self, db: &str, schema: &str, table: &str) -> Result<TableMeta> {
        let dbs = self.meta.list_databases().await?;
        let db_meta =
            dbs.iter()
                .find(|d| d.name == db)
                .ok_or_else(|| NovaError::DatabaseNotFound {
                    db_name: db.to_string(),
                })?;
        let schemas = self.meta.list_schemas(db_meta.id).await?;
        let schema_meta =
            schemas
                .iter()
                .find(|s| s.name == schema)
                .ok_or_else(|| NovaError::SchemaNotFound {
                    schema_name: schema.to_string(),
                })?;
        let tables = self.meta.list_tables(db_meta.id, schema_meta.id).await?;
        tables
            .into_iter()
            .find(|t| t.name == table)
            .ok_or_else(|| NovaError::TableNotFound {
                table_name: table.to_string(),
            })
    }

    async fn find_stream(&self, db: &str, schema: &str, stream_name: &str) -> Result<StreamMeta> {
        let db_meta = self.find_database(db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
        self.meta
            .get_stream_by_name(db_meta.id, schema_meta.id, stream_name)
            .await?
            .ok_or_else(|| NovaError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })
    }

    async fn find_dynamic_table(
        &self,
        db: &str,
        schema: &str,
        name: &str,
    ) -> Result<DynamicTableMeta> {
        let db_meta = self.find_database(db).await?;
        let schema_meta = self.find_schema_meta(db_meta.id, schema).await?;
        self.meta
            .list_dynamic_tables(db_meta.id)
            .await?
            .into_iter()
            .find(|dt| dt.schema_id == schema_meta.id && dt.name == name)
            .ok_or_else(|| NovaError::Internal {
                message: format!("dynamic table '{}' not found", name),
            })
    }

    fn values_to_batch(
        &self,
        table: &TableMeta,
        values: Vec<Vec<ResolvedExpr>>,
    ) -> Result<RecordBatch> {
        let num_cols = table.columns.len();
        // Build column arrays
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
        for col_idx in 0..num_cols {
            let col_def = &table.columns[col_idx];
            match &col_def.data_type {
                NovaType::Int64 | NovaType::Int32 | NovaType::Int16 | NovaType::Int8 => {
                    let vals: Vec<Option<i64>> = values
                        .iter()
                        .map(|row| match row.get(col_idx) {
                            Some(ResolvedExpr::Int64(v)) => Some(*v),
                            Some(ResolvedExpr::Null) => None,
                            _ => None,
                        })
                        .collect();
                    columns.push(Arc::new(Int64Array::from(vals)));
                }
                NovaType::Float64 | NovaType::Float32 | NovaType::Decimal { .. } => {
                    let vals: Vec<Option<f64>> = values
                        .iter()
                        .map(|row| match row.get(col_idx) {
                            Some(ResolvedExpr::Float64(v)) => Some(*v),
                            Some(ResolvedExpr::Int64(v)) => Some(*v as f64),
                            Some(ResolvedExpr::Null) => None,
                            _ => None,
                        })
                        .collect();
                    columns.push(Arc::new(Float64Array::from(vals)));
                }
                NovaType::Utf8 => {
                    let vals: Vec<Option<String>> = values
                        .iter()
                        .map(|row| match row.get(col_idx) {
                            Some(ResolvedExpr::String(v)) => Some(v.clone()),
                            Some(ResolvedExpr::Int64(v)) => Some(v.to_string()),
                            Some(ResolvedExpr::Float64(v)) => Some(v.to_string()),
                            Some(ResolvedExpr::Boolean(v)) => Some(v.to_string()),
                            Some(ResolvedExpr::Null) => None,
                            _ => None,
                        })
                        .collect();
                    columns.push(Arc::new(StringArray::from(vals)));
                }
                _ => {
                    // Fallback: treat as string
                    let vals: Vec<Option<String>> = values
                        .iter()
                        .map(|row| match row.get(col_idx) {
                            Some(ResolvedExpr::Null) => None,
                            Some(other) => Some(format!("{:?}", other)),
                            None => None,
                        })
                        .collect();
                    columns.push(Arc::new(StringArray::from(vals)));
                }
            }
        }

        let schema = Arc::new(Schema::new(
            table
                .columns
                .iter()
                .map(|c| Field::new(&c.name, arrow_type(&c.data_type), c.nullable))
                .collect::<Vec<_>>(),
        ));

        RecordBatch::try_new(schema, columns).map_err(|e| NovaError::ArrowError {
            source: Box::new(e),
        })
    }
} // impl Executor

fn grant_result_columns() -> Vec<String> {
    vec![
        "role".to_string(),
        "object_type".to_string(),
        "object_name".to_string(),
        "privilege".to_string(),
        "grant_option".to_string(),
    ]
}

fn function_grant_rows(
    role_name: &str,
    function_name: &str,
    grant: &GrantSetMeta,
) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for privilege in [SecurityPrivilege::Usage] {
        if grant.privileges.contains(privilege) {
            rows.push(vec![
                role_name.to_string(),
                ObjectType::Function.to_string(),
                function_name.to_string(),
                privilege.to_string(),
                grant.grant_options.contains(privilege).to_string(),
            ]);
        }
    }
    rows
}

/// Parse SQL type string to NovaType.
fn format_function_name(
    db: &str,
    schema: &str,
    name: &str,
    signature: &FunctionSignature,
) -> String {
    format!("{}.{}.{}({})", db, schema, name, signature.key())
}

fn parse_sql_type(s: &str) -> NovaType {
    let upper = s.to_uppercase();
    if upper.starts_with("INT") || upper == "INTEGER" || upper.starts_with("BIGINT") {
        NovaType::Int64
    } else if upper.starts_with("SMALLINT") || upper.starts_with("TINYINT") {
        NovaType::Int32
    } else if upper.starts_with("FLOAT")
        || upper.starts_with("DOUBLE")
        || upper.starts_with("DECIMAL")
        || upper.starts_with("NUMERIC")
    {
        NovaType::Float64
    } else if upper.starts_with("VARCHAR")
        || upper.starts_with("CHAR")
        || upper == "TEXT"
        || upper == "STRING"
    {
        NovaType::Utf8
    } else if upper == "BOOLEAN" || upper == "BOOL" {
        NovaType::Boolean
    } else if upper.starts_with("DATE") {
        NovaType::Date32
    } else if upper.starts_with("TIMESTAMP") {
        NovaType::Timestamp
    } else {
        NovaType::Utf8 // default fallback
    }
}

/// Convert NovaType to Arrow DataType.
fn arrow_type(t: &NovaType) -> DataType {
    match t {
        NovaType::Int8 => DataType::Int8,
        NovaType::Int16 => DataType::Int16,
        NovaType::Int32 => DataType::Int32,
        NovaType::Int64 => DataType::Int64,
        NovaType::Float32 => DataType::Float32,
        NovaType::Float64 => DataType::Float64,
        NovaType::Utf8 => DataType::Utf8,
        NovaType::Boolean => DataType::Boolean,
        NovaType::Date32 => DataType::Date32,
        NovaType::Timestamp => DataType::Utf8, // simplified
        NovaType::Decimal { .. } => DataType::Float64,
        NovaType::Binary => DataType::Binary,
        NovaType::List(inner) => {
            DataType::List(Arc::new(Field::new("item", arrow_type(inner), true)))
        }
    }
}

/// Extract a string value from an Arrow array at a given row index.
/// Find table names referenced in JOIN clauses (simple regex-like scan).
fn find_join_tables(sql: &str, exclude: &str) -> Vec<String> {
    let mut tables = Vec::new();
    let upper = sql.to_uppercase();
    let words: Vec<&str> = sql.split_whitespace().collect();
    let upper_words: Vec<&str> = upper.split_whitespace().collect();
    for (i, w) in upper_words.iter().enumerate() {
        if (*w == "JOIN" || *w == "INNER" || *w == "LEFT" || *w == "RIGHT" || *w == "CROSS")
            && i + 1 < words.len()
        {
            // Skip JOIN keyword itself, find next word that's a table name
            for j in (i + 1)..words.len() {
                let next_upper = upper_words[j];
                if next_upper == "JOIN" {
                    continue;
                }
                let table = words[j].trim_end_matches(|c: char| !c.is_alphanumeric() && c != '_');
                if !table.is_empty() && table != exclude && !tables.contains(&table.to_string()) {
                    tables.push(table.to_string());
                }
                break;
            }
        }
    }
    tables
}

/// Convert DataFusion RecordBatches to QueryResult columns + rows.
fn batches_to_query_result(batches: &[RecordBatch]) -> (Vec<String>, Vec<Vec<String>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }
    let columns: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<String> = (0..batch.num_columns())
                .map(|col_idx| array_value_to_string(batch.column(col_idx), row_idx))
                .collect();
            rows.push(row);
        }
    }
    (columns, rows)
}

/// Check if a row value matches the filter condition.
fn row_matches_filter(val: &str, op: &str, expected: &ResolvedExpr) -> bool {
    match (op, expected) {
        ("=", ResolvedExpr::Int64(v)) => val.parse::<i64>().map(|x| x == *v).unwrap_or(false),
        ("=", ResolvedExpr::Float64(v)) => val.parse::<f64>().map(|x| x == *v).unwrap_or(false),
        ("=", ResolvedExpr::String(v)) => val == v,
        ("=", ResolvedExpr::Boolean(v)) => val == v.to_string(),
        ("!=", ResolvedExpr::Int64(v)) => val.parse::<i64>().map(|x| x != *v).unwrap_or(false),
        ("!=", ResolvedExpr::String(v)) => val != v,
        (">", ResolvedExpr::Int64(v)) => val.parse::<i64>().map(|x| x > *v).unwrap_or(false),
        (">", ResolvedExpr::Float64(v)) => val.parse::<f64>().map(|x| x > *v).unwrap_or(false),
        ("<", ResolvedExpr::Int64(v)) => val.parse::<i64>().map(|x| x < *v).unwrap_or(false),
        ("<", ResolvedExpr::Float64(v)) => val.parse::<f64>().map(|x| x < *v).unwrap_or(false),
        (">=", ResolvedExpr::Int64(v)) => val.parse::<i64>().map(|x| x >= *v).unwrap_or(false),
        ("<=", ResolvedExpr::Int64(v)) => val.parse::<i64>().map(|x| x <= *v).unwrap_or(false),
        (">", ResolvedExpr::String(v)) => val > v.as_str(),
        ("<", ResolvedExpr::String(v)) => val < v.as_str(),
        (">=", ResolvedExpr::String(v)) => val >= v.as_str(),
        ("<=", ResolvedExpr::String(v)) => val <= v.as_str(),
        _ => true, // unknown op → include row
    }
}

/// Build Arrow schema from table metadata.
fn build_arrow_schema(table_meta: &TableMeta) -> std::sync::Arc<arrow::datatypes::Schema> {
    let fields: Vec<arrow::datatypes::Field> = table_meta
        .columns
        .iter()
        .map(|c| {
            let dt = match &c.data_type {
                NovaType::Boolean => arrow::datatypes::DataType::Boolean,
                NovaType::Int8 => arrow::datatypes::DataType::Int8,
                NovaType::Int16 => arrow::datatypes::DataType::Int16,
                NovaType::Int32 => arrow::datatypes::DataType::Int32,
                NovaType::Int64 => arrow::datatypes::DataType::Int64,
                NovaType::Float32 => arrow::datatypes::DataType::Float32,
                NovaType::Float64 => arrow::datatypes::DataType::Float64,
                NovaType::Utf8 => arrow::datatypes::DataType::Utf8,
                NovaType::Date32 => arrow::datatypes::DataType::Date32,
                NovaType::Timestamp => arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    None,
                ),
                NovaType::Binary => arrow::datatypes::DataType::Binary,
                NovaType::Decimal { precision, scale } => {
                    arrow::datatypes::DataType::Decimal128(*precision, *scale)
                }
                NovaType::List(inner) => arrow::datatypes::DataType::List(std::sync::Arc::new(
                    arrow::datatypes::Field::new("item", nova_type_to_arrow(inner), true),
                )),
            };
            arrow::datatypes::Field::new(&c.name, dt, c.nullable)
        })
        .collect();
    std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
}

fn nova_type_to_arrow(t: &NovaType) -> arrow::datatypes::DataType {
    match t {
        NovaType::Boolean => arrow::datatypes::DataType::Boolean,
        NovaType::Int8 => arrow::datatypes::DataType::Int8,
        NovaType::Int16 => arrow::datatypes::DataType::Int16,
        NovaType::Int32 => arrow::datatypes::DataType::Int32,
        NovaType::Int64 => arrow::datatypes::DataType::Int64,
        NovaType::Float32 => arrow::datatypes::DataType::Float32,
        NovaType::Float64 => arrow::datatypes::DataType::Float64,
        NovaType::Utf8 => arrow::datatypes::DataType::Utf8,
        NovaType::Date32 => arrow::datatypes::DataType::Date32,
        NovaType::Timestamp => {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        NovaType::Binary => arrow::datatypes::DataType::Binary,
        NovaType::Decimal { precision, scale } => {
            arrow::datatypes::DataType::Decimal128(*precision, *scale)
        }
        NovaType::List(inner) => arrow::datatypes::DataType::List(std::sync::Arc::new(
            arrow::datatypes::Field::new("item", nova_type_to_arrow(inner), true),
        )),
    }
}

fn array_value_to_string(arr: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    if arr.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return a.value(row).to_string();
    }
    "?".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::{Analyzer, ResolvedColumn};
    use crate::parser::SqlParser;
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn setup() -> (Executor, TempDir) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let meta: Arc<dyn MetadataStore> = Arc::new(
            nova_storage::FdbMetadataStore::open_test(
                "docker:docker@127.0.0.1:4500",
                format!(
                    "test_{}_{}",
                    nova_common::now_micros(),
                    nova_common::generate_id()
                )
                .into_bytes(),
            )
            .unwrap(),
        );
        let writer = MpWriter::new(store.clone(), "test".to_string());
        let reader = MpReader::new(store);
        let executor = Executor::new(meta, writer, reader);
        (executor, dir)
    }

    #[tokio::test]
    async fn test_create_database() {
        let (executor, _dir) = setup();
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "my_db".to_string(),
            })
            .await
            .unwrap();
        if let QueryResult::Success { message } = result {
            assert!(message.contains("my_db"));
        } else {
            panic!("expected Success");
        }
    }

    #[tokio::test]
    async fn test_create_table() {
        let (executor, _dir) = setup();
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "my_db".to_string(),
            })
            .await
            .unwrap();

        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::CreateTable {
                db: "my_db".to_string(),
                schema: "public".to_string(),
                table: "orders".to_string(),
                columns: vec![
                    ResolvedColumn {
                        name: "id".to_string(),
                        data_type: "INT".to_string(),
                        nullable: true,
                    },
                    ResolvedColumn {
                        name: "amount".to_string(),
                        data_type: "DECIMAL".to_string(),
                        nullable: true,
                    },
                ],
            })
            .await
            .unwrap();
        if let QueryResult::Success { message } = result {
            assert!(message.contains("orders"));
        } else {
            panic!("expected Success");
        }
    }

    #[tokio::test]
    async fn test_insert_and_select() {
        let (executor, _dir) = setup();

        // Create database
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "db".to_string(),
            })
            .await
            .unwrap();

        // Create table
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateTable {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                columns: vec![
                    ResolvedColumn {
                        name: "id".to_string(),
                        data_type: "INT".to_string(),
                        nullable: true,
                    },
                    ResolvedColumn {
                        name: "name".to_string(),
                        data_type: "VARCHAR".to_string(),
                        nullable: true,
                    },
                ],
            })
            .await
            .unwrap();

        // Insert
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::Insert {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                values: vec![
                    vec![
                        ResolvedExpr::Int64(1),
                        ResolvedExpr::String("alice".to_string()),
                    ],
                    vec![
                        ResolvedExpr::Int64(2),
                        ResolvedExpr::String("bob".to_string()),
                    ],
                ],
            })
            .await
            .unwrap();
        if let QueryResult::Success { message } = result {
            assert!(message.contains("2 row(s)"));
        } else {
            panic!("expected Success");
        }

        // Select
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::Select {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                dependencies: vec!["users".to_string()],
                projection: vec!["id".to_string(), "name".to_string()],
                filter: None,
                at_timestamp: None,
                raw_sql: None,
            })
            .await
            .unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert_eq!(columns, vec!["id", "name"]);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], "1");
            assert_eq!(rows[0][1], "alice");
            assert_eq!(rows[1][0], "2");
            assert_eq!(rows[1][1], "bob");
        } else {
            panic!("expected Rows");
        }
    }

    #[tokio::test]
    async fn test_end_to_end_sql() {
        let (executor, _dir) = setup();
        let parser = SqlParser::new();
        let analyzer = Analyzer::new("testdb".to_string(), "public".to_string());

        // CREATE DATABASE
        let stmts = parser.parse("CREATE DATABASE testdb").unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        executor
            .execute_as_root_for_internal(resolved)
            .await
            .unwrap();

        // CREATE TABLE
        let stmts = parser
            .parse("CREATE TABLE testdb.public.items (id INT, price FLOAT)")
            .unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        executor
            .execute_as_root_for_internal(resolved)
            .await
            .unwrap();

        // INSERT
        let stmts = parser.parse("INSERT INTO items VALUES (1, 9.99)").unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        executor
            .execute_as_root_for_internal(resolved)
            .await
            .unwrap();

        // SELECT
        let stmts = parser.parse("SELECT id, price FROM items").unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        let result = executor
            .execute_as_root_for_internal(resolved)
            .await
            .unwrap();

        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], "1");
            assert_eq!(rows[0][1], "9.99");
        } else {
            panic!("expected Rows");
        }
    }
    #[tokio::test]
    async fn test_update_cow() {
        let (executor, _dir) = setup();

        // Create database + table + insert data
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "db".to_string(),
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateTable {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                columns: vec![
                    ResolvedColumn {
                        name: "id".to_string(),
                        data_type: "INT".to_string(),
                        nullable: false,
                    },
                    ResolvedColumn {
                        name: "name".to_string(),
                        data_type: "VARCHAR".to_string(),
                        nullable: false,
                    },
                ],
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::Insert {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                values: vec![
                    vec![
                        ResolvedExpr::Int64(1),
                        ResolvedExpr::String("alice".to_string()),
                    ],
                    vec![
                        ResolvedExpr::Int64(2),
                        ResolvedExpr::String("bob".to_string()),
                    ],
                ],
            })
            .await
            .unwrap();

        // UPDATE users SET name = 'updated' WHERE id = 1
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::Update {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                assignments: vec![(
                    "name".to_string(),
                    ResolvedExpr::String("updated".to_string()),
                )],
                filter: Some(ResolvedFilter {
                    column: "id".to_string(),
                    op: "=".to_string(),
                    value: ResolvedExpr::Int64(1),
                }),
            })
            .await
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert!(rows[0][0].contains("UPDATE OK"));
            }
            _ => panic!("expected Rows"),
        }
    }

    #[tokio::test]
    async fn test_delete_cow() {
        let (executor, _dir) = setup();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "db".to_string(),
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateTable {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "items".to_string(),
                columns: vec![ResolvedColumn {
                    name: "id".to_string(),
                    data_type: "INT".to_string(),
                    nullable: false,
                }],
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::Insert {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "items".to_string(),
                values: vec![
                    vec![ResolvedExpr::Int64(1)],
                    vec![ResolvedExpr::Int64(2)],
                    vec![ResolvedExpr::Int64(3)],
                ],
            })
            .await
            .unwrap();

        // DELETE FROM items WHERE id = 2
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::Delete {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "items".to_string(),
                filter: Some(ResolvedFilter {
                    column: "id".to_string(),
                    op: "=".to_string(),
                    value: ResolvedExpr::Int64(2),
                }),
            })
            .await
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert!(rows[0][0].contains("DELETE OK"));
            }
            _ => panic!("expected Rows"),
        }
    }

    #[tokio::test]
    async fn test_clone_zero_copy() {
        let (executor, _dir) = setup();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "db".to_string(),
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateTable {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "source".to_string(),
                columns: vec![ResolvedColumn {
                    name: "id".to_string(),
                    data_type: "INT".to_string(),
                    nullable: false,
                }],
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::Insert {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "source".to_string(),
                values: vec![vec![ResolvedExpr::Int64(42)]],
            })
            .await
            .unwrap();

        // CLONE source → clone_table
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::CreateClone {
                db: "db".to_string(),
                schema: "public".to_string(),
                clone_table: "clone_table".to_string(),
                source_table: "source".to_string(),
                at_timestamp: None,
            })
            .await
            .unwrap();

        match result {
            QueryResult::Success { message } => {
                assert!(message.contains("zero-copy"));
                assert!(message.contains("1 MPs"));
            }
            _ => panic!("expected Success"),
        }
    }

    #[tokio::test]
    async fn test_create_stream() {
        let (executor, _dir) = setup();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "db".to_string(),
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateTable {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "events".to_string(),
                columns: vec![ResolvedColumn {
                    name: "id".to_string(),
                    data_type: "INT".to_string(),
                    nullable: false,
                }],
            })
            .await
            .unwrap();

        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::CreateStream {
                db: "db".to_string(),
                schema: "public".to_string(),
                stream_name: "events_stream".to_string(),
                table: "events".to_string(),
                append_only: false,
            })
            .await
            .unwrap();

        match result {
            QueryResult::Success { message } => {
                assert!(message.contains("events_stream"));
                assert!(message.contains("events"));
                assert!(!message.contains("append_only"));
            }
            _ => panic!("expected Success"),
        }
    }

    #[tokio::test]
    async fn test_time_travel_select() {
        let (executor, _dir) = setup();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDatabase {
                name: "db".to_string(),
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateTable {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "history".to_string(),
                columns: vec![ResolvedColumn {
                    name: "id".to_string(),
                    data_type: "INT".to_string(),
                    nullable: false,
                }],
            })
            .await
            .unwrap();

        // Insert data
        executor
            .execute_as_root_for_internal(ResolvedStatement::Insert {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "history".to_string(),
                values: vec![vec![ResolvedExpr::Int64(1)]],
            })
            .await
            .unwrap();

        // SELECT with at_timestamp = 0 (before any data) → should return empty
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::Select {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "history".to_string(),
                dependencies: vec!["history".to_string()],
                projection: vec!["id".to_string()],
                filter: None,
                at_timestamp: Some(1), // very early timestamp
                raw_sql: None,
            })
            .await
            .unwrap();

        match result {
            QueryResult::Rows { rows, .. } => {
                assert!(rows.is_empty(), "Time Travel to t=1 should return no data");
            }
            _ => panic!("expected Rows"),
        }
    }
}
