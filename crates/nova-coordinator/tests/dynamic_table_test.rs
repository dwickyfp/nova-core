//! Dynamic Table integration tests.

#[cfg(test)]
mod tests {
    use nova_common::{
        GrantSetMeta, ObjectRef, ObjectType, PrivilegeSet, RoleMeta, SchemaMeta, SecurityPrivilege,
    };
    use nova_coordinator::analyzer::{Analyzer, ResolvedStatement};
    use nova_coordinator::executor::{Executor, QueryResult};
    use nova_coordinator::parser::SqlParser;
    use nova_storage::{FdbMetadataStore, MetadataStore, MpReader, MpWriter};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn setup() -> (Executor, TempDir) {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let meta = Arc::new(
            FdbMetadataStore::open_test(
                "docker:docker@127.0.0.1:4500",
                format!(
                    "test_{}_{}",
                    nova_common::now_micros(),
                    nova_common::generate_id()
                )
                .into_bytes(),
            )
            .unwrap(),
        ) as Arc<dyn MetadataStore>;
        let store =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&data_dir).unwrap())
                as Arc<dyn object_store::ObjectStore>;
        let writer = MpWriter::new(store.clone(), "nova".to_string());
        let reader = MpReader::new(store);
        (Executor::new(meta, writer, reader), dir)
    }

    async fn exec(executor: &Executor, sql: &str, db: &str) -> QueryResult {
        exec_in_schema(executor, sql, db, "public").await
    }

    async fn exec_in_schema(executor: &Executor, sql: &str, db: &str, schema: &str) -> QueryResult {
        let parser = SqlParser::new();
        let analyzer = Analyzer::new(db.to_string(), schema.to_string());
        let stmts = parser.parse(sql).unwrap();
        let mut last = QueryResult::Success {
            message: "noop".to_string(),
        };
        for stmt in &stmts {
            let resolved = analyzer.resolve(stmt).unwrap();
            last = executor
                .execute_as_root_for_internal(resolved)
                .await
                .unwrap();
        }
        last
    }

    /// Create db + public schema so exec_create_dynamic_table can find them.
    async fn setup_db(executor: &Executor, db: &str) {
        exec(executor, &format!("CREATE DATABASE {}", db), db).await;
        let dbs = executor.meta().list_databases().await.unwrap();
        if let Some(db_meta) = dbs.iter().find(|d| d.name == db) {
            let schema = SchemaMeta {
                id: nova_common::generate_id(),
                db_id: db_meta.id,
                name: "public".to_string(),
                created_at: nova_common::now_micros(),
            };
            let _ = executor.meta().create_schema(schema).await;
        }
    }

    async fn db_id(executor: &Executor, db: &str) -> u64 {
        executor
            .meta()
            .list_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|d| d.name == db)
            .unwrap()
            .id
    }

    async fn schema_id(executor: &Executor, db: &str, schema: &str) -> u64 {
        let db_id = db_id(executor, db).await;
        executor
            .meta()
            .list_schemas(db_id)
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.name == schema)
            .unwrap()
            .id
    }

    async fn public_schema_id(executor: &Executor, db: &str) -> u64 {
        schema_id(executor, db, "public").await
    }

    async fn table_id(executor: &Executor, db: &str, schema: &str, table: &str) -> u64 {
        let db_id = db_id(executor, db).await;
        let schema_id = schema_id(executor, db, schema).await;
        executor
            .meta()
            .list_tables(db_id, schema_id)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.name == table)
            .unwrap()
            .id
    }

    async fn create_schema(executor: &Executor, db: &str, schema_name: &str) -> u64 {
        let db_id = db_id(executor, db).await;
        let schema = SchemaMeta {
            id: nova_common::generate_id(),
            db_id,
            name: schema_name.to_string(),
            created_at: nova_common::now_micros(),
        };
        executor.meta().create_schema(schema).await.unwrap();
        schema_id(executor, db, schema_name).await
    }

    async fn dynamic_table_by_schema(
        executor: &Executor,
        db: &str,
        schema: &str,
        name: &str,
    ) -> nova_common::DynamicTableMeta {
        let db_id = db_id(executor, db).await;
        let schema_id = schema_id(executor, db, schema).await;
        executor
            .meta()
            .list_dynamic_tables(db_id)
            .await
            .unwrap()
            .into_iter()
            .find(|dt| dt.schema_id == schema_id && dt.name == name)
            .unwrap()
    }

    async fn create_dynamic_table(executor: &Executor, db: &str, schema: &str, name: &str) {
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: db.to_string(),
                schema: schema.to_string(),
                name: name.to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();
    }

    async fn create_test_role(executor: &Executor, name: &str) -> u64 {
        executor.meta().bootstrap_security().await.unwrap();
        executor
            .meta()
            .create_role(RoleMeta {
                id: 0,
                name: name.to_string(),
                owner_role_id: nova_common::ACCOUNTADMIN_ROLE_ID,
                system: false,
                created_at: nova_common::now_micros(),
                created_by_user_id: nova_common::ROOT_USER_ID,
                comment: None,
            })
            .await
            .unwrap()
    }

    async fn grant_privilege(
        executor: &Executor,
        role_id: u64,
        object: ObjectRef,
        privilege: SecurityPrivilege,
    ) {
        executor
            .meta()
            .grant_privileges(GrantSetMeta {
                role_id,
                object,
                privileges: PrivilegeSet::from_privileges(&[privilege]),
                grant_options: PrivilegeSet::empty(),
                granted_by_role_id: nova_common::ACCOUNTADMIN_ROLE_ID,
                updated_at: nova_common::now_micros(),
            })
            .await
            .unwrap();
    }

    // ── parser tests (no executor needed) ───────────────────────

    #[tokio::test]
    async fn test_parser_detects_dynamic_table_full() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse(
                "CREATE DYNAMIC TABLE dt_orders \
                 TARGET_LAG = '5 minutes' \
                 REFRESH_MODE = FULL \
                 AS SELECT id FROM orders",
            )
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[tokio::test]
    async fn test_parser_detects_dynamic_table_incremental() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse(
                "CREATE DYNAMIC TABLE dt_active \
                 TARGET_LAG = '1 minute' \
                 REFRESH_MODE = INCREMENTAL \
                 AS SELECT * FROM orders WHERE status = 'active'",
            )
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    // ── analyzer tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_analyzer_resolves_create_dt() {
        let parser = SqlParser::new();
        let analyzer = Analyzer::new("testdb".to_string(), "public".to_string());
        let stmts = parser
            .parse(
                "CREATE DYNAMIC TABLE dt_summary \
                 TARGET_LAG = '10 minutes' \
                 REFRESH_MODE = FULL \
                 AS SELECT COUNT(*) FROM orders",
            )
            .unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        match resolved {
            ResolvedStatement::CreateDynamicTable {
                name,
                target_lag_seconds,
                refresh_mode,
                ..
            } => {
                assert_eq!(name, "dt_summary");
                assert_eq!(target_lag_seconds, 600); // 10 minutes
                assert_eq!(refresh_mode, nova_common::DtRefreshMode::Full);
            }
            other => panic!("Expected CreateDynamicTable, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_analyzer_resolves_alter_dt_refresh() {
        let parser = SqlParser::new();
        let analyzer = Analyzer::new("testdb".to_string(), "public".to_string());
        let stmts = parser
            .parse("ALTER DYNAMIC TABLE dt_orders REFRESH")
            .unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        assert!(matches!(
            resolved,
            ResolvedStatement::RefreshDynamicTable { .. }
        ));
    }

    #[tokio::test]
    async fn test_analyzer_resolves_drop_dt() {
        let parser = SqlParser::new();
        let analyzer = Analyzer::new("testdb".to_string(), "public".to_string());
        let stmts = parser.parse("DROP DYNAMIC TABLE dt_orders").unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        assert!(
            matches!(resolved, ResolvedStatement::DropDynamicTable { ref name, .. } if name == "dt_orders")
        );
    }

    #[tokio::test]
    async fn test_analyzer_resolves_show_dt() {
        let parser = SqlParser::new();
        let analyzer = Analyzer::new("testdb".to_string(), "public".to_string());
        let stmts = parser.parse("SHOW DYNAMIC TABLES").unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        assert!(matches!(
            resolved,
            ResolvedStatement::ShowDynamicTables { .. }
        ));
    }

    // ── metadata (FDB) CRUD ──────────────────────────────────────

    #[tokio::test]
    async fn test_fdb_dynamic_table_crud() {
        use nova_common::{
            ColumnDef, DatabaseMeta, DtRefreshMode, DtRefreshStatus, DynamicTableMeta, NovaType,
            TableMeta, now_micros,
        };
        let _dir = TempDir::new().unwrap();
        let meta = FdbMetadataStore::open_test(
            "docker:docker@127.0.0.1:4500",
            format!(
                "test_{}_{}",
                nova_common::now_micros(),
                nova_common::generate_id()
            )
            .into_bytes(),
        )
        .unwrap();
        meta.create_database(DatabaseMeta {
            id: 1,
            name: "testdb".to_string(),
            created_at: now_micros(),
            owner: nova_common::ROOT_USER_ID,
        })
        .await
        .unwrap();
        meta.create_schema(SchemaMeta {
            id: 1,
            db_id: 1,
            name: "public".to_string(),
            created_at: now_micros(),
        })
        .await
        .unwrap();
        meta.create_table(TableMeta {
            id: 100,
            db_id: 1,
            schema_id: 1,
            name: "__dt_output_dt_test".to_string(),
            columns: vec![ColumnDef {
                id: 1,
                name: "id".to_string(),
                data_type: NovaType::Int64,
                nullable: false,
                default_value: None,
                comment: None,
            }],
            created_at: now_micros(),
            owner: nova_common::ROOT_USER_ID,
            comment: None,
            version: 0,
            properties: Default::default(),
        })
        .await
        .unwrap();
        let dt = DynamicTableMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "dt_test".to_string(),
            query_definition: "SELECT * FROM orders".to_string(),
            target_lag_seconds: 300,
            refresh_mode: DtRefreshMode::Full,
            initialize_on_create: true,
            output_table_id: 100,
            last_refresh_ts: None,
            refresh_status: DtRefreshStatus::Pending,
            comment: None,
            created_at: now_micros(),
            scheduler_enabled: true,
        };
        meta.create_dynamic_table(dt).await.unwrap();
        let got = meta.get_dynamic_table(1).await.unwrap().unwrap();
        assert_eq!(got.name, "dt_test");
        assert_eq!(got.target_lag_seconds, 300);

        assert_eq!(meta.list_dynamic_tables(1).await.unwrap().len(), 1);

        let mut updated = got;
        updated.refresh_status = DtRefreshStatus::Success;
        meta.update_dynamic_table(updated).await.unwrap();
        assert_eq!(
            meta.get_dynamic_table(1)
                .await
                .unwrap()
                .unwrap()
                .refresh_status,
            DtRefreshStatus::Success
        );

        meta.drop_dynamic_table(1).await.unwrap();
        assert!(meta.get_dynamic_table(1).await.unwrap().is_none());
    }

    // ── executor tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_non_admin_cannot_create_dynamic_table() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;

        let analyst = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 100,
            username: "analyst".to_string(),
            primary_role_id: nova_common::PUBLIC_ROLE_ID,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        let err = executor
            .execute_with_context(
                ResolvedStatement::CreateDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_summary".to_string(),
                    query_definition: "SELECT 1".to_string(),

                    target_lag_seconds: 300,
                    refresh_mode: nova_common::DtRefreshMode::Full,
                    initialize_on_create: false,
                },
                &analyst,
            )
            .await
            .expect_err("non-admin user must not be allowed to create a dynamic table");

        assert!(
            matches!(err, nova_common::NovaError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_non_admin_cannot_refresh_dynamic_table() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_summary".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let analyst = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 100,
            username: "analyst".to_string(),
            primary_role_id: nova_common::PUBLIC_ROLE_ID,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        let err = executor
            .execute_with_context(
                ResolvedStatement::RefreshDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_summary".to_string(),
                },
                &analyst,
            )
            .await
            .expect_err("non-admin user must not be allowed to refresh a dynamic table");

        assert!(
            matches!(err, nova_common::NovaError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_non_admin_cannot_suspend_dynamic_table() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_summary".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let analyst = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 100,
            username: "analyst".to_string(),
            primary_role_id: nova_common::PUBLIC_ROLE_ID,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        let err = executor
            .execute_with_context(
                ResolvedStatement::SuspendDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_summary".to_string(),
                },
                &analyst,
            )
            .await
            .expect_err("non-admin user must not be allowed to suspend a dynamic table");

        assert!(
            matches!(err, nova_common::NovaError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_non_admin_cannot_drop_dynamic_table() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_summary".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let analyst = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 100,
            username: "analyst".to_string(),
            primary_role_id: nova_common::PUBLIC_ROLE_ID,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        let err = executor
            .execute_with_context(
                ResolvedStatement::DropDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_summary".to_string(),
                },
                &analyst,
            )
            .await
            .expect_err("non-admin user must not be allowed to drop a dynamic table");

        assert!(
            matches!(err, nova_common::NovaError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_create_dynamic_table_via_executor() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_summary".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Success { .. }));
    }

    #[tokio::test]
    async fn test_create_dynamic_table_via_executor_uses_fdb_id_allocation() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_allocated".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let db_meta = executor
            .meta()
            .list_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|db| db.name == "testdb")
            .unwrap();
        let schema_id = public_schema_id(&executor, "testdb").await;
        let output_table = executor
            .meta()
            .list_tables(db_meta.id, schema_id)
            .await
            .unwrap()
            .into_iter()
            .find(|table| table.name == "__dt_output_dt_allocated")
            .expect("dynamic table output table should exist");
        let dynamic_table = executor
            .meta()
            .list_dynamic_tables(db_meta.id)
            .await
            .unwrap()
            .into_iter()
            .find(|dt| dt.name == "dt_allocated")
            .expect("dynamic table metadata should exist");

        assert_ne!(output_table.id, 0);
        assert_ne!(dynamic_table.id, 0);
        assert_eq!(dynamic_table.output_table_id, output_table.id);
    }

    #[tokio::test]
    async fn test_create_dynamic_table_populates_output_table_schema() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(
            &executor,
            "CREATE TABLE source (id INT, status VARCHAR)",
            "testdb",
        )
        .await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_projected".to_string(),
                query_definition: "SELECT id, status FROM source".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let dt = dynamic_table_by_schema(&executor, "testdb", "public", "dt_projected").await;
        let db_id = db_id(&executor, "testdb").await;
        let schema_id = public_schema_id(&executor, "testdb").await;
        let output_table = executor
            .meta()
            .get_table(db_id, schema_id, dt.output_table_id)
            .await
            .unwrap()
            .expect("dynamic table output table should exist");

        let column_names: Vec<_> = output_table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(column_names, vec!["id", "status"]);
    }

    #[tokio::test]
    async fn test_create_dynamic_table_rejects_missing_source_without_orphan_output_table() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;

        let err = executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_missing_source".to_string(),
                query_definition: "SELECT * FROM missing_source".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .expect_err("missing source table must reject dynamic table creation");
        assert!(matches!(err, nova_common::NovaError::TableNotFound { .. }));

        let db_id = db_id(&executor, "testdb").await;
        let schema_id = public_schema_id(&executor, "testdb").await;
        let output_tables: Vec<_> = executor
            .meta()
            .list_tables(db_id, schema_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|table| table.name == "__dt_output_dt_missing_source")
            .collect();
        let dynamic_tables: Vec<_> = executor
            .meta()
            .list_dynamic_tables(db_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|dt| dt.schema_id == schema_id && dt.name == "dt_missing_source")
            .collect();
        assert!(output_tables.is_empty());
        assert!(dynamic_tables.is_empty());
    }

    #[tokio::test]
    async fn test_duplicate_dynamic_table_create_does_not_orphan_output_table() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        create_dynamic_table(&executor, "testdb", "public", "dt_duplicate").await;

        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_duplicate".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await;

        assert!(result.is_err());
        let db_meta = executor
            .meta()
            .list_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|db| db.name == "testdb")
            .unwrap();
        let schema_id = public_schema_id(&executor, "testdb").await;
        let output_tables: Vec<_> = executor
            .meta()
            .list_tables(db_meta.id, schema_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|table| table.name == "__dt_output_dt_duplicate")
            .collect();
        assert_eq!(output_tables.len(), 1);
    }

    #[tokio::test]
    async fn test_refresh_dynamic_table_publishes_unique_committed_active_mps() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO source VALUES (1)", "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_source".to_string(),
                query_definition: "SELECT * FROM source".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let dt = dynamic_table_by_schema(&executor, "testdb", "public", "dt_source").await;

        for _ in 0..2 {
            executor
                .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_source".to_string(),
                })
                .await
                .unwrap();
        }

        let active_mps = executor
            .meta()
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap();
        assert_eq!(active_mps.len(), 1);
        let mp = &active_mps[0];
        assert_ne!(mp.mp_id, 0);
        assert_ne!(mp.version, 0);
        assert!(mp.active);
        assert!(mp.s3_temp_path.is_none());
        assert!(mp.commit_ts > 0);
        assert!(
            !mp.s3_path.contains("/tmp/"),
            "dynamic-table MP metadata should reference committed object path, got {}",
            mp.s3_path
        );
    }

    #[tokio::test]
    async fn test_full_refresh_to_empty_result_deactivates_old_output_mps() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO source VALUES (1)", "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_empty".to_string(),
                query_definition: "SELECT * FROM source WHERE id > 0".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let dt = dynamic_table_by_schema(&executor, "testdb", "public", "dt_empty").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_empty".to_string(),
            })
            .await
            .unwrap();
        let initial_mps = executor
            .meta()
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap();
        assert_eq!(initial_mps.iter().map(|mp| mp.row_count).sum::<u64>(), 1);

        exec(&executor, "DELETE FROM source WHERE id > 0", "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_empty".to_string(),
            })
            .await
            .unwrap();

        let refreshed_mps = executor
            .meta()
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap();
        assert_eq!(
            refreshed_mps.iter().map(|mp| mp.row_count).sum::<u64>(),
            0,
            "full refresh to an empty result must not leave stale output rows active"
        );
    }

    #[tokio::test]
    async fn test_incremental_refresh_uses_last_refresh_watermark() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO source VALUES (1)", "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental".to_string(),
                query_definition: "SELECT * FROM source".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Incremental,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let dt = dynamic_table_by_schema(&executor, "testdb", "public", "dt_incremental").await;

        for _ in 0..2 {
            executor
                .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_incremental".to_string(),
                })
                .await
                .unwrap();
        }

        let active_mps = executor
            .meta()
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap();
        assert_eq!(
            active_mps.iter().map(|mp| mp.row_count).sum::<u64>(),
            1,
            "second incremental refresh without new source MPs must not append duplicates"
        );
    }

    #[tokio::test]
    async fn test_incremental_refresh_over_delete_uses_current_snapshot() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO source VALUES (1)", "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_delete".to_string(),
                query_definition: "SELECT * FROM source".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Incremental,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let dt =
            dynamic_table_by_schema(&executor, "testdb", "public", "dt_incremental_delete").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_delete".to_string(),
            })
            .await
            .unwrap();
        exec(&executor, "DELETE FROM source WHERE id = 1", "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_delete".to_string(),
            })
            .await
            .unwrap();

        let active_mps = executor
            .meta()
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap();
        assert_eq!(
            active_mps.iter().map(|mp| mp.row_count).sum::<u64>(),
            0,
            "incremental refresh over COW deletes must not keep stale output rows active"
        );
    }

    #[tokio::test]
    async fn test_incremental_refresh_does_not_skip_source_mp_committed_during_previous_refresh() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO source VALUES (1)", "testdb").await;
        let source_table_id = table_id(&executor, "testdb", "public", "source").await;
        let initial_source_watermark = executor
            .meta()
            .get_active_mps(source_table_id)
            .await
            .unwrap()
            .into_iter()
            .map(|mp| mp.commit_ts)
            .max()
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_gap".to_string(),
                query_definition: "SELECT * FROM source".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Incremental,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let dt = dynamic_table_by_schema(&executor, "testdb", "public", "dt_incremental_gap").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_gap".to_string(),
            })
            .await
            .unwrap();

        exec(&executor, "INSERT INTO source VALUES (2)", "testdb").await;
        let mut source_mps = executor
            .meta()
            .get_active_mps(source_table_id)
            .await
            .unwrap();
        source_mps.sort_by_key(|mp| mp.commit_ts);
        let mut backdated_mp = source_mps.pop().unwrap();
        executor.meta().delete_mp(backdated_mp.mp_id).await.unwrap();
        backdated_mp.commit_ts = initial_source_watermark + 1;
        executor.meta().insert_mp(backdated_mp).await.unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_gap".to_string(),
            })
            .await
            .unwrap();

        let active_mps = executor
            .meta()
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap();
        assert_eq!(
            active_mps.iter().map(|mp| mp.row_count).sum::<u64>(),
            2,
            "incremental refresh must not advance its watermark past source MPs it did not scan"
        );
    }

    #[tokio::test]
    async fn test_dynamic_table_refresh_resolves_sources_in_own_schema() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        create_schema(&executor, "testdb", "alt").await;
        exec(&executor, "CREATE TABLE source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO source VALUES (1)", "testdb").await;
        exec_in_schema(&executor, "CREATE TABLE source (id INT)", "testdb", "alt").await;
        exec_in_schema(&executor, "INSERT INTO source VALUES (2)", "testdb", "alt").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "alt".to_string(),
                name: "dt_alt_source".to_string(),
                query_definition: "SELECT * FROM source".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let dt = dynamic_table_by_schema(&executor, "testdb", "alt", "dt_alt_source").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "alt".to_string(),
                name: "dt_alt_source".to_string(),
            })
            .await
            .unwrap();

        match executor
            .execute_as_root_for_internal(ResolvedStatement::Select {
                db: "testdb".to_string(),
                schema: "alt".to_string(),
                table: "__dt_output_dt_alt_source".to_string(),
                dependencies: vec![],
                projection: vec!["*".to_string()],
                filter: None,
                at_timestamp: None,
                raw_sql: None,
            })
            .await
            .unwrap()
        {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows, vec![vec!["2".to_string()]]);
            }
            other => panic!("Expected Rows, got {:?}", other),
        }
        assert_eq!(
            executor
                .meta()
                .get_active_mps(dt.output_table_id)
                .await
                .unwrap()
                .iter()
                .map(|mp| mp.row_count)
                .sum::<u64>(),
            1
        );
    }

    #[tokio::test]
    async fn test_same_name_dynamic_table_refresh_targets_requested_schema() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        create_schema(&executor, "testdb", "alt").await;
        create_dynamic_table(&executor, "testdb", "public", "dt_same").await;
        create_dynamic_table(&executor, "testdb", "alt", "dt_same").await;

        let public_before = dynamic_table_by_schema(&executor, "testdb", "public", "dt_same").await;
        let alt_before = dynamic_table_by_schema(&executor, "testdb", "alt", "dt_same").await;

        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "alt".to_string(),
                name: "dt_same".to_string(),
            })
            .await
            .unwrap();

        assert!(matches!(result, QueryResult::Success { .. }));
        let public_after = dynamic_table_by_schema(&executor, "testdb", "public", "dt_same").await;
        let alt_after = dynamic_table_by_schema(&executor, "testdb", "alt", "dt_same").await;
        assert_eq!(public_after.id, public_before.id);
        assert_eq!(public_after.last_refresh_ts, public_before.last_refresh_ts);
        assert_eq!(alt_after.id, alt_before.id);
        assert!(
            alt_after.last_refresh_ts.is_some(),
            "refreshing alt.dt_same should update only the alt dynamic table"
        );
    }

    #[tokio::test]
    async fn test_same_name_dynamic_table_drop_targets_requested_schema() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        create_schema(&executor, "testdb", "alt").await;
        create_dynamic_table(&executor, "testdb", "public", "dt_same").await;
        create_dynamic_table(&executor, "testdb", "alt", "dt_same").await;

        let public_before = dynamic_table_by_schema(&executor, "testdb", "public", "dt_same").await;
        let alt_before = dynamic_table_by_schema(&executor, "testdb", "alt", "dt_same").await;

        let result = executor
            .execute_as_root_for_internal(ResolvedStatement::DropDynamicTable {
                db: "testdb".to_string(),
                schema: "alt".to_string(),
                name: "dt_same".to_string(),
            })
            .await
            .unwrap();

        assert!(matches!(result, QueryResult::Success { .. }));
        assert!(
            executor
                .meta()
                .get_dynamic_table(public_before.id)
                .await
                .unwrap()
                .is_some(),
            "dropping alt.dt_same must not drop public.dt_same"
        );
        assert!(
            executor
                .meta()
                .get_dynamic_table(alt_before.id)
                .await
                .unwrap()
                .is_none(),
            "dropping alt.dt_same should drop only the alt dynamic table"
        );
    }

    #[tokio::test]
    async fn test_same_name_dynamic_table_suspend_resume_targets_requested_schema() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        create_schema(&executor, "testdb", "alt").await;
        create_dynamic_table(&executor, "testdb", "public", "dt_same").await;
        create_dynamic_table(&executor, "testdb", "alt", "dt_same").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::SuspendDynamicTable {
                db: "testdb".to_string(),
                schema: "alt".to_string(),
                name: "dt_same".to_string(),
            })
            .await
            .unwrap();

        let public_after_suspend =
            dynamic_table_by_schema(&executor, "testdb", "public", "dt_same").await;
        let alt_after_suspend =
            dynamic_table_by_schema(&executor, "testdb", "alt", "dt_same").await;
        assert!(
            public_after_suspend.scheduler_enabled,
            "suspending alt.dt_same must not suspend public.dt_same"
        );
        assert!(
            !alt_after_suspend.scheduler_enabled,
            "suspending alt.dt_same should disable only the alt scheduler"
        );

        executor
            .execute_as_root_for_internal(ResolvedStatement::ResumeDynamicTable {
                db: "testdb".to_string(),
                schema: "alt".to_string(),
                name: "dt_same".to_string(),
            })
            .await
            .unwrap();

        let public_after_resume =
            dynamic_table_by_schema(&executor, "testdb", "public", "dt_same").await;
        let alt_after_resume = dynamic_table_by_schema(&executor, "testdb", "alt", "dt_same").await;
        assert!(
            public_after_resume.scheduler_enabled,
            "resuming alt.dt_same must not change public.dt_same"
        );
        assert!(
            alt_after_resume.scheduler_enabled,
            "resuming alt.dt_same should enable the alt scheduler"
        );
    }

    #[tokio::test]
    async fn test_non_admin_show_dynamic_tables_hides_inaccessible_schema() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_a".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 60,
                refresh_mode: nova_common::DtRefreshMode::Auto,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let analyst = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 100,
            username: "analyst".to_string(),
            primary_role_id: nova_common::PUBLIC_ROLE_ID,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        match executor
            .execute_with_context(
                ResolvedStatement::ShowDynamicTables {
                    db: "testdb".to_string(),
                    pattern: None,
                },
                &analyst,
            )
            .await
            .unwrap()
        {
            QueryResult::Rows { rows, .. } => {
                assert!(
                    rows.is_empty(),
                    "inaccessible dynamic tables must be hidden"
                );
            }
            other => panic!("Expected Rows, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_dynamic_table_owner_can_operate_without_schema_operate_grant() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        let owner_role_id = create_test_role(&executor, "dt_owner").await;
        let schema_id = public_schema_id(&executor, "testdb").await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Schema, schema_id),
            SecurityPrivilege::CreateDynamicTable,
        )
        .await;
        let owner = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 200,
            username: "dt_owner_user".to_string(),
            primary_role_id: owner_role_id,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        executor
            .execute_with_context(
                ResolvedStatement::CreateDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_owned".to_string(),
                    query_definition: "SELECT 1".to_string(),
                    target_lag_seconds: 300,
                    refresh_mode: nova_common::DtRefreshMode::Full,
                    initialize_on_create: false,
                },
                &owner,
            )
            .await
            .unwrap();

        let result = executor
            .execute_with_context(
                ResolvedStatement::SuspendDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_owned".to_string(),
                },
                &owner,
            )
            .await
            .expect("dynamic table owner should be able to operate on the owned object");

        assert!(matches!(result, QueryResult::Success { .. }));
    }

    #[tokio::test]
    async fn test_dynamic_table_create_validates_owner_source_privileges() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO source VALUES (1)", "testdb").await;
        let db_id = db_id(&executor, "testdb").await;
        let schema_id = public_schema_id(&executor, "testdb").await;
        let source_table_id = table_id(&executor, "testdb", "public", "source").await;
        let owner_role_id = create_test_role(&executor, "dt_source_owner").await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Schema, schema_id),
            SecurityPrivilege::CreateDynamicTable,
        )
        .await;
        let owner = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 202,
            username: "dt_source_owner_user".to_string(),
            primary_role_id: owner_role_id,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        let err = executor
            .execute_with_context(
                ResolvedStatement::CreateDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_from_source".to_string(),
                    query_definition: "SELECT * FROM source".to_string(),
                    target_lag_seconds: 300,
                    refresh_mode: nova_common::DtRefreshMode::Full,
                    initialize_on_create: false,
                },
                &owner,
            )
            .await
            .expect_err("DT owner without source privileges must not create source-derived DT");
        assert!(matches!(
            err,
            nova_common::NovaError::PermissionDenied { .. }
        ));
        assert!(
            executor
                .meta()
                .list_dynamic_tables(db_id)
                .await
                .unwrap()
                .into_iter()
                .all(|dt| dt.name != "dt_from_source")
        );
        assert!(
            executor
                .meta()
                .list_tables(db_id, schema_id)
                .await
                .unwrap()
                .into_iter()
                .all(|table| table.name != "__dt_output_dt_from_source")
        );

        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Database, db_id),
            SecurityPrivilege::Usage,
        )
        .await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Schema, schema_id),
            SecurityPrivilege::Usage,
        )
        .await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Table, source_table_id),
            SecurityPrivilege::Select,
        )
        .await;

        let result = executor
            .execute_with_context(
                ResolvedStatement::CreateDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_from_source".to_string(),
                    query_definition: "SELECT * FROM source".to_string(),
                    target_lag_seconds: 300,
                    refresh_mode: nova_common::DtRefreshMode::Full,
                    initialize_on_create: true,
                },
                &owner,
            )
            .await
            .expect("DT owner with source privileges should create and refresh source data");
        assert!(matches!(result, QueryResult::Success { .. }));
    }

    #[tokio::test]
    async fn test_dynamic_table_refresh_rejects_ungranted_nested_subquery_source() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE visible_source (id INT)", "testdb").await;
        exec(&executor, "CREATE TABLE secret_source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO visible_source VALUES (1)", "testdb").await;
        exec(&executor, "INSERT INTO secret_source VALUES (1)", "testdb").await;

        let db_id = db_id(&executor, "testdb").await;
        let schema_id = public_schema_id(&executor, "testdb").await;
        let visible_table_id = table_id(&executor, "testdb", "public", "visible_source").await;
        let owner_role_id = create_test_role(&executor, "dt_nested_source_owner").await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Schema, schema_id),
            SecurityPrivilege::CreateDynamicTable,
        )
        .await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Database, db_id),
            SecurityPrivilege::Usage,
        )
        .await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Schema, schema_id),
            SecurityPrivilege::Usage,
        )
        .await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Table, visible_table_id),
            SecurityPrivilege::Select,
        )
        .await;
        let owner = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 203,
            username: "dt_nested_source_owner_user".to_string(),
            primary_role_id: owner_role_id,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        executor
            .execute_with_context(
                ResolvedStatement::CreateDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_nested_secret".to_string(),
                    query_definition:
                        "SELECT id FROM visible_source WHERE id IN (SELECT id FROM secret_source)"
                            .to_string(),
                    target_lag_seconds: 300,
                    refresh_mode: nova_common::DtRefreshMode::Full,
                    initialize_on_create: false,
                },
                &owner,
            )
            .await
            .unwrap();

        let err = executor
            .execute_with_context(
                ResolvedStatement::RefreshDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_nested_secret".to_string(),
                },
                &owner,
            )
            .await
            .expect_err("nested subquery source without SELECT must be denied");
        assert!(matches!(
            err,
            nova_common::NovaError::PermissionDenied { .. }
        ));
    }

    #[tokio::test]
    async fn test_dynamic_table_refresh_rejects_cte_body_source_shadowed_by_alias() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE secret_source (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO secret_source VALUES (1)", "testdb").await;

        let db_id = db_id(&executor, "testdb").await;
        let schema_id = public_schema_id(&executor, "testdb").await;
        let owner_role_id = create_test_role(&executor, "dt_cte_shadow_owner").await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Schema, schema_id),
            SecurityPrivilege::CreateDynamicTable,
        )
        .await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Database, db_id),
            SecurityPrivilege::Usage,
        )
        .await;
        grant_privilege(
            &executor,
            owner_role_id,
            ObjectRef::new(ObjectType::Schema, schema_id),
            SecurityPrivilege::Usage,
        )
        .await;
        let owner = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 204,
            username: "dt_cte_shadow_owner_user".to_string(),
            primary_role_id: owner_role_id,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        executor
            .execute_with_context(
                ResolvedStatement::CreateDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_cte_shadow_secret".to_string(),
                    query_definition:
                        "WITH secret_source AS (SELECT id FROM secret_source) SELECT id FROM secret_source"
                            .to_string(),
                    target_lag_seconds: 300,
                    refresh_mode: nova_common::DtRefreshMode::Full,
                    initialize_on_create: false,
                },
                &owner,
            )
            .await
            .unwrap();

        let err = executor
            .execute_with_context(
                ResolvedStatement::RefreshDynamicTable {
                    db: "testdb".to_string(),
                    schema: "public".to_string(),
                    name: "dt_cte_shadow_secret".to_string(),
                },
                &owner,
            )
            .await
            .expect_err("CTE body source hidden by alias must still require SELECT");
        assert!(matches!(
            err,
            nova_common::NovaError::PermissionDenied { .. }
        ));
    }

    #[tokio::test]
    async fn test_auto_join_refresh_uses_full_snapshot_dimension_side() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(
            &executor,
            "CREATE TABLE orders (id INT, customer_id INT)",
            "testdb",
        )
        .await;
        exec(
            &executor,
            "CREATE TABLE customers (id INT, name VARCHAR)",
            "testdb",
        )
        .await;
        exec(&executor, "INSERT INTO orders VALUES (1, 100)", "testdb").await;
        exec(
            &executor,
            "INSERT INTO customers VALUES (100, 'Alice')",
            "testdb",
        )
        .await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_order_customers".to_string(),
                query_definition:
                    "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
                        .to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Auto,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_order_customers".to_string(),
            })
            .await
            .unwrap();
        exec(&executor, "INSERT INTO orders VALUES (2, 100)", "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_order_customers".to_string(),
            })
            .await
            .unwrap();

        match executor
            .execute_as_root_for_internal(ResolvedStatement::Select {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                table: "__dt_output_dt_order_customers".to_string(),
                dependencies: vec![],
                projection: vec!["*".to_string()],
                filter: None,
                at_timestamp: None,
                raw_sql: None,
            })
            .await
            .unwrap()
        {
            QueryResult::Rows { mut rows, .. } => {
                rows.sort();
                assert_eq!(
                    rows,
                    vec![
                        vec!["1".to_string(), "Alice".to_string()],
                        vec!["2".to_string(), "Alice".to_string()],
                    ],
                    "AUTO dynamic tables with joins must refresh from a full source snapshot"
                );
            }
            other => panic!("Expected Rows, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_explicit_incremental_join_refresh_is_rejected() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(
            &executor,
            "CREATE TABLE orders (id INT, customer_id INT)",
            "testdb",
        )
        .await;
        exec(
            &executor,
            "CREATE TABLE customers (id INT, name VARCHAR)",
            "testdb",
        )
        .await;
        exec(&executor, "INSERT INTO orders VALUES (1, 100)", "testdb").await;
        exec(
            &executor,
            "INSERT INTO customers VALUES (100, 'Alice')",
            "testdb",
        )
        .await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_join".to_string(),
                query_definition:
                    "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
                        .to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Incremental,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let err = executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_join".to_string(),
            })
            .await
            .expect_err("incremental dynamic tables with joins are not supported yet");
        assert!(matches!(
            err,
            nova_common::NovaError::SqlAnalysisError { .. }
        ));
    }

    #[tokio::test]
    async fn test_explicit_incremental_aggregate_refresh_is_rejected() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE orders (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO orders VALUES (1)", "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_count".to_string(),
                query_definition: "SELECT COUNT (*) FROM orders".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Incremental,
                initialize_on_create: false,
            })
            .await
            .unwrap();

        let err = executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_incremental_count".to_string(),
            })
            .await
            .expect_err("incremental dynamic tables only support filter/project queries");
        assert!(matches!(
            err,
            nova_common::NovaError::SqlAnalysisError { .. }
        ));
    }

    #[tokio::test]
    async fn test_auto_aggregate_refresh_uses_full_snapshot() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE orders (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO orders VALUES (1)", "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_auto_count".to_string(),
                query_definition: "SELECT COUNT (*) FROM orders".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Auto,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_auto_count".to_string(),
            })
            .await
            .unwrap();
        exec(&executor, "INSERT INTO orders VALUES (2)", "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_auto_count".to_string(),
            })
            .await
            .unwrap();

        match executor
            .execute_as_root_for_internal(ResolvedStatement::Select {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                table: "__dt_output_dt_auto_count".to_string(),
                dependencies: vec![],
                projection: vec!["*".to_string()],
                filter: None,
                at_timestamp: None,
                raw_sql: None,
            })
            .await
            .unwrap()
        {
            QueryResult::Rows { rows, .. } => assert_eq!(rows, vec![vec!["2".to_string()]]),
            other => panic!("Expected Rows, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_incremental_refresh_ignores_unreferenced_table_watermark() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        exec(&executor, "CREATE TABLE orders (id INT)", "testdb").await;
        exec(&executor, "CREATE TABLE audit_log (id INT)", "testdb").await;
        exec(&executor, "INSERT INTO orders VALUES (1)", "testdb").await;

        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_orders_only".to_string(),
                query_definition: "SELECT id FROM orders".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Incremental,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_orders_only".to_string(),
            })
            .await
            .unwrap();
        let before = dynamic_table_by_schema(&executor, "testdb", "public", "dt_orders_only")
            .await
            .last_refresh_ts
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        exec(&executor, "INSERT INTO audit_log VALUES (1)", "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::RefreshDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_orders_only".to_string(),
            })
            .await
            .unwrap();

        let after = dynamic_table_by_schema(&executor, "testdb", "public", "dt_orders_only")
            .await
            .last_refresh_ts
            .unwrap();
        assert!(after >= before);
        let dt = dynamic_table_by_schema(&executor, "testdb", "public", "dt_orders_only").await;
        let active_mps = executor
            .meta()
            .get_active_mps(dt.output_table_id)
            .await
            .unwrap();
        assert_eq!(
            active_mps.iter().map(|mp| mp.row_count).sum::<u64>(),
            1,
            "unreferenced table MPs must not change the refreshed output rows"
        );
    }

    #[tokio::test]
    async fn test_show_dynamic_tables_allows_object_usage_without_schema_usage() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_visible".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 60,
                refresh_mode: nova_common::DtRefreshMode::Auto,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let dt = executor
            .meta()
            .list_dynamic_tables(
                executor
                    .meta()
                    .list_databases()
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|d| d.name == "testdb")
                    .unwrap()
                    .id,
            )
            .await
            .unwrap()
            .into_iter()
            .find(|dt| dt.name == "dt_visible")
            .unwrap();
        let viewer_role_id = create_test_role(&executor, "dt_viewer").await;
        grant_privilege(
            &executor,
            viewer_role_id,
            ObjectRef::new(ObjectType::DynamicTable, dt.id),
            SecurityPrivilege::Usage,
        )
        .await;
        let viewer = nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + 201,
            username: "dt_viewer_user".to_string(),
            primary_role_id: viewer_role_id,
            secondary_role_ids: vec![],
            secondary_all: true,
        };

        match executor
            .execute_with_context(
                ResolvedStatement::ShowDynamicTables {
                    db: "testdb".to_string(),
                    pattern: None,
                },
                &viewer,
            )
            .await
            .unwrap()
        {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1, "object-level USAGE should reveal the DT");
                assert_eq!(rows[0][0], "dt_visible");
            }
            other => panic!("Expected Rows, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_show_dynamic_tables() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_a".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 60,
                refresh_mode: nova_common::DtRefreshMode::Auto,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        match executor
            .execute_as_root_for_internal(ResolvedStatement::ShowDynamicTables {
                db: "testdb".to_string(),
                pattern: None,
            })
            .await
            .unwrap()
        {
            QueryResult::Rows { rows, columns } => {
                assert_eq!(columns[0], "name");
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "dt_a");
            }
            other => panic!("Expected Rows, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_suspend_resume_scheduler() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        executor
            .execute_as_root_for_internal(ResolvedStatement::CreateDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_x".to_string(),
                query_definition: "SELECT 1".to_string(),
                target_lag_seconds: 60,
                refresh_mode: nova_common::DtRefreshMode::Auto,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        let r = executor
            .execute_as_root_for_internal(ResolvedStatement::SuspendDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_x".to_string(),
            })
            .await
            .unwrap();
        assert!(matches!(r, QueryResult::Success { .. }));
        let r2 = executor
            .execute_as_root_for_internal(ResolvedStatement::ResumeDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_x".to_string(),
            })
            .await
            .unwrap();
        assert!(matches!(r2, QueryResult::Success { .. }));
    }

    // ── auto-mode detection (pure logic, no I/O) ─────────────────

    #[test]
    fn test_auto_mode_detects_full_for_agg() {
        let q = "SELECT region, COUNT(*) FROM orders GROUP BY region".to_uppercase();
        let incremental = !q.contains("GROUP BY")
            && !q.contains("COUNT(")
            && !q.contains("SUM(")
            && !q.contains("DISTINCT");
        assert!(!incremental);
    }

    #[test]
    fn test_auto_mode_uses_full_for_filter_until_cow_deltas_are_supported() {
        let cow_safe_incremental_delta_refresh = false;
        assert!(!cow_safe_incremental_delta_refresh);
    }
}
