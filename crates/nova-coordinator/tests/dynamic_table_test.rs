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
                format!("test_{}", nova_common::now_micros()).into_bytes(),
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
        let parser = SqlParser::new();
        let analyzer = Analyzer::new(db.to_string(), "public".to_string());
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

    async fn public_schema_id(executor: &Executor, db: &str) -> u64 {
        let db_meta = executor
            .meta()
            .list_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|d| d.name == db)
            .unwrap();
        executor
            .meta()
            .list_schemas(db_meta.id)
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.name == "public")
            .unwrap()
            .id
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
        use nova_common::{DtRefreshMode, DtRefreshStatus, DynamicTableMeta, now_micros};
        let _dir = TempDir::new().unwrap();
        let meta = FdbMetadataStore::open_test(
            "docker:docker@127.0.0.1:4500",
            format!("test_{}", nova_common::now_micros()).into_bytes(),
        )
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
                    query_definition: "SELECT * FROM orders".to_string(),
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
                query_definition: "SELECT * FROM orders".to_string(),
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
                query_definition: "SELECT * FROM orders".to_string(),
                target_lag_seconds: 300,
                refresh_mode: nova_common::DtRefreshMode::Full,
                initialize_on_create: false,
            })
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Success { .. }));
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
    fn test_auto_mode_detects_incremental_for_filter() {
        let q = "SELECT id, amount FROM orders WHERE status = 'active'".to_uppercase();
        let incremental = !q.contains("GROUP BY")
            && !q.contains("COUNT(")
            && !q.contains("SUM(")
            && !q.contains("DISTINCT");
        assert!(incremental);
    }
}
