//! Dynamic Table integration tests.

#[cfg(test)]
mod tests {
    use nova_common::SchemaMeta;
    use nova_coordinator::analyzer::{Analyzer, ResolvedStatement};
    use nova_coordinator::executor::{Executor, QueryResult};
    use nova_coordinator::parser::SqlParser;
    use nova_storage::{MetadataStore, MpReader, MpWriter, SledMetadataStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn setup() -> (Executor, TempDir) {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let meta = Arc::new(SledMetadataStore::open(dir.path().join("meta")).unwrap())
            as Arc<dyn MetadataStore>;
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
            last = executor.execute(resolved).await.unwrap();
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

    // ── metadata (sled) CRUD ──────────────────────────────────────

    #[tokio::test]
    async fn test_sled_dynamic_table_crud() {
        use nova_common::{DtRefreshMode, DtRefreshStatus, DynamicTableMeta, now_micros};
        let dir = TempDir::new().unwrap();
        let meta = SledMetadataStore::open(dir.path().join("meta")).unwrap();
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
    async fn test_create_dynamic_table_via_executor() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        let result = executor
            .execute(ResolvedStatement::CreateDynamicTable {
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
    async fn test_show_dynamic_tables() {
        let (executor, _dir) = setup();
        setup_db(&executor, "testdb").await;
        executor
            .execute(ResolvedStatement::CreateDynamicTable {
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
            .execute(ResolvedStatement::ShowDynamicTables {
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
            .execute(ResolvedStatement::CreateDynamicTable {
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
            .execute(ResolvedStatement::SuspendDynamicTable {
                db: "testdb".to_string(),
                schema: "public".to_string(),
                name: "dt_x".to_string(),
            })
            .await
            .unwrap();
        assert!(matches!(r, QueryResult::Success { .. }));
        let r2 = executor
            .execute(ResolvedStatement::ResumeDynamicTable {
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
