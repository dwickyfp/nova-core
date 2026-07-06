# RBAC Foundation Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish Option A RBAC hardening by making FDB object IDs/name uniqueness deterministic and adding Phase 2/3 acceptance coverage.

**Architecture:** Keep FoundationDB as the source of truth for metadata IDs, name indexes, security metadata, grants, and ownership. Add deterministic name-index writes to FDB metadata create/drop paths, then validate the session and executor authorization paths through existing `SecurityContext` plumbing.

**Tech Stack:** Rust, Tokio, FoundationDB, DataFusion, Apache Arrow/Parquet, `nova-common`, `nova-storage`, `nova-coordinator`.

## Global Constraints

- Current phase scope is Phase 1 Foundation plus Phase 16 RBAC hardening only.
- Do not implement Phase 4 SQL `CREATE ROLE/USER`, `GRANT/REVOKE`, or `SHOW GRANTS` in this plan.
- Do not add new third-party dependencies.
- Keep immutable micro-partition storage semantics unchanged.
- Do not commit secrets or local-only credentials.
- Existing ID `0` means “allocate a stable unique ID” for database, schema, table, dynamic table, user, and role create paths.
- Reserved explicit non-zero security IDs remain explicit: `ROOT_USER_ID`, `ACCOUNTADMIN_ROLE_ID`, and `PUBLIC_ROLE_ID`.
- Authorization failures must return `NovaError::PermissionDenied`.
- Login/session failures for disabled users or invalid default-role state must return an auth/security error on the current path.
- Tests should match error variants or `is_err()`, not brittle exact error strings.

---

## File Structure

- Modify `crates/nova-storage/src/metadata/fdb_store.rs`
  - Add metadata name-index helpers.
  - Add atomic ID allocation for schema and dynamic table create paths.
  - Add duplicate-name precondition writes for database, schema, table, dynamic table, and stream create paths.
  - Clear name indexes during drop paths where the object metadata is removed.
  - Add FDB-backed Phase 0 metadata reopen tests in this file’s test module.

- Modify `crates/nova-storage/src/metadata/security_impl.rs`
  - Add explicit reopen/duplicate coverage for FDB-backed users and roles if existing tests do not prove the Phase 0 checklist clearly enough.
  - Keep current `SecurityStore` interface unchanged.

- Modify `crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs`
  - Add async FDB-backed session security-context tests for disabled users and default roles that are missing/not granted.

- Modify `crates/nova-coordinator/tests/e2e_tests.rs`
  - Add Phase 3 executor authorization acceptance tests using `execute_with_context` through existing `exec_sql_as`.
  - Add small test helpers only inside this test module.

- Modify `docs/design/enterprise-rbac-roadmap.md`
  - Mark only verified Phase 0, Phase 2 acceptance, and Phase 3 acceptance checkboxes complete after tests pass.

---

### Task 1: Add FDB metadata name-index helpers and database/schema/table tests

**Files:**
- Modify: `crates/nova-storage/src/metadata/fdb_store.rs:1-822`
- Test: `crates/nova-storage/src/metadata/fdb_store.rs` test module appended after the `impl MetadataStore for FdbMetadataStore` block

**Interfaces:**
- Consumes:
  - `FdbMetadataStore::open_test(cluster_file: &str, subspace: Vec<u8>) -> Result<Self>`
  - `MetadataStore::create_database(&self, db: DatabaseMeta) -> Result<()>`
  - `MetadataStore::create_schema(&self, schema: SchemaMeta) -> Result<()>`
  - `MetadataStore::create_table(&self, table: TableMeta) -> Result<()>`
  - `MetadataStore::list_databases(&self) -> Result<Vec<DatabaseMeta>>`
  - `MetadataStore::list_schemas(&self, db_id: DatabaseId) -> Result<Vec<SchemaMeta>>`
  - `MetadataStore::list_tables(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<Vec<TableMeta>>`
- Produces:
  - Database name index key: `("db_by_name", normalize_ident(name)) -> id`
  - Schema name index key: `("schema_by_name", db_id, normalize_ident(name)) -> id`
  - Table name index key: `("table_by_name", db_id, schema_id, normalize_ident(name)) -> id`
  - Database/schema/table create paths that allocate ID when `id == 0` and fail duplicate names before overwrite.

- [ ] **Step 1: Write the failing database/schema/table metadata test**

Add this test module to the end of `crates/nova-storage/src/metadata/fdb_store.rs`, after line 822. If a `#[cfg(test)] mod tests` already exists by execution time, add the imports/helpers/tests inside the existing module instead of creating a second one.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn fdb_cluster_file() -> Option<String> {
        std::env::var("NOVA_FDB_CLUSTER_FILE").ok()
    }

    fn test_subspace(name: &str) -> Vec<u8> {
        format!("nova_test_{}_{}", name, now_micros()).into_bytes()
    }

    fn database(name: &str) -> DatabaseMeta {
        DatabaseMeta {
            id: 0,
            name: name.to_string(),
            created_at: now_micros(),
            owner: ROOT_USER_ID,
        }
    }

    fn schema(db_id: DatabaseId, name: &str) -> SchemaMeta {
        SchemaMeta {
            id: 0,
            db_id,
            name: name.to_string(),
            created_at: now_micros(),
        }
    }

    fn table(db_id: DatabaseId, schema_id: SchemaId, name: &str) -> TableMeta {
        TableMeta {
            id: 0,
            db_id,
            schema_id,
            name: name.to_string(),
            columns: vec![ColumnDef {
                id: 0,
                name: "id".to_string(),
                data_type: NovaType::Int64,
                nullable: false,
                default_value: None,
                comment: None,
            }],
            created_at: now_micros(),
            owner: ROOT_USER_ID,
            comment: None,
            version: 0,
            properties: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn phase0_database_schema_table_ids_and_name_indexes_survive_reopen() -> Result<()> {
        let Some(cluster_file) = fdb_cluster_file() else {
            return Ok(());
        };
        let subspace = test_subspace("metadata_ids");
        let store = FdbMetadataStore::open_test(&cluster_file, subspace.clone())?;

        store.create_database(database("sales")).await?;
        store.create_database(database("marketing")).await?;
        let mut dbs = store.list_databases().await?;
        dbs.sort_by(|left, right| left.name.cmp(&right.name));
        let marketing_id = dbs.iter().find(|db| db.name == "marketing").unwrap().id;
        let sales_id = dbs.iter().find(|db| db.name == "sales").unwrap().id;
        assert_ne!(sales_id, 0);
        assert_ne!(marketing_id, 0);
        assert_ne!(sales_id, marketing_id);

        store.create_schema(schema(sales_id, "public")).await?;
        store.create_schema(schema(sales_id, "analytics")).await?;
        let schemas = store.list_schemas(sales_id).await?;
        let public_id = schemas.iter().find(|schema| schema.name == "public").unwrap().id;
        let analytics_id = schemas
            .iter()
            .find(|schema| schema.name == "analytics")
            .unwrap()
            .id;
        assert_ne!(public_id, 0);
        assert_ne!(analytics_id, 0);
        assert_ne!(public_id, analytics_id);

        store.create_table(table(sales_id, public_id, "orders")).await?;
        store.create_table(table(sales_id, public_id, "customers")).await?;
        let tables = store.list_tables(sales_id, public_id).await?;
        let orders_id = tables.iter().find(|table| table.name == "orders").unwrap().id;
        let customers_id = tables.iter().find(|table| table.name == "customers").unwrap().id;
        assert_ne!(orders_id, 0);
        assert_ne!(customers_id, 0);
        assert_ne!(orders_id, customers_id);

        assert!(store.create_database(database("SALES")).await.is_err());
        assert!(store.create_schema(schema(sales_id, "PUBLIC")).await.is_err());
        assert!(store.create_table(table(sales_id, public_id, "ORDERS")).await.is_err());

        let reopened = FdbMetadataStore::open_test(&cluster_file, subspace)?;
        let reopened_sales = reopened
            .list_databases()
            .await?
            .into_iter()
            .find(|db| db.name == "sales")
            .expect("sales database should survive reopen");
        assert_eq!(reopened_sales.id, sales_id);

        let reopened_public = reopened
            .list_schemas(sales_id)
            .await?
            .into_iter()
            .find(|schema| schema.name == "public")
            .expect("public schema should survive reopen");
        assert_eq!(reopened_public.id, public_id);

        let reopened_orders = reopened
            .list_tables(sales_id, public_id)
            .await?
            .into_iter()
            .find(|table| table.name == "orders")
            .expect("orders table should survive reopen");
        assert_eq!(reopened_orders.id, orders_id);
        assert!(reopened.create_table(table(sales_id, public_id, "orders")).await.is_err());

        Ok(())
    }
}
```

- [ ] **Step 2: Run the new failing test**

Run:

```bash
cargo test -p nova-storage phase0_database_schema_table_ids_and_name_indexes_survive_reopen -- --nocapture
```

Expected before implementation:

```text
FAILED
```

The failure should show at least one of these behaviors: duplicate names are accepted, schema IDs stay `0`, or table/database duplicates overwrite metadata.

- [ ] **Step 3: Add metadata name-index helper functions**

In `crates/nova-storage/src/metadata/fdb_store.rs`, update the imports at line 24 to include `normalize_ident` through the existing glob import already present. No import change is required because line 24 is currently:

```rust
use nova_common::{NovaError, Result, *};
```

Inside `impl FdbMetadataStore`, immediately after `pub(crate) fn pack(&self, tuple: &impl TuplePack) -> Vec<u8> { ... }`, add these helpers:

```rust
    fn u64_bytes(value: u64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    fn duplicate_name_error(kind: &str, name: &str) -> NovaError {
        NovaError::Internal {
            message: format!("{} '{}' already exists", kind, name),
        }
    }
```

- [ ] **Step 4: Implement checked database/schema/table create writes**

Replace the existing `create_database`, `create_schema`, and `create_table` implementations in `crates/nova-storage/src/metadata/fdb_store.rs` with this code:

```rust
    async fn create_database(&self, mut db: DatabaseMeta) -> Result<()> {
        let name = normalize_ident(&db.name);
        let name_key = self.pack(&("db_by_name", name.clone()));
        if self.fdb_get(name_key.clone()).await?.is_some() {
            return Err(Self::duplicate_name_error("database", &db.name));
        }
        if db.id == 0 {
            db.id = self.fdb_atomic_inc(self.pack(&("next_id", "database"))).await?;
        } else if self.get_database(db.id).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("database id '{}' already exists", db.id),
            });
        }
        let db_key = self.pack(&("db", db.id));
        self.fdb_checked_write_batch(
            vec![db_key.clone(), name_key.clone()],
            vec![
                (db_key, Self::serialize(&db)?),
                (name_key, Self::u64_bytes(db.id)),
            ],
            vec![],
            false,
        )
        .await
        .map(|_| ())
    }

    async fn create_schema(&self, mut schema: SchemaMeta) -> Result<()> {
        let name = normalize_ident(&schema.name);
        let name_key = self.pack(&("schema_by_name", schema.db_id, name.clone()));
        if self.fdb_get(name_key.clone()).await?.is_some() {
            return Err(Self::duplicate_name_error("schema", &schema.name));
        }
        if schema.id == 0 {
            schema.id = self.fdb_atomic_inc(self.pack(&("next_id", "schema"))).await?;
        } else if self.get_schema(schema.db_id, schema.id).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("schema id '{}' already exists", schema.id),
            });
        }
        let schema_key = self.pack(&("schema", schema.db_id, schema.id));
        self.fdb_checked_write_batch(
            vec![schema_key.clone(), name_key.clone()],
            vec![
                (schema_key, Self::serialize(&schema)?),
                (name_key, Self::u64_bytes(schema.id)),
            ],
            vec![],
            false,
        )
        .await
        .map(|_| ())
    }

    async fn create_table(&self, mut table: TableMeta) -> Result<()> {
        let name = normalize_ident(&table.name);
        let name_key = self.pack(&("table_by_name", table.db_id, table.schema_id, name.clone()));
        if self.fdb_get(name_key.clone()).await?.is_some() {
            return Err(Self::duplicate_name_error("table", &table.name));
        }
        if table.id == 0 {
            table.id = self.fdb_atomic_inc(self.pack(&("next_id", "table"))).await?;
        } else if self.get_table(table.db_id, table.schema_id, table.id).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("table id '{}' already exists", table.id),
            });
        }
        let table_key = self.pack(&("table", table.db_id, table.schema_id, table.id));
        self.fdb_checked_write_batch(
            vec![table_key.clone(), name_key.clone()],
            vec![
                (table_key, Self::serialize(&table)?),
                (name_key, Self::u64_bytes(table.id)),
            ],
            vec![],
            false,
        )
        .await
        .map(|_| ())
    }
```

- [ ] **Step 5: Implement name-index cleanup for database/schema/table drops**

Replace `drop_database`, `drop_schema`, and `drop_table` in `crates/nova-storage/src/metadata/fdb_store.rs` with this code:

```rust
    async fn drop_database(&self, id: DatabaseId) -> Result<()> {
        let schemas = self.list_schemas(id).await?;
        if !schemas.is_empty() {
            return Err(NovaError::Internal {
                message: format!("Cannot drop database {}: has {} schemas", id, schemas.len()),
            });
        }
        let Some(db) = self.get_database(id).await? else {
            return Ok(());
        };
        self.fdb_write_batch(
            vec![],
            vec![
                self.pack(&("db", id)),
                self.pack(&("db_by_name", normalize_ident(&db.name))),
            ],
            false,
        )
        .await
        .map(|_| ())
    }

    async fn drop_schema(&self, db_id: DatabaseId, schema_id: SchemaId) -> Result<()> {
        let tables = self.list_tables(db_id, schema_id).await?;
        if !tables.is_empty() {
            return Err(NovaError::Internal {
                message: format!("Cannot drop schema: has {} tables", tables.len()),
            });
        }
        let Some(schema) = self.get_schema(db_id, schema_id).await? else {
            return Ok(());
        };
        self.fdb_write_batch(
            vec![],
            vec![
                self.pack(&("schema", db_id, schema_id)),
                self.pack(&("schema_by_name", db_id, normalize_ident(&schema.name))),
            ],
            false,
        )
        .await
        .map(|_| ())
    }

    async fn drop_table(&self, table_id: TableId) -> Result<()> {
        let mut table_meta = None;
        let (table_start, table_end) = self.category_range(&"table");
        for (_, value) in self.fdb_get_range(table_start, table_end).await? {
            let table: TableMeta = Self::deserialize(&value)?;
            if table.id == table_id {
                table_meta = Some(table);
                break;
            }
        }
        let Some(table) = table_meta else {
            return Ok(());
        };

        let active_mps = self.get_active_mps(table_id).await?;
        let table_key = self.pack(&("table", table.db_id, table.schema_id, table.id));
        let table_name_key = self.pack(&(
            "table_by_name",
            table.db_id,
            table.schema_id,
            normalize_ident(&table.name),
        ));
        let ver_key = self.pack(&("table_version", table_id));
        let (mp_start, mp_end) = self.category_range(&("table_mps", table_id));
        let mp_keys: Vec<Vec<u8>> = active_mps
            .iter()
            .map(|mp| self.pack(&("mp", mp.mp_id)))
            .collect();

        self.db
            .run(|trx, _| {
                let table_key = table_key.clone();
                let table_name_key = table_name_key.clone();
                let ver_key = ver_key.clone();
                let mp_start = mp_start.clone();
                let mp_end = mp_end.clone();
                let mp_keys = mp_keys.clone();
                async move {
                    trx.clear(&table_key);
                    trx.clear(&table_name_key);
                    trx.clear(&ver_key);
                    trx.clear_range(&mp_start, &mp_end);
                    for key in &mp_keys {
                        trx.clear(key);
                    }
                    Ok::<_, fdb::FdbBindingError>(())
                }
            })
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("FDB drop_table failed: {}", e),
            })?;

        Ok(())
    }
```

- [ ] **Step 6: Run the database/schema/table test again**

Run:

```bash
cargo test -p nova-storage phase0_database_schema_table_ids_and_name_indexes_survive_reopen -- --nocapture
```

Expected:

```text
test metadata::fdb_store::tests::phase0_database_schema_table_ids_and_name_indexes_survive_reopen ... ok
```

- [ ] **Step 7: Run focused storage regression tests**

Run:

```bash
cargo test -p nova-storage metadata -- --nocapture
```

Expected:

```text
test result: ok
```

---

### Task 2: Harden dynamic table and stream metadata IDs/name indexes

**Files:**
- Modify: `crates/nova-storage/src/metadata/fdb_store.rs:741-821`
- Test: `crates/nova-storage/src/metadata/fdb_store.rs` test module from Task 1

**Interfaces:**
- Consumes:
  - `MetadataStore::create_dynamic_table(&self, dt: DynamicTableMeta) -> Result<()>`
  - `MetadataStore::update_dynamic_table(&self, dt: DynamicTableMeta) -> Result<()>`
  - `MetadataStore::drop_dynamic_table(&self, dt_id: TableId) -> Result<()>`
  - `MetadataStore::list_dynamic_tables(&self, db_id: DatabaseId) -> Result<Vec<DynamicTableMeta>>`
  - `MetadataStore::create_stream(&self, stream: StreamMeta) -> Result<()>`
  - `MetadataStore::get_stream(&self, stream_id: StreamId) -> Result<Option<StreamMeta>>`
- Produces:
  - Dynamic table name index key: `("dynamic_table_by_name", db_id, schema_id, normalize_ident(name)) -> id`
  - Dynamic table ID allocation when `dt.id == 0`.
  - Stream name index key scoped to current metadata shape: `("stream_by_name", table_id, normalize_ident(name)) -> stream_id`.
  - Stream duplicate-name checks for the same source table.

- [ ] **Step 1: Write the failing dynamic table and stream metadata test**

Append this test inside the `#[cfg(test)] mod tests` in `crates/nova-storage/src/metadata/fdb_store.rs`:

```rust
    fn dynamic_table(db_id: DatabaseId, schema_id: SchemaId, name: &str) -> DynamicTableMeta {
        DynamicTableMeta {
            id: 0,
            db_id,
            schema_id,
            name: name.to_string(),
            query_definition: "SELECT id FROM source".to_string(),
            target_lag_seconds: 60,
            refresh_mode: DtRefreshMode::Full,
            initialize_on_create: false,
            output_table_id: 99,
            last_refresh_ts: None,
            refresh_status: DtRefreshStatus::Pending,
            comment: None,
            created_at: now_micros(),
            scheduler_enabled: true,
        }
    }

    fn stream(stream_id: StreamId, table_id: TableId, name: &str) -> StreamMeta {
        StreamMeta {
            stream_id,
            table_id,
            name: name.to_string(),
            append_only: false,
            created_at: now_micros(),
        }
    }

    #[tokio::test]
    async fn phase0_dynamic_table_and_stream_name_indexes_survive_reopen() -> Result<()> {
        let Some(cluster_file) = fdb_cluster_file() else {
            return Ok(());
        };
        let subspace = test_subspace("dynamic_stream_ids");
        let store = FdbMetadataStore::open_test(&cluster_file, subspace.clone())?;

        store.create_database(database("dt_db")).await?;
        let db_id = store.list_databases().await?.into_iter().next().unwrap().id;
        store.create_schema(schema(db_id, "public")).await?;
        let schema_id = store.list_schemas(db_id).await?.into_iter().next().unwrap().id;
        store.create_table(table(db_id, schema_id, "source")).await?;
        let table_id = store.list_tables(db_id, schema_id).await?.into_iter().next().unwrap().id;

        store
            .create_dynamic_table(dynamic_table(db_id, schema_id, "dt_orders"))
            .await?;
        store
            .create_dynamic_table(dynamic_table(db_id, schema_id, "dt_customers"))
            .await?;
        let dynamic_tables = store.list_dynamic_tables(db_id).await?;
        let dt_orders_id = dynamic_tables
            .iter()
            .find(|dt| dt.name == "dt_orders")
            .unwrap()
            .id;
        let dt_customers_id = dynamic_tables
            .iter()
            .find(|dt| dt.name == "dt_customers")
            .unwrap()
            .id;
        assert_ne!(dt_orders_id, 0);
        assert_ne!(dt_customers_id, 0);
        assert_ne!(dt_orders_id, dt_customers_id);
        assert!(
            store
                .create_dynamic_table(dynamic_table(db_id, schema_id, "DT_ORDERS"))
                .await
                .is_err()
        );

        store.create_stream(stream(101, table_id, "orders_stream")).await?;
        assert!(store.create_stream(stream(102, table_id, "ORDERS_STREAM")).await.is_err());

        let reopened = FdbMetadataStore::open_test(&cluster_file, subspace)?;
        let reopened_dt = reopened
            .list_dynamic_tables(db_id)
            .await?
            .into_iter()
            .find(|dt| dt.name == "dt_orders")
            .expect("dynamic table should survive reopen");
        assert_eq!(reopened_dt.id, dt_orders_id);
        assert!(reopened.get_stream(101).await?.is_some());
        assert!(reopened.create_stream(stream(103, table_id, "orders_stream")).await.is_err());

        Ok(())
    }
```

- [ ] **Step 2: Run the new dynamic table/stream failing test**

Run:

```bash
cargo test -p nova-storage phase0_dynamic_table_and_stream_name_indexes_survive_reopen -- --nocapture
```

Expected before implementation:

```text
FAILED
```

The expected failure is duplicate dynamic table or stream names being accepted, or dynamic table `id == 0` being persisted as `0`.

- [ ] **Step 3: Replace `create_stream` with duplicate-name protection**

Replace the current `create_stream` implementation in `crates/nova-storage/src/metadata/fdb_store.rs` with:

```rust
    async fn create_stream(&self, stream: StreamMeta) -> Result<()> {
        if stream.stream_id == 0 {
            return Err(NovaError::Internal {
                message: "stream id must be allocated by caller before create_stream".to_string(),
            });
        }
        let name_key = self.pack(&(
            "stream_by_name",
            stream.table_id,
            normalize_ident(&stream.name),
        ));
        if self.fdb_get(name_key.clone()).await?.is_some() {
            return Err(Self::duplicate_name_error("stream", &stream.name));
        }
        let stream_key = self.pack(&("stream", stream.stream_id));
        self.fdb_checked_write_batch(
            vec![stream_key.clone(), name_key.clone()],
            vec![
                (stream_key, Self::serialize(&stream)?),
                (name_key, Self::u64_bytes(stream.stream_id)),
            ],
            vec![],
            false,
        )
        .await
        .map(|_| ())
    }
```

This keeps the public trait unchanged. The executor already allocates stream IDs before calling `create_stream`, so this hardens current behavior without an interface migration.

- [ ] **Step 4: Replace dynamic table create/update/drop with checked create and unchecked update**

Replace `create_dynamic_table`, `update_dynamic_table`, and `drop_dynamic_table` in `crates/nova-storage/src/metadata/fdb_store.rs` with:

```rust
    async fn create_dynamic_table(&self, mut dt: DynamicTableMeta) -> Result<()> {
        let name_key = self.pack(&(
            "dynamic_table_by_name",
            dt.db_id,
            dt.schema_id,
            normalize_ident(&dt.name),
        ));
        if self.fdb_get(name_key.clone()).await?.is_some() {
            return Err(Self::duplicate_name_error("dynamic table", &dt.name));
        }
        if dt.id == 0 {
            dt.id = self
                .fdb_atomic_inc(self.pack(&("next_id", "dynamic_table")))
                .await?;
        } else if self.get_dynamic_table(dt.id).await?.is_some() {
            return Err(NovaError::Internal {
                message: format!("dynamic table id '{}' already exists", dt.id),
            });
        }
        let dt_key = self.pack(&("dynamic_table", dt.id));
        self.fdb_checked_write_batch(
            vec![dt_key.clone(), name_key.clone()],
            vec![
                (dt_key, Self::serialize(&dt)?),
                (name_key, Self::u64_bytes(dt.id)),
            ],
            vec![],
            false,
        )
        .await
        .map(|_| ())
    }

    async fn update_dynamic_table(&self, dt: DynamicTableMeta) -> Result<()> {
        let Some(existing) = self.get_dynamic_table(dt.id).await? else {
            return Err(NovaError::Internal {
                message: format!("dynamic table '{}' not found", dt.id),
            });
        };
        let old_name_key = self.pack(&(
            "dynamic_table_by_name",
            existing.db_id,
            existing.schema_id,
            normalize_ident(&existing.name),
        ));
        let new_name_key = self.pack(&(
            "dynamic_table_by_name",
            dt.db_id,
            dt.schema_id,
            normalize_ident(&dt.name),
        ));
        let dt_key = self.pack(&("dynamic_table", dt.id));
        let mut clears = Vec::new();
        let mut must_not_exist = Vec::new();
        if old_name_key != new_name_key {
            clears.push(old_name_key);
            must_not_exist.push(new_name_key.clone());
        }
        self.fdb_checked_write_batch(
            must_not_exist,
            vec![
                (dt_key, Self::serialize(&dt)?),
                (new_name_key, Self::u64_bytes(dt.id)),
            ],
            clears,
            false,
        )
        .await
        .map(|_| ())
    }

    async fn drop_dynamic_table(&self, dt_id: TableId) -> Result<()> {
        let Some(dt) = self.get_dynamic_table(dt_id).await? else {
            return Ok(());
        };
        self.fdb_write_batch(
            vec![],
            vec![
                self.pack(&("dynamic_table", dt_id)),
                self.pack(&(
                    "dynamic_table_by_name",
                    dt.db_id,
                    dt.schema_id,
                    normalize_ident(&dt.name),
                )),
            ],
            false,
        )
        .await
        .map(|_| ())
    }
```

- [ ] **Step 5: Run the dynamic table/stream test again**

Run:

```bash
cargo test -p nova-storage phase0_dynamic_table_and_stream_name_indexes_survive_reopen -- --nocapture
```

Expected:

```text
test metadata::fdb_store::tests::phase0_dynamic_table_and_stream_name_indexes_survive_reopen ... ok
```

- [ ] **Step 6: Run all storage tests**

Run:

```bash
cargo test -p nova-storage
```

Expected:

```text
test result: ok
```

---

### Task 3: Add explicit user/role Phase 0 ID and duplicate-name persistence coverage

**Files:**
- Modify: `crates/nova-storage/src/metadata/security_impl.rs:630-928`
- Test: `crates/nova-storage/src/metadata/security_impl.rs`

**Interfaces:**
- Consumes:
  - `SecurityStore::bootstrap_security(&self) -> Result<()>`
  - `SecurityStore::create_role(&self, role: RoleMeta) -> Result<RoleId>`
  - `SecurityStore::create_user(&self, user: UserMeta) -> Result<UserId>`
  - `SecurityStore::get_role_by_name(&self, name: &str) -> Result<Option<RoleMeta>>`
  - `SecurityStore::get_user_by_name(&self, name: &str) -> Result<Option<UserMeta>>`
- Produces:
  - Tests proving user and role `id == 0` allocation remains above reserved IDs, names survive reopen, and duplicate names fail after reopen.

- [ ] **Step 1: Add the user/role Phase 0 persistence test**

Append this test inside the existing `#[cfg(test)] mod tests` in `crates/nova-storage/src/metadata/security_impl.rs`, before the closing brace at line 928:

```rust
    #[tokio::test]
    async fn phase0_user_and_role_ids_and_name_indexes_survive_reopen() -> Result<()> {
        let Ok(cluster_file) = std::env::var("NOVA_FDB_CLUSTER_FILE") else {
            return Ok(());
        };
        let subspace = format!("nova_test_security_phase0_{}", now_micros()).into_bytes();
        let store = FdbMetadataStore::open_test(&cluster_file, subspace.clone())?;
        store.bootstrap_security().await?;

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
        let engineer = store
            .create_role(RoleMeta {
                id: 0,
                name: "engineer".to_string(),
                owner_role_id: ACCOUNTADMIN_ROLE_ID,
                system: false,
                created_at: now_micros(),
                created_by_user_id: ROOT_USER_ID,
                comment: None,
            })
            .await?;
        assert_ne!(analyst, 0);
        assert_ne!(engineer, 0);
        assert_ne!(analyst, engineer);
        assert!(analyst > PUBLIC_ROLE_ID);
        assert!(engineer > PUBLIC_ROLE_ID);

        let alice = store
            .create_user(UserMeta {
                id: 0,
                name: "alice".to_string(),
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
        let bob = store
            .create_user(UserMeta {
                id: 0,
                name: "bob".to_string(),
                password_hash: String::new(),
                mysql_native_hash: Vec::new(),
                default_role_id: engineer,
                disabled: false,
                created_at: now_micros(),
                created_by_user_id: ROOT_USER_ID,
                created_by_role_id: ACCOUNTADMIN_ROLE_ID,
                comment: None,
            })
            .await?;
        assert_ne!(alice, 0);
        assert_ne!(bob, 0);
        assert_ne!(alice, bob);
        assert!(alice > ROOT_USER_ID);
        assert!(bob > ROOT_USER_ID);

        let reopened = FdbMetadataStore::open_test(&cluster_file, subspace)?;
        assert_eq!(reopened.get_role_by_name("ANALYST").await?.unwrap().id, analyst);
        assert_eq!(reopened.get_role_by_name("engineer").await?.unwrap().id, engineer);
        assert_eq!(reopened.get_user_by_name("ALICE").await?.unwrap().id, alice);
        assert_eq!(reopened.get_user_by_name("bob").await?.unwrap().id, bob);

        assert!(
            reopened
                .create_role(RoleMeta {
                    id: 0,
                    name: "Analyst".to_string(),
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
            reopened
                .create_user(UserMeta {
                    id: 0,
                    name: "Alice".to_string(),
                    password_hash: String::new(),
                    mysql_native_hash: Vec::new(),
                    default_role_id: analyst,
                    disabled: false,
                    created_at: now_micros(),
                    created_by_user_id: ROOT_USER_ID,
                    created_by_role_id: ACCOUNTADMIN_ROLE_ID,
                    comment: None,
                })
                .await
                .is_err()
        );

        Ok(())
    }
```

- [ ] **Step 2: Run the user/role Phase 0 test**

Run:

```bash
cargo test -p nova-storage phase0_user_and_role_ids_and_name_indexes_survive_reopen -- --nocapture
```

Expected:

```text
test metadata::security_impl::tests::phase0_user_and_role_ids_and_name_indexes_survive_reopen ... ok
```

If this fails because implementation changed during Tasks 1-2, fix only `create_user`/`create_role` in `crates/nova-storage/src/metadata/security_impl.rs` while preserving these existing behaviors:

```rust
if user.id == 0 {
    user.id = self.security_next_id("user", ROOT_USER_ID + 1).await?;
}

if role.id == 0 {
    role.id = self.security_next_id("role", PUBLIC_ROLE_ID + 1).await?;
}
```

- [ ] **Step 3: Run storage security tests**

Run:

```bash
cargo test -p nova-storage security -- --nocapture
```

Expected:

```text
test result: ok
```

---

### Task 4: Add Phase 2 disabled-user and invalid-default-role acceptance tests

**Files:**
- Modify: `crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs:1-71`
- Uses existing implementation: `crates/nova-coordinator/src/executor.rs:43-83`
- Uses existing handshake call-site: `crates/nova-coordinator/src/mysql_protocol/server.rs:202-253`

**Interfaces:**
- Consumes:
  - `Executor::user_for_auth(&self, username: &str) -> Result<Option<UserMeta>>`
  - `Executor::security_context_for_user(&self, username: &str) -> Result<SecurityContext>`
  - `SecurityStore::create_role`, `create_user`, `revoke_role_from_user`
- Produces:
  - Tests proving disabled users are visible to auth lookup but rejected by `security_context_for_user`.
  - Tests proving a user whose default role is no longer granted cannot get a session `SecurityContext`.

- [ ] **Step 1: Add async FDB setup imports and helper**

At the top of `crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs`, replace the current imports with:

```rust
use nova_common::{
    ACCOUNTADMIN_ROLE_ID, NovaError, PUBLIC_ROLE_ID, ROOT_USER_ID, RoleMeta, SecurityContext,
    UserMeta, now_micros,
};
use nova_coordinator::executor::Executor;
use nova_coordinator::mysql_protocol::server::{RoleCommand, parse_role_command};
use nova_storage::{FdbMetadataStore, MetadataStore, MpReader, MpWriter};
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tempfile::TempDir;
```

Then add this helper after the imports:

```rust
fn setup_executor() -> Option<(Executor, TempDir)> {
    let cluster_file = std::env::var("NOVA_FDB_CLUSTER_FILE").ok()?;
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let meta = Arc::new(
        FdbMetadataStore::open_test(
            &cluster_file,
            format!("nova_test_phase2_{}", now_micros()).into_bytes(),
        )
        .unwrap(),
    ) as Arc<dyn MetadataStore>;
    let store = Arc::new(LocalFileSystem::new_with_prefix(&data_dir).unwrap())
        as Arc<dyn object_store::ObjectStore>;
    let writer = MpWriter::new(store.clone(), "nova".to_string());
    let reader = MpReader::new(store);
    Some((Executor::new(meta, writer, reader), dir))
}
```

- [ ] **Step 2: Add disabled-user acceptance test**

Append this test to `crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs`:

```rust
#[tokio::test]
async fn phase2_login_fails_for_disabled_user() {
    let Some((executor, _dir)) = setup_executor() else {
        return;
    };
    executor.meta().bootstrap_security().await.unwrap();
    let role_id = executor
        .meta()
        .create_role(RoleMeta {
            id: 0,
            name: "disabled_role".to_string(),
            owner_role_id: ACCOUNTADMIN_ROLE_ID,
            system: false,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            comment: None,
        })
        .await
        .unwrap();
    executor
        .meta()
        .create_user(UserMeta {
            id: 0,
            name: "disabled_user".to_string(),
            password_hash: String::new(),
            mysql_native_hash: Vec::new(),
            default_role_id: role_id,
            disabled: true,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            created_by_role_id: ACCOUNTADMIN_ROLE_ID,
            comment: None,
        })
        .await
        .unwrap();

    let user = executor
        .user_for_auth("disabled_user")
        .await
        .unwrap()
        .expect("auth lookup should find disabled user so handshake can reject it");
    assert!(user.disabled);

    let err = executor
        .security_context_for_user("disabled_user")
        .await
        .expect_err("disabled user must not receive a session security context");
    assert!(matches!(err, NovaError::AuthFailed { .. }));
}
```

- [ ] **Step 3: Add default-role-not-granted acceptance test**

Append this test to `crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs`:

```rust
#[tokio::test]
async fn phase2_login_fails_when_default_role_is_not_granted() {
    let Some((executor, _dir)) = setup_executor() else {
        return;
    };
    executor.meta().bootstrap_security().await.unwrap();
    let role_id = executor
        .meta()
        .create_role(RoleMeta {
            id: 0,
            name: "revoked_default".to_string(),
            owner_role_id: ACCOUNTADMIN_ROLE_ID,
            system: false,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            comment: None,
        })
        .await
        .unwrap();
    let user_id = executor
        .meta()
        .create_user(UserMeta {
            id: 0,
            name: "missing_default_user".to_string(),
            password_hash: String::new(),
            mysql_native_hash: Vec::new(),
            default_role_id: role_id,
            disabled: false,
            created_at: now_micros(),
            created_by_user_id: ROOT_USER_ID,
            created_by_role_id: ACCOUNTADMIN_ROLE_ID,
            comment: None,
        })
        .await
        .unwrap();
    executor
        .meta()
        .revoke_role_from_user(user_id, role_id)
        .await
        .unwrap();

    let err = executor
        .security_context_for_user("missing_default_user")
        .await
        .expect_err("user default role must be granted to create a session context");
    assert!(matches!(err, NovaError::AuthFailed { .. }));
}
```

- [ ] **Step 4: Run Phase 2 tests**

Run:

```bash
cargo test -p nova-coordinator --test rbac_phase2_security_context_test -- --nocapture
```

Expected:

```text
test result: ok
```

If the disabled-user test fails because `security_context_for_user` allows disabled users, ensure `crates/nova-coordinator/src/executor.rs:51-83` contains this guard before role listing:

```rust
        if user.disabled {
            return Err(NovaError::AuthFailed {
                reason: format!("user '{}' is disabled", username),
            });
        }
```

If the default-role test fails because a missing default role silently falls back to `PUBLIC`, keep the current roadmap behavior and ensure `security_context_for_user` contains this guard:

```rust
        let roles = self.meta.list_user_roles(user.id).await?;
        if !roles.contains(&user.default_role_id) {
            return Err(NovaError::AuthFailed {
                reason: format!(
                    "default role '{}' is not granted to user '{}'",
                    user.default_role_id, user.name
                ),
            });
        }
```

---

### Task 5: Add Phase 3 table authorization acceptance tests

**Files:**
- Modify: `crates/nova-coordinator/tests/e2e_tests.rs:1-917`
- Uses existing implementation: `crates/nova-coordinator/src/executor.rs:148-243`, `crates/nova-coordinator/src/executor.rs:254-520`, `crates/nova-coordinator/src/executor.rs:670-758`

**Interfaces:**
- Consumes:
  - Existing `setup() -> (Executor, TempDir)` in `e2e_tests.rs`
  - Existing `exec_sql(...)` root helper in `e2e_tests.rs`
  - Existing `exec_sql_as(...)` security-context helper in `e2e_tests.rs`
  - `SecurityStore::create_role`, `grant_privileges`, `set_object_owner`
- Produces:
  - Acceptance tests for:
    - user without table `SELECT` cannot read.
    - user with table `SELECT` but without parent DB/schema `USAGE` cannot read.
    - object owner can operate on owned table.
    - non-owner cannot drop table without `OWNERSHIP`.

- [ ] **Step 1: Add test helper functions inside `e2e_tests.rs` test module**

In `crates/nova-coordinator/tests/e2e_tests.rs`, add these helpers after `exec_sql_as` at line 79:

```rust
    async fn create_role(executor: &Executor, name: &str) -> nova_common::RoleId {
        executor.meta().bootstrap_security().await.unwrap();
        executor
            .meta()
            .create_role(nova_common::RoleMeta {
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

    fn context_for(role_id: nova_common::RoleId, username: &str) -> nova_common::SecurityContext {
        nova_common::SecurityContext {
            user_id: nova_common::ROOT_USER_ID + role_id + 1_000,
            username: username.to_string(),
            primary_role_id: role_id,
            secondary_role_ids: vec![],
            secondary_all: true,
        }
    }

    async fn grant(
        executor: &Executor,
        role_id: nova_common::RoleId,
        object: nova_common::ObjectRef,
        privilege: nova_common::SecurityPrivilege,
    ) {
        executor
            .meta()
            .grant_privileges(nova_common::GrantSetMeta {
                role_id,
                object,
                privileges: nova_common::PrivilegeSet::from_privileges(&[privilege]),
                grant_options: nova_common::PrivilegeSet::empty(),
                granted_by_role_id: nova_common::ACCOUNTADMIN_ROLE_ID,
                updated_at: nova_common::now_micros(),
            })
            .await
            .unwrap();
    }

    async fn securedb_objects(
        executor: &Executor,
        table_name: &str,
    ) -> (
        nova_common::DatabaseMeta,
        nova_common::SchemaMeta,
        nova_common::TableMeta,
    ) {
        let db_meta = executor
            .meta()
            .list_databases()
            .await
            .unwrap()
            .into_iter()
            .find(|db| db.name == "securedb")
            .unwrap();
        let schema_meta = executor
            .meta()
            .list_schemas(db_meta.id)
            .await
            .unwrap()
            .into_iter()
            .find(|schema| schema.name == "public")
            .unwrap();
        let table_meta = executor
            .meta()
            .list_tables(db_meta.id, schema_meta.id)
            .await
            .unwrap()
            .into_iter()
            .find(|table| table.name == table_name)
            .unwrap();
        (db_meta, schema_meta, table_meta)
    }

    async fn grant_parent_usage(
        executor: &Executor,
        role_id: nova_common::RoleId,
        db_meta: &nova_common::DatabaseMeta,
        schema_meta: &nova_common::SchemaMeta,
    ) {
        grant(
            executor,
            role_id,
            nova_common::ObjectRef::new(nova_common::ObjectType::Database, db_meta.id),
            nova_common::SecurityPrivilege::Usage,
        )
        .await;
        grant(
            executor,
            role_id,
            nova_common::ObjectRef::new(nova_common::ObjectType::Schema, schema_meta.id),
            nova_common::SecurityPrivilege::Usage,
        )
        .await;
    }
```

- [ ] **Step 2: Add missing-SELECT denial acceptance test**

Append this test near the existing RBAC tests, after `test_non_admin_cannot_create_stream_without_table_access`:

```rust
    #[tokio::test]
    async fn phase3_user_without_select_cannot_read_table() {
        let (executor, _dir) = setup();
        exec_sql(&executor, "CREATE DATABASE securedb", "securedb")
            .await
            .unwrap();
        exec_sql(&executor, "CREATE TABLE sensitive (id INT)", "securedb")
            .await
            .unwrap();
        exec_sql(&executor, "INSERT INTO sensitive VALUES (1)", "securedb")
            .await
            .unwrap();

        let role_id = create_role(&executor, "no_select_role").await;
        let (db_meta, schema_meta, _table_meta) = securedb_objects(&executor, "sensitive").await;
        grant_parent_usage(&executor, role_id, &db_meta, &schema_meta).await;
        let analyst = context_for(role_id, "no_select_user");

        let err = exec_sql_as(&executor, "SELECT * FROM sensitive", "securedb", &analyst)
            .await
            .expect_err("user with parent USAGE but no SELECT must not read table");
        assert!(
            matches!(err, nova_common::NovaError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }
```

- [ ] **Step 3: Add missing-parent-USAGE denial acceptance test**

Append this test after the missing-SELECT test:

```rust
    #[tokio::test]
    async fn phase3_select_without_parent_usage_cannot_read_table() {
        let (executor, _dir) = setup();
        exec_sql(&executor, "CREATE DATABASE securedb", "securedb")
            .await
            .unwrap();
        exec_sql(&executor, "CREATE TABLE sensitive (id INT)", "securedb")
            .await
            .unwrap();
        exec_sql(&executor, "INSERT INTO sensitive VALUES (1)", "securedb")
            .await
            .unwrap();

        let role_id = create_role(&executor, "table_select_only_role").await;
        let (_db_meta, _schema_meta, table_meta) = securedb_objects(&executor, "sensitive").await;
        grant(
            &executor,
            role_id,
            nova_common::ObjectRef::new(nova_common::ObjectType::Table, table_meta.id),
            nova_common::SecurityPrivilege::Select,
        )
        .await;
        let analyst = context_for(role_id, "table_select_only_user");

        let err = exec_sql_as(&executor, "SELECT * FROM sensitive", "securedb", &analyst)
            .await
            .expect_err("table SELECT without parent USAGE must not read table");
        assert!(
            matches!(err, nova_common::NovaError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }
```

- [ ] **Step 4: Add owner-can-operate acceptance test**

Append this test after the missing-parent-USAGE test:

```rust
    #[tokio::test]
    async fn phase3_owner_can_operate_on_owned_table() {
        let (executor, _dir) = setup();
        exec_sql(&executor, "CREATE DATABASE securedb", "securedb")
            .await
            .unwrap();
        exec_sql(&executor, "CREATE TABLE owned_table (id INT)", "securedb")
            .await
            .unwrap();

        let owner_role_id = create_role(&executor, "table_owner_role").await;
        let (db_meta, schema_meta, table_meta) = securedb_objects(&executor, "owned_table").await;
        grant_parent_usage(&executor, owner_role_id, &db_meta, &schema_meta).await;
        executor
            .meta()
            .set_object_owner(nova_common::ObjectOwnerMeta {
                object: nova_common::ObjectRef::new(nova_common::ObjectType::Table, table_meta.id),
                owner_role_id,
                created_by_user_id: nova_common::ROOT_USER_ID,
                created_at: nova_common::now_micros(),
                transferred_at: None,
            })
            .await
            .unwrap();
        let owner = context_for(owner_role_id, "table_owner_user");

        exec_sql_as(&executor, "INSERT INTO owned_table VALUES (1)", "securedb", &owner)
            .await
            .expect("table owner should be able to insert");
        exec_sql_as(&executor, "SELECT * FROM owned_table", "securedb", &owner)
            .await
            .expect("table owner should be able to select");
        exec_sql_as(
            &executor,
            "ALTER TABLE owned_table ADD COLUMN note VARCHAR",
            "securedb",
            &owner,
        )
        .await
        .expect("table owner should be able to alter");
    }
```

- [ ] **Step 5: Add non-owner-drop denial acceptance test**

Append this test after the owner-can-operate test:

```rust
    #[tokio::test]
    async fn phase3_non_owner_cannot_drop_table_without_ownership() {
        let (executor, _dir) = setup();
        exec_sql(&executor, "CREATE DATABASE securedb", "securedb")
            .await
            .unwrap();
        exec_sql(&executor, "CREATE TABLE sensitive (id INT)", "securedb")
            .await
            .unwrap();

        let role_id = create_role(&executor, "drop_denied_role").await;
        let (db_meta, schema_meta, table_meta) = securedb_objects(&executor, "sensitive").await;
        grant_parent_usage(&executor, role_id, &db_meta, &schema_meta).await;
        grant(
            &executor,
            role_id,
            nova_common::ObjectRef::new(nova_common::ObjectType::Table, table_meta.id),
            nova_common::SecurityPrivilege::Select,
        )
        .await;
        let analyst = context_for(role_id, "drop_denied_user");

        let err = exec_sql_as(&executor, "DROP TABLE sensitive", "securedb", &analyst)
            .await
            .expect_err("non-owner without OWNERSHIP must not drop table");
        assert!(
            matches!(err, nova_common::NovaError::PermissionDenied { .. }),
            "expected PermissionDenied, got {err:?}"
        );
    }
```

- [ ] **Step 6: Run the new Phase 3 tests**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests phase3_ -- --nocapture
```

Expected:

```text
test result: ok
```

If `phase3_owner_can_operate_on_owned_table` fails on parent `USAGE`, do not remove parent `USAGE` requirements. Fix the test grants first. If it fails on table privileges, inspect `Executor::has_privilege` and preserve this owner shortcut:

```rust
        if let Some(owner) = self.meta.get_object_owner(object).await?
            && active_roles.contains(&owner.owner_role_id)
        {
            return Ok(true);
        }
```

- [ ] **Step 7: Run all E2E tests**

Run:

```bash
cargo test -p nova-coordinator --test e2e_tests
```

Expected:

```text
test result: ok
```

---

### Task 6: Update RBAC roadmap and run final verification

**Files:**
- Modify: `docs/design/enterprise-rbac-roadmap.md:903-1005`

**Interfaces:**
- Consumes:
  - Passing verification output from Tasks 1-5.
- Produces:
  - Roadmap checkboxes accurately reflecting verified Phase 0, Phase 2 acceptance, and Phase 3 acceptance completion.

- [ ] **Step 1: Update Phase 0 roadmap checkboxes**

In `docs/design/enterprise-rbac-roadmap.md`, update Phase 0 lines 909-919 from unchecked to checked only after Task 1-3 storage tests pass:

```markdown
- [x] Add atomic ID allocation for database/schema/table/dynamic_table/user/role.
- [x] Add name indexes where missing.
- [x] Prevent duplicate names in same scope.
- [x] Reopen tests prove IDs persist.
- [x] Existing ID `0` behavior no longer creates collisions.
```

And:

```markdown
- [x] Create two databases; IDs differ and survive reopen.
- [x] Create two tables in same schema; IDs differ and survive reopen.
- [x] Duplicate table name fails.
```

- [ ] **Step 2: Update Phase 2 roadmap acceptance checkboxes**

In `docs/design/enterprise-rbac-roadmap.md`, update lines 980-981 after Task 4 passes:

```markdown
- [x] Login fails for disabled user.
- [x] Login fails if default role is not granted.
```

- [ ] **Step 3: Update Phase 3 roadmap acceptance checkboxes**

In `docs/design/enterprise-rbac-roadmap.md`, update lines 1000-1003 after Task 5 passes:

```markdown
- [x] User without `SELECT` cannot read table.
- [x] User with `SELECT` but without parent `USAGE` cannot read table.
- [x] Owner can operate on owned table.
- [x] Non-owner cannot drop table without ownership.
```

Do not mark any Phase 4 or Phase 5 boxes complete in this task.

- [ ] **Step 4: Run required storage verification**

Run:

```bash
cargo test -p nova-storage
```

Expected:

```text
test result: ok
```

- [ ] **Step 5: Run required coordinator test verification**

Run:

```bash
cargo test -p nova-coordinator --test rbac_phase2_security_context_test
cargo test -p nova-coordinator --test e2e_tests
cargo test -p nova-coordinator --test dynamic_table_test
```

Expected for each command:

```text
test result: ok
```

- [ ] **Step 6: Run clippy**

Run:

```bash
cargo clippy -p nova-storage -p nova-coordinator --all-targets -- -D warnings
```

Expected:

```text
Finished
```

and no warnings/errors.

- [ ] **Step 7: Run format check**

Run:

```bash
cargo fmt --all -- --check
```

Expected:

```text
```

No output and exit code `0`.

- [ ] **Step 8: Inspect final diff for secrets and unintended files**

Run:

```bash
git status --short --untracked-files=all
git diff --stat
git diff --check
```

Expected:

```text
```

`git diff --check` should print no whitespace errors. `git status` should only show intended source/doc files and must not show `.claude/settings.json`, `.config/opencode/opencode.json`, `.opencode/combo-api-key`, or `.opencode/node_modules/`.

- [ ] **Step 9: Commit only if explicitly requested**

If the user asks to commit, inspect before committing:

```bash
git status --short --untracked-files=all
git diff
git log --oneline -10
```

Then stage only intended files:

```bash
git add crates/nova-storage/src/metadata/fdb_store.rs \
  crates/nova-storage/src/metadata/security_impl.rs \
  crates/nova-coordinator/tests/rbac_phase2_security_context_test.rs \
  crates/nova-coordinator/tests/e2e_tests.rs \
  docs/design/enterprise-rbac-roadmap.md
```

Commit message:

```bash
git commit -m "feat(security): harden RBAC foundation acceptance"
```
