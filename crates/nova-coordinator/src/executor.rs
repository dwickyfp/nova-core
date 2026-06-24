// Executor — wire resolved SQL statements to storage layer.

use crate::analyzer::{ResolvedExpr, ResolvedFilter, ResolvedStatement};
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use nova_common::*;
use nova_storage::{MetadataStore, MpReader, MpWriter};
use std::sync::Arc;

/// SQL execution engine. Wires resolved statements to storage layer.
pub struct Executor {
    meta: Arc<dyn MetadataStore>,
    writer: MpWriter,
    reader: MpReader,
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

impl Executor {
    pub fn new(meta: Arc<dyn MetadataStore>, writer: MpWriter, reader: MpReader) -> Self {
        Self {
            meta,
            writer,
            reader,
        }
    }

    /// Execute a resolved statement.
    pub async fn execute(&self, stmt: ResolvedStatement) -> Result<QueryResult> {
        match stmt {
            ResolvedStatement::CreateDatabase { name } => self.exec_create_database(name).await,
            ResolvedStatement::CreateTable {
                db,
                schema,
                table,
                columns,
            } => self.exec_create_table(db, schema, table, columns).await,
            ResolvedStatement::Insert {
                db,
                schema,
                table,
                values,
            } => self.exec_insert(db, schema, table, values).await,
            ResolvedStatement::Select {
                db,
                schema,
                table,
                projection,
                ..
            } => self.exec_select(db, schema, table, projection).await,
            ResolvedStatement::Update {
                db,
                schema,
                table,
                assignments,
                filter,
            } => {
                self.exec_update(&db, &schema, &table, assignments, filter)
                    .await
            }
            ResolvedStatement::Delete {
                db,
                schema,
                table,
                filter,
            } => self.exec_delete(&db, &schema, &table, filter).await,
        }
    }

    async fn exec_create_database(&self, name: String) -> Result<QueryResult> {
        let db = DatabaseMeta {
            id: 0,
            name: name.clone(),
            created_at: now_micros(),
            owner: 1,
        };
        self.meta.create_database(db).await?;
        Ok(QueryResult::Success {
            message: format!("Database '{}' created", name),
        })
    }

    async fn exec_create_table(
        &self,
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

        Ok(QueryResult::Success {
            message: format!("Table '{}.{}.{}' created", db, schema, table),
        })
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

        // Write as micro-partition
        let mp_id = generate_id();
        let version = self.meta.increment_table_version(table_meta.id).await?;
        let txn_id = self.meta.begin_transaction().await?;

        let mp = self
            .writer
            .write(table_meta.id, mp_id, version, &[batch], txn_id)
            .await?;

        // Insert MP metadata with commit timestamp (skip commit_mp for local storage)
        let mut committed_mp = mp;
        committed_mp.s3_path = committed_mp
            .s3_temp_path
            .take()
            .unwrap_or(committed_mp.s3_path);
        committed_mp.commit_ts = now_micros();
        committed_mp.active = true;
        self.meta.insert_mp(committed_mp).await?;

        self.meta.commit_transaction(txn_id).await?;

        Ok(QueryResult::Success {
            message: format!(
                "{} row(s) inserted into '{}.{}.{}'",
                row_count, db, schema, table
            ),
        })
    }

    async fn exec_select(
        &self,
        db: String,
        schema: String,
        table: String,
        projection: Vec<String>,
    ) -> Result<QueryResult> {
        let table_meta = self.find_table(&db, &schema, &table).await?;

        // Get active micro-partitions
        let mps = self.meta.get_active_mps(table_meta.id).await?;
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

        // Determine column indices for projection
        let col_indices: Option<Vec<usize>> = if projection.contains(&"*".to_string()) {
            None
        } else {
            Some(
                projection
                    .iter()
                    .filter_map(|p| table_meta.columns.iter().position(|c| &c.name == p))
                    .collect(),
            )
        };

        // Read all MPs
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for mp in &mps {
            let batches = self.reader.read(mp, col_indices.as_deref()).await?;
            all_batches.extend(batches);
        }

        // Convert to string rows for display
        let result_columns = if projection.contains(&"*".to_string()) {
            table_meta.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            projection
        };

        let mut rows: Vec<Vec<String>> = Vec::new();
        for batch in &all_batches {
            for row_idx in 0..batch.num_rows() {
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

        // For each MP: read, apply UPDATE to matching rows, write new MP
        for mp in &mps {
            let batches = self.reader.read(mp, None).await?;
            let mut modified_batches = Vec::new();
            for batch in batches {
                let modified =
                    self.apply_update_to_batch(&batch, &assignments, &filter, &table_meta)?;
                modified_batches.push(modified);
            }
            // Write new MP
            let new_mp = self
                .writer
                .write(
                    table_meta.id,
                    mp.mp_id + 1000,
                    mp.version + 1,
                    &modified_batches,
                    1,
                )
                .await?;
            // Mark old MP as superseded
            self.meta.mark_superseded(mp.mp_id, new_mp.mp_id).await?;
        }

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

        for mp in &mps {
            let batches = self.reader.read(mp, None).await?;
            let mut kept_batches = Vec::new();
            for batch in batches {
                let kept = self.apply_delete_to_batch(&batch, &filter, &table_meta)?;
                if kept.num_rows() > 0 {
                    kept_batches.push(kept);
                }
            }
            // Write new MP only if there are remaining rows
            if !kept_batches.is_empty() {
                let new_mp = self
                    .writer
                    .write(
                        table_meta.id,
                        mp.mp_id + 1000,
                        mp.version + 1,
                        &kept_batches,
                        1,
                    )
                    .await?;
                self.meta.mark_superseded(mp.mp_id, new_mp.mp_id).await?;
            } else {
                // All rows deleted — just mark old MP as superseded (no new MP)
                self.meta.mark_superseded(mp.mp_id, mp.mp_id + 1000).await?;
            }
        }

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
}

/// Parse SQL type string to NovaType.
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
        let meta: Arc<dyn MetadataStore> =
            Arc::new(nova_storage::SledMetadataStore::open_temporary().unwrap());
        let writer = MpWriter::new(store.clone(), "test".to_string());
        let reader = MpReader::new(store);
        let executor = Executor::new(meta, writer, reader);
        (executor, dir)
    }

    #[tokio::test]
    async fn test_create_database() {
        let (executor, _dir) = setup();
        let result = executor
            .execute(ResolvedStatement::CreateDatabase {
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
            .execute(ResolvedStatement::CreateDatabase {
                name: "my_db".to_string(),
            })
            .await
            .unwrap();

        let result = executor
            .execute(ResolvedStatement::CreateTable {
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
            .execute(ResolvedStatement::CreateDatabase {
                name: "db".to_string(),
            })
            .await
            .unwrap();

        // Create table
        executor
            .execute(ResolvedStatement::CreateTable {
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
            .execute(ResolvedStatement::Insert {
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
            .execute(ResolvedStatement::Select {
                db: "db".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                projection: vec!["id".to_string(), "name".to_string()],
                filter: None,
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
        executor.execute(resolved).await.unwrap();

        // CREATE TABLE
        let stmts = parser
            .parse("CREATE TABLE testdb.public.items (id INT, price FLOAT)")
            .unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        executor.execute(resolved).await.unwrap();

        // INSERT
        let stmts = parser.parse("INSERT INTO items VALUES (1, 9.99)").unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        executor.execute(resolved).await.unwrap();

        // SELECT
        let stmts = parser.parse("SELECT id, price FROM items").unwrap();
        let resolved = analyzer.resolve(&stmts[0]).unwrap();
        let result = executor.execute(resolved).await.unwrap();

        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], "1");
            assert_eq!(rows[0][1], "9.99");
        } else {
            panic!("expected Rows");
        }
    }
}
