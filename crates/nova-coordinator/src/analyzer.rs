// Analyzer — resolve table/column names, check types.

use nova_common::{
    FunctionArg, FunctionBody, FunctionLanguage, FunctionNullHandling, FunctionSignature,
    FunctionVolatility, NovaError, Result, StreamReadMode, Timestamp,
};
use sqlparser::ast::Statement;

/// Resolved SQL statement ready for execution.
#[derive(Debug)]
pub enum ResolvedStatement {
    CreateDatabase {
        name: String,
    },
    CreateTable {
        db: String,
        schema: String,
        table: String,
        columns: Vec<ResolvedColumn>,
    },
    CreateFunction {
        db: String,
        schema: String,
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
    },
    DropFunction {
        db: String,
        schema: String,
        name: String,
        signature: FunctionSignature,
        if_exists: bool,
    },
    Insert {
        db: String,
        schema: String,
        table: String,
        values: Vec<Vec<ResolvedExpr>>,
    },
    Select {
        db: String,
        schema: String,
        table: String,
        dependencies: Vec<String>,
        projection: Vec<String>,
        filter: Option<ResolvedFilter>,
        at_timestamp: Option<Timestamp>,
        raw_sql: Option<String>,
    },
    Update {
        db: String,
        schema: String,
        table: String,
        assignments: Vec<(String, ResolvedExpr)>,
        filter: Option<ResolvedFilter>,
    },
    Delete {
        db: String,
        schema: String,
        table: String,
        filter: Option<ResolvedFilter>,
    },
    CreateClone {
        db: String,
        schema: String,
        clone_table: String,
        source_table: String,
        at_timestamp: Option<Timestamp>,
    },
    CreateStream {
        db: String,
        schema: String,
        stream_name: String,
        table: String,
        append_only: bool,
    },
    ReadStream {
        db: String,
        schema: String,
        stream_name: String,
        projection: Vec<String>,
        read_mode: StreamReadMode,
        raw_sql: String,
    },
    DropStream {
        db: String,
        schema: String,
        name: String,
    },
    ShowStreams {
        db: String,
        schema: String,
        pattern: Option<String>,
    },
    DescribeStream {
        db: String,
        schema: String,
        name: String,
    },
    SystemStreamHasData {
        db: String,
        schema: String,
        stream_name: String,
    },
    /// Garbage collect expired micro-partitions.
    Gc {
        retention_days: u32,
    },
    /// DROP TABLE <name>
    DropTable {
        db: String,
        schema: String,
        table: String,
    },
    /// DROP DATABASE <name>
    DropDatabase {
        name: String,
    },
    /// DROP SCHEMA <name>
    DropSchema {
        db: String,
        schema: String,
    },
    /// BEGIN TRANSACTION
    Begin,
    /// COMMIT TRANSACTION
    Commit,
    /// ROLLBACK TRANSACTION
    Rollback,
    /// BACKUP [TO <path>]
    Backup {
        path: Option<String>,
    },
    /// ALTER TABLE <table> ADD COLUMN <name> <type>
    AlterTable {
        db: String,
        schema: String,
        table: String,
        action: AlterAction,
    },
    /// RESTORE FROM <path>
    Restore {
        path: String,
    },
    /// CREATE DYNAMIC TABLE
    CreateDynamicTable {
        db: String,
        schema: String,
        name: String,
        query_definition: String,
        target_lag_seconds: u64,
        refresh_mode: nova_common::DtRefreshMode,
        initialize_on_create: bool,
    },
    /// ALTER DYNAMIC TABLE <name> REFRESH
    RefreshDynamicTable {
        db: String,
        schema: String,
        name: String,
    },
    /// ALTER DYNAMIC TABLE <name> SUSPEND
    SuspendDynamicTable {
        db: String,
        schema: String,
        name: String,
    },
    /// ALTER DYNAMIC TABLE <name> RESUME
    ResumeDynamicTable {
        db: String,
        schema: String,
        name: String,
    },
    /// DROP DYNAMIC TABLE <name>
    DropDynamicTable {
        db: String,
        schema: String,
        name: String,
    },
    /// SHOW DYNAMIC TABLES
    ShowDynamicTables {
        db: String,
        pattern: Option<String>,
    },
}

/// ALTER TABLE action types.
#[derive(Debug, Clone)]
pub enum AlterAction {
    AddColumn { name: String, data_type: String },
    DropColumn { name: String },
}

#[derive(Debug)]
pub struct ResolvedColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub enum ResolvedExpr {
    Int64(i64),
    Float64(f64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone)]
pub struct ResolvedFilter {
    pub column: String,
    pub op: String,
    pub value: ResolvedExpr,
}

pub struct Analyzer {
    default_db: String,
    default_schema: String,
}

impl Analyzer {
    fn decode_hex_payload(value: &str) -> Result<String> {
        if !value.len().is_multiple_of(2) {
            return Err(NovaError::SqlAnalysisError {
                message: "invalid encoded stream payload".to_string(),
            });
        }
        let mut bytes = Vec::with_capacity(value.len() / 2);
        for idx in (0..value.len()).step_by(2) {
            let byte = u8::from_str_radix(&value[idx..idx + 2], 16).map_err(|_| {
                NovaError::SqlAnalysisError {
                    message: "invalid encoded stream payload".to_string(),
                }
            })?;
            bytes.push(byte);
        }
        String::from_utf8(bytes).map_err(|_| NovaError::SqlAnalysisError {
            message: "invalid UTF-8 stream payload".to_string(),
        })
    }

    fn resolve_object_name(
        &self,
        name: &sqlparser::ast::ObjectName,
    ) -> Result<(String, String, String)> {
        let parts: Vec<String> = name.0.iter().map(|ident| ident.value.clone()).collect();
        match parts.as_slice() {
            [function] => Ok((
                self.default_db.clone(),
                self.default_schema.clone(),
                function.clone(),
            )),
            [schema, function] => Ok((self.default_db.clone(), schema.clone(), function.clone())),
            [db, schema, function] => Ok((db.clone(), schema.clone(), function.clone())),
            _ => Err(NovaError::SqlAnalysisError {
                message: format!("unsupported object name: {}", name),
            }),
        }
    }

    fn sql_function_body(body: &sqlparser::ast::CreateFunctionBody) -> FunctionBody {
        let expr = match body {
            sqlparser::ast::CreateFunctionBody::AsBeforeOptions(expr)
            | sqlparser::ast::CreateFunctionBody::AsAfterOptions(expr)
            | sqlparser::ast::CreateFunctionBody::Return(expr) => expr,
        };
        match expr {
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(source)) => {
                FunctionBody::SqlExpression(source.clone())
            }
            _ => FunctionBody::SqlExpression(expr.to_string()),
        }
    }

    fn function_volatility(
        behavior: &Option<sqlparser::ast::FunctionBehavior>,
    ) -> FunctionVolatility {
        match behavior {
            Some(sqlparser::ast::FunctionBehavior::Stable) => FunctionVolatility::Stable,
            Some(sqlparser::ast::FunctionBehavior::Volatile) => FunctionVolatility::Volatile,
            _ => FunctionVolatility::Immutable,
        }
    }

    fn function_null_handling(
        called_on_null: &Option<sqlparser::ast::FunctionCalledOnNull>,
    ) -> FunctionNullHandling {
        match called_on_null {
            Some(sqlparser::ast::FunctionCalledOnNull::CalledOnNullInput) => {
                FunctionNullHandling::CalledOnNullInput
            }
            Some(sqlparser::ast::FunctionCalledOnNull::Strict) => FunctionNullHandling::Strict,
            _ => FunctionNullHandling::ReturnsNullOnNullInput,
        }
    }

    fn resolve_function_args(
        args: Option<&Vec<sqlparser::ast::OperateFunctionArg>>,
    ) -> Result<Vec<FunctionArg>> {
        args.into_iter()
            .flatten()
            .enumerate()
            .map(|(idx, arg)| {
                if matches!(
                    arg.mode,
                    Some(sqlparser::ast::ArgMode::Out | sqlparser::ast::ArgMode::InOut)
                ) {
                    return Err(NovaError::SqlAnalysisError {
                        message: "only scalar IN function arguments are supported".to_string(),
                    });
                }
                Ok(FunctionArg {
                    name: arg
                        .name
                        .as_ref()
                        .map(|name| name.value.clone())
                        .unwrap_or_else(|| format!("arg{}", idx + 1)),
                    data_type: arg.data_type.to_string(),
                    default_expr: arg.default_expr.as_ref().map(|expr| expr.to_string()),
                })
            })
            .collect()
    }

    fn select_tables(select: &sqlparser::ast::Select) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        for twj in &select.from {
            Self::collect_table_factor(&twj.relation, &mut tables)?;
            for join in &twj.joins {
                Self::collect_table_factor(&join.relation, &mut tables)?;
            }
        }
        tables.sort();
        tables.dedup();
        Ok(tables)
    }

    fn collect_table_factor(
        factor: &sqlparser::ast::TableFactor,
        tables: &mut Vec<String>,
    ) -> Result<()> {
        match factor {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                if let Some(ident) = name.0.last() {
                    tables.push(ident.value.clone());
                }
                Ok(())
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                if let sqlparser::ast::SetExpr::Select(select) = subquery.body.as_ref() {
                    tables.extend(Self::select_tables(select)?);
                    Ok(())
                } else {
                    Err(NovaError::SqlAnalysisError {
                        message: "unsupported subquery dependency".to_string(),
                    })
                }
            }
            _ => Err(NovaError::SqlAnalysisError {
                message: "unsupported FROM clause".to_string(),
            }),
        }
    }

    pub fn new(default_db: String, default_schema: String) -> Self {
        Self {
            default_db,
            default_schema,
        }
    }

    /// Resolve a parsed statement into an executable form.
    pub fn resolve(&self, stmt: &Statement) -> Result<ResolvedStatement> {
        match stmt {
            Statement::CreateDatabase { db_name, .. } => {
                let name = db_name.0.first().map(|i| i.value.clone()).ok_or_else(|| {
                    NovaError::SqlAnalysisError {
                        message: "missing database name".to_string(),
                    }
                })?;
                Ok(ResolvedStatement::CreateDatabase { name })
            }
            Statement::CreateFunction {
                or_replace,
                if_not_exists,
                name,
                args,
                return_type,
                function_body,
                behavior,
                called_on_null,
                language,
                ..
            } => {
                let (db, schema, function_name) = self.resolve_object_name(name)?;
                let language_name = language
                    .as_ref()
                    .map(|language| language.value.as_str())
                    .unwrap_or("SQL");
                let function_language = FunctionLanguage::from_name(language_name);
                if function_language != FunctionLanguage::Sql {
                    return Err(NovaError::SqlAnalysisError {
                        message: format!(
                            "function language {} is not supported yet; only SQL is supported",
                            function_language
                        ),
                    });
                }
                let return_type = return_type
                    .as_ref()
                    .ok_or_else(|| NovaError::SqlAnalysisError {
                        message: "CREATE FUNCTION requires RETURNS <type>".to_string(),
                    })?
                    .to_string();
                let body = function_body
                    .as_ref()
                    .map(Self::sql_function_body)
                    .ok_or_else(|| NovaError::SqlAnalysisError {
                        message: "CREATE FUNCTION requires AS <expression>".to_string(),
                    })?;
                let args = Self::resolve_function_args(args.as_ref())?;
                let signature = FunctionSignature::new(
                    args.iter()
                        .map(|arg| arg.data_type.clone())
                        .collect::<Vec<_>>(),
                );
                Ok(ResolvedStatement::CreateFunction {
                    db,
                    schema,
                    name: function_name,
                    args,
                    signature,
                    return_type,
                    language: function_language,
                    body,
                    volatility: Self::function_volatility(behavior),
                    null_handling: Self::function_null_handling(called_on_null),
                    or_replace: *or_replace,
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::CreateTable(ct) => {
                // Detect Dynamic Table: table name starts with __dt_
                if ct.name.0.iter().any(|i| i.value.starts_with("__dt_")) {
                    // Format: __dt_<name>__<lag_secs>__<mode>__<init>
                    let table_ident = ct
                        .name
                        .0
                        .iter()
                        .find(|i| i.value.starts_with("__dt_"))
                        .map(|i| i.value.clone())
                        .unwrap_or_default();
                    let parts: Vec<&str> = table_ident.splitn(5, "__").collect();
                    // table_ident = __dt_<name>__<lag>__<mode>__<init>
                    // splitn(5, "__") = ["", "dt_<name>", "<lag>", "<mode>", "<init>"]
                    let name = parts
                        .get(1)
                        .unwrap_or(&"")
                        .strip_prefix("dt_")
                        .unwrap_or("")
                        .to_string();
                    let lag_secs: u64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(300);
                    let mode_str = parts.get(3).unwrap_or(&"AUTO");
                    let refresh_mode = match *mode_str {
                        "INCREMENTAL" => nova_common::DtRefreshMode::Incremental,
                        "FULL" => nova_common::DtRefreshMode::Full,
                        _ => nova_common::DtRefreshMode::Auto,
                    };
                    // Extract query from column comment if present
                    let query_definition = ct
                        .columns
                        .first()
                        .and_then(|c| {
                            c.options.iter().find_map(|o| {
                                if let sqlparser::ast::ColumnOption::Comment(s) = &o.option {
                                    Some(s.replace('_', " "))
                                } else {
                                    None
                                }
                            })
                        })
                        .unwrap_or_default();
                    // Parse init from table_ident — 4th part (0-indexed)
                    let init_str = table_ident.splitn(5, "__").nth(4).unwrap_or("ON_CREATE");
                    let initialize_on_create = init_str != "ON_SCHEDULE";
                    return Ok(ResolvedStatement::CreateDynamicTable {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        name,
                        query_definition,
                        target_lag_seconds: lag_secs,
                        refresh_mode,
                        initialize_on_create,
                    });
                }
                // Detect Clone: table name contains __CLONE__
                if let Some(clone_idx) = ct
                    .name
                    .0
                    .iter()
                    .position(|i| i.value.starts_with("__CLONE__"))
                {
                    let source_table = ct.name.0[clone_idx]
                        .value
                        .strip_prefix("__CLONE__")
                        .unwrap_or("");
                    let clone_table = ct.name.0[0].value.clone();
                    return Ok(ResolvedStatement::CreateClone {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        clone_table,
                        source_table: source_table.to_string(),
                        at_timestamp: None,
                    });
                }
                // Detect Stream: table name contains __STREAM__
                if let Some(stream_idx) = ct
                    .name
                    .0
                    .iter()
                    .position(|i| i.value.starts_with("__STREAM__"))
                {
                    let stream_info = &ct.name.0[stream_idx].value;
                    // Format: __STREAM__<name>__<table>__<append_only>
                    let parts: Vec<&str> = stream_info.split("__").collect();
                    let stream_name = parts.get(2).unwrap_or(&"").to_string();
                    let table = parts.get(3).unwrap_or(&"").to_string();
                    let append_only = parts.get(4).map(|p| *p == "true").unwrap_or(false);
                    return Ok(ResolvedStatement::CreateStream {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        stream_name,
                        table,
                        append_only,
                    });
                }
                let table_name = ct.name.0.last().map(|i| i.value.clone()).ok_or_else(|| {
                    NovaError::SqlAnalysisError {
                        message: "missing table name".to_string(),
                    }
                })?;
                let resolved_cols = ct
                    .columns
                    .iter()
                    .map(|c| ResolvedColumn {
                        name: c.name.value.clone(),
                        data_type: c.data_type.to_string(),
                        nullable: !c
                            .options
                            .iter()
                            .any(|o| matches!(o.option, sqlparser::ast::ColumnOption::NotNull)),
                    })
                    .collect();
                Ok(ResolvedStatement::CreateTable {
                    db: self.default_db.clone(),
                    schema: self.default_schema.clone(),
                    table: table_name,
                    columns: resolved_cols,
                })
            }
            Statement::Insert(ins) => {
                let tbl = ins
                    .table_name
                    .0
                    .last()
                    .map(|i| i.value.clone())
                    .ok_or_else(|| NovaError::SqlAnalysisError {
                        message: "missing table name in INSERT".to_string(),
                    })?;
                let mut all_values = Vec::new();
                if let Some(query) = &ins.source
                    && let sqlparser::ast::SetExpr::Values(values) = query.body.as_ref()
                {
                    for row in &values.rows {
                        let mut row_vals = Vec::new();
                        for expr in row {
                            row_vals.push(self.resolve_expr(expr)?);
                        }
                        all_values.push(row_vals);
                    }
                }
                Ok(ResolvedStatement::Insert {
                    db: self.default_db.clone(),
                    schema: self.default_schema.clone(),
                    table: tbl,
                    values: all_values,
                })
            }
            Statement::Query(query) => {
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    let from = select
                        .from
                        .first()
                        .ok_or_else(|| NovaError::SqlAnalysisError {
                            message: "SELECT must have a FROM clause".to_string(),
                        })?;
                    let dependencies = Self::select_tables(select)?;
                    let table_name = match &from.relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => {
                            name.0.last().map(|i| i.value.clone()).unwrap_or_default()
                        }
                        _ => {
                            return Err(NovaError::SqlAnalysisError {
                                message: "unsupported FROM clause".to_string(),
                            });
                        }
                    };
                    let projection = select
                        .projection
                        .iter()
                        .map(|p| match p {
                            sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                                sqlparser::ast::Expr::Identifier(id) => id.value.clone(),
                                _ => expr.to_string(),
                            },
                            sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
                            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                                alias.value.clone()
                            }
                            _ => p.to_string(),
                        })
                        .collect();
                    Ok(ResolvedStatement::Select {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        table: table_name,
                        dependencies,
                        projection,
                        filter: None,
                        at_timestamp: None,
                        raw_sql: None,
                    })
                } else {
                    Err(NovaError::SqlAnalysisError {
                        message: "unsupported query type".to_string(),
                    })
                }
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table_name = match &table.relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => {
                        name.0.last().map(|i| i.value.clone()).unwrap_or_default()
                    }
                    _ => {
                        return Err(NovaError::SqlAnalysisError {
                            message: "unsupported table in UPDATE".to_string(),
                        });
                    }
                };
                let resolved_assignments = assignments
                    .iter()
                    .map(|a| {
                        let col = match &a.target {
                            sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                name.0.last().map(|i| i.value.clone()).unwrap_or_default()
                            }
                            _ => String::new(),
                        };
                        let val = self.resolve_expr(&a.value)?;
                        Ok((col, val))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let filter = selection.as_ref().and_then(|s| self.resolve_filter(s));
                Ok(ResolvedStatement::Update {
                    db: self.default_db.clone(),
                    schema: self.default_schema.clone(),
                    table: table_name,
                    assignments: resolved_assignments,
                    filter,
                })
            }
            Statement::Delete(delete) => {
                let from = match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(tables)
                    | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                        tables.first().ok_or_else(|| NovaError::SqlAnalysisError {
                            message: "missing table in DELETE".to_string(),
                        })?
                    }
                };
                let table = match &from.relation {
                    sqlparser::ast::TableFactor::Table { name, .. } => {
                        name.0.last().map(|i| i.value.clone()).unwrap_or_default()
                    }
                    _ => {
                        return Err(NovaError::SqlAnalysisError {
                            message: "unsupported table in DELETE".to_string(),
                        });
                    }
                };
                let filter = delete
                    .selection
                    .as_ref()
                    .and_then(|s| self.resolve_filter(s));
                Ok(ResolvedStatement::Delete {
                    db: self.default_db.clone(),
                    schema: self.default_schema.clone(),
                    table,
                    filter,
                })
            }
            Statement::DropFunction {
                if_exists,
                func_desc,
                ..
            } => {
                let function = func_desc
                    .first()
                    .ok_or_else(|| NovaError::SqlAnalysisError {
                        message: "DROP FUNCTION requires a function name".to_string(),
                    })?;
                if func_desc.len() > 1 {
                    return Err(NovaError::SqlAnalysisError {
                        message: "DROP FUNCTION supports one function at a time".to_string(),
                    });
                }
                let (db, schema, name) = self.resolve_object_name(&function.name)?;
                let args = Self::resolve_function_args(function.args.as_ref())?;
                let signature = FunctionSignature::new(
                    args.iter()
                        .map(|arg| arg.data_type.clone())
                        .collect::<Vec<_>>(),
                );
                Ok(ResolvedStatement::DropFunction {
                    db,
                    schema,
                    name,
                    signature,
                    if_exists: *if_exists,
                })
            }
            Statement::Drop {
                names, object_type, ..
            } => {
                let table_name = names
                    .first()
                    .and_then(|n| n.0.first())
                    .map(|i| i.value.clone())
                    .unwrap_or_default();

                // Detect Time Travel: DROP TABLE __tt_<ts>__<sql>
                if table_name.starts_with("__tt_") {
                    let rest = table_name.strip_prefix("__tt_").unwrap_or("");
                    let parts: Vec<&str> = rest.splitn(2, "__").collect();
                    let ts: u64 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
                    let clean_sql = parts.get(1).unwrap_or(&"").replace('_', " ");
                    // Return as Select with at_timestamp
                    return Ok(ResolvedStatement::Select {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        table: String::new(),
                        dependencies: vec![],
                        projection: vec!["*".to_string()],
                        filter: None,
                        at_timestamp: Some(ts),
                        raw_sql: Some(clean_sql),
                    });
                }
                // Detect GC: DROP TABLE __gc_<retention>__
                if table_name.starts_with("__gc_") {
                    let retention: u32 = table_name
                        .trim_start_matches("__gc_")
                        .trim_end_matches("__")
                        .parse()
                        .unwrap_or(30);
                    return Ok(ResolvedStatement::Gc {
                        retention_days: retention,
                    });
                }
                // Detect DROP STREAM: DROP TABLE __drop_stream__<stream>
                if table_name.starts_with("__drop_stream__") {
                    let name = table_name
                        .strip_prefix("__drop_stream__")
                        .unwrap_or("")
                        .to_string();
                    return Ok(ResolvedStatement::DropStream {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        name,
                    });
                }
                // Detect SHOW STREAMS: DROP TABLE __show_streams_hex__<hex-pattern>
                if table_name.starts_with("__show_streams_hex__") {
                    let pattern_str = table_name
                        .strip_prefix("__show_streams_hex__")
                        .unwrap_or("");
                    let pattern = if pattern_str.is_empty() {
                        None
                    } else {
                        Some(Self::decode_hex_payload(pattern_str)?)
                    };
                    return Ok(ResolvedStatement::ShowStreams {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        pattern,
                    });
                }
                // Detect DESCRIBE STREAM: DROP TABLE __describe_stream__<stream>
                if table_name.starts_with("__describe_stream__") {
                    let name = table_name
                        .strip_prefix("__describe_stream__")
                        .unwrap_or("")
                        .to_string();
                    return Ok(ResolvedStatement::DescribeStream {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        name,
                    });
                }
                // Detect stream preview SELECT: DROP TABLE __stream_read_preview_hex__<stream-hex>__<sql-hex>
                if table_name.starts_with("__stream_read_preview_hex__") {
                    let payload = table_name
                        .strip_prefix("__stream_read_preview_hex__")
                        .unwrap_or("");
                    let (stream_hex, sql_hex) = payload.split_once("__").ok_or_else(|| {
                        NovaError::UnsupportedStreamSyntax {
                            message: "invalid stream preview marker".to_string(),
                        }
                    })?;
                    let stream_name = Self::decode_hex_payload(stream_hex)?;
                    let raw_sql = Self::decode_hex_payload(sql_hex)?;
                    return Ok(ResolvedStatement::ReadStream {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        stream_name,
                        projection: vec!["*".to_string()],
                        read_mode: StreamReadMode::Preview,
                        raw_sql,
                    });
                }
                // Detect legacy stream preview SELECT: DROP TABLE __stream_read_preview__<stream>
                if table_name.starts_with("__stream_read_preview__") {
                    let stream_name = table_name
                        .strip_prefix("__stream_read_preview__")
                        .unwrap_or("")
                        .to_string();
                    return Ok(ResolvedStatement::ReadStream {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        stream_name: stream_name.clone(),
                        projection: vec!["*".to_string()],
                        read_mode: StreamReadMode::Preview,
                        raw_sql: format!("SELECT * FROM {}", stream_name),
                    });
                }
                // Detect SYSTEM$STREAM_HAS_DATA: DROP TABLE __stream_has_data__<stream>
                if table_name.starts_with("__stream_has_data__") {
                    let stream_name = table_name
                        .strip_prefix("__stream_has_data__")
                        .unwrap_or("")
                        .to_string();
                    return Ok(ResolvedStatement::SystemStreamHasData {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        stream_name,
                    });
                }
                // Detect BACKUP: DROP TABLE __backup__[<path>]
                if table_name.starts_with("__backup__") {
                    let path = table_name
                        .strip_prefix("__backup__")
                        .filter(|p| !p.is_empty())
                        .map(|p| p.to_string());
                    return Ok(ResolvedStatement::Backup { path });
                }
                // Detect RESTORE: DROP TABLE __restore__<path>
                if table_name.starts_with("__restore__") {
                    let path = table_name
                        .strip_prefix("__restore__")
                        .unwrap_or("")
                        .to_string();
                    return Ok(ResolvedStatement::Restore { path });
                }

                // Detect Dynamic Table ALTER: DROP TABLE __alter_dt__<name>_<action>
                if table_name.starts_with("__alter_dt__") {
                    let rest = table_name.strip_prefix("__alter_dt__").unwrap_or("");
                    // Format: <name>_<ACTION>
                    let last_sep = rest.rfind('_').unwrap_or(rest.len());
                    let dt_name = &rest[..last_sep];
                    let action = if last_sep < rest.len() {
                        &rest[last_sep + 1..]
                    } else {
                        "REFRESH"
                    };
                    return match action.to_uppercase().as_str() {
                        "SUSPEND" => Ok(ResolvedStatement::SuspendDynamicTable {
                            db: self.default_db.clone(),
                            schema: self.default_schema.clone(),
                            name: dt_name.to_string(),
                        }),
                        "RESUME" => Ok(ResolvedStatement::ResumeDynamicTable {
                            db: self.default_db.clone(),
                            schema: self.default_schema.clone(),
                            name: dt_name.to_string(),
                        }),
                        _ => Ok(ResolvedStatement::RefreshDynamicTable {
                            db: self.default_db.clone(),
                            schema: self.default_schema.clone(),
                            name: dt_name.to_string(),
                        }),
                    };
                }
                // Detect DROP DYNAMIC TABLE: DROP TABLE __drop_dt__<name>
                if table_name.starts_with("__drop_dt__") {
                    let name = table_name
                        .strip_prefix("__drop_dt__")
                        .unwrap_or("")
                        .to_string();
                    return Ok(ResolvedStatement::DropDynamicTable {
                        db: self.default_db.clone(),
                        schema: self.default_schema.clone(),
                        name,
                    });
                }
                // Detect SHOW DYNAMIC TABLES: DROP TABLE __show_dt__<pattern>
                if table_name.starts_with("__show_dt__") {
                    let pattern_str = table_name.strip_prefix("__show_dt__").unwrap_or("");
                    let pattern = if pattern_str.is_empty() {
                        None
                    } else {
                        Some(pattern_str.to_string())
                    };
                    return Ok(ResolvedStatement::ShowDynamicTables {
                        db: self.default_db.clone(),
                        pattern,
                    });
                }

                // Real DROP TABLE / DROP DATABASE / DROP SCHEMA
                match object_type {
                    sqlparser::ast::ObjectType::Table => {
                        let table = names
                            .first()
                            .and_then(|n| n.0.last())
                            .map(|i| i.value.clone())
                            .unwrap_or_default();
                        Ok(ResolvedStatement::DropTable {
                            db: self.default_db.clone(),
                            schema: self.default_schema.clone(),
                            table,
                        })
                    }
                    sqlparser::ast::ObjectType::Database => {
                        let name = names
                            .first()
                            .and_then(|n| n.0.last())
                            .map(|i| i.value.clone())
                            .unwrap_or_default();
                        Ok(ResolvedStatement::DropDatabase { name })
                    }
                    sqlparser::ast::ObjectType::Schema => {
                        let schema = names
                            .first()
                            .and_then(|n| n.0.last())
                            .map(|i| i.value.clone())
                            .unwrap_or_default();
                        Ok(ResolvedStatement::DropSchema {
                            db: self.default_db.clone(),
                            schema,
                        })
                    }
                    _ => Err(NovaError::SqlAnalysisError {
                        message: format!("unsupported DROP: {}", table_name),
                    }),
                }
            }
            Statement::AlterTable {
                name, operations, ..
            } => {
                let table = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
                for item in operations {
                    if let sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } = item
                    {
                        let col_name = column_def.name.value.clone();
                        let col_type = column_def.data_type.to_string();
                        return Ok(ResolvedStatement::AlterTable {
                            db: self.default_db.clone(),
                            schema: self.default_schema.clone(),
                            table,
                            action: AlterAction::AddColumn {
                                name: col_name,
                                data_type: col_type,
                            },
                        });
                    }
                }
                Err(NovaError::SqlAnalysisError {
                    message: "unsupported ALTER TABLE operation".to_string(),
                })
            }
            Statement::StartTransaction { .. } => Ok(ResolvedStatement::Begin),
            Statement::Commit { .. } => Ok(ResolvedStatement::Commit),
            Statement::Rollback { .. } => Ok(ResolvedStatement::Rollback),
            _ => Err(NovaError::SqlAnalysisError {
                message: format!("unsupported statement: {:?}", stmt),
            }),
        }
    }

    fn resolve_filter(&self, expr: &sqlparser::ast::Expr) -> Option<ResolvedFilter> {
        match expr {
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                let col = match left.as_ref() {
                    sqlparser::ast::Expr::Identifier(id) => id.value.clone(),
                    _ => return None,
                };
                let val = self.resolve_expr(right).ok()?;
                let op_str = match op {
                    sqlparser::ast::BinaryOperator::Eq => "=",
                    sqlparser::ast::BinaryOperator::NotEq => "!=",
                    sqlparser::ast::BinaryOperator::Gt => ">",
                    sqlparser::ast::BinaryOperator::GtEq => ">=",
                    sqlparser::ast::BinaryOperator::Lt => "<",
                    sqlparser::ast::BinaryOperator::LtEq => "<=",
                    _ => return None,
                };
                Some(ResolvedFilter {
                    column: col,
                    op: op_str.to_string(),
                    value: val,
                })
            }
            _ => None,
        }
    }

    fn resolve_expr(&self, expr: &sqlparser::ast::Expr) -> Result<ResolvedExpr> {
        use sqlparser::ast::{Expr, Value};
        match expr {
            Expr::Value(v) => match v {
                Value::Number(n, _) => {
                    if n.contains('.') {
                        Ok(ResolvedExpr::Float64(n.parse().unwrap_or(0.0)))
                    } else {
                        Ok(ResolvedExpr::Int64(n.parse().unwrap_or(0)))
                    }
                }
                Value::SingleQuotedString(s) => Ok(ResolvedExpr::String(s.clone())),
                Value::Boolean(b) => Ok(ResolvedExpr::Boolean(*b)),
                Value::Null => Ok(ResolvedExpr::Null),
                _ => Err(NovaError::SqlAnalysisError {
                    message: format!("unsupported value: {:?}", v),
                }),
            },
            _ => Err(NovaError::SqlAnalysisError {
                message: format!("unsupported expression: {:?}", expr),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    fn analyzer() -> Analyzer {
        Analyzer::new("my_db".to_string(), "public".to_string())
    }

    #[test]
    fn test_resolve_create_database() {
        let parser = SqlParser::new();
        let stmts = parser.parse("CREATE DATABASE test_db").unwrap();
        let resolved = analyzer().resolve(&stmts[0]).unwrap();
        if let ResolvedStatement::CreateDatabase { name } = resolved {
            assert_eq!(name, "test_db");
        } else {
            panic!("expected CreateDatabase");
        }
    }

    #[test]
    fn test_resolve_create_table() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("CREATE TABLE orders (id INT, name VARCHAR)")
            .unwrap();
        let resolved = analyzer().resolve(&stmts[0]).unwrap();
        if let ResolvedStatement::CreateTable { table, columns, .. } = resolved {
            assert_eq!(table, "orders");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].name, "id");
            assert_eq!(columns[1].name, "name");
        } else {
            panic!("expected CreateTable");
        }
    }

    #[test]
    fn test_resolve_insert() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("INSERT INTO orders VALUES (1, 500.00, 'pending')")
            .unwrap();
        let resolved = analyzer().resolve(&stmts[0]).unwrap();
        if let ResolvedStatement::Insert { table, values, .. } = resolved {
            assert_eq!(table, "orders");
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].len(), 3);
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn test_resolve_select() {
        let parser = SqlParser::new();
        let stmts = parser.parse("SELECT id, amount FROM orders").unwrap();
        let resolved = analyzer().resolve(&stmts[0]).unwrap();
        if let ResolvedStatement::Select {
            table, projection, ..
        } = resolved
        {
            assert_eq!(table, "orders");
            assert_eq!(projection, vec!["id", "amount"]);
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn phase3_join_dependencies_are_extracted() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id")
            .unwrap();
        let resolved = analyzer().resolve(&stmts[0]).unwrap();
        if let ResolvedStatement::Select { dependencies, .. } = resolved {
            assert_eq!(dependencies, vec!["customers", "orders"]);
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn resolves_sql_create_function() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE SQL AS 'x + 1'")
            .unwrap();
        let resolved = analyzer().resolve(&stmts[0]).unwrap();

        if let ResolvedStatement::CreateFunction {
            db,
            schema,
            name,
            args,
            return_type,
            language,
            body,
            or_replace,
            ..
        } = resolved
        {
            assert_eq!(db, "my_db");
            assert_eq!(schema, "public");
            assert_eq!(name, "add_one");
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].name, "x");
            assert_eq!(args[0].data_type, "INT");
            assert_eq!(return_type, "INT");
            assert_eq!(language, FunctionLanguage::Sql);
            assert_eq!(body, FunctionBody::SqlExpression("x + 1".to_string()));
            assert!(!or_replace);
        } else {
            panic!("expected CreateFunction");
        }
    }

    #[test]
    fn rejects_non_sql_create_function_language() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("CREATE FUNCTION py_one(x INT) RETURNS INT LANGUAGE PYTHON AS 'x + 1'")
            .unwrap();
        let err = analyzer().resolve(&stmts[0]).unwrap_err();

        assert!(
            err.to_string()
                .contains("function language PYTHON is not supported yet; only SQL is supported")
        );
    }

    #[test]
    fn resolves_drop_function_signature() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("DROP FUNCTION IF EXISTS add_one(INT)")
            .unwrap();
        let resolved = analyzer().resolve(&stmts[0]).unwrap();

        if let ResolvedStatement::DropFunction {
            db,
            schema,
            name,
            signature,
            if_exists,
        } = resolved
        {
            assert_eq!(db, "my_db");
            assert_eq!(schema, "public");
            assert_eq!(name, "add_one");
            assert_eq!(signature.key(), "INT");
            assert!(if_exists);
        } else {
            panic!("expected DropFunction");
        }
    }
}
