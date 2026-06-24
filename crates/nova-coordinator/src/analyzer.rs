// Analyzer — resolve table/column names, check types.

use nova_common::{NovaError, Result, Timestamp};
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
        projection: Vec<String>,
        filter: Option<ResolvedFilter>,
        at_timestamp: Option<Timestamp>,
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
}

#[derive(Debug)]
pub struct ResolvedColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug)]
pub enum ResolvedExpr {
    Int64(i64),
    Float64(f64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug)]
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
            Statement::CreateTable(ct) => {
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
                        projection,
                        filter: None,
                        at_timestamp: None,
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
}
