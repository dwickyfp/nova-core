//! SQL function runtime integration for DataFusion.
//!
//! Function metadata remains durable in FoundationDB; this module turns supported
//! SQL scalar functions into per-query DataFusion UDF registrations. External
//! runtimes can add parallel adapters without changing metadata storage.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, DataFusionError, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, Operator, ScalarFunctionImplementation, ScalarUDF, Volatility,
    binary_expr, col, create_udf, lit,
};
use datafusion::physical_expr::create_physical_expr;
use nova_common::{
    FunctionBody, FunctionLanguage, FunctionMeta, FunctionVolatility, NovaError, Result,
};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, JoinConstraint,
    JoinOperator, Query, SelectItem, SetExpr, Statement, UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Reference to a function call found in a SQL statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionCallRef {
    pub name: String,
    pub arg_count: usize,
}

/// Adapter for SQL scalar functions stored in `FunctionMeta`.
pub struct SqlFunctionRuntime;

impl SqlFunctionRuntime {
    /// Create a DataFusion scalar UDF from durable SQL function metadata.
    pub fn create_udf(function: &FunctionMeta) -> Result<ScalarUDF> {
        if function.language != FunctionLanguage::Sql {
            return Err(NovaError::Internal {
                message: format!(
                    "function language {} is not executable by SQL runtime",
                    function.language
                ),
            });
        }
        let FunctionBody::SqlExpression(body) = &function.body else {
            return Err(NovaError::Internal {
                message: format!(
                    "function '{}' does not contain a SQL expression",
                    function.name
                ),
            });
        };

        let input_types = function
            .args
            .iter()
            .map(|arg| function_type_to_arrow(&arg.data_type))
            .collect::<Result<Vec<_>>>()?;
        let return_type = function_type_to_arrow(&function.return_type)?;
        let body_expr = parse_body_expression(body)?;
        let df_expr = sql_expr_to_datafusion(&body_expr)?;

        let fields = function
            .args
            .iter()
            .zip(input_types.iter())
            .map(|(arg, data_type)| Field::new(&arg.name, data_type.clone(), true))
            .collect::<Vec<_>>();
        let arrow_schema = Arc::new(Schema::new(fields));
        let df_schema =
            DFSchema::try_from(arrow_schema.clone()).map_err(|err| NovaError::Internal {
                message: format!("function '{}' schema build failed: {}", function.name, err),
            })?;
        let physical_expr = create_physical_expr(&df_expr, &df_schema, &ExecutionProps::default())
            .map_err(|err| NovaError::Internal {
                message: format!(
                    "function '{}' expression planning failed: {}",
                    function.name, err
                ),
            })?;
        let expected_args = input_types.len();
        let return_type_for_cast = return_type.clone();
        let udf_schema = arrow_schema.clone();
        let function_name = function.name.clone();
        let implementation: ScalarFunctionImplementation = Arc::new(move |args| {
            if args.len() != expected_args {
                return Err(DataFusionError::Execution(format!(
                    "function '{}' expected {} arguments, got {}",
                    function_name,
                    expected_args,
                    args.len()
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let batch = RecordBatch::try_new(udf_schema.clone(), arrays)
                .map_err(|err| DataFusionError::ArrowError(err, None))?;
            let value = physical_expr.evaluate(&batch)?;
            value.cast_to(&return_type_for_cast, None)
        });

        Ok(create_udf(
            &function.name,
            input_types,
            return_type,
            volatility_to_datafusion(function.volatility),
            implementation,
        ))
    }
}

/// Find scalar function calls in a SELECT statement so the executor can apply
/// RBAC and register persisted SQL functions before DataFusion planning.
pub fn referenced_function_calls(sql: &str) -> Result<Vec<FunctionCallRef>> {
    let statements =
        Parser::parse_sql(&GenericDialect {}, sql).map_err(|err| NovaError::SqlParseError {
            message: err.to_string(),
        })?;
    let mut calls = Vec::new();
    for statement in &statements {
        collect_statement_functions(statement, &mut calls);
    }
    let mut seen = HashSet::new();
    calls.retain(|call| seen.insert((call.name.to_ascii_lowercase(), call.arg_count)));
    Ok(calls)
}

fn volatility_to_datafusion(volatility: FunctionVolatility) -> Volatility {
    match volatility {
        FunctionVolatility::Immutable => Volatility::Immutable,
        FunctionVolatility::Stable => Volatility::Stable,
        FunctionVolatility::Volatile => Volatility::Volatile,
    }
}

fn function_type_to_arrow(data_type: &str) -> Result<DataType> {
    let upper = data_type.trim().to_ascii_uppercase();
    if upper.starts_with("INT") || upper == "INTEGER" || upper.starts_with("BIGINT") {
        Ok(DataType::Int64)
    } else if upper.starts_with("SMALLINT") || upper.starts_with("TINYINT") {
        Ok(DataType::Int32)
    } else if upper.starts_with("FLOAT")
        || upper.starts_with("DOUBLE")
        || upper.starts_with("DECIMAL")
        || upper.starts_with("NUMERIC")
    {
        Ok(DataType::Float64)
    } else if upper.starts_with("VARCHAR")
        || upper.starts_with("CHAR")
        || upper == "TEXT"
        || upper == "STRING"
    {
        Ok(DataType::Utf8)
    } else if upper == "BOOLEAN" || upper == "BOOL" {
        Ok(DataType::Boolean)
    } else if upper.starts_with("DATE") {
        Ok(DataType::Date32)
    } else if upper.starts_with("TIMESTAMP") {
        Ok(DataType::Utf8)
    } else {
        Err(NovaError::SqlAnalysisError {
            message: format!("unsupported function type '{}'", data_type),
        })
    }
}

fn parse_body_expression(body: &str) -> Result<Expr> {
    let statements =
        Parser::parse_sql(&GenericDialect {}, &format!("SELECT {body}")).map_err(|err| {
            NovaError::SqlParseError {
                message: format!("function SQL body parse failed: {}", err),
            }
        })?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Err(NovaError::SqlAnalysisError {
            message: "function SQL body must be a scalar expression".to_string(),
        });
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(NovaError::SqlAnalysisError {
            message: "function SQL body must be a scalar SELECT expression".to_string(),
        });
    };
    let Some(item) = select.projection.first() else {
        return Err(NovaError::SqlAnalysisError {
            message: "function SQL body must project one expression".to_string(),
        });
    };
    if select.projection.len() != 1 {
        return Err(NovaError::SqlAnalysisError {
            message: "function SQL body must project exactly one expression".to_string(),
        });
    }
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(expr.clone()),
        SelectItem::ExprWithAlias { expr, .. } => Ok(expr.clone()),
        _ => Err(NovaError::SqlAnalysisError {
            message: "function SQL body must be a scalar expression".to_string(),
        }),
    }
}

fn sql_expr_to_datafusion(expr: &Expr) -> Result<DfExpr> {
    match expr {
        Expr::Identifier(ident) => Ok(col(&ident.value)),
        Expr::CompoundIdentifier(idents) => {
            let name = idents
                .last()
                .ok_or_else(|| NovaError::SqlAnalysisError {
                    message: "empty compound identifier in function body".to_string(),
                })?
                .value
                .clone();
            Ok(col(name))
        }
        Expr::Value(value) => value_to_datafusion_literal(value),
        Expr::Nested(inner) => sql_expr_to_datafusion(inner),
        Expr::BinaryOp { left, op, right } => Ok(binary_expr(
            sql_expr_to_datafusion(left)?,
            binary_operator_to_datafusion(op)?,
            sql_expr_to_datafusion(right)?,
        )),
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus => sql_expr_to_datafusion(expr),
            UnaryOperator::Minus => Ok(DfExpr::Negative(Box::new(sql_expr_to_datafusion(expr)?))),
            UnaryOperator::Not => Ok(DfExpr::Not(Box::new(sql_expr_to_datafusion(expr)?))),
            _ => Err(NovaError::SqlAnalysisError {
                message: format!("unsupported unary operator '{}' in SQL function body", op),
            }),
        },
        Expr::IsNull(inner) => Ok(DfExpr::IsNull(Box::new(sql_expr_to_datafusion(inner)?))),
        _ => Err(NovaError::SqlAnalysisError {
            message: format!("unsupported SQL function body expression: {}", expr),
        }),
    }
}

fn value_to_datafusion_literal(value: &Value) -> Result<DfExpr> {
    match value {
        Value::Number(source, _) => {
            if source.contains('.') {
                let parsed = source
                    .parse::<f64>()
                    .map_err(|err| NovaError::SqlAnalysisError {
                        message: format!(
                            "invalid numeric literal '{}' in function body: {}",
                            source, err
                        ),
                    })?;
                Ok(lit(parsed))
            } else {
                let parsed = source
                    .parse::<i64>()
                    .map_err(|err| NovaError::SqlAnalysisError {
                        message: format!(
                            "invalid integer literal '{}' in function body: {}",
                            source, err
                        ),
                    })?;
                Ok(lit(parsed))
            }
        }
        Value::SingleQuotedString(source) | Value::DoubleQuotedString(source) => {
            Ok(lit(source.clone()))
        }
        Value::Boolean(value) => Ok(lit(*value)),
        Value::Null => Ok(lit(ScalarValue::Null)),
        _ => Err(NovaError::SqlAnalysisError {
            message: format!("unsupported literal '{}' in SQL function body", value),
        }),
    }
}

fn binary_operator_to_datafusion(op: &BinaryOperator) -> Result<Operator> {
    match op {
        BinaryOperator::Plus => Ok(Operator::Plus),
        BinaryOperator::Minus => Ok(Operator::Minus),
        BinaryOperator::Multiply => Ok(Operator::Multiply),
        BinaryOperator::Divide => Ok(Operator::Divide),
        BinaryOperator::Modulo => Ok(Operator::Modulo),
        BinaryOperator::Eq => Ok(Operator::Eq),
        BinaryOperator::NotEq => Ok(Operator::NotEq),
        BinaryOperator::Lt => Ok(Operator::Lt),
        BinaryOperator::LtEq => Ok(Operator::LtEq),
        BinaryOperator::Gt => Ok(Operator::Gt),
        BinaryOperator::GtEq => Ok(Operator::GtEq),
        BinaryOperator::And => Ok(Operator::And),
        BinaryOperator::Or => Ok(Operator::Or),
        BinaryOperator::StringConcat => Ok(Operator::StringConcat),
        _ => Err(NovaError::SqlAnalysisError {
            message: format!("unsupported binary operator '{}' in SQL function body", op),
        }),
    }
}

fn collect_statement_functions(statement: &Statement, calls: &mut Vec<FunctionCallRef>) {
    if let Statement::Query(query) = statement {
        collect_query_functions(query, calls);
    }
}

fn collect_query_functions(query: &Query, calls: &mut Vec<FunctionCallRef>) {
    collect_set_expr_functions(query.body.as_ref(), calls);
    if let Some(order_by) = &query.order_by {
        for expr in &order_by.exprs {
            collect_expr_functions(&expr.expr, calls);
        }
    }
    if let Some(limit) = &query.limit {
        collect_expr_functions(limit, calls);
    }
    for expr in &query.limit_by {
        collect_expr_functions(expr, calls);
    }
}

fn collect_set_expr_functions(set_expr: &SetExpr, calls: &mut Vec<FunctionCallRef>) {
    match set_expr {
        SetExpr::Select(select) => {
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        collect_expr_functions(expr, calls);
                    }
                    _ => {}
                }
            }
            if let Some(expr) = &select.prewhere {
                collect_expr_functions(expr, calls);
            }
            if let Some(expr) = &select.selection {
                collect_expr_functions(expr, calls);
            }
            match &select.group_by {
                sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
                    for expr in exprs {
                        collect_expr_functions(expr, calls);
                    }
                }
                sqlparser::ast::GroupByExpr::All(_) => {}
            }
            if let Some(expr) = &select.having {
                collect_expr_functions(expr, calls);
            }
            if let Some(expr) = &select.qualify {
                collect_expr_functions(expr, calls);
            }
            for table_with_joins in &select.from {
                for join in &table_with_joins.joins {
                    collect_join_operator_functions(&join.join_operator, calls);
                }
            }
        }
        SetExpr::Query(query) => collect_query_functions(query, calls),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_functions(left, calls);
            collect_set_expr_functions(right, calls);
        }
        _ => {}
    }
}

fn collect_join_operator_functions(join_operator: &JoinOperator, calls: &mut Vec<FunctionCallRef>) {
    match join_operator {
        JoinOperator::Inner(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => {
            collect_join_constraint_functions(constraint, calls)
        }
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            collect_expr_functions(match_condition, calls);
            collect_join_constraint_functions(constraint, calls);
        }
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {}
    }
}

fn collect_join_constraint_functions(
    constraint: &JoinConstraint,
    calls: &mut Vec<FunctionCallRef>,
) {
    if let JoinConstraint::On(expr) = constraint {
        collect_expr_functions(expr, calls);
    }
}

fn collect_expr_functions(expr: &Expr, calls: &mut Vec<FunctionCallRef>) {
    match expr {
        Expr::Function(function) => {
            let arg_count = match &function.args {
                FunctionArguments::List(args) => args.args.len(),
                FunctionArguments::None => 0,
                FunctionArguments::Subquery(_) => 1,
            };
            if let Some(name) = function.name.0.last() {
                calls.push(FunctionCallRef {
                    name: name.value.clone(),
                    arg_count,
                });
            }
            collect_function_arguments(&function.args, calls);
            if let Some(filter) = &function.filter {
                collect_expr_functions(filter, calls);
            }
            for order in &function.within_group {
                collect_expr_functions(&order.expr, calls);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_functions(left, calls);
            collect_expr_functions(right, calls);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => collect_expr_functions(expr, calls),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_functions(expr, calls);
            collect_expr_functions(low, calls);
            collect_expr_functions(high, calls);
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_functions(expr, calls);
            for item in list {
                collect_expr_functions(item, calls);
            }
        }
        Expr::Cast { expr, .. } => collect_expr_functions(expr, calls),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expr_functions(operand, calls);
            }
            for condition in conditions {
                collect_expr_functions(condition, calls);
            }
            for result in results {
                collect_expr_functions(result, calls);
            }
            if let Some(else_result) = else_result {
                collect_expr_functions(else_result, calls);
            }
        }
        _ => {}
    }
}

fn collect_function_arguments(arguments: &FunctionArguments, calls: &mut Vec<FunctionCallRef>) {
    if let FunctionArguments::List(args) = arguments {
        for arg in &args.args {
            match arg {
                FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                    collect_function_arg_expr(arg, calls);
                }
            }
        }
    }
}

fn collect_function_arg_expr(arg: &FunctionArgExpr, calls: &mut Vec<FunctionCallRef>) {
    if let FunctionArgExpr::Expr(expr) = arg {
        collect_expr_functions(expr, calls);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use nova_common::{FunctionArg as NovaFunctionArg, FunctionNullHandling, FunctionSignature};
    use std::collections::HashMap;

    #[test]
    fn discovers_function_calls_in_select_expressions() {
        let calls = referenced_function_calls(
            "SELECT add_one(id), SUM(amount) FROM orders WHERE is_positive(amount) ORDER BY add_one(id)",
        )
        .unwrap();

        assert!(calls.contains(&FunctionCallRef {
            name: "add_one".to_string(),
            arg_count: 1,
        }));
        assert!(calls.contains(&FunctionCallRef {
            name: "SUM".to_string(),
            arg_count: 1,
        }));
        assert!(calls.contains(&FunctionCallRef {
            name: "is_positive".to_string(),
            arg_count: 1,
        }));
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.name.eq_ignore_ascii_case("add_one"))
                .count(),
            1
        );
    }

    #[test]
    fn sql_runtime_evaluates_scalar_expression_over_arrays() {
        let function = FunctionMeta {
            id: 1,
            db_id: 1,
            schema_id: 1,
            name: "add_one".to_string(),
            signature: FunctionSignature::new(vec!["INT".to_string()]),
            args: vec![NovaFunctionArg {
                name: "x".to_string(),
                data_type: "INT".to_string(),
                default_expr: None,
            }],
            return_type: "INT".to_string(),
            language: FunctionLanguage::Sql,
            body: FunctionBody::SqlExpression("x + 1".to_string()),
            volatility: FunctionVolatility::Immutable,
            null_handling: FunctionNullHandling::ReturnsNullOnNullInput,
            created_at: 1,
            updated_at: 1,
            owner_role_id: 1,
            comment: None,
            properties: HashMap::new(),
        };

        let udf = SqlFunctionRuntime::create_udf(&function).unwrap();
        let result = udf
            .invoke_batch(
                &[ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    1, 2, 3,
                ])))],
                3,
            )
            .unwrap()
            .into_array(3)
            .unwrap();
        let values = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(values.value(0), 2);
        assert_eq!(values.value(1), 3);
        assert_eq!(values.value(2), 4);
    }
}
