// SQL Parser — wraps sqlparser-rs with Nova custom dialect support.

use nova_common::{NovaError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Nova SQL parser. Wraps sqlparser-rs with Nova dialect extensions.
pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    /// Parse SQL text into one or more statements.
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        Parser::parse_sql(&GenericDialect {}, sql.trim()).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_database() {
        let parser = SqlParser::new();
        let stmts = parser.parse("CREATE DATABASE my_db").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_create_table() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("CREATE TABLE orders (id INT, amount DECIMAL(10,2), status VARCHAR(20))")
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_insert() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("INSERT INTO orders VALUES (1, 500.00, 'pending')")
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_select() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("SELECT id, amount FROM orders WHERE amount > 100")
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_select_star() {
        let parser = SqlParser::new();
        let stmts = parser.parse("SELECT * FROM orders").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_multiple_statements() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t")
            .unwrap();
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn test_parse_error() {
        let parser = SqlParser::new();
        let result = parser.parse("NOT VALID SQL !!!");
        assert!(result.is_err());
    }
}
