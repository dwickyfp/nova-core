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
    /// Also handles Nova custom syntax (CLONE, STREAM, GC) via pre-parse.
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        let sql = sql.trim();
        // Nova custom syntax: CLONE, STREAM, GC
        // These are not standard SQL, so we pre-parse them.
        let upper = sql.to_uppercase();
        if upper.contains(" CLONE ") {
            return self.parse_clone(sql);
        }
        if upper.starts_with("CREATE STREAM ") {
            return self.parse_stream(sql);
        }
        if upper.starts_with("GC") || upper.starts_with("VACUUM") {
            return self.parse_gc(sql);
        }
        if upper.starts_with("BACKUP") {
            return self.parse_backup(sql);
        }
        if upper.starts_with("RESTORE") {
            return self.parse_restore(sql);
        }
        Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: CREATE TABLE <new_table> CLONE <source_table> [AT(TIMESTAMP => <ts>)]
    fn parse_clone(&self, sql: &str) -> Result<Vec<Statement>> {
        // Extract: CREATE TABLE <new> CLONE <source>
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // Expected: CREATE TABLE <new> CLONE <source>
        if parts.len() < 5 {
            return Err(NovaError::SqlParseError {
                message: "CLONE syntax: CREATE TABLE <new> CLONE <source>".to_string(),
            });
        }
        // Find CLONE keyword position
        let clone_pos = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("CLONE"))
            .ok_or_else(|| NovaError::SqlParseError {
                message: "missing CLONE keyword".to_string(),
            })?;
        let new_table = parts[clone_pos - 1].trim_end_matches(';');
        let source_table = parts[clone_pos + 1].trim_end_matches(';');

        // Reconstruct as a custom statement using CREATE TABLE with properties
        // ponytail: sqlparser doesn't support CLONE, so we encode it as a CREATE TABLE
        // with a special comment that the analyzer detects. Upgrade to custom AST when needed.
        let fake_sql = format!(
            "CREATE TABLE {} (__clone_source__ VARCHAR, __clone_ts__ VARCHAR)",
            new_table
        );
        let mut stmts = Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| {
            NovaError::SqlParseError {
                message: e.to_string(),
            }
        })?;
        // Attach clone info via mutation of the CreateTable statement
        if let Some(Statement::CreateTable(ct)) = stmts.first_mut() {
            ct.columns.clear();
            ct.columns.push(sqlparser::ast::ColumnDef {
                name: sqlparser::ast::Ident::new("__clone_source__"),
                data_type: sqlparser::ast::DataType::Varchar(None),
                options: vec![],
                collation: None,
            });
            // Store source table name in table comment
            ct.name.0.push(sqlparser::ast::Ident::new(format!(
                "__CLONE__{}",
                source_table
            )));
        }
        Ok(stmts)
    }

    /// Parse: CREATE STREAM <name> ON TABLE <table> [APPEND ONLY]
    fn parse_stream(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // Expected: CREATE STREAM <name> ON TABLE <table>
        if parts.len() < 6 {
            return Err(NovaError::SqlParseError {
                message: "STREAM syntax: CREATE STREAM <name> ON TABLE <table>".to_string(),
            });
        }
        let stream_name = parts[2].trim_end_matches(';');
        // Find ON TABLE
        let table_pos = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("TABLE"))
            .ok_or_else(|| NovaError::SqlParseError {
                message: "missing TABLE keyword in CREATE STREAM".to_string(),
            })?;
        let table = parts[table_pos + 1].trim_end_matches(';');
        let append_only = sql.to_uppercase().contains("APPEND ONLY");

        // Encode as CREATE TABLE with stream metadata
        let fake_sql = format!(
            "CREATE TABLE __stream_{}_on_{} (__stream_name__ VARCHAR)",
            stream_name, table
        );
        let mut stmts = Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| {
            NovaError::SqlParseError {
                message: e.to_string(),
            }
        })?;
        if let Some(Statement::CreateTable(ct)) = stmts.first_mut() {
            ct.columns.clear();
            ct.name.0.push(sqlparser::ast::Ident::new(format!(
                "__STREAM__{}__{}__{}",
                stream_name, table, append_only
            )));
        }
        Ok(stmts)
    }

    /// Parse: GC <retention_days> or VACUUM <retention_days>
    fn parse_gc(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let retention: u32 = parts
            .get(1)
            .and_then(|p| p.trim_end_matches(';').parse().ok())
            .unwrap_or(30);
        // Encode as a DROP TABLE with special name that analyzer detects
        let fake_sql = format!("DROP TABLE __gc_{}__", retention);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: BACKUP [TO <path>]
    fn parse_backup(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // BACKUP TO /path/to/backup → DROP TABLE __backup__/path/to/backup
        // BACKUP (no path) → DROP TABLE __backup__
        let path = parts.get(2).map(|p| p.trim_end_matches(';').to_string());
        let encoded = match &path {
            Some(p) => format!("__backup__{}", p),
            None => "__backup__".to_string(),
        };
        let fake_sql = format!("DROP TABLE {}", encoded);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: RESTORE FROM <path>
    fn parse_restore(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // RESTORE FROM /path/to/backup → DROP TABLE __restore__/path/to/backup
        let path = parts
            .get(2)
            .map(|p| p.trim_end_matches(';').to_string())
            .unwrap_or_default();
        let fake_sql = format!("DROP TABLE __restore__{}", path);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
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
