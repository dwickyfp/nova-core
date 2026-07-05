// SQL Parser — wraps sqlparser-rs with Nova custom dialect support.

use nova_common::{NovaError, Result};
use sqlparser::ast::{Action, GrantObjects, Ident, ObjectName, ObjectType, Privileges, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Nova SQL parser. Wraps sqlparser-rs with Nova dialect extensions.
pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    /// Parse SQL text into one or more statements.
    /// Also handles Nova custom syntax (CLONE, STREAM, GC, DYNAMIC TABLE) via pre-parse.
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>> {
        let sql = sql.trim();
        // Nova custom syntax: CLONE, STREAM, GC, DYNAMIC TABLE
        // These are not standard SQL, so we pre-parse them.
        let upper = sql.to_uppercase();
        if upper.contains(" CLONE ") {
            return self.parse_clone(sql);
        }
        if upper.starts_with("CREATE STREAM ") {
            return self.parse_stream(sql);
        }
        if upper.starts_with("DROP STREAM ") {
            return self.parse_drop_stream(sql);
        }
        if upper.starts_with("SHOW STREAMS") {
            return self.parse_show_streams(sql);
        }
        if upper.starts_with("DESCRIBE STREAM ") || upper.starts_with("DESC STREAM ") {
            return self.parse_describe_stream(sql);
        }
        if upper.starts_with("SELECT SYSTEM$STREAM_HAS_DATA") {
            return self.parse_system_stream_has_data(sql);
        }
        if upper.starts_with("SELECT ") && upper.contains(" WITH (COMMIT") {
            return self.parse_stream_read(sql);
        }
        if upper.starts_with("CREATE DYNAMIC TABLE")
            || upper.starts_with("CREATE OR REPLACE DYNAMIC TABLE")
        {
            return self.parse_dynamic_table(sql);
        }
        if upper.starts_with("ALTER DYNAMIC TABLE") {
            return self.parse_alter_dynamic_table(sql);
        }
        if upper.starts_with("DROP DYNAMIC TABLE") {
            return self.parse_drop_dynamic_table(sql);
        }
        if upper.starts_with("SHOW DYNAMIC TABLES") {
            return self.parse_show_dynamic_tables(sql);
        }
        if upper.starts_with("GRANT ") && upper.contains(" ON FUNCTION ") {
            return self.parse_function_grant(sql, false);
        }
        if upper.starts_with("REVOKE ") && upper.contains(" ON FUNCTION ") {
            return self.parse_function_grant(sql, true);
        }
        if upper.starts_with("SHOW GRANTS") {
            return self.parse_show_grants(sql);
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
        // Time Travel: SELECT ... FROM t AT(TIMESTAMP => <unix_micros>)
        if upper.contains(" AT(TIMESTAMP") || upper.contains(" AT(TIMESTAMP ") {
            return self.parse_time_travel(sql);
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
        if sql.to_uppercase().contains("APPEND ONLY") {
            return Err(NovaError::UnsupportedStreamSyntax {
                message: "APPEND ONLY streams are not supported".to_string(),
            });
        }
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

    /// Parse: DROP STREAM <name>
    fn parse_drop_stream(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts
            .get(2)
            .ok_or_else(|| NovaError::SqlParseError {
                message: "DROP STREAM syntax: DROP STREAM <name>".to_string(),
            })?
            .trim_end_matches(';');
        let fake_sql = format!("DROP TABLE __drop_stream__{}", name);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: SHOW STREAMS [LIKE '<pattern>']
    fn parse_show_streams(&self, sql: &str) -> Result<Vec<Statement>> {
        let upper = sql.to_uppercase();
        let pattern = if let Some(like_pos) = upper.find(" LIKE ") {
            sql[like_pos + 6..]
                .trim()
                .trim_matches(';')
                .trim()
                .trim_matches('\'')
                .to_string()
        } else {
            String::new()
        };
        let fake_sql = format!(
            "DROP TABLE __show_streams_hex__{}",
            Self::hex_payload(&pattern)
        );
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: DESCRIBE STREAM <name>
    fn parse_describe_stream(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts
            .get(2)
            .ok_or_else(|| NovaError::SqlParseError {
                message: "DESCRIBE STREAM syntax: DESCRIBE STREAM <name>".to_string(),
            })?
            .trim_end_matches(';');
        let fake_sql = format!("DROP TABLE __describe_stream__{}", name);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: SELECT ... FROM <stream> WITH (COMMIT = FALSE)
    fn parse_stream_read(&self, sql: &str) -> Result<Vec<Statement>> {
        let upper = sql.to_uppercase();
        if !upper.contains("COMMIT = FALSE") && !upper.contains("COMMIT=FALSE") {
            return Err(NovaError::UnsupportedStreamSyntax {
                message: "stream SELECT WITH currently supports COMMIT = FALSE only".to_string(),
            });
        }
        let stream_name = Self::extract_select_from_relation(sql).ok_or_else(|| {
            NovaError::UnsupportedStreamSyntax {
                message: "stream SELECT syntax: SELECT ... FROM <stream> WITH (COMMIT = FALSE)"
                    .to_string(),
            }
        })?;
        let fake_sql = format!(
            "DROP TABLE __stream_read_preview_hex__{}__{}",
            Self::hex_payload(&stream_name),
            Self::hex_payload(sql)
        );
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: SELECT SYSTEM$STREAM_HAS_DATA('<stream>')
    fn parse_system_stream_has_data(&self, sql: &str) -> Result<Vec<Statement>> {
        let open = sql
            .find('(')
            .ok_or_else(|| NovaError::UnsupportedStreamSyntax {
                message: "SYSTEM$STREAM_HAS_DATA requires a stream name argument".to_string(),
            })?;
        let close = sql
            .rfind(')')
            .ok_or_else(|| NovaError::UnsupportedStreamSyntax {
                message: "SYSTEM$STREAM_HAS_DATA requires a closing ')'".to_string(),
            })?;
        if close <= open {
            return Err(NovaError::UnsupportedStreamSyntax {
                message: "SYSTEM$STREAM_HAS_DATA requires a stream name argument".to_string(),
            });
        }
        let stream_name = sql[open + 1..close]
            .trim()
            .trim_matches(|c: char| c == '\'' || c == '"')
            .trim_end_matches(';')
            .to_string();
        if stream_name.is_empty() {
            return Err(NovaError::UnsupportedStreamSyntax {
                message: "SYSTEM$STREAM_HAS_DATA requires a non-empty stream name".to_string(),
            });
        }
        let fake_sql = format!("DROP TABLE __stream_has_data__{}", stream_name);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    fn hex_payload(value: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(value.len() * 2);
        for byte in value.as_bytes() {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    fn extract_select_from_relation(sql: &str) -> Option<String> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let from_pos = parts
            .iter()
            .position(|part| part.eq_ignore_ascii_case("FROM"))?;
        let relation = parts.get(from_pos + 1)?;
        Some(
            relation
                .trim_end_matches(';')
                .trim_end_matches(',')
                .to_string(),
        )
    }

    /// Parse: CREATE [OR REPLACE] DYNAMIC TABLE <name>
    ///   TARGET_LAG = '<n> seconds|minutes|hours|days'
    ///   [REFRESH_MODE = FULL|INCREMENTAL|AUTO]
    ///   [INITIALIZE = ON_CREATE|ON_SCHEDULE]
    ///   [COMMENT = '<str>']
    ///   AS <query>
    ///
    /// Encoded as: CREATE TABLE __dt_<name>__<lag_secs>__<mode>__<init> (<query encoded>)
    fn parse_dynamic_table(&self, sql: &str) -> Result<Vec<Statement>> {
        let upper = sql.to_uppercase();
        // Find AS keyword — query starts after it
        let as_pos = upper.find(" AS ").ok_or_else(|| NovaError::SqlParseError {
            message: "DYNAMIC TABLE syntax: missing AS <query>".to_string(),
        })?;
        let query_def = sql[as_pos + 4..].trim().to_string();
        let header = &upper[..as_pos];

        // Extract name — token after DYNAMIC TABLE or REPLACE DYNAMIC TABLE
        let parts: Vec<&str> = sql[..as_pos].split_whitespace().collect();
        let dt_pos = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("TABLE"))
            .unwrap_or(0);
        let name = parts
            .get(dt_pos + 1)
            .unwrap_or(&"unknown")
            .trim_end_matches(';')
            .to_string();

        // Parse TARGET_LAG = '<n> unit'
        let lag_secs = Self::parse_target_lag(header).unwrap_or(300);

        // Parse REFRESH_MODE
        let refresh_mode = if header.contains("REFRESH_MODE = INCREMENTAL")
            || header.contains("REFRESH_MODE=INCREMENTAL")
        {
            "INCREMENTAL"
        } else if header.contains("REFRESH_MODE = FULL") || header.contains("REFRESH_MODE=FULL") {
            "FULL"
        } else {
            "AUTO"
        };

        // Parse INITIALIZE
        let init = if header.contains("INITIALIZE = ON_SCHEDULE")
            || header.contains("INITIALIZE=ON_SCHEDULE")
        {
            "ON_SCHEDULE"
        } else {
            "ON_CREATE"
        };

        // Encode as fake CREATE TABLE with metadata in name + comment
        // ponytail: switch to custom AST node when sqlparser adds extensibility
        let encoded_query = query_def
            .chars()
            .map(|c| match c {
                ' ' => '_',
                '\'' | '"' => ' ',
                other => other,
            })
            .filter(|&c| c != ' ')
            .collect::<String>();
        let fake_sql = format!(
            "CREATE TABLE __dt_{}__{}__{}__{} (__dt_query__ VARCHAR COMMENT '{}')",
            name, lag_secs, refresh_mode, init, encoded_query
        );
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: format!("dynamic table encode failed: {}", e),
        })
    }

    /// Parse TARGET_LAG = '<n> seconds|minutes|hours|days' → seconds
    fn parse_target_lag(header: &str) -> Option<u64> {
        let lag_pos = header.find("TARGET_LAG")?;
        let after = &header[lag_pos..];
        let quote_start = after.find('\'')?;
        let after_quote = &after[quote_start + 1..];
        let quote_end = after_quote.find('\'')?;
        let lag_str = after_quote[..quote_end].trim().to_uppercase();
        let parts: Vec<&str> = lag_str.splitn(2, ' ').collect();
        let n: u64 = parts.first()?.parse().ok()?;
        let multiplier = match parts.get(1).unwrap_or(&"") {
            s if s.starts_with("SECOND") => 1,
            s if s.starts_with("MINUTE") => 60,
            s if s.starts_with("HOUR") => 3600,
            s if s.starts_with("DAY") => 86400,
            _ => 60,
        };
        Some(n * multiplier)
    }

    /// Parse: ALTER DYNAMIC TABLE <name> REFRESH|SUSPEND|RESUME
    fn parse_alter_dynamic_table(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // ALTER DYNAMIC TABLE <name> <action>
        if parts.len() < 5 {
            return Err(NovaError::SqlParseError {
                message:
                    "ALTER DYNAMIC TABLE syntax: ALTER DYNAMIC TABLE <name> REFRESH|SUSPEND|RESUME"
                        .to_string(),
            });
        }
        let name = parts[3].trim_end_matches(';');
        let action = parts[4].trim_end_matches(';').to_uppercase();
        let fake_sql = format!("DROP TABLE __alter_dt__{}_{}", name, action);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: DROP DYNAMIC TABLE <name>
    fn parse_drop_dynamic_table(&self, sql: &str) -> Result<Vec<Statement>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 4 {
            return Err(NovaError::SqlParseError {
                message: "DROP DYNAMIC TABLE syntax: DROP DYNAMIC TABLE <name>".to_string(),
            });
        }
        let name = parts[3].trim_end_matches(';');
        let fake_sql = format!("DROP TABLE __drop_dt__{}", name);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: SHOW DYNAMIC TABLES [LIKE '<pattern>']
    fn parse_show_dynamic_tables(&self, sql: &str) -> Result<Vec<Statement>> {
        let upper = sql.to_uppercase();
        let pattern = if let Some(like_pos) = upper.find(" LIKE ") {
            let after = &sql[like_pos + 6..]
                .trim()
                .trim_matches(';')
                .trim()
                .to_string();
            after.trim_matches('\'').to_string()
        } else {
            String::new()
        };
        let fake_sql = format!("DROP TABLE __show_dt__{}", pattern);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
    }

    /// Parse: GRANT|REVOKE USAGE ON FUNCTION <name>(<types>) TO|FROM ROLE <role>.
    fn parse_function_grant(&self, sql: &str, revoke: bool) -> Result<Vec<Statement>> {
        let upper = sql.to_uppercase();
        let on_function_pos =
            upper
                .find(" ON FUNCTION ")
                .ok_or_else(|| NovaError::SqlParseError {
                    message: "FUNCTION grant syntax: missing ON FUNCTION".to_string(),
                })?;
        let privilege_text = sql
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .trim()
            .to_ascii_uppercase();
        if privilege_text != "USAGE" {
            return Err(NovaError::SqlParseError {
                message: "only USAGE can be granted on FUNCTION".to_string(),
            });
        }

        let role_marker = if revoke { " FROM ROLE " } else { " TO ROLE " };
        let role_marker_pos = upper
            .find(role_marker)
            .ok_or_else(|| NovaError::SqlParseError {
                message: format!(
                    "FUNCTION {} syntax: missing{}",
                    if revoke { "revoke" } else { "grant" },
                    role_marker.trim_end()
                ),
            })?;
        let function_text = sql[on_function_pos + " ON FUNCTION ".len()..role_marker_pos].trim();
        let role_name = sql[role_marker_pos + role_marker.len()..]
            .trim()
            .trim_end_matches(';')
            .trim();
        if function_text.is_empty() || role_name.is_empty() {
            return Err(NovaError::SqlParseError {
                message: "FUNCTION grant syntax requires function signature and role".to_string(),
            });
        }

        let object_name = ObjectName(vec![Ident::new(format!(
            "__fn_rbac__{}__{}",
            function_text.replace(' ', ""),
            role_name
        ))]);
        let privileges = Privileges::Actions(vec![Action::Usage]);
        let objects = GrantObjects::Tables(vec![object_name]);
        if revoke {
            Ok(vec![Statement::Revoke {
                privileges,
                objects,
                grantees: vec![Ident::new(role_name)],
                granted_by: None,
                cascade: false,
            }])
        } else {
            Ok(vec![Statement::Grant {
                privileges,
                objects,
                grantees: vec![Ident::new(role_name)],
                with_grant_option: false,
                granted_by: None,
            }])
        }
    }

    /// Parse: SHOW GRANTS ON FUNCTION <name>(<types>) or SHOW GRANTS TO ROLE <role>.
    fn parse_show_grants(&self, sql: &str) -> Result<Vec<Statement>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_uppercase();
        let encoded = if let Some(pos) = upper.find("SHOW GRANTS ON FUNCTION ") {
            let function_text = trimmed[pos + "SHOW GRANTS ON FUNCTION ".len()..].trim();
            if function_text.is_empty() {
                return Err(NovaError::SqlParseError {
                    message: "SHOW GRANTS ON FUNCTION requires a function signature".to_string(),
                });
            }
            format!("__show_fn_grants_on__{}", function_text.replace(' ', ""))
        } else if let Some(pos) = upper.find("SHOW GRANTS TO ROLE ") {
            let role_name = trimmed[pos + "SHOW GRANTS TO ROLE ".len()..].trim();
            if role_name.is_empty() {
                return Err(NovaError::SqlParseError {
                    message: "SHOW GRANTS TO ROLE requires a role name".to_string(),
                });
            }
            format!("__show_fn_grants_to__{}", role_name)
        } else {
            return Err(NovaError::SqlParseError {
                message: "SHOW GRANTS syntax: SHOW GRANTS ON FUNCTION <name>(<types>) or SHOW GRANTS TO ROLE <role>".to_string(),
            });
        };
        let statement = Statement::Drop {
            object_type: ObjectType::Table,
            if_exists: false,
            names: vec![ObjectName(vec![Ident::new(encoded)])],
            cascade: false,
            restrict: false,
            purge: false,
            temporary: false,
        };
        Ok(vec![statement])
    }

    /// Parse: SELECT ... FROM t AT(TIMESTAMP => <unix_micros>)
    /// Strips AT(TIMESTAMP => ...) and encodes timestamp via DROP TABLE __tt_<ts>__<sql>
    fn parse_time_travel(&self, sql: &str) -> Result<Vec<Statement>> {
        // Extract timestamp value from AT(TIMESTAMP => <value>)
        let at_pos =
            sql.to_uppercase()
                .find("AT(TIMESTAMP")
                .ok_or_else(|| NovaError::SqlParseError {
                    message: "missing AT(TIMESTAMP".to_string(),
                })?;
        let after_at = &sql[at_pos..];
        let ts_start = after_at
            .find("=>")
            .ok_or_else(|| NovaError::SqlParseError {
                message: "missing => in AT(TIMESTAMP => ...)".to_string(),
            })?;
        let after_arrow = &after_at[ts_start + 2..];
        let ts_end = after_arrow.find(')').unwrap_or(after_arrow.len());
        let ts_str = after_arrow[..ts_end]
            .trim()
            .trim_matches(|c: char| c == '\'' || c == '"');
        let ts: u64 = ts_str.parse().unwrap_or(0);

        // Strip AT(TIMESTAMP => ...) from SQL to get plain SELECT
        let clean_sql = sql[..at_pos].trim_end().trim_end_matches(',').to_string();
        // Encode as DROP TABLE __tt_<ts>__<clean_sql>
        // ponytail: encode timestamp + SQL in table name for analyzer detection.
        // Upgrade to custom AST when sqlparser supports AT() natively.
        let encoded = format!("__tt_{}__{}", ts, clean_sql.replace(' ', "_"));
        let fake_sql = format!("DROP TABLE {}", encoded);
        Parser::parse_sql(&GenericDialect {}, &fake_sql).map_err(|e| NovaError::SqlParseError {
            message: e.to_string(),
        })
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

    #[test]
    fn test_parse_dynamic_table_full() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse(
                "CREATE DYNAMIC TABLE dt_orders \
                 TARGET_LAG = '5 minutes' \
                 REFRESH_MODE = FULL \
                 AS SELECT id, amount FROM orders",
            )
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_dynamic_table_incremental() {
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

    #[test]
    fn test_parse_alter_dynamic_table_refresh() {
        let parser = SqlParser::new();
        let stmts = parser
            .parse("ALTER DYNAMIC TABLE dt_orders REFRESH")
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_drop_dynamic_table() {
        let parser = SqlParser::new();
        let stmts = parser.parse("DROP DYNAMIC TABLE dt_orders").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_show_dynamic_tables() {
        let parser = SqlParser::new();
        let stmts = parser.parse("SHOW DYNAMIC TABLES").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_stream_select_commit_false() {
        use crate::analyzer::{Analyzer, ResolvedStatement};
        use nova_common::StreamReadMode;

        let parser = SqlParser::new();
        let analyzer = Analyzer::new("db".to_string(), "public".to_string());
        let stmt = parser
            .parse("SELECT * FROM orders_stream WITH (COMMIT = FALSE)")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let resolved = analyzer.resolve(&stmt).unwrap();
        match resolved {
            ResolvedStatement::ReadStream {
                stream_name,
                read_mode,
                ..
            } => {
                assert_eq!(stream_name, "orders_stream");
                assert_eq!(read_mode, StreamReadMode::Preview);
            }
            other => panic!("expected ReadStream, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_stream_has_data() {
        use crate::analyzer::{Analyzer, ResolvedStatement};

        let parser = SqlParser::new();
        let analyzer = Analyzer::new("db".to_string(), "public".to_string());
        let stmt = parser
            .parse("SELECT SYSTEM$STREAM_HAS_DATA('orders_stream')")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let resolved = analyzer.resolve(&stmt).unwrap();
        match resolved {
            ResolvedStatement::SystemStreamHasData { stream_name, .. } => {
                assert_eq!(stream_name, "orders_stream");
            }
            other => panic!("expected SystemStreamHasData, got {other:?}"),
        }
    }

    #[test]
    fn parse_stream_lifecycle_sql() {
        use crate::analyzer::{Analyzer, ResolvedStatement};

        let parser = SqlParser::new();
        let analyzer = Analyzer::new("db".to_string(), "public".to_string());

        let drop_stmt = parser
            .parse("DROP STREAM orders_stream")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        match analyzer.resolve(&drop_stmt).unwrap() {
            ResolvedStatement::DropStream { name, .. } => assert_eq!(name, "orders_stream"),
            other => panic!("expected DropStream, got {other:?}"),
        }

        let show_stmt = parser
            .parse("SHOW STREAMS LIKE 'orders%'")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        match analyzer.resolve(&show_stmt).unwrap() {
            ResolvedStatement::ShowStreams { pattern, .. } => {
                assert_eq!(pattern.as_deref(), Some("orders%"));
            }
            other => panic!("expected ShowStreams, got {other:?}"),
        }

        let desc_stmt = parser
            .parse("DESCRIBE STREAM orders_stream")
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        match analyzer.resolve(&desc_stmt).unwrap() {
            ResolvedStatement::DescribeStream { name, .. } => assert_eq!(name, "orders_stream"),
            other => panic!("expected DescribeStream, got {other:?}"),
        }
    }

    #[test]
    fn create_stream_rejects_append_only() {
        let parser = SqlParser::new();
        let err = parser
            .parse("CREATE STREAM orders_stream ON TABLE orders APPEND ONLY")
            .expect_err("append-only streams are not supported");
        assert!(matches!(err, NovaError::UnsupportedStreamSyntax { .. }));
    }
}
