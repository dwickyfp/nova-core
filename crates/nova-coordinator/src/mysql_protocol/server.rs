// MySQL Protocol Server
//
// Production-grade MySQL wire protocol server. Accepts connections from
// standard MySQL clients (mysql CLI, DBeaver, DataGrip, etc.)

use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use nova_common::{NovaError, Result};

use crate::mysql_protocol::auth::{AuthPlugin, verify_mysql_native_password};
use crate::mysql_protocol::capabilities::ClientCapabilities;
use crate::mysql_protocol::codec::PacketCodec;
use crate::mysql_protocol::commands::{
    ColumnDef, Command, CommandResult, build_column_count_packet, build_column_def_packet,
    build_eof_packet, build_error_packet, build_ok_packet, build_row_packet, handle_init_db,
    handle_ping, handle_quit, handle_reset_connection, handle_set_option, handle_stmt_close,
    handle_stmt_prepare,
};
use crate::mysql_protocol::connection::Session;
use crate::mysql_protocol::errors::MySqlError;
use crate::mysql_protocol::packets::{build_handshake_packet, parse_handshake_response};
use crate::mysql_protocol::query_engine::QueryEngine;
use crate::mysql_protocol::types::ColumnType;

/// MySQL protocol server
pub struct MySqlServer {
    listener: TcpListener,
    engine: Arc<dyn QueryEngine>,
    auth: Option<Arc<crate::auth::AuthManager>>,
    sessions: Arc<Mutex<Vec<u32>>>,
    next_connection_id: Arc<Mutex<u32>>,
}

/// Server configuration
pub struct MySqlServerConfig {
    pub host: String,
    pub port: u16,
    pub auth_plugin: AuthPlugin,
    pub max_connections: usize,
}

impl Default for MySqlServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3306,
            auth_plugin: AuthPlugin::MysqlNativePassword,
            max_connections: 100,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RoleCommand<'a> {
    UseRole(&'a str),
    UseSecondaryAll,
    UseSecondaryNone,
}

pub fn parse_role_command(sql: &str) -> Option<RoleCommand<'_>> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let mut parts = sql.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("use") {
        return None;
    }
    let second = parts.next()?;
    if second.eq_ignore_ascii_case("role") {
        let role = sql[sql.to_ascii_lowercase().find("role")? + 4..]
            .trim()
            .trim_matches('`')
            .trim_matches('"');
        return (!role.is_empty()).then_some(RoleCommand::UseRole(role));
    }
    if second.eq_ignore_ascii_case("secondary") && parts.next()?.eq_ignore_ascii_case("roles") {
        return match parts.next()? {
            word if word.eq_ignore_ascii_case("all") => Some(RoleCommand::UseSecondaryAll),
            word if word.eq_ignore_ascii_case("none") => Some(RoleCommand::UseSecondaryNone),
            _ => None,
        };
    }
    None
}

impl MySqlServer {
    /// Bind to address and create server.
    /// If auth is provided, MySQL handshake password verification is enforced.
    pub async fn bind(
        addr: &str,
        engine: Arc<dyn QueryEngine>,
        auth: Option<Arc<crate::auth::AuthManager>>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("Failed to bind to {}: {}", addr, e),
            })?;

        Ok(Self {
            listener,
            engine,
            auth,
            sessions: Arc::new(Mutex::new(Vec::new())),
            next_connection_id: Arc::new(Mutex::new(1)),
        })
    }

    /// Start accepting connections
    pub async fn run(&self) -> Result<()> {
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let mut id_guard = self.next_connection_id.lock().await;
                    let connection_id = *id_guard;
                    *id_guard += 1;
                    drop(id_guard);

                    let sessions = Arc::clone(&self.sessions);
                    let engine = Arc::clone(&self.engine);
                    let auth = self.auth.clone();

                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_connection(stream, connection_id, sessions, engine, auth).await
                        {
                            tracing::error!(
                                connection_id = connection_id,
                                addr = %addr,
                                "Connection error: {:?}",
                                e
                            );
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Accept error: {}", e);
                }
            }
        }
    }
}

/// Handle a single client connection
async fn handle_connection(
    stream: TcpStream,
    connection_id: u32,
    sessions: Arc<Mutex<Vec<u32>>>,
    engine: Arc<dyn QueryEngine>,
    auth: Option<Arc<crate::auth::AuthManager>>,
) -> Result<()> {
    let mut codec = PacketCodec::new(stream);

    // Generate scramble for authentication
    let scramble = crate::mysql_protocol::auth::AuthContext::generate_scramble();

    // Send handshake packet (Protocol::HandshakeV10)
    let handshake =
        build_handshake_packet(connection_id, AuthPlugin::MysqlNativePassword, &scramble);
    codec.write_packet(&handshake).await?;

    // Read handshake response from client
    let response_packet = codec.read_packet().await?;
    let handshake_response =
        parse_handshake_response(&response_packet.payload).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to parse handshake response, using defaults");
            crate::mysql_protocol::packets::HandshakeResponse {
                capabilities: ClientCapabilities::default_server_capabilities(),
                max_packet_size: 16777216,
                charset: 45,
                username: "root".to_string(),
                auth_response: Vec::new(),
                database: None,
                auth_plugin: None,
                connect_attrs: Vec::new(),
            }
        });

    // Create session
    let mut session = Session::new(
        handshake_response.username.clone(),
        AuthPlugin::MysqlNativePassword,
        scramble.clone(),
    );
    session.client_capabilities = handshake_response.capabilities;

    if let Some(db) = &handshake_response.database {
        session.set_database(db);
    }

    // Store connect attributes
    for (key, value) in &handshake_response.connect_attrs {
        session.connect_attrs.insert(key.clone(), value.clone());
    }

    // Add session to tracking list
    {
        let mut sessions_guard = sessions.lock().await;
        sessions_guard.push(connection_id);
    }

    // Authentication: verify password against FDB-backed user metadata when enabled.
    // When auth is None (dev mode), accept as root.
    if auth.is_some() {
        let username = &handshake_response.username;
        let auth_response = &handshake_response.auth_response;
        let user = match engine.user_for_auth(username).await {
            Ok(Some(user)) if !user.disabled => user,
            Ok(Some(_)) => {
                let err = build_error_packet(
                    MySqlError::ER_ACCESS_DENIED_ERROR,
                    &format!("Access denied for user '{}'@'{}'", username, "unknown"),
                );
                codec.write_packet(&err.payload).await?;
                return Ok(());
            }
            _ => {
                let err = build_error_packet(
                    MySqlError::ER_ACCESS_DENIED_ERROR,
                    &format!(
                        "Access denied for user '{}'@'{}' (using password: YES)",
                        username, "unknown"
                    ),
                );
                codec.write_packet(&err.payload).await?;
                return Ok(());
            }
        };

        if !user.mysql_native_hash.is_empty()
            && !verify_mysql_native_password(&scramble, &user.mysql_native_hash, auth_response)
        {
            let err = build_error_packet(
                MySqlError::ER_ACCESS_DENIED_ERROR,
                &format!(
                    "Access denied for user '{}'@'{}' (using password: YES)",
                    username, "unknown"
                ),
            );
            codec.write_packet(&err.payload).await?;
            return Ok(());
        }

        match engine.security_context_for_user(&user.name).await {
            Ok(security) => session.security = security,
            Err(e) => {
                let err = build_error_packet(MySqlError::ER_ACCESS_DENIED_ERROR, &e.to_string());
                codec.write_packet(&err.payload).await?;
                return Ok(());
            }
        }
        let ok_packet = build_ok_packet(0, 0, session.server_status, "");
        codec.write_packet(&ok_packet.payload).await?;
    } else {
        session.security = nova_common::SecurityContext::root();
        let ok_packet = build_ok_packet(0, 0, session.server_status, "");
        codec.write_packet(&ok_packet.payload).await?;
    }

    tracing::info!(
        connection_id = connection_id,
        username = %session.username,
        database = %session.current_db,
        "Client authenticated"
    );

    // Command loop
    loop {
        let packet = match codec.read_packet().await {
            Ok(p) => p,
            Err(_) => break, // Connection closed
        };

        if packet.payload.is_empty() {
            break;
        }

        let command_byte = packet.payload[0];
        let command = Command::from_u8(command_byte);
        let payload = &packet.payload[1..];

        tracing::debug!(
            connection_id = connection_id,
            command = command.name(),
            "Received command"
        );

        let result = match command {
            Command::ComQuit => handle_quit(&mut session),
            Command::ComPing => handle_ping(&mut session),
            Command::ComInitDb => handle_init_db(&mut session, payload),
            Command::ComQuery => {
                // When CLIENT_QUERY_ATTRIBUTES is set, COM_QUERY payload has:
                //   [flags:1] [iteration_count:1] [param_count: lenenc_int] [params...] [SQL]
                // Some clients (C extension) omit param_count when 0 params:
                //   [flags:1] [iteration_count:1] [SQL]
                let sql = if session.client_capabilities.supports_query_attributes()
                    && payload.len() >= 2
                {
                    let after_hdr = &payload[2..];
                    // Try to skip param_count if it looks like a lenenc int (0x00 = 0 params)
                    if !after_hdr.is_empty() && after_hdr[0] == 0x00 {
                        &after_hdr[1..]
                    } else {
                        after_hdr
                    }
                } else {
                    payload
                };

                let sql = String::from_utf8_lossy(sql).to_string();
                let sql = sql.trim_end_matches('\0').trim();
                tracing::debug!(sql = %sql, "Query received");

                // Handle special commands first
                let sql_lower = sql.to_lowercase();
                if let Some(role_command) = parse_role_command(sql) {
                    match role_command {
                        RoleCommand::UseRole(role) => match engine.role_id_by_name(role).await {
                            Ok(Some(role_id))
                                if session.security.active_role_ids().contains(&role_id) =>
                            {
                                session.security.primary_role_id = role_id;
                                CommandResult::Ok {
                                    affected_rows: 0,
                                    last_insert_id: 0,
                                    message: String::new(),
                                }
                            }
                            Ok(Some(_)) => CommandResult::Error {
                                code: MySqlError::ER_ACCESS_DENIED_ERROR,
                                message: format!("role '{}' is not granted to current user", role),
                            },
                            Ok(None) => CommandResult::Error {
                                code: MySqlError::ER_PARSE_ERROR,
                                message: format!("role '{}' not found", role),
                            },
                            Err(e) => CommandResult::Error {
                                code: MySqlError::ER_PARSE_ERROR,
                                message: e.to_string(),
                            },
                        },
                        RoleCommand::UseSecondaryAll => {
                            session.security.secondary_all = true;
                            CommandResult::Ok {
                                affected_rows: 0,
                                last_insert_id: 0,
                                message: String::new(),
                            }
                        }
                        RoleCommand::UseSecondaryNone => {
                            session.security.secondary_all = false;
                            CommandResult::Ok {
                                affected_rows: 0,
                                last_insert_id: 0,
                                message: String::new(),
                            }
                        }
                    }
                } else if sql_lower.starts_with("use ") {
                    let db = sql[4..].trim().trim_end_matches(';').to_string();
                    session.set_database(&db);
                    CommandResult::Ok {
                        affected_rows: 0,
                        last_insert_id: 0,
                        message: String::new(),
                    }
                } else if sql_lower.starts_with("show databases")
                    || sql_lower.starts_with("show schemas")
                {
                    let db_names = engine
                        .list_databases(&session.security)
                        .await
                        .unwrap_or_default();
                    let mut rows: Vec<Vec<Option<String>>> =
                        db_names.into_iter().map(|n| vec![Some(n)]).collect();
                    // Always include system databases
                    for sys_db in ["information_schema", "mysql", "performance_schema", "sys"] {
                        if !rows.iter().any(|r| r[0].as_deref() == Some(sys_db)) {
                            rows.push(vec![Some(sys_db.to_string())]);
                        }
                    }
                    CommandResult::ResultSet {
                        columns: vec![ColumnDef {
                            name: "Database".to_string(),
                            col_type: ColumnType::VarString,
                            flags: 0,
                            decimals: 0,
                        }],
                        rows,
                    }
                } else if sql_lower.starts_with("select @@version")
                    || sql_lower.starts_with("select version()")
                {
                    CommandResult::ResultSet {
                        columns: vec![ColumnDef {
                            name: "@@version".to_string(),
                            col_type: ColumnType::VarString,
                            flags: 0,
                            decimals: 0,
                        }],
                        rows: vec![vec![Some("8.0.35-nova".to_string())]],
                    }
                } else if sql_lower.starts_with("set ") {
                    // Handle SET commands (e.g., SET NAMES utf8mb4, SET autocommit=1)
                    CommandResult::Ok {
                        affected_rows: 0,
                        last_insert_id: 0,
                        message: String::new(),
                    }
                } else if sql_lower.starts_with("show warnings")
                    || sql_lower.starts_with("show status")
                {
                    CommandResult::ResultSet {
                        columns: vec![ColumnDef {
                            name: "Level".to_string(),
                            col_type: ColumnType::VarString,
                            flags: 0,
                            decimals: 0,
                        }],
                        rows: vec![],
                    }
                } else if sql_lower.starts_with("show tables") {
                    let current_db = if session.current_db.is_empty() {
                        "nova"
                    } else {
                        &session.current_db
                    };
                    let table_names = engine
                        .list_tables(current_db, &session.security)
                        .await
                        .unwrap_or_default();
                    let col_name = format!("Tables_in_{}", session.current_db);
                    let rows = table_names.into_iter().map(|n| vec![Some(n)]).collect();
                    CommandResult::ResultSet {
                        columns: vec![ColumnDef {
                            name: col_name,
                            col_type: ColumnType::VarString,
                            flags: 0,
                            decimals: 0,
                        }],
                        rows,
                    }
                } else if sql_lower.starts_with("select 1") && !sql_lower.contains("from") {
                    CommandResult::ResultSet {
                        columns: vec![ColumnDef {
                            name: "1".to_string(),
                            col_type: ColumnType::Long,
                            flags: 0,
                            decimals: 0,
                        }],
                        rows: vec![vec![Some("1".to_string())]],
                    }
                } else {
                    // Execute via query engine
                    let current_db = if session.current_db.is_empty() {
                        "nova"
                    } else {
                        &session.current_db
                    };

                    match engine.execute_sql(sql, current_db, &session.security).await {
                        Ok(result) => {
                            use crate::executor::QueryResult;
                            match result {
                                QueryResult::Success { message } => CommandResult::Ok {
                                    affected_rows: 0,
                                    last_insert_id: 0,
                                    message,
                                },
                                QueryResult::Rows { columns, rows } => CommandResult::ResultSet {
                                    columns: columns
                                        .into_iter()
                                        .map(|name| ColumnDef {
                                            name,
                                            col_type: ColumnType::VarString,
                                            flags: 0,
                                            decimals: 0,
                                        })
                                        .collect(),
                                    rows: rows
                                        .into_iter()
                                        .map(|r| r.into_iter().map(Some).collect())
                                        .collect(),
                                },
                            }
                        }
                        Err(e) => CommandResult::Error {
                            code: MySqlError::ER_PARSE_ERROR,
                            message: e.to_string(),
                        },
                    }
                }
            }
            Command::ComStmtPrepare => handle_stmt_prepare(&mut session, payload),
            Command::ComStmtExecute => {
                if payload.len() < 4 {
                    CommandResult::Error {
                        code: MySqlError::ER_SYNTAX_ERROR,
                        message: "Invalid COM_STMT_EXECUTE payload".to_string(),
                    }
                } else {
                    let stmt_id =
                        u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    match session.get_prepared_statement(stmt_id) {
                        None => CommandResult::Error {
                            code: MySqlError::ER_UNKNOWN_STMT_HANDLER,
                            message: format!(
                                "Unknown prepared statement handler ({}) given",
                                stmt_id
                            ),
                        },
                        Some(stmt) if stmt.num_params != 0 => CommandResult::Error {
                            code: MySqlError::ER_NOT_SUPPORTED_YET,
                            message: "Prepared statement parameters are not yet implemented"
                                .to_string(),
                        },
                        Some(stmt) => {
                            let sql = stmt.sql.clone();
                            let current_db = if session.current_db.is_empty() {
                                "nova"
                            } else {
                                &session.current_db
                            };
                            match engine
                                .execute_sql(&sql, current_db, &session.security)
                                .await
                            {
                                Ok(result) => {
                                    use crate::executor::QueryResult;
                                    match result {
                                        QueryResult::Success { message } => CommandResult::Ok {
                                            affected_rows: 0,
                                            last_insert_id: 0,
                                            message,
                                        },
                                        QueryResult::Rows { columns, rows } => {
                                            CommandResult::ResultSet {
                                                columns: columns
                                                    .into_iter()
                                                    .map(|name| ColumnDef {
                                                        name,
                                                        col_type: ColumnType::VarString,
                                                        flags: 0,
                                                        decimals: 0,
                                                    })
                                                    .collect(),
                                                rows: rows
                                                    .into_iter()
                                                    .map(|r| r.into_iter().map(Some).collect())
                                                    .collect(),
                                            }
                                        }
                                    }
                                }
                                Err(e) => CommandResult::Error {
                                    code: MySqlError::ER_PARSE_ERROR,
                                    message: e.to_string(),
                                },
                            }
                        }
                    }
                }
            }
            Command::ComStmtClose => {
                // COM_STMT_CLOSE is silent — no response, just remove statement
                handle_stmt_close(&mut session, payload);
                CommandResult::Eof
            }
            Command::ComSetOption => handle_set_option(&mut session, payload),
            Command::ComResetConnection => handle_reset_connection(&mut session),
            _ => CommandResult::Error {
                code: MySqlError::ER_UNKNOWN_COM_ERROR,
                message: format!(
                    "Unsupported command: {} (0x{:02x})",
                    command.name(),
                    command_byte
                ),
            },
        };

        // Send response based on CommandResult
        match result {
            CommandResult::Ok {
                affected_rows,
                last_insert_id,
                message,
            } => {
                let ok = build_ok_packet(
                    affected_rows,
                    last_insert_id,
                    session.server_status,
                    &message,
                );
                codec.write_packet(&ok.payload).await?;
            }
            CommandResult::Error { code, message } => {
                let err = build_error_packet(code, &message);
                codec.write_packet(&err.payload).await?;
            }
            CommandResult::ResultSet { columns, rows } => {
                // Send column count
                let col_count = build_column_count_packet(columns.len() as u64);
                codec.write_packet(&col_count.payload).await?;

                // Send column definitions
                for col in &columns {
                    let col_def = build_column_def_packet(col);
                    codec.write_packet(&col_def.payload).await?;
                }

                // Send EOF after columns (if not CLIENT_DEPRECATE_EOF)
                if !session.client_capabilities.supports_deprecate_eof() {
                    let eof = build_eof_packet(session.server_status, session.warning_count);
                    codec.write_packet(&eof.payload).await?;
                }

                // Send rows
                for row in &rows {
                    let row_packet = build_row_packet(row);
                    codec.write_packet(&row_packet.payload).await?;
                }

                // Send final EOF or OK
                if session.client_capabilities.supports_deprecate_eof() {
                    let ok = build_ok_packet(0, 0, session.server_status, "");
                    codec.write_packet(&ok.payload).await?;
                } else {
                    let eof = build_eof_packet(session.server_status, session.warning_count);
                    codec.write_packet(&eof.payload).await?;
                }
            }
            CommandResult::Eof => {
                // For COM_STMT_CLOSE: send nothing (silent command)
                // For other EOF: send EOF packet
                if command != Command::ComStmtClose {
                    let eof = build_eof_packet(session.server_status, session.warning_count);
                    codec.write_packet(&eof.payload).await?;
                }
            }
            CommandResult::Close => {
                break;
            }
        }

        // Reset sequence for next command
        codec.reset_seq();

        if command == Command::ComQuit {
            break;
        }
    }

    // Remove session from tracking list
    {
        let mut sessions_guard = sessions.lock().await;
        sessions_guard.retain(|&id| id != connection_id);
    }

    session.close();
    tracing::info!(connection_id = connection_id, "Connection closed");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = MySqlServerConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 3306);
        assert_eq!(config.max_connections, 100);
    }
}
