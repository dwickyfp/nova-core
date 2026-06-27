// MySQL Protocol Server
//
// Production-grade MySQL wire protocol server. Accepts connections from
// standard MySQL clients (mysql CLI, DBeaver, DataGrip, etc.)

use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use nova_common::{NovaError, Result};

use crate::mysql_protocol::auth::AuthPlugin;
use crate::mysql_protocol::codec::PacketCodec;
use crate::mysql_protocol::commands::{
    Command, CommandResult, build_column_count_packet, build_column_def_packet, build_eof_packet,
    build_error_packet, build_ok_packet, build_row_packet, handle_init_db, handle_ping,
    handle_query, handle_quit, handle_reset_connection, handle_set_option, handle_stmt_close,
    handle_stmt_execute, handle_stmt_prepare,
};
use crate::mysql_protocol::connection::Session;
use crate::mysql_protocol::errors::MySqlError;
use crate::mysql_protocol::packets::{build_handshake_packet, parse_handshake_response};

/// MySQL protocol server
pub struct MySqlServer {
    listener: TcpListener,
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

impl MySqlServer {
    /// Bind to address and create server
    pub async fn bind(addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("Failed to bind to {}: {}", addr, e),
            })?;

        Ok(Self {
            listener,
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

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, connection_id, sessions).await {
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
    let handshake_response = parse_handshake_response(&response_packet.payload)?;

    // Create session
    let mut session = Session::new(
        handshake_response.username.clone(),
        AuthPlugin::MysqlNativePassword,
        scramble,
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

    // Send OK packet (auth success)
    let ok_packet = build_ok_packet(0, 0, session.server_status, "");
    codec.write_packet(&ok_packet.payload).await?;

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
            Command::ComQuery => handle_query(&mut session, payload),
            Command::ComStmtPrepare => handle_stmt_prepare(&mut session, payload),
            Command::ComStmtExecute => handle_stmt_execute(&mut session, payload),
            Command::ComStmtClose => handle_stmt_close(&mut session, payload),
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
                let eof = build_eof_packet(session.server_status, session.warning_count);
                codec.write_packet(&eof.payload).await?;
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
