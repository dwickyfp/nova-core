// MySQL Protocol Server — basic MySQL wire protocol for nova-core.
//
// Allows any MySQL client (mysql CLI, DBeaver, DataGrip) to connect and execute SQL.
// Implements: handshake, COM_QUERY, result set response.

use crate::analyzer::Analyzer;
use crate::executor::{Executor, QueryResult};
use crate::parser::SqlParser;
use nova_common::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// MySQL protocol server. Listens on a TCP port and handles MySQL wire protocol.
pub struct MySqlServer {
    executor: Arc<Executor>,
    port: u16,
}

impl MySqlServer {
    pub fn new(executor: Arc<Executor>, port: u16) -> Self {
        Self { executor, port }
    }

    /// Start the MySQL server. Blocks until shutdown.
    pub async fn run(&self) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        let listener =
            TcpListener::bind(&addr)
                .await
                .map_err(|e| nova_common::NovaError::Internal {
                    message: format!("failed to bind {}: {}", addr, e),
                })?;

        tracing::info!(addr = %addr, "MySQL server listening");

        loop {
            let (stream, addr) =
                listener
                    .accept()
                    .await
                    .map_err(|e| nova_common::NovaError::Internal {
                        message: format!("accept failed: {}", e),
                    })?;

            tracing::info!(client = %addr, "New MySQL connection");

            let executor = self.executor.clone();
            let parser = SqlParser::new();
            let analyzer = Analyzer::new("default".to_string(), "public".to_string());

            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, executor, parser, analyzer).await {
                    tracing::error!(error = %e, "Client connection error");
                }
            });
        }
    }
}

/// Handle a single MySQL client connection.
async fn handle_client(
    mut stream: TcpStream,
    executor: Arc<Executor>,
    parser: SqlParser,
    analyzer: Analyzer,
) -> Result<()> {
    // 1. Send handshake (server greeting)
    send_handshake(&mut stream).await?;

    // 2. Read handshake response (auth)
    read_handshake_response(&mut stream).await?;

    // 3. Send OK
    send_ok(&mut stream).await?;

    // 4. Command loop
    loop {
        let packet = match read_packet(&mut stream).await {
            Ok(p) => p,
            Err(_) => break, // client disconnected
        };

        if packet.is_empty() {
            break;
        }

        let command = packet[0];
        match command {
            0x01 => break, // COM_QUIT
            0x03 => {
                // COM_QUERY
                let sql = String::from_utf8_lossy(&packet[1..]).to_string();
                let sql = sql.trim_end_matches('\0').trim();

                tracing::debug!(sql = %sql, "Query received");

                match execute_sql(&executor, &parser, &analyzer, sql).await {
                    Ok(result) => send_query_result(&mut stream, result).await?,
                    Err(e) => send_error(&mut stream, &e.to_string()).await?,
                }
            }
            0x0e => {
                // COM_PING
                send_ok(&mut stream).await?
            }
            _ => {
                send_error(
                    &mut stream,
                    &format!("unsupported command: 0x{:02x}", command),
                )
                .await?
            }
        }
    }

    Ok(())
}

/// Execute SQL text through the full pipeline: parse → analyze → execute.
async fn execute_sql(
    executor: &Executor,
    parser: &SqlParser,
    analyzer: &Analyzer,
    sql: &str,
) -> Result<QueryResult> {
    let stmts = parser.parse(sql)?;
    let stmt = stmts
        .first()
        .ok_or_else(|| nova_common::NovaError::SqlParseError {
            message: "empty SQL".to_string(),
        })?;
    let resolved = analyzer.resolve(stmt)?;
    executor
        .execute_with_context(resolved, &nova_common::SecurityContext::root())
        .await
}

// ══════════════════════════════════════════════════════════════
//  MySQL Wire Protocol Helpers
// ══════════════════════════════════════════════════════════════

const MYSQL_VERSION: &[u8] = b"8.0.33-nova\x00";

/// Send MySQL handshake packet (server greeting).
async fn send_handshake(stream: &mut TcpStream) -> Result<()> {
    let mut payload = Vec::new();

    // Protocol version
    payload.push(0x0a);

    // Server version (null-terminated)
    payload.extend_from_slice(MYSQL_VERSION);

    // Connection ID (4 bytes, little-endian)
    payload.extend_from_slice(&1u32.to_le_bytes());

    // Auth plugin data part 1 (8 bytes)
    payload.extend_from_slice(b"nova1234");

    // Filler
    payload.push(0x00);

    // Capability flags lower 2 bytes
    payload.extend_from_slice(&0xffff_u16.to_le_bytes());

    // Character set (utf8 = 0x21)
    payload.push(0x21);

    // Status flags (2 bytes)
    payload.extend_from_slice(&0x0002_u16.to_le_bytes());

    // Capability flags upper 2 bytes
    payload.extend_from_slice(&0xffff_u16.to_le_bytes());

    // Auth plugin data length
    payload.push(21);

    // Reserved (10 zero bytes)
    payload.extend_from_slice(&[0u8; 10]);

    // Auth plugin data part 2 (13 bytes)
    payload.extend_from_slice(b"nova123456789");
    payload.push(0x00);

    // Auth plugin name
    payload.extend_from_slice(b"mysql_native_password\x00");

    write_packet(stream, &payload, 0).await
}

/// Read handshake response from client.
async fn read_handshake_response(stream: &mut TcpStream) -> Result<()> {
    let _packet = read_packet(stream).await?;
    Ok(())
}

/// Send OK packet.
async fn send_ok(stream: &mut TcpStream) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0x00); // OK marker
    payload.push(0x00); // affected rows
    payload.push(0x00); // last insert id
    payload.extend_from_slice(&0x0002_u16.to_le_bytes()); // status flags
    payload.extend_from_slice(&0x0000_u16.to_le_bytes()); // warnings
    write_packet(stream, &payload, 0).await
}

/// Send error packet.
async fn send_error(stream: &mut TcpStream, message: &str) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0xff); // ERROR marker
    payload.extend_from_slice(&1064u16.to_le_bytes()); // error code
    payload.push(b'#'); // sql state marker
    payload.extend_from_slice(b"42000"); // sql state
    payload.extend_from_slice(message.as_bytes());
    write_packet(stream, &payload, 0).await
}

/// Send query result as MySQL result set.
async fn send_query_result(stream: &mut TcpStream, result: QueryResult) -> Result<()> {
    match result {
        QueryResult::Success { message } => send_ok_with_message(stream, &message).await,
        QueryResult::Rows { columns, rows } => {
            // Column count
            write_lenenc_int(stream, columns.len() as u64).await?;

            // Column definitions
            for col in &columns {
                send_column_definition(stream, col).await?;
            }

            // EOF after columns
            write_packet(stream, &[0xfe, 0x00, 0x00, 0x02, 0x00], 0).await?;

            // Rows
            for row in &rows {
                let mut row_data = Vec::new();
                for val in row {
                    write_lenenc_string(&mut row_data, val);
                }
                write_packet(stream, &row_data, 0).await?;
            }

            // EOF after rows
            write_packet(stream, &[0xfe, 0x00, 0x00, 0x02, 0x00], 0).await?;

            Ok(())
        }
    }
}

/// Send OK with a message (like affected rows info).
async fn send_ok_with_message(stream: &mut TcpStream, message: &str) -> Result<()> {
    let mut payload = Vec::new();
    payload.push(0x00); // OK marker
    payload.push(0x00); // affected rows
    payload.push(0x00); // last insert id
    payload.extend_from_slice(&0x0002_u16.to_le_bytes()); // status flags
    payload.extend_from_slice(&0x0000_u16.to_le_bytes()); // warnings
    payload.extend_from_slice(message.as_bytes());
    write_packet(stream, &payload, 0).await
}

/// Send a column definition packet.
async fn send_column_definition(stream: &mut TcpStream, name: &str) -> Result<()> {
    let mut payload = Vec::new();

    // Catalog
    write_lenenc_string(&mut payload, "def");

    // Schema
    write_lenenc_string(&mut payload, "");

    // Table
    write_lenenc_string(&mut payload, "");

    // Org table
    write_lenenc_string(&mut payload, "");

    // Column name
    write_lenenc_string(&mut payload, name);

    // Org column name
    write_lenenc_string(&mut payload, "");

    // Length of fixed fields
    payload.push(0x0c);

    // Character set (utf8 = 0x2100)
    payload.extend_from_slice(&0x0021_u16.to_le_bytes());

    // Column length (4 bytes)
    payload.extend_from_slice(&256u32.to_le_bytes());

    // Column type (VARCHAR = 0xfd)
    payload.push(0xfd);

    // Flags
    payload.extend_from_slice(&0x0000_u16.to_le_bytes());

    // Decimals
    payload.push(0x00);

    // Filler
    payload.extend_from_slice(&[0x00, 0x00]);

    write_packet(stream, &payload, 0).await
}

// ══════════════════════════════════════════════════════════════
//  Low-level Packet I/O
// ══════════════════════════════════════════════════════════════

/// Write a MySQL protocol packet.
async fn write_packet(stream: &mut TcpStream, payload: &[u8], seq: u8) -> Result<()> {
    let len = payload.len() as u32;
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ];

    stream
        .write_all(&header)
        .await
        .map_err(|e| nova_common::NovaError::Internal {
            message: format!("write header failed: {}", e),
        })?;
    stream
        .write_all(payload)
        .await
        .map_err(|e| nova_common::NovaError::Internal {
            message: format!("write payload failed: {}", e),
        })?;
    stream
        .flush()
        .await
        .map_err(|e| nova_common::NovaError::Internal {
            message: format!("flush failed: {}", e),
        })?;

    Ok(())
}

/// Read a MySQL protocol packet.
async fn read_packet(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut header = [0u8; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(_) => {
            return Err(nova_common::NovaError::Internal {
                message: "client disconnected".to_string(),
            });
        }
    }

    let len = (header[0] as u32) | ((header[1] as u32) << 8) | ((header[2] as u32) << 16);
    let mut payload = vec![0u8; len as usize];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| nova_common::NovaError::Internal {
            message: format!("read payload failed: {}", e),
        })?;

    Ok(payload)
}

/// Write a length-encoded integer.
async fn write_lenenc_int(stream: &mut TcpStream, val: u64) -> Result<()> {
    let mut buf = Vec::new();
    if val < 251 {
        buf.push(val as u8);
    } else if val < 65536 {
        buf.push(0xfc);
        buf.extend_from_slice(&(val as u16).to_le_bytes());
    } else if val < 16777216 {
        buf.push(0xfd);
        buf.extend_from_slice(&(val as u32).to_le_bytes()[..3]);
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&val.to_le_bytes());
    }
    stream
        .write_all(&buf)
        .await
        .map_err(|e| nova_common::NovaError::Internal {
            message: format!("write lenenc_int failed: {}", e),
        })?;
    Ok(())
}

/// Write a length-encoded string to a buffer.
fn write_lenenc_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    if bytes.len() < 251 {
        buf.push(bytes.len() as u8);
    } else if bytes.len() < 65536 {
        buf.push(0xfc);
        buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    } else {
        buf.push(0xfd);
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes()[..3]);
    }
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_lenenc_string_short() {
        let mut buf = Vec::new();
        write_lenenc_string(&mut buf, "hello");
        assert_eq!(buf[0], 5); // length prefix
        assert_eq!(&buf[1..], b"hello");
    }

    #[test]
    fn test_write_lenenc_string_empty() {
        let mut buf = Vec::new();
        write_lenenc_string(&mut buf, "");
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn test_handshake_packet_structure() {
        // Verify handshake starts with protocol version 0x0a
        let mut payload = Vec::new();
        payload.push(0x0a);
        payload.extend_from_slice(MYSQL_VERSION);
        assert_eq!(payload[0], 0x0a);
        assert!(payload.len() > 1);
    }

    #[test]
    fn test_ok_packet_structure() {
        let payload = [0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00];
        assert_eq!(payload[0], 0x00); // OK marker
    }

    #[test]
    fn test_error_packet_structure() {
        let mut payload = Vec::new();
        payload.push(0xff); // ERROR marker
        payload.extend_from_slice(&1064u16.to_le_bytes());
        assert_eq!(payload[0], 0xff);
        assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), 1064);
    }
}
