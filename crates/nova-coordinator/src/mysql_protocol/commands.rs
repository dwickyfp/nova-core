// MySQL command handlers
//
// Implements all MySQL COM_* commands

use bytes::{Buf, BufMut, BytesMut};

use crate::mysql_protocol::codec::Packet;
use crate::mysql_protocol::connection::Session;
use crate::mysql_protocol::errors::MySqlError;
use crate::mysql_protocol::types::ColumnType;

/// MySQL command codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Command {
    ComSleep = 0x00,
    ComQuit = 0x01,
    ComInitDb = 0x02,
    ComQuery = 0x03,
    ComFieldList = 0x04,
    ComCreateDb = 0x05,
    ComDropDb = 0x06,
    ComRefresh = 0x07,
    ComShutdown = 0x08,
    ComStatistics = 0x09,
    ComProcessInfo = 0x0a,
    ComConnect = 0x0b,
    ComProcessKill = 0x0c,
    ComDebug = 0x0d,
    ComPing = 0x0e,
    ComTime = 0x0f,
    ComDelayedInsert = 0x10,
    ComChangeUser = 0x11,
    ComBinlogDump = 0x12,
    ComTableDump = 0x13,
    ComConnectOut = 0x14,
    ComRegisterSlave = 0x15,
    ComStmtPrepare = 0x16,
    ComStmtExecute = 0x17,
    ComStmtSendLongData = 0x18,
    ComStmtClose = 0x19,
    ComStmtReset = 0x1a,
    ComSetOption = 0x1b,
    ComStmtFetch = 0x1c,
    ComDaemon = 0x1d,
    ComBinlogDumpGtid = 0x1e,
    ComResetConnection = 0x1f,
    ComUnknown = 0xff,
}

impl Command {
    pub fn from_u8(val: u8) -> Self {
        match val {
            0x00 => Self::ComSleep,
            0x01 => Self::ComQuit,
            0x02 => Self::ComInitDb,
            0x03 => Self::ComQuery,
            0x04 => Self::ComFieldList,
            0x05 => Self::ComCreateDb,
            0x06 => Self::ComDropDb,
            0x07 => Self::ComRefresh,
            0x08 => Self::ComShutdown,
            0x09 => Self::ComStatistics,
            0x0a => Self::ComProcessInfo,
            0x0b => Self::ComConnect,
            0x0c => Self::ComProcessKill,
            0x0d => Self::ComDebug,
            0x0e => Self::ComPing,
            0x0f => Self::ComTime,
            0x10 => Self::ComDelayedInsert,
            0x11 => Self::ComChangeUser,
            0x12 => Self::ComBinlogDump,
            0x13 => Self::ComTableDump,
            0x14 => Self::ComConnectOut,
            0x15 => Self::ComRegisterSlave,
            0x16 => Self::ComStmtPrepare,
            0x17 => Self::ComStmtExecute,
            0x18 => Self::ComStmtSendLongData,
            0x19 => Self::ComStmtClose,
            0x1a => Self::ComStmtReset,
            0x1b => Self::ComSetOption,
            0x1c => Self::ComStmtFetch,
            0x1d => Self::ComDaemon,
            0x1e => Self::ComBinlogDumpGtid,
            0x1f => Self::ComResetConnection,
            _ => Self::ComUnknown,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::ComSleep => "COM_SLEEP",
            Self::ComQuit => "COM_QUIT",
            Self::ComInitDb => "COM_INIT_DB",
            Self::ComQuery => "COM_QUERY",
            Self::ComFieldList => "COM_FIELD_LIST",
            Self::ComCreateDb => "COM_CREATE_DB",
            Self::ComDropDb => "COM_DROP_DB",
            Self::ComRefresh => "COM_REFRESH",
            Self::ComShutdown => "COM_SHUTDOWN",
            Self::ComStatistics => "COM_STATISTICS",
            Self::ComProcessInfo => "COM_PROCESS_INFO",
            Self::ComConnect => "COM_CONNECT",
            Self::ComProcessKill => "COM_PROCESS_KILL",
            Self::ComDebug => "COM_DEBUG",
            Self::ComPing => "COM_PING",
            Self::ComTime => "COM_TIME",
            Self::ComDelayedInsert => "COM_DELAYED_INSERT",
            Self::ComChangeUser => "COM_CHANGE_USER",
            Self::ComBinlogDump => "COM_BINLOG_DUMP",
            Self::ComTableDump => "COM_TABLE_DUMP",
            Self::ComConnectOut => "COM_CONNECT_OUT",
            Self::ComRegisterSlave => "COM_REGISTER_SLAVE",
            Self::ComStmtPrepare => "COM_STMT_PREPARE",
            Self::ComStmtExecute => "COM_STMT_EXECUTE",
            Self::ComStmtSendLongData => "COM_STMT_SEND_LONG_DATA",
            Self::ComStmtClose => "COM_STMT_CLOSE",
            Self::ComStmtReset => "COM_STMT_RESET",
            Self::ComSetOption => "COM_SET_OPTION",
            Self::ComStmtFetch => "COM_STMT_FETCH",
            Self::ComDaemon => "COM_DAEMON",
            Self::ComBinlogDumpGtid => "COM_BINLOG_DUMP_GTID",
            Self::ComResetConnection => "COM_RESET_CONNECTION",
            Self::ComUnknown => "COM_UNKNOWN",
        }
    }
}

/// Command handler result
pub enum CommandResult {
    /// OK packet
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
        message: String,
    },
    /// Error packet
    Error { code: u16, message: String },
    /// Result set (for queries)
    ResultSet {
        columns: Vec<ColumnDef>,
        rows: Vec<Vec<Option<String>>>,
    },
    /// EOF (end of result set)
    Eof,
    /// Connection should be closed
    Close,
}

/// Column definition for result sets
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub flags: u16,
    pub decimals: u8,
}

/// Handle COM_QUIT
pub fn handle_quit(_session: &mut Session) -> CommandResult {
    CommandResult::Close
}

/// Handle COM_PING
pub fn handle_ping(_: &mut Session) -> CommandResult {
    CommandResult::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        message: String::new(),
    }
}

/// Handle COM_INIT_DB
pub fn handle_init_db(session: &mut Session, payload: &[u8]) -> CommandResult {
    let db_name = String::from_utf8_lossy(payload)
        .trim_end_matches('\0')
        .to_string();

    // TODO: Check if database exists
    session.set_database(&db_name);

    CommandResult::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        message: String::new(),
    }
}

/// Handle COM_QUERY
pub fn handle_query(session: &mut Session, payload: &[u8]) -> CommandResult {
    let query = String::from_utf8_lossy(payload).to_string();
    let query_lower = query.to_lowercase();

    // Reset result state
    session.reset_result_state();

    // Handle special queries
    if query_lower.starts_with("show databases") || query_lower.starts_with("show schemas") {
        return handle_show_databases(session);
    }

    if query_lower.starts_with("show tables") {
        return handle_show_tables(session);
    }

    if query_lower.starts_with("select @@version") {
        return handle_select_version(session);
    }

    if query_lower.starts_with("use ") {
        let db = query[4..].trim().trim_end_matches(';').to_string();
        session.set_database(&db);
        return CommandResult::Ok {
            affected_rows: 0,
            last_insert_id: 0,
            message: String::new(),
        };
    }

    // TODO: Execute actual query through query engine
    // For now, return error
    CommandResult::Error {
        code: MySqlError::ER_NOT_SUPPORTED_YET,
        message: "Query execution not yet implemented".to_string(),
    }
}

/// Handle SHOW DATABASES
fn handle_show_databases(_: &mut Session) -> CommandResult {
    let columns = vec![ColumnDef {
        name: "Database".to_string(),
        col_type: ColumnType::VarString,
        flags: 0,
        decimals: 0,
    }];

    let rows = vec![
        vec![Some("information_schema".to_string())],
        vec![Some("mysql".to_string())],
        vec![Some("performance_schema".to_string())],
        vec![Some("sys".to_string())],
    ];

    CommandResult::ResultSet { columns, rows }
}

/// Handle SHOW TABLES
fn handle_show_tables(session: &mut Session) -> CommandResult {
    let db = &session.current_db;
    let col_name = format!("Tables_in_{}", db);

    let columns = vec![ColumnDef {
        name: col_name,
        col_type: ColumnType::VarString,
        flags: 0,
        decimals: 0,
    }];

    // TODO: Query actual tables from metadata
    let rows = vec![];

    CommandResult::ResultSet { columns, rows }
}

/// Handle SELECT @@version
fn handle_select_version(_session: &mut Session) -> CommandResult {
    let columns = vec![ColumnDef {
        name: "@@version".to_string(),
        col_type: ColumnType::VarString,
        flags: 0,
        decimals: 0,
    }];

    let rows = vec![vec![Some("8.0.35-nova".to_string())]];

    CommandResult::ResultSet { columns, rows }
}

/// Handle COM_STMT_PREPARE
pub fn handle_stmt_prepare(session: &mut Session, payload: &[u8]) -> CommandResult {
    let sql = String::from_utf8_lossy(payload).to_string();

    // Count parameters (simple heuristic: count ? marks)
    let num_params = sql.matches('?').count() as u16;

    let stmt_id = session.add_prepared_statement(sql, num_params);

    // TODO: Return proper prepared statement response
    CommandResult::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        message: format!("Statement {} prepared", stmt_id),
    }
}

/// Handle COM_STMT_EXECUTE
pub fn handle_stmt_execute(session: &mut Session, payload: &[u8]) -> CommandResult {
    if payload.len() < 4 {
        return CommandResult::Error {
            code: MySqlError::ER_SYNTAX_ERROR,
            message: "Invalid COM_STMT_EXECUTE payload".to_string(),
        };
    }

    let mut buf = payload;
    let stmt_id = buf.get_u32_le();

    if session.get_prepared_statement(stmt_id).is_none() {
        return CommandResult::Error {
            code: MySqlError::ER_UNKNOWN_STMT_HANDLER,
            message: format!("Unknown prepared statement handler ({}) given", stmt_id),
        };
    }

    // TODO: Execute prepared statement with parameters
    CommandResult::Error {
        code: MySqlError::ER_NOT_SUPPORTED_YET,
        message: "Prepared statement execution not yet implemented".to_string(),
    }
}

/// Handle COM_STMT_CLOSE — silent, no response sent.
pub fn handle_stmt_close(session: &mut Session, payload: &[u8]) -> CommandResult {
    if payload.len() < 4 {
        return CommandResult::Error {
            code: MySqlError::ER_SYNTAX_ERROR,
            message: "Invalid COM_STMT_CLOSE payload".to_string(),
        };
    }

    let mut buf = payload;
    let stmt_id = buf.get_u32_le();

    session.remove_prepared_statement(stmt_id);

    // COM_STMT_CLOSE sends NO response — return Eof which we'll skip
    CommandResult::Eof
}

/// Handle COM_SET_OPTION
pub fn handle_set_option(_session: &mut Session, payload: &[u8]) -> CommandResult {
    if payload.len() < 2 {
        return CommandResult::Error {
            code: MySqlError::ER_SYNTAX_ERROR,
            message: "Invalid COM_SET_OPTION payload".to_string(),
        };
    }

    let mut buf = payload;
    let option = buf.get_u16_le();

    // MYSQL_OPTION_MULTI_STATEMENTS_ON = 0
    // MYSQL_OPTION_MULTI_STATEMENTS_OFF = 1
    let _ = option; // TODO: Handle multi-statement option

    CommandResult::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        message: String::new(),
    }
}

/// Handle COM_RESET_CONNECTION
pub fn handle_reset_connection(session: &mut Session) -> CommandResult {
    // Reset session state but keep connection
    session.current_db.clear();
    session.transaction_state = crate::mysql_protocol::connection::TransactionState::None;
    session.server_status = crate::mysql_protocol::connection::ServerStatus::AUTOCOMMIT;
    session.warning_count = 0;
    session.affected_rows = 0;
    session.last_insert_id = 0;

    CommandResult::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        message: String::new(),
    }
}

/// Build OK packet
pub fn build_ok_packet(
    affected_rows: u64,
    last_insert_id: u64,
    status: u16,
    message: &str,
) -> Packet {
    let mut payload = BytesMut::new();

    // Header: 0x00
    payload.put_u8(0x00);

    // Affected rows (length-encoded)
    encode_lenenc_int(&mut payload, affected_rows);

    // Last insert ID (length-encoded)
    encode_lenenc_int(&mut payload, last_insert_id);

    // Status flags
    payload.put_u16_le(status);

    // Warnings
    payload.put_u16_le(0);

    // Message (optional)
    if !message.is_empty() {
        payload.put_slice(message.as_bytes());
    }

    Packet::new(0, payload.to_vec())
}

/// Build error packet
pub fn build_error_packet(code: u16, message: &str) -> Packet {
    let mut payload = BytesMut::new();

    // Header: 0xff
    payload.put_u8(0xff);

    // Error code
    payload.put_u16_le(code);

    // SQL state marker '#'
    payload.put_u8(b'#');

    // SQL state (5 bytes)
    let sql_state = MySqlError::sql_state(code);
    payload.put_slice(sql_state.as_bytes());

    // Error message
    payload.put_slice(message.as_bytes());

    Packet::new(0, payload.to_vec())
}

/// Build EOF packet
pub fn build_eof_packet(status: u16, warnings: u16) -> Packet {
    let mut payload = BytesMut::new();

    // Header: 0xfe
    payload.put_u8(0xfe);

    // Status flags
    payload.put_u16_le(status);

    // Warnings
    payload.put_u16_le(warnings);

    Packet::new(0, payload.to_vec())
}

/// Build column definition packet
pub fn build_column_def_packet(col: &ColumnDef) -> Packet {
    let mut payload = BytesMut::new();

    // Catalog (always "def")
    encode_lenenc_string(&mut payload, "def");

    // Schema
    encode_lenenc_string(&mut payload, "");

    // Table
    encode_lenenc_string(&mut payload, "");

    // Org table
    encode_lenenc_string(&mut payload, "");

    // Name
    encode_lenenc_string(&mut payload, &col.name);

    // Org name
    encode_lenenc_string(&mut payload, &col.name);

    // Fixed length fields marker
    payload.put_u8(0x0c);

    // Character set (utf8mb4 = 45)
    payload.put_u16_le(45);

    // Column length
    payload.put_u32_le(crate::mysql_protocol::types::column_length(col.col_type));

    // Column type
    payload.put_u8(col.col_type.to_u8());

    // Flags
    payload.put_u16_le(col.flags);

    // Decimals
    payload.put_u8(col.decimals);

    // Filler
    payload.put_u16_le(0);

    Packet::new(0, payload.to_vec())
}

/// Build column count packet
pub fn build_column_count_packet(count: u64) -> Packet {
    let mut payload = BytesMut::new();
    encode_lenenc_int(&mut payload, count);
    Packet::new(0, payload.to_vec())
}

/// Build row packet (text protocol)
pub fn build_row_packet(values: &[Option<String>]) -> Packet {
    let mut payload = BytesMut::new();

    for value in values {
        match value {
            Some(v) => encode_lenenc_string(&mut payload, v),
            None => payload.put_u8(0xfb), // NULL
        }
    }

    Packet::new(0, payload.to_vec())
}

/// Helper: encode length-encoded integer
fn encode_lenenc_int(buf: &mut BytesMut, val: u64) {
    if val < 251 {
        buf.put_u8(val as u8);
    } else if val < 65536 {
        buf.put_u8(0xfc);
        buf.put_u16_le(val as u16);
    } else if val < 16777216 {
        buf.put_u8(0xfd);
        buf.put_u8((val & 0xff) as u8);
        buf.put_u8(((val >> 8) & 0xff) as u8);
        buf.put_u8(((val >> 16) & 0xff) as u8);
    } else {
        buf.put_u8(0xfe);
        buf.put_u64_le(val);
    }
}

/// Helper: encode length-encoded string
fn encode_lenenc_string(buf: &mut BytesMut, s: &str) {
    encode_lenenc_int(buf, s.len() as u64);
    buf.put_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql_protocol::auth::AuthPlugin;

    #[test]
    fn test_command_from_u8() {
        assert_eq!(Command::from_u8(0x01), Command::ComQuit);
        assert_eq!(Command::from_u8(0x03), Command::ComQuery);
        assert_eq!(Command::from_u8(0x0e), Command::ComPing);
    }

    #[test]
    fn test_handle_ping() {
        let scramble = vec![0u8; 20];
        let mut session = Session::new("user".to_string(), AuthPlugin::None, scramble);

        let result = handle_ping(&mut session);
        match result {
            CommandResult::Ok { .. } => (),
            _ => panic!("Expected Ok result"),
        }
    }

    #[test]
    fn test_handle_init_db() {
        let scramble = vec![0u8; 20];
        let mut session = Session::new("user".to_string(), AuthPlugin::None, scramble);

        let payload = b"testdb\0";
        let result = handle_init_db(&mut session, payload);

        match result {
            CommandResult::Ok { .. } => {
                assert_eq!(session.current_db, "testdb");
            }
            _ => panic!("Expected Ok result"),
        }
    }

    #[test]
    fn test_build_ok_packet() {
        let packet = build_ok_packet(5, 10, 0x0002, "test");
        assert_eq!(packet.payload[0], 0x00); // OK marker
    }

    #[test]
    fn test_build_error_packet() {
        let packet = build_error_packet(1045, "Access denied");
        assert_eq!(packet.payload[0], 0xff); // Error marker
    }
}
