// MySQL connection state
//
// Manages per-connection session state including:
// - Connection ID
// - Current database
// - Session variables
// - Transaction state
// - Prepared statements

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::mysql_protocol::auth::{AuthContext, AuthPlugin};
use crate::mysql_protocol::capabilities::ClientCapabilities;

/// Global connection ID counter
static CONNECTION_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Allocate next connection ID
pub fn next_connection_id() -> u32 {
    CONNECTION_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// No active transaction
    None,
    /// Transaction started but no writes yet
    Active,
    /// Transaction has pending writes
    Dirty,
}

/// Session status flags (SERVER_* flags in OK/EOF packets)
pub struct ServerStatus;

impl ServerStatus {
    pub const IN_TRANS: u16 = 0x0001;
    pub const AUTOCOMMIT: u16 = 0x0002;
    pub const MORE_RESULTS_EXISTS: u16 = 0x0008;
    pub const QUERY_NO_GOOD_INDEX_USED: u16 = 0x0010;
    pub const QUERY_NO_INDEX_USED: u16 = 0x0020;
    pub const CURSOR_EXISTS: u16 = 0x0040;
    pub const LAST_ROW_SENT: u16 = 0x0080;
    pub const DB_DROPPED: u16 = 0x0100;
    pub const NO_BACKSLASH_ESCAPES: u16 = 0x0200;
    pub const METADATA_CHANGED: u16 = 0x0400;
    pub const QUERY_WAS_SLOW: u16 = 0x0800;
    pub const PS_OUT_PARAMS: u16 = 0x1000;
    pub const IN_TRANS_READONLY: u16 = 0x2000;
    pub const SESSION_STATE_CHANGED: u16 = 0x4000;
}

/// Per-connection session state
pub struct Session {
    /// Unique connection ID
    pub connection_id: u32,

    /// Current database (schema)
    pub current_db: String,

    /// Current username
    pub username: String,

    /// Authentication context
    pub auth: AuthContext,

    /// Negotiated client capabilities
    pub client_capabilities: ClientCapabilities,

    /// Server status flags
    pub server_status: u16,

    /// Session variables (SET var = value)
    pub variables: HashMap<String, String>,

    /// Transaction state
    pub transaction_state: TransactionState,

    /// Character set (default: utf8mb4 = 45)
    pub charset: u16,

    /// Connection attributes from client (program name, os, etc.)
    pub connect_attrs: HashMap<String, String>,

    /// Warnings count
    pub warning_count: u16,

    /// Affected rows from last statement
    pub affected_rows: u64,

    /// Last insert ID
    pub last_insert_id: u64,

    /// Prepared statements
    pub prepared_statements: HashMap<u32, PreparedStatement>,

    /// Next prepared statement ID
    next_stmt_id: u32,

    /// Is connection closed
    pub closed: bool,
}

/// Prepared statement state
pub struct PreparedStatement {
    pub stmt_id: u32,
    pub sql: String,
    pub num_params: u16,
    pub num_columns: u16,
    pub params: Vec<ParamBinding>,
}

/// Parameter binding for prepared statement
pub struct ParamBinding {
    pub param_type: u16,
    pub value: Option<Vec<u8>>,
    pub is_null: bool,
}

impl Session {
    /// Create new session
    pub fn new(username: String, auth_plugin: AuthPlugin, scramble: Vec<u8>) -> Self {
        let connection_id = next_connection_id();
        let auth = AuthContext::new(auth_plugin, username.clone(), scramble);

        let mut variables = HashMap::new();
        // Default MySQL session variables
        variables.insert("autocommit".to_string(), "1".to_string());
        variables.insert("character_set_client".to_string(), "utf8mb4".to_string());
        variables.insert(
            "character_set_connection".to_string(),
            "utf8mb4".to_string(),
        );
        variables.insert("character_set_results".to_string(), "utf8mb4".to_string());
        variables.insert(
            "collation_connection".to_string(),
            "utf8mb4_general_ci".to_string(),
        );
        variables.insert("sql_mode".to_string(), "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION".to_string());
        variables.insert("wait_timeout".to_string(), "28800".to_string());
        variables.insert("interactive_timeout".to_string(), "28800".to_string());
        variables.insert("max_allowed_packet".to_string(), "67108864".to_string());
        variables.insert("net_buffer_length".to_string(), "16384".to_string());
        variables.insert("version".to_string(), "8.0.35-nova".to_string());
        variables.insert(
            "version_comment".to_string(),
            "Nova Analytical Engine".to_string(),
        );

        Self {
            connection_id,
            current_db: String::new(),
            username,
            auth,
            client_capabilities: ClientCapabilities::empty(),
            server_status: ServerStatus::AUTOCOMMIT,
            variables,
            transaction_state: TransactionState::None,
            charset: 45, // utf8mb4
            connect_attrs: HashMap::new(),
            warning_count: 0,
            affected_rows: 0,
            last_insert_id: 0,
            prepared_statements: HashMap::new(),
            next_stmt_id: 1,
            closed: false,
        }
    }

    /// Set current database
    pub fn set_database(&mut self, db: &str) {
        self.current_db = db.to_string();
    }

    /// Set a session variable
    pub fn set_variable(&mut self, name: &str, value: &str) {
        let lower = name.to_lowercase();
        let is_autocommit = lower == "autocommit";
        self.variables.insert(lower, value.to_string());

        // Update server status based on autocommit
        if is_autocommit {
            if value == "1" || value.eq_ignore_ascii_case("on") {
                self.server_status |= ServerStatus::AUTOCOMMIT;
            } else {
                self.server_status &= !ServerStatus::AUTOCOMMIT;
            }
        }
    }

    /// Get a session variable
    pub fn get_variable(&self, name: &str) -> Option<&str> {
        self.variables.get(&name.to_lowercase()).map(|s| s.as_str())
    }

    /// Begin transaction
    pub fn begin_transaction(&mut self) {
        self.transaction_state = TransactionState::Active;
        self.server_status |= ServerStatus::IN_TRANS;
    }

    /// Commit transaction
    pub fn commit_transaction(&mut self) {
        self.transaction_state = TransactionState::None;
        self.server_status &= !ServerStatus::IN_TRANS;
    }

    /// Rollback transaction
    pub fn rollback_transaction(&mut self) {
        self.transaction_state = TransactionState::None;
        self.server_status &= !ServerStatus::IN_TRANS;
    }

    /// Add a prepared statement
    pub fn add_prepared_statement(&mut self, sql: String, num_params: u16) -> u32 {
        let stmt_id = self.next_stmt_id;
        self.next_stmt_id += 1;

        let stmt = PreparedStatement {
            stmt_id,
            sql,
            num_params,
            num_columns: 0,
            params: Vec::new(),
        };

        self.prepared_statements.insert(stmt_id, stmt);
        stmt_id
    }

    /// Get a prepared statement
    pub fn get_prepared_statement(&self, stmt_id: u32) -> Option<&PreparedStatement> {
        self.prepared_statements.get(&stmt_id)
    }

    /// Get mutable prepared statement
    pub fn get_prepared_statement_mut(&mut self, stmt_id: u32) -> Option<&mut PreparedStatement> {
        self.prepared_statements.get_mut(&stmt_id)
    }

    /// Remove a prepared statement
    pub fn remove_prepared_statement(&mut self, stmt_id: u32) -> bool {
        self.prepared_statements.remove(&stmt_id).is_some()
    }

    /// Reset affected rows and warnings
    pub fn reset_result_state(&mut self) {
        self.affected_rows = 0;
        self.last_insert_id = 0;
        self.warning_count = 0;
    }

    /// Close session
    pub fn close(&mut self) {
        self.closed = true;
        self.prepared_statements.clear();
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        tracing::debug!(
            connection_id = self.connection_id,
            username = %self.username,
            "Session dropped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let scramble = vec![0u8; 20];
        let session = Session::new(
            "test_user".to_string(),
            AuthPlugin::MysqlNativePassword,
            scramble,
        );

        assert_eq!(session.username, "test_user");
        assert_eq!(session.current_db, "");
        assert!(session.server_status & ServerStatus::AUTOCOMMIT != 0);
        assert_eq!(session.transaction_state, TransactionState::None);
    }

    #[test]
    fn test_session_database() {
        let scramble = vec![0u8; 20];
        let mut session = Session::new("user".to_string(), AuthPlugin::None, scramble);

        session.set_database("mydb");
        assert_eq!(session.current_db, "mydb");
    }

    #[test]
    fn test_session_variables() {
        let scramble = vec![0u8; 20];
        let mut session = Session::new("user".to_string(), AuthPlugin::None, scramble);

        session.set_variable("autocommit", "0");
        assert_eq!(session.get_variable("autocommit"), Some("0"));
        assert!(session.server_status & ServerStatus::AUTOCOMMIT == 0);

        session.set_variable("autocommit", "1");
        assert!(session.server_status & ServerStatus::AUTOCOMMIT != 0);
    }

    #[test]
    fn test_transaction_state() {
        let scramble = vec![0u8; 20];
        let mut session = Session::new("user".to_string(), AuthPlugin::None, scramble);

        assert_eq!(session.transaction_state, TransactionState::None);

        session.begin_transaction();
        assert_eq!(session.transaction_state, TransactionState::Active);
        assert!(session.server_status & ServerStatus::IN_TRANS != 0);

        session.commit_transaction();
        assert_eq!(session.transaction_state, TransactionState::None);
        assert!(session.server_status & ServerStatus::IN_TRANS == 0);
    }

    #[test]
    fn test_prepared_statements() {
        let scramble = vec![0u8; 20];
        let mut session = Session::new("user".to_string(), AuthPlugin::None, scramble);

        let stmt_id = session.add_prepared_statement("SELECT ?".to_string(), 1);
        assert!(session.get_prepared_statement(stmt_id).is_some());

        let stmt = session.get_prepared_statement(stmt_id).unwrap();
        assert_eq!(stmt.sql, "SELECT ?");
        assert_eq!(stmt.num_params, 1);

        assert!(session.remove_prepared_statement(stmt_id));
        assert!(session.get_prepared_statement(stmt_id).is_none());
    }

    #[test]
    fn test_connection_id_unique() {
        let id1 = next_connection_id();
        let id2 = next_connection_id();
        assert_ne!(id1, id2);
    }
}
