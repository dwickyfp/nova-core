// MySQL error codes
//
// Standard MySQL error codes used in error packets

/// MySQL error codes
pub struct MySqlError;

impl MySqlError {
    // Connection errors
    pub const ER_ACCESS_DENIED_ERROR: u16 = 1045;
    pub const ER_BAD_DB_ERROR: u16 = 1049;
    pub const ER_DBACCESS_DENIED_ERROR: u16 = 1044;
    pub const ER_HOST_NOT_PRIVILEGED: u16 = 1130;

    // Syntax and access errors
    pub const ER_PARSE_ERROR: u16 = 1064;
    pub const ER_NO_SUCH_TABLE: u16 = 1146;
    pub const ER_BAD_FIELD_ERROR: u16 = 1054;
    pub const ER_NO_TABLES_USED: u16 = 1096;
    pub const ER_TABLE_EXISTS_ERROR: u16 = 1050;

    // Command errors
    pub const ER_UNKNOWN_COM_ERROR: u16 = 1047;
    pub const ER_NOT_SUPPORTED_YET: u16 = 1235;
    pub const ER_SYNTAX_ERROR: u16 = 1149;

    // Query errors
    pub const ER_QUERY_INTERRUPTED: u16 = 1317;
    pub const ER_SERVER_SHUTDOWN: u16 = 1053;
    pub const ER_ABORTING_CONNECTION: u16 = 1152;

    // Prepared statement errors
    pub const ER_UNKNOWN_STMT_HANDLER: u16 = 1243;
    pub const ER_STMT_HAS_NO_OPEN_CURSOR: u16 = 1325;
    pub const ER_WRONG_NUMBER_OF_PARAMETERS_IN_PREPARED_CALL: u16 = 1318;

    // Get error message for error code
    pub fn message(code: u16) -> &'static str {
        match code {
            Self::ER_ACCESS_DENIED_ERROR => "Access denied for user",
            Self::ER_BAD_DB_ERROR => "Unknown database",
            Self::ER_DBACCESS_DENIED_ERROR => "Access denied for database",
            Self::ER_HOST_NOT_PRIVILEGED => "Host not privileged",
            Self::ER_PARSE_ERROR => "Parse error",
            Self::ER_NO_SUCH_TABLE => "Table doesn't exist",
            Self::ER_BAD_FIELD_ERROR => "Unknown column",
            Self::ER_NO_TABLES_USED => "No tables used",
            Self::ER_TABLE_EXISTS_ERROR => "Table already exists",
            Self::ER_UNKNOWN_COM_ERROR => "Unknown command",
            Self::ER_NOT_SUPPORTED_YET => "Not supported yet",
            Self::ER_SYNTAX_ERROR => "Syntax error",
            Self::ER_QUERY_INTERRUPTED => "Query interrupted",
            Self::ER_SERVER_SHUTDOWN => "Server shutdown in progress",
            Self::ER_ABORTING_CONNECTION => "Connection aborted",
            Self::ER_UNKNOWN_STMT_HANDLER => "Unknown prepared statement handler",
            Self::ER_STMT_HAS_NO_OPEN_CURSOR => "Statement has no open cursor",
            Self::ER_WRONG_NUMBER_OF_PARAMETERS_IN_PREPARED_CALL => {
                "Incorrect number of parameters"
            }
            _ => "Unknown error",
        }
    }

    // Get SQL state for error code
    pub fn sql_state(code: u16) -> &'static str {
        match code {
            Self::ER_ACCESS_DENIED_ERROR => "28000",
            Self::ER_BAD_DB_ERROR => "42000",
            Self::ER_DBACCESS_DENIED_ERROR => "42000",
            Self::ER_HOST_NOT_PRIVILEGED => "HY000",
            Self::ER_PARSE_ERROR => "42000",
            Self::ER_NO_SUCH_TABLE => "42S02",
            Self::ER_BAD_FIELD_ERROR => "42S22",
            Self::ER_NO_TABLES_USED => "42S02",
            Self::ER_TABLE_EXISTS_ERROR => "42S01",
            Self::ER_UNKNOWN_COM_ERROR => "08S01",
            Self::ER_NOT_SUPPORTED_YET => "42000",
            Self::ER_SYNTAX_ERROR => "42000",
            Self::ER_QUERY_INTERRUPTED => "70100",
            Self::ER_SERVER_SHUTDOWN => "08S01",
            Self::ER_ABORTING_CONNECTION => "08S01",
            Self::ER_UNKNOWN_STMT_HANDLER => "HY000",
            Self::ER_STMT_HAS_NO_OPEN_CURSOR => "HY000",
            Self::ER_WRONG_NUMBER_OF_PARAMETERS_IN_PREPARED_CALL => "21S01",
            _ => "HY000",
        }
    }
}
