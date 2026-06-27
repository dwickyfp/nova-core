// MySQL Protocol Capabilities — CLIENT_* flags for handshake negotiation.
//
// Implements MySQL 8.0 capability flags used during connection handshake.
// Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html

/// MySQL client capability flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientCapabilities(pub u32);

impl ClientCapabilities {
    // Core capabilities
    pub const LONG_PASSWORD: u32 = 1 << 0;
    pub const FOUND_ROWS: u32 = 1 << 1;
    pub const LONG_FLAG: u32 = 1 << 2;
    pub const CONNECT_WITH_DB: u32 = 1 << 3;
    pub const NO_SCHEMA: u32 = 1 << 4;
    pub const COMPRESS: u32 = 1 << 5;
    pub const ODBC: u32 = 1 << 6;
    pub const LOCAL_FILES: u32 = 1 << 7;
    pub const IGNORE_SPACE: u32 = 1 << 8;
    pub const PROTOCOL_41: u32 = 1 << 9;
    pub const INTERACTIVE: u32 = 1 << 10;
    pub const SSL: u32 = 1 << 11;
    pub const IGNORE_SIGPIPE: u32 = 1 << 12;
    pub const TRANSACTIONS: u32 = 1 << 13;
    pub const RESERVED: u32 = 1 << 14;
    pub const SECURE_CONNECTION: u32 = 1 << 15;
    pub const MULTI_STATEMENTS: u32 = 1 << 16;
    pub const MULTI_RESULTS: u32 = 1 << 17;
    pub const PS_MULTI_RESULTS: u32 = 1 << 18;
    pub const PLUGIN_AUTH: u32 = 1 << 19;
    pub const CONNECT_ATTRS: u32 = 1 << 20;
    pub const PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
    pub const CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 1 << 22;
    pub const SESSION_TRACK: u32 = 1 << 23;
    pub const DEPRECATE_EOF: u32 = 1 << 24;
    pub const OPTIONAL_RESULTSET_METADATA: u32 = 1 << 25;
    pub const ZSTD_COMPRESSION_ALGORITHM: u32 = 1 << 26;
    pub const QUERY_ATTRIBUTES: u32 = 1 << 27;
    pub const MULTI_FACTOR_AUTHENTICATION: u32 = 1 << 28;
    pub const CAPABILITY_EXTENSION: u32 = 1 << 29;
    pub const SSL_VERIFY_SERVER_CERT: u32 = 1 << 30;
    pub const REMEMBER_OPTIONS: u32 = 1 << 31;

    /// Create empty capabilities.
    pub fn empty() -> Self {
        Self(0)
    }

    /// Create from raw u32.
    pub fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    /// Get raw u32 value.
    pub fn raw(&self) -> u32 {
        self.0
    }

    /// Check if a capability flag is set.
    pub fn has(&self, flag: u32) -> bool {
        (self.0 & flag) != 0
    }

    /// Set a capability flag.
    pub fn set(&mut self, flag: u32) {
        self.0 |= flag;
    }

    /// Clear a capability flag.
    pub fn clear(&mut self, flag: u32) {
        self.0 &= !flag;
    }

    /// Check if client supports protocol 4.1 (required for modern features).
    pub fn supports_protocol_41(&self) -> bool {
        self.has(Self::PROTOCOL_41)
    }

    /// Check if client supports secure connection (mysql_native_password).
    pub fn supports_secure_connection(&self) -> bool {
        self.has(Self::SECURE_CONNECTION)
    }

    /// Check if client supports plugin authentication.
    pub fn supports_plugin_auth(&self) -> bool {
        self.has(Self::PLUGIN_AUTH)
    }

    /// Check if client supports connect attributes.
    pub fn supports_connect_attrs(&self) -> bool {
        self.has(Self::CONNECT_ATTRS)
    }

    /// Check if client supports multi-statements.
    pub fn supports_multi_statements(&self) -> bool {
        self.has(Self::MULTI_STATEMENTS)
    }

    /// Check if client supports multi-results.
    pub fn supports_multi_results(&self) -> bool {
        self.has(Self::MULTI_RESULTS)
    }

    /// Check if client supports prepared statement multi-results.
    pub fn supports_ps_multi_results(&self) -> bool {
        self.has(Self::PS_MULTI_RESULTS)
    }

    /// Check if client supports session tracking.
    pub fn supports_session_track(&self) -> bool {
        self.has(Self::SESSION_TRACK)
    }

    /// Check if client supports deprecated EOF.
    pub fn supports_deprecate_eof(&self) -> bool {
        self.has(Self::DEPRECATE_EOF)
    }

    /// Check if client supports SSL.
    pub fn supports_ssl(&self) -> bool {
        self.has(Self::SSL)
    }

    /// Check if client wants to connect with a database.
    pub fn wants_connect_with_db(&self) -> bool {
        self.has(Self::CONNECT_WITH_DB)
    }

    /// Check if client supports compression.
    pub fn supports_compress(&self) -> bool {
        self.has(Self::COMPRESS)
    }

    /// Check if client supports plugin auth with length-encoded data.
    pub fn supports_plugin_auth_lenenc(&self) -> bool {
        self.has(Self::PLUGIN_AUTH_LENENC_CLIENT_DATA)
    }

    /// Check if client can handle expired passwords.
    pub fn can_handle_expired_passwords(&self) -> bool {
        self.has(Self::CAN_HANDLE_EXPIRED_PASSWORDS)
    }

    /// Check if client supports optional resultset metadata.
    pub fn supports_optional_metadata(&self) -> bool {
        self.has(Self::OPTIONAL_RESULTSET_METADATA)
    }

    /// Check if client supports query attributes.
    pub fn supports_query_attributes(&self) -> bool {
        self.has(Self::QUERY_ATTRIBUTES)
    }

    /// Check if client supports multi-factor authentication.
    pub fn supports_multi_factor_auth(&self) -> bool {
        self.has(Self::MULTI_FACTOR_AUTHENTICATION)
    }

    /// Get default server capabilities.
    pub fn default_server_capabilities() -> Self {
        Self(
            Self::LONG_PASSWORD
                | Self::FOUND_ROWS
                | Self::LONG_FLAG
                | Self::CONNECT_WITH_DB
                | Self::NO_SCHEMA
                | Self::IGNORE_SPACE
                | Self::PROTOCOL_41
                | Self::TRANSACTIONS
                | Self::SECURE_CONNECTION
                | Self::MULTI_STATEMENTS
                | Self::MULTI_RESULTS
                | Self::PS_MULTI_RESULTS
                | Self::PLUGIN_AUTH
                | Self::CONNECT_ATTRS
                | Self::PLUGIN_AUTH_LENENC_CLIENT_DATA
                | Self::CAN_HANDLE_EXPIRED_PASSWORDS
                | Self::SESSION_TRACK
                | Self::DEPRECATE_EOF
                | Self::OPTIONAL_RESULTSET_METADATA
                | Self::QUERY_ATTRIBUTES,
        )
    }

    /// Negotiate capabilities between client and server.
    /// Returns the intersection of client and server capabilities.
    pub fn negotiate(client_caps: Self, server_caps: Self) -> Self {
        Self(client_caps.0 & server_caps.0)
    }

    /// Encode capabilities to bytes (4 bytes, little-endian).
    pub fn encode(&self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    /// Decode capabilities from bytes.
    #[allow(clippy::needless_borrows_for_generic_args)]
    pub fn decode(bytes: &[u8]) -> nova_common::Result<Self> {
        if bytes.len() < 4 {
            return Err(nova_common::NovaError::Internal {
                message: "capabilities need 4 bytes".to_string(),
            });
        }
        let raw = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        Ok(Self(raw))
    }
}

impl std::fmt::Display for ClientCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut flags = Vec::new();

        if self.has(Self::LONG_PASSWORD) {
            flags.push("LONG_PASSWORD");
        }
        if self.has(Self::FOUND_ROWS) {
            flags.push("FOUND_ROWS");
        }
        if self.has(Self::LONG_FLAG) {
            flags.push("LONG_FLAG");
        }
        if self.has(Self::CONNECT_WITH_DB) {
            flags.push("CONNECT_WITH_DB");
        }
        if self.has(Self::NO_SCHEMA) {
            flags.push("NO_SCHEMA");
        }
        if self.has(Self::COMPRESS) {
            flags.push("COMPRESS");
        }
        if self.has(Self::ODBC) {
            flags.push("ODBC");
        }
        if self.has(Self::LOCAL_FILES) {
            flags.push("LOCAL_FILES");
        }
        if self.has(Self::IGNORE_SPACE) {
            flags.push("IGNORE_SPACE");
        }
        if self.has(Self::PROTOCOL_41) {
            flags.push("PROTOCOL_41");
        }
        if self.has(Self::INTERACTIVE) {
            flags.push("INTERACTIVE");
        }
        if self.has(Self::SSL) {
            flags.push("SSL");
        }
        if self.has(Self::IGNORE_SIGPIPE) {
            flags.push("IGNORE_SIGPIPE");
        }
        if self.has(Self::TRANSACTIONS) {
            flags.push("TRANSACTIONS");
        }
        if self.has(Self::SECURE_CONNECTION) {
            flags.push("SECURE_CONNECTION");
        }
        if self.has(Self::MULTI_STATEMENTS) {
            flags.push("MULTI_STATEMENTS");
        }
        if self.has(Self::MULTI_RESULTS) {
            flags.push("MULTI_RESULTS");
        }
        if self.has(Self::PS_MULTI_RESULTS) {
            flags.push("PS_MULTI_RESULTS");
        }
        if self.has(Self::PLUGIN_AUTH) {
            flags.push("PLUGIN_AUTH");
        }
        if self.has(Self::CONNECT_ATTRS) {
            flags.push("CONNECT_ATTRS");
        }
        if self.has(Self::PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            flags.push("PLUGIN_AUTH_LENENC_CLIENT_DATA");
        }
        if self.has(Self::CAN_HANDLE_EXPIRED_PASSWORDS) {
            flags.push("CAN_HANDLE_EXPIRED_PASSWORDS");
        }
        if self.has(Self::SESSION_TRACK) {
            flags.push("SESSION_TRACK");
        }
        if self.has(Self::DEPRECATE_EOF) {
            flags.push("DEPRECATE_EOF");
        }
        if self.has(Self::OPTIONAL_RESULTSET_METADATA) {
            flags.push("OPTIONAL_RESULTSET_METADATA");
        }
        if self.has(Self::ZSTD_COMPRESSION_ALGORITHM) {
            flags.push("ZSTD_COMPRESSION_ALGORITHM");
        }
        if self.has(Self::QUERY_ATTRIBUTES) {
            flags.push("QUERY_ATTRIBUTES");
        }
        if self.has(Self::MULTI_FACTOR_AUTHENTICATION) {
            flags.push("MULTI_FACTOR_AUTHENTICATION");
        }
        if self.has(Self::SSL_VERIFY_SERVER_CERT) {
            flags.push("SSL_VERIFY_SERVER_CERT");
        }
        if self.has(Self::REMEMBER_OPTIONS) {
            flags.push("REMEMBER_OPTIONS");
        }

        write!(f, "ClientCapabilities({})", flags.join(" | "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities_empty() {
        let caps = ClientCapabilities::empty();
        assert_eq!(caps.raw(), 0);
        assert!(!caps.has(ClientCapabilities::LONG_PASSWORD));
    }

    #[test]
    fn test_capabilities_set_clear() {
        let mut caps = ClientCapabilities::empty();
        caps.set(ClientCapabilities::LONG_PASSWORD);
        assert!(caps.has(ClientCapabilities::LONG_PASSWORD));

        caps.clear(ClientCapabilities::LONG_PASSWORD);
        assert!(!caps.has(ClientCapabilities::LONG_PASSWORD));
    }

    #[test]
    fn test_capabilities_encode_decode() {
        let caps = ClientCapabilities::default_server_capabilities();
        let encoded = caps.encode();
        let decoded = ClientCapabilities::decode(&encoded).unwrap();
        assert_eq!(caps.raw(), decoded.raw());
    }

    #[test]
    fn test_capabilities_negotiate() {
        let client =
            ClientCapabilities(ClientCapabilities::LONG_PASSWORD | ClientCapabilities::PROTOCOL_41);
        let server = ClientCapabilities(
            ClientCapabilities::PROTOCOL_41 | ClientCapabilities::SECURE_CONNECTION,
        );
        let negotiated = ClientCapabilities::negotiate(client, server);
        assert!(negotiated.has(ClientCapabilities::PROTOCOL_41));
        assert!(!negotiated.has(ClientCapabilities::LONG_PASSWORD));
        assert!(!negotiated.has(ClientCapabilities::SECURE_CONNECTION));
    }

    #[test]
    fn test_capabilities_display() {
        let caps =
            ClientCapabilities(ClientCapabilities::LONG_PASSWORD | ClientCapabilities::PROTOCOL_41);
        let display = format!("{}", caps);
        assert!(display.contains("LONG_PASSWORD"));
        assert!(display.contains("PROTOCOL_41"));
    }

    #[test]
    fn test_default_server_capabilities() {
        let caps = ClientCapabilities::default_server_capabilities();
        assert!(caps.has(ClientCapabilities::PROTOCOL_41));
        assert!(caps.has(ClientCapabilities::SECURE_CONNECTION));
        assert!(caps.has(ClientCapabilities::PLUGIN_AUTH));
    }
}
