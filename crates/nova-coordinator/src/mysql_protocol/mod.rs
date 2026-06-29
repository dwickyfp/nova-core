// MySQL Protocol — Production-grade MySQL wire protocol implementation.
//
// Implements MySQL 8.0 protocol specification:
// - Packet framing with sequence numbers
// - Multi-packet support for >16MB payloads
// - Length-encoded integers and strings
// - All COM_* commands
// - Prepared statements
// - Authentication (mysql_native_password, caching_sha2_password)
// - Session state management
// - Graceful shutdown

pub mod auth;
pub mod capabilities;
pub mod codec;
pub mod commands;
pub mod connection;
pub mod errors;
pub mod nova_engine;
pub mod packets;
pub mod query_engine;
pub mod server;
pub mod types;

// Re-exports
pub use auth::{AuthContext, AuthPlugin, AuthState, ClientAuth};
pub use capabilities::ClientCapabilities;
pub use codec::{MAX_PACKET_SIZE, Packet, PacketCodec};
pub use commands::{ColumnDef, Command, CommandResult};
pub use connection::{ParamBinding, PreparedStatement, ServerStatus, Session, TransactionState};
pub use errors::MySqlError;
pub use packets::{HandshakeResponse, build_handshake_packet, parse_handshake_response};
pub use server::MySqlServer;
pub use types::{ColumnFlags, ColumnType, arrow_to_mysql};
