// MySQL handshake packets
//
// Builds and parses the initial handshake between server and client

use bytes::{BufMut, BytesMut};

use crate::mysql_protocol::auth::AuthPlugin;
use crate::mysql_protocol::capabilities::ClientCapabilities;
use nova_common::{NovaError, Result};

/// Encode null-terminated string into BytesMut
fn put_null_terminated_string(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0x00);
}

/// Server version string
pub const SERVER_VERSION: &str = "8.0.35-nova";

/// Build initial handshake packet (Protocol::Handshake)
///
/// Format:
/// - 1 byte: protocol version (10)
/// - null-terminated string: server version
/// - 4 bytes: connection ID
/// - 8 bytes: auth-plugin-data-part-1 (scramble[0..8])
/// - 1 byte: filler (0x00)
/// - 2 bytes: capability flags (lower 2 bytes)
/// - 1 byte: character set (utf8mb4 = 45)
/// - 2 bytes: status flags
/// - 2 bytes: capability flags (upper 2 bytes)
/// - 1 byte: auth-plugin data len (or 0x00 if CLIENT_PLUGIN_AUTH not set)
/// - 10 bytes: reserved (all 0x00)
/// - null-terminated string: auth-plugin-data-part-2 (scramble[8..20])
/// - null-terminated string: auth-plugin name (if CLIENT_PLUGIN_AUTH)
pub fn build_handshake_packet(
    connection_id: u32,
    auth_plugin: AuthPlugin,
    scramble: &[u8],
) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(128);

    // Protocol version
    buf.put_u8(10);

    // Server version (null-terminated)
    put_null_terminated_string(&mut buf, SERVER_VERSION);

    // Connection ID
    buf.put_u32_le(connection_id);

    // Auth-plugin-data-part-1 (first 8 bytes of scramble)
    buf.put_slice(&scramble[..8]);

    // Filler
    buf.put_u8(0x00);

    // Capability flags (lower 2 bytes)
    let caps = ClientCapabilities::default_server_capabilities();
    let caps_raw = caps.raw();
    buf.put_u16_le((caps_raw & 0xffff) as u16);

    // Character set (utf8mb4 = 45)
    buf.put_u8(45);

    // Status flags
    buf.put_u16_le(0x0002); // SERVER_STATUS_AUTOCOMMIT

    // Capability flags (upper 2 bytes)
    buf.put_u16_le(((caps_raw >> 16) & 0xffff) as u16);

    // Auth-plugin data length (21 = 8 + 13 for mysql_native_password)
    buf.put_u8(21);

    // Reserved (10 bytes of 0x00)
    buf.put_slice(&[0u8; 10]);

    // Auth-plugin-data-part-2 (remaining 12 bytes of scramble + null terminator)
    buf.put_slice(&scramble[8..20]);
    buf.put_u8(0x00); // null terminator

    // Auth-plugin name (null-terminated)
    put_null_terminated_string(&mut buf, auth_plugin.name());

    buf.to_vec()
}

/// Parsed handshake response from client
pub struct HandshakeResponse {
    pub capabilities: ClientCapabilities,
    pub max_packet_size: u32,
    pub charset: u8,
    pub username: String,
    pub auth_response: Vec<u8>,
    pub database: Option<String>,
    pub auth_plugin: Option<String>,
    pub connect_attrs: Vec<(String, String)>,
}

/// Parse client handshake response (HandshakeResponse41)
///
/// Format (after CLIENT_PROTOCOL_41):
/// - 4 bytes: capability flags
/// - 4 bytes: max packet size
/// - 1 byte: character set
/// - 23 bytes: reserved (all 0x00)
/// - null-terminated string: username
/// - lenenc string: auth response (if CLIENT_SECURE_CONNECTION)
///   OR null-terminated string: auth response (if not)
/// - null-terminated string: database (if CLIENT_CONNECT_WITH_DB)
/// - null-terminated string: auth plugin name (if CLIENT_PLUGIN_AUTH)
/// - lenenc int: connect attributes length (if CLIENT_CONNECT_ATTRS)
/// - connect attributes (key-value pairs)
pub fn parse_handshake_response(payload: &[u8]) -> Result<HandshakeResponse> {
    if payload.len() < 32 {
        return Err(NovaError::Internal {
            message: "handshake response too short".to_string(),
        });
    }

    let mut pos = 0;

    // Capability flags (4 bytes)
    let caps_raw = u32::from_le_bytes([
        payload[pos],
        payload[pos + 1],
        payload[pos + 2],
        payload[pos + 3],
    ]);
    let capabilities = ClientCapabilities::from_raw(caps_raw);
    pos += 4;

    // Max packet size (4 bytes)
    let max_packet_size = u32::from_le_bytes([
        payload[pos],
        payload[pos + 1],
        payload[pos + 2],
        payload[pos + 3],
    ]);
    pos += 4;

    // Character set (1 byte)
    let charset = payload[pos];
    pos += 1;

    // Reserved (23 bytes)
    pos += 23;

    // Username (null-terminated)
    let null_pos =
        payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| NovaError::Internal {
                message: "username null terminator not found".to_string(),
            })?;
    let username = String::from_utf8_lossy(&payload[pos..pos + null_pos]).to_string();
    pos += null_pos + 1;

    // Auth response
    let auth_response = if capabilities.supports_plugin_auth_lenenc() {
        // Length-encoded string
        let (len, len_size) = crate::mysql_protocol::codec::decode_lenenc_int(&payload[pos..])?;
        pos += len_size;
        let end = pos
            .checked_add(len as usize)
            .ok_or_else(|| NovaError::Internal {
                message: "auth response length overflow".to_string(),
            })?;
        if end > payload.len() {
            return Err(NovaError::Internal {
                message: "auth response truncated".to_string(),
            });
        }
        let data = payload[pos..end].to_vec();
        pos = end;
        data
    } else if capabilities.supports_secure_connection() {
        // Length-prefixed (1 byte length)
        if pos >= payload.len() {
            return Err(NovaError::Internal {
                message: "auth response length missing".to_string(),
            });
        }
        let len = payload[pos] as usize;
        pos += 1;
        let end = pos.checked_add(len).ok_or_else(|| NovaError::Internal {
            message: "auth response length overflow".to_string(),
        })?;
        if end > payload.len() {
            return Err(NovaError::Internal {
                message: "auth response truncated".to_string(),
            });
        }
        let data = payload[pos..end].to_vec();
        pos = end;
        data
    } else {
        // Null-terminated
        let null_pos = payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(payload.len() - pos);
        let data = payload[pos..pos + null_pos].to_vec();
        pos += null_pos + 1;
        data
    };

    // Database (if CLIENT_CONNECT_WITH_DB)
    let database = if capabilities.wants_connect_with_db() && pos < payload.len() {
        let null_pos = payload[pos..].iter().position(|&b| b == 0);
        if let Some(np) = null_pos {
            let db = String::from_utf8_lossy(&payload[pos..pos + np]).to_string();
            pos += np + 1;
            Some(db)
        } else {
            None
        }
    } else {
        None
    };

    // Auth plugin name (if CLIENT_PLUGIN_AUTH)
    let auth_plugin = if capabilities.supports_plugin_auth() && pos < payload.len() {
        let null_pos = payload[pos..].iter().position(|&b| b == 0);
        if let Some(np) = null_pos {
            let name = String::from_utf8_lossy(&payload[pos..pos + np]).to_string();
            pos += np + 1;
            Some(name)
        } else {
            None
        }
    } else {
        None
    };

    // Connect attributes (if CLIENT_CONNECT_ATTRS)
    let mut connect_attrs = Vec::new();
    if capabilities.supports_connect_attrs()
        && pos < payload.len()
        && let Ok((attrs_len, len_size)) =
            crate::mysql_protocol::codec::decode_lenenc_int(&payload[pos..])
    {
        pos += len_size;
        let attrs_end = pos
            .checked_add(attrs_len as usize)
            .ok_or_else(|| NovaError::Internal {
                message: "connect attributes length overflow".to_string(),
            })?;
        if attrs_end > payload.len() {
            return Err(NovaError::Internal {
                message: "connect attributes truncated".to_string(),
            });
        }

        while pos < attrs_end {
            // Key
            if let Ok((key, key_size)) =
                crate::mysql_protocol::codec::decode_lenenc_string(&payload[pos..])
            {
                pos += key_size;
                // Value
                if let Ok((value, val_size)) =
                    crate::mysql_protocol::codec::decode_lenenc_string(&payload[pos..])
                {
                    pos += val_size;
                    connect_attrs.push((key, value));
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    Ok(HandshakeResponse {
        capabilities,
        max_packet_size,
        charset,
        username,
        auth_response,
        database,
        auth_plugin,
        connect_attrs,
    })
}

/// Build AuthSwitchRequest packet
pub fn build_auth_switch_request(plugin_name: &str, scramble: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(64);

    // Header: 0xfe
    buf.put_u8(0xfe);

    // Plugin name (null-terminated)
    put_null_terminated_string(&mut buf, plugin_name);

    // Scramble data
    buf.put_slice(scramble);
    buf.put_u8(0x00); // null terminator

    buf.to_vec()
}

/// Build AuthMoreData packet
pub fn build_auth_more_data(data: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(data.len() + 1);
    buf.put_u8(0x01); // AuthMoreData header
    buf.put_slice(data);
    buf.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_handshake_response_payload(capabilities: ClientCapabilities) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&capabilities.raw().to_le_bytes());
        payload.extend_from_slice(&16_777_216u32.to_le_bytes());
        payload.push(45);
        payload.extend_from_slice(&[0u8; 23]);
        payload.extend_from_slice(b"root\0");
        payload
    }

    #[test]
    fn test_build_handshake_packet() {
        let scramble = vec![1u8; 20];
        let packet = build_handshake_packet(1, AuthPlugin::MysqlNativePassword, &scramble);

        // Protocol version
        assert_eq!(packet[0], 10);

        // Should contain server version
        let version_end = packet[1..].iter().position(|&b| b == 0).unwrap();
        let version = String::from_utf8_lossy(&packet[1..1 + version_end]);
        assert_eq!(version, SERVER_VERSION);
    }

    #[test]
    fn test_build_auth_switch_request() {
        let scramble = vec![2u8; 20];
        let packet = build_auth_switch_request("caching_sha2_password", &scramble);

        assert_eq!(packet[0], 0xfe); // Auth switch marker
    }

    #[test]
    fn test_parse_handshake_response_rejects_truncated_secure_auth_response() {
        let caps = ClientCapabilities::from_raw(
            ClientCapabilities::PROTOCOL_41 | ClientCapabilities::SECURE_CONNECTION,
        );
        let mut payload = base_handshake_response_payload(caps);
        payload.push(5);
        payload.extend_from_slice(&[1, 2]);

        assert!(parse_handshake_response(&payload).is_err());
    }

    #[test]
    fn test_parse_handshake_response_rejects_truncated_connect_attrs() {
        let caps = ClientCapabilities::from_raw(
            ClientCapabilities::PROTOCOL_41
                | ClientCapabilities::SECURE_CONNECTION
                | ClientCapabilities::CONNECT_ATTRS,
        );
        let mut payload = base_handshake_response_payload(caps);
        payload.push(0); // empty auth response
        payload.push(4); // declared connect-attrs length
        payload.push(1); // truncated key length without key/value bytes

        assert!(parse_handshake_response(&payload).is_err());
    }

    #[test]
    fn test_build_auth_more_data() {
        let data = vec![3, 4, 5];
        let packet = build_auth_more_data(&data);

        assert_eq!(packet[0], 0x01); // AuthMoreData marker
        assert_eq!(&packet[1..], &[3, 4, 5]);
    }
}
