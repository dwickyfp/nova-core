// MySQL Wire Protocol Codec — Low-level packet I/O.
//
// Implements MySQL protocol packet framing:
// - 4-byte header: 3-byte length + 1-byte sequence ID
// - Multi-packet support for payloads >16MB (0xFFFFFF bytes)
// - Length-encoded integers and strings
// - Proper sequence number tracking

use nova_common::{NovaError, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Maximum payload size per packet (16MB - 1).
pub const MAX_PACKET_SIZE: u32 = 0xFFFFFF;

/// MySQL protocol packet.
#[derive(Debug, Clone)]
pub struct Packet {
    pub seq: u8,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn new(seq: u8, payload: Vec<u8>) -> Self {
        Self { seq, payload }
    }

    /// Total packet size including header.
    pub fn total_size(&self) -> usize {
        4 + self.payload.len()
    }
}

/// Packet reader/writer with sequence tracking.
pub struct PacketCodec {
    stream: TcpStream,
    seq: u8,
}

impl PacketCodec {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream, seq: 0 }
    }

    /// Reset sequence number (e.g., after command completion).
    pub fn reset_seq(&mut self) {
        self.seq = 0;
    }

    /// Set sequence number explicitly.
    pub fn set_seq(&mut self, seq: u8) {
        self.seq = seq;
    }

    /// Get current sequence number.
    pub fn seq(&self) -> u8 {
        self.seq
    }

    /// Read a single packet from the stream.
    pub async fn read_packet(&mut self) -> Result<Packet> {
        let mut header = [0u8; 4];
        self.stream
            .read_exact(&mut header)
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("read packet header failed: {}", e),
            })?;

        let len = (header[0] as u32) | ((header[1] as u32) << 8) | ((header[2] as u32) << 16);
        let seq = header[3];

        let mut payload = vec![0u8; len as usize];
        self.stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("read packet payload failed: {}", e),
            })?;

        // Update sequence for next packet
        self.seq = seq.wrapping_add(1);

        Ok(Packet { seq, payload })
    }

    /// Read a complete payload, handling multi-packet (>16MB) responses.
    pub async fn read_full_payload(&mut self) -> Result<Vec<u8>> {
        let mut full_payload = Vec::new();

        loop {
            let packet = self.read_packet().await?;
            let len = packet.payload.len() as u32;

            full_payload.extend(packet.payload);

            // If payload is exactly MAX_PACKET_SIZE, there may be more packets
            if len < MAX_PACKET_SIZE {
                break;
            }
        }

        Ok(full_payload)
    }

    /// Write a single packet to the stream.
    pub async fn write_packet(&mut self, payload: &[u8]) -> Result<()> {
        self.write_packet_with_seq(payload, self.seq).await
    }

    /// Write a packet with explicit sequence number.
    pub async fn write_packet_with_seq(&mut self, payload: &[u8], seq: u8) -> Result<()> {
        let len = payload.len() as u32;
        let header = [
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
            seq,
        ];

        // Combine header + payload into single write to avoid TCP fragmentation
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&header);
        buf.extend_from_slice(payload);

        self.stream
            .write_all(&buf)
            .await
            .map_err(|e| NovaError::Internal {
                message: format!("write packet failed: {}", e),
            })?;

        self.stream.flush().await.map_err(|e| NovaError::Internal {
            message: format!("flush failed: {}", e),
        })?;

        // Update sequence for next packet
        self.seq = seq.wrapping_add(1);

        Ok(())
    }

    /// Write a large payload, splitting into multiple packets if >16MB.
    pub async fn write_large_payload(&mut self, payload: &[u8]) -> Result<()> {
        let mut offset = 0;
        let mut seq = self.seq;

        while offset < payload.len() {
            let chunk_size = std::cmp::min(MAX_PACKET_SIZE as usize, payload.len() - offset);
            let chunk = &payload[offset..offset + chunk_size];

            self.write_packet_with_seq(chunk, seq).await?;
            seq = seq.wrapping_add(1);
            offset += chunk_size;

            // If we just wrote exactly MAX_PACKET_SIZE, we need to send an empty packet
            // to signal end of multi-packet
            if chunk_size == MAX_PACKET_SIZE as usize && offset == payload.len() {
                self.write_packet_with_seq(&[], seq).await?;
            }
        }

        Ok(())
    }

    /// Get mutable reference to underlying stream.
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// Consume codec and return stream.
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }
}

// ══════════════════════════════════════════════════════════════
//  Length-Encoded Integer & String Helpers
// ══════════════════════════════════════════════════════════════

/// Encode a length-encoded integer into a buffer.
pub fn encode_lenenc_int(buf: &mut Vec<u8>, val: u64) {
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
}

/// Decode a length-encoded integer from a buffer.
/// Returns (value, bytes_consumed).
pub fn decode_lenenc_int(buf: &[u8]) -> Result<(u64, usize)> {
    if buf.is_empty() {
        return Err(NovaError::Internal {
            message: "empty buffer for lenenc_int".to_string(),
        });
    }

    match buf[0] {
        0..=250 => Ok((buf[0] as u64, 1)),
        0xfc => {
            if buf.len() < 3 {
                return Err(NovaError::Internal {
                    message: "lenenc_int 0xfc needs 3 bytes".to_string(),
                });
            }
            let val = u16::from_le_bytes([buf[1], buf[2]]) as u64;
            Ok((val, 3))
        }
        0xfd => {
            if buf.len() < 4 {
                return Err(NovaError::Internal {
                    message: "lenenc_int 0xfd needs 4 bytes".to_string(),
                });
            }
            let val = ((buf[1] as u32) | ((buf[2] as u32) << 8) | ((buf[3] as u32) << 16)) as u64;
            Ok((val, 4))
        }
        0xfe => {
            if buf.len() < 9 {
                return Err(NovaError::Internal {
                    message: "lenenc_int 0xfe needs 9 bytes".to_string(),
                });
            }
            let val = u64::from_le_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((val, 9))
        }
        _ => Err(NovaError::Internal {
            message: format!("invalid lenenc_int prefix: 0x{:02x}", buf[0]),
        }),
    }
}

/// Encode a length-encoded string into a buffer.
pub fn encode_lenenc_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    encode_lenenc_int(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

/// Decode a length-encoded string from a buffer.
/// Returns (string, bytes_consumed).
pub fn decode_lenenc_string(buf: &[u8]) -> Result<(String, usize)> {
    let (len, len_size) = decode_lenenc_int(buf)?;
    let total = len_size + len as usize;

    if buf.len() < total {
        return Err(NovaError::Internal {
            message: format!("lenenc_string needs {} bytes, got {}", total, buf.len()),
        });
    }

    let s = String::from_utf8_lossy(&buf[len_size..total]).to_string();
    Ok((s, total))
}

/// Encode a null-terminated string.
pub fn encode_null_terminated_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0x00);
}

/// Decode a null-terminated string from a buffer.
/// Returns (string, bytes_consumed including null).
pub fn decode_null_terminated_string(buf: &[u8]) -> Result<(String, usize)> {
    let null_pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| NovaError::Internal {
            message: "null terminator not found".to_string(),
        })?;

    let s = String::from_utf8_lossy(&buf[..null_pos]).to_string();
    Ok((s, null_pos + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lenenc_int_small() {
        let mut buf = Vec::new();
        encode_lenenc_int(&mut buf, 42);
        assert_eq!(buf, vec![42]);
        let (val, consumed) = decode_lenenc_int(&buf).unwrap();
        assert_eq!(val, 42);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_lenenc_int_medium() {
        let mut buf = Vec::new();
        encode_lenenc_int(&mut buf, 1000);
        assert_eq!(buf[0], 0xfc);
        let (val, consumed) = decode_lenenc_int(&buf).unwrap();
        assert_eq!(val, 1000);
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_lenenc_int_large() {
        let mut buf = Vec::new();
        encode_lenenc_int(&mut buf, 1_000_000);
        assert_eq!(buf[0], 0xfd);
        let (val, consumed) = decode_lenenc_int(&buf).unwrap();
        assert_eq!(val, 1_000_000);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_lenenc_int_huge() {
        let mut buf = Vec::new();
        encode_lenenc_int(&mut buf, 10_000_000_000);
        assert_eq!(buf[0], 0xfe);
        let (val, consumed) = decode_lenenc_int(&buf).unwrap();
        assert_eq!(val, 10_000_000_000);
        assert_eq!(consumed, 9);
    }

    #[test]
    fn test_lenenc_string() {
        let mut buf = Vec::new();
        encode_lenenc_string(&mut buf, "hello");
        assert_eq!(buf[0], 5);
        assert_eq!(&buf[1..], b"hello");
        let (s, consumed) = decode_lenenc_string(&buf).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_null_terminated_string() {
        let mut buf = Vec::new();
        encode_null_terminated_string(&mut buf, "test");
        assert_eq!(buf, b"test\x00");
        let (s, consumed) = decode_null_terminated_string(&buf).unwrap();
        assert_eq!(s, "test");
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_packet_size() {
        let pkt = Packet::new(0, vec![1, 2, 3, 4, 5]);
        assert_eq!(pkt.total_size(), 9); // 4 header + 5 payload
    }
}
