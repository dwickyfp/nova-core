#![allow(clippy::needless_borrows_for_generic_args)]
// MySQL Authentication Plugins
//
// Implements MySQL 8.0 authentication methods:
// - mysql_native_password (SHA1-based, legacy but widely used)
// - caching_sha2_password (SHA256-based, default in MySQL 8.0+)
//
// Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_authentication_methods.html

use nova_common::{NovaError, Result};
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// Authentication plugin name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthPlugin {
    /// mysql_native_password (SHA1-based)
    MysqlNativePassword,
    /// caching_sha2_password (SHA256-based, MySQL 8.0+ default)
    CachingSha2Password,
    /// No authentication required
    None,
}

impl AuthPlugin {
    /// Get plugin name as string.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MysqlNativePassword => "mysql_native_password",
            Self::CachingSha2Password => "caching_sha2_password",
            Self::None => "",
        }
    }

    /// Parse plugin name from string.
    pub fn from_name(name: &str) -> Result<Self> {
        match name {
            "mysql_native_password" => Ok(Self::MysqlNativePassword),
            "caching_sha2_password" => Ok(Self::CachingSha2Password),
            "" => Ok(Self::None),
            _ => Err(NovaError::Internal {
                message: format!("unsupported auth plugin: {}", name),
            }),
        }
    }
}

/// Authentication state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthState {
    /// Initial state, waiting for client auth response
    WaitingForAuth,
    /// Auth successful
    Success,
    /// Auth failed
    Failed,
    /// Need more data (for multi-step auth like caching_sha2)
    NeedMoreData,
    /// Fast auth successful (caching_sha2 only)
    FastAuthSuccess,
    /// Full auth required (caching_sha2 only)
    FullAuthRequired,
}

/// Authentication context for a connection.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub plugin: AuthPlugin,
    pub state: AuthState,
    pub username: String,
    pub password_hash: Option<Vec<u8>>,
    pub scramble: Vec<u8>, // 20-byte random challenge
}

impl AuthContext {
    /// Create new auth context.
    pub fn new(plugin: AuthPlugin, username: String, scramble: Vec<u8>) -> Self {
        Self {
            plugin,
            state: AuthState::WaitingForAuth,
            username,
            password_hash: None,
            scramble,
        }
    }

    /// Generate random scramble (20 bytes)
    pub fn generate_scramble() -> Vec<u8> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut scramble = vec![0u8; 20];
        for byte in scramble.iter_mut() {
            *byte = rng.r#gen::<u8>();
        }
        scramble
    }

    /// Verify client authentication response.
    pub fn verify(&mut self, client_response: &[u8], stored_password_hash: &[u8]) -> Result<bool> {
        match self.plugin {
            AuthPlugin::MysqlNativePassword => {
                self.verify_mysql_native_password(client_response, stored_password_hash)
            }
            AuthPlugin::CachingSha2Password => {
                self.verify_caching_sha2_password(client_response, stored_password_hash)
            }
            AuthPlugin::None => {
                // No auth required
                self.state = AuthState::Success;
                Ok(true)
            }
        }
    }

    /// Verify mysql_native_password (SHA1-based).
    ///
    /// Algorithm:
    /// 1. SHA1(password) -> hash_stage1
    /// 2. SHA1(hash_stage1) -> hash_stage2 (this is stored_password_hash)
    /// 3. SHA1(scramble + hash_stage2) -> scramble_result
    /// 4. XOR(hash_stage1, scramble_result) -> expected_response
    /// 5. Compare client_response with expected_response
    fn verify_mysql_native_password(
        &mut self,
        client_response: &[u8],
        stored_password_hash: &[u8],
    ) -> Result<bool> {
        let ok =
            verify_mysql_native_password(&self.scramble, stored_password_hash, client_response);
        self.state = if ok {
            AuthState::Success
        } else {
            AuthState::Failed
        };
        Ok(ok)
    }

    /// Verify caching_sha2_password (SHA256-based).
    ///
    /// This is a simplified implementation. In production, you'd need:
    /// - Fast auth (cache hit): XOR(password_hash, SHA256(scramble + SHA256(password_hash)))
    /// - Full auth (cache miss): RSA encrypted password exchange
    fn verify_caching_sha2_password(
        &mut self,
        client_response: &[u8],
        stored_password_hash: &[u8],
    ) -> Result<bool> {
        let ok =
            verify_caching_sha2_password(&self.scramble, stored_password_hash, client_response);
        self.state = if ok {
            AuthState::FastAuthSuccess
        } else {
            AuthState::FullAuthRequired
        };
        Ok(ok)
    }
}

/// Verify a client's mysql_native_password response against stored hash.
///
/// Client sends: XOR(SHA1(password), SHA1(scramble + SHA1(SHA1(password))))
/// We have: SHA1(SHA1(password)) = password_hash (stored)
/// We receive: client_response = XOR(SHA1(password), SHA1(scramble + password_hash))
///
/// To verify: SHA1(scramble + password_hash) XOR client_response = SHA1(password)
/// Then SHA1(SHA1(password)) should equal password_hash.
pub fn verify_mysql_native_password(
    scramble: &[u8],
    stored_hash: &[u8],
    client_response: &[u8],
) -> bool {
    if stored_hash.is_empty() && client_response.is_empty() {
        return true; // no password required
    }
    if stored_hash.is_empty() || client_response.is_empty() {
        return false;
    }
    // SHA1(scramble + stored_hash)
    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(stored_hash);
    let scramble_hash = hasher.finalize();

    // XOR client_response with scramble_hash to recover SHA1(password)
    let stage1: Vec<u8> = client_response
        .iter()
        .zip(scramble_hash.iter())
        .map(|(a, b)| a ^ b)
        .collect();

    // SHA1(stage1) should equal stored_hash
    let mut hasher2 = Sha1::new();
    hasher2.update(&stage1);
    let computed = hasher2.finalize();
    computed.as_slice() == stored_hash
}

/// Verify a client's caching_sha2_password response against stored hash.
///
/// Same algorithm as mysql_native_password but with SHA256.
pub fn verify_caching_sha2_password(
    scramble: &[u8],
    stored_hash: &[u8],
    client_response: &[u8],
) -> bool {
    if stored_hash.is_empty() && client_response.is_empty() {
        return true;
    }
    if stored_hash.is_empty() || client_response.is_empty() {
        return false;
    }
    let mut hasher = Sha256::new();
    hasher.update(scramble);
    hasher.update(stored_hash);
    let scramble_hash = hasher.finalize();

    let stage1: Vec<u8> = client_response
        .iter()
        .zip(scramble_hash.iter())
        .map(|(a, b)| a ^ b)
        .collect();

    let mut hasher2 = Sha256::new();
    hasher2.update(&stage1);
    let computed = hasher2.finalize();
    computed.as_slice() == stored_hash
}

/// Hash password for storage (mysql_native_password).
/// Returns SHA1(SHA1(password))
pub fn hash_password_mysql_native(password: &str) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    // SHA1(password)
    let mut hasher1 = Sha1::new();
    hasher1.update(password.as_bytes());
    let stage1 = hasher1.finalize();

    // SHA1(SHA1(password))
    let mut hasher2 = Sha1::new();
    hasher2.update(&stage1);
    hasher2.finalize().to_vec()
}

/// Hash password for storage (caching_sha2_password).
/// Returns SHA256(SHA256(password))
pub fn hash_password_caching_sha2(password: &str) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    // SHA256(password)
    let mut hasher1 = Sha256::new();
    hasher1.update(password.as_bytes());
    let stage1 = hasher1.finalize();

    // SHA256(SHA256(password))
    let mut hasher2 = Sha256::new();
    hasher2.update(&stage1);
    hasher2.finalize().to_vec()
}

/// Client-side authentication response generator.
///
/// This is what the client computes and sends to the server.
pub struct ClientAuth;

impl ClientAuth {
    /// Generate mysql_native_password auth response.
    ///
    /// Given:
    /// - password: plaintext password
    /// - scramble: 20-byte challenge from server
    ///
    /// Returns: XOR(SHA1(password), SHA1(scramble + SHA1(SHA1(password))))
    pub fn mysql_native_password(password: &str, scramble: &[u8]) -> Vec<u8> {
        if password.is_empty() {
            return Vec::new();
        }

        // SHA1(password)
        let mut hasher1 = Sha1::new();
        hasher1.update(password.as_bytes());
        let stage1 = hasher1.finalize();

        // SHA1(SHA1(password))
        let mut hasher2 = Sha1::new();
        hasher2.update(&stage1);
        let stage2 = hasher2.finalize();

        // SHA1(scramble + SHA1(SHA1(password)))
        let mut hasher3 = Sha1::new();
        hasher3.update(scramble);
        hasher3.update(&stage2);
        let scramble_hash = hasher3.finalize();

        // XOR(SHA1(password), SHA1(scramble + SHA1(SHA1(password))))
        stage1
            .iter()
            .zip(scramble_hash.iter())
            .map(|(a, b)| a ^ b)
            .collect()
    }

    /// Generate caching_sha2_password auth response.
    ///
    /// Given:
    /// - password: plaintext password
    /// - scramble: 20-byte challenge from server
    ///
    /// Returns: XOR(SHA256(password), SHA256(scramble + SHA256(SHA256(password))))
    pub fn caching_sha2_password(password: &str, scramble: &[u8]) -> Vec<u8> {
        if password.is_empty() {
            return Vec::new();
        }

        // SHA256(password)
        let mut hasher1 = Sha256::new();
        hasher1.update(password.as_bytes());
        let stage1 = hasher1.finalize();

        // SHA256(SHA256(password))
        let mut hasher2 = Sha256::new();
        hasher2.update(&stage1);
        let stage2 = hasher2.finalize();

        // SHA256(scramble + SHA256(SHA256(password)))
        let mut hasher3 = Sha256::new();
        hasher3.update(scramble);
        hasher3.update(&stage2);
        let scramble_hash = hasher3.finalize();

        // XOR(SHA256(password), SHA256(scramble + SHA256(SHA256(password))))
        stage1
            .iter()
            .zip(scramble_hash.iter())
            .map(|(a, b)| a ^ b)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_plugin_name() {
        assert_eq!(
            AuthPlugin::MysqlNativePassword.name(),
            "mysql_native_password"
        );
        assert_eq!(
            AuthPlugin::CachingSha2Password.name(),
            "caching_sha2_password"
        );
        assert_eq!(AuthPlugin::None.name(), "");
    }

    #[test]
    fn test_auth_plugin_from_name() {
        assert_eq!(
            AuthPlugin::from_name("mysql_native_password").unwrap(),
            AuthPlugin::MysqlNativePassword
        );
        assert_eq!(
            AuthPlugin::from_name("caching_sha2_password").unwrap(),
            AuthPlugin::CachingSha2Password
        );
        assert_eq!(AuthPlugin::from_name("").unwrap(), AuthPlugin::None);
        assert!(AuthPlugin::from_name("unknown").is_err());
    }

    #[test]
    fn test_generate_scramble() {
        let scramble = AuthContext::generate_scramble();
        assert_eq!(scramble.len(), 20);
    }

    #[test]
    fn test_hash_password_mysql_native() {
        let hash1 = hash_password_mysql_native("test");
        let hash2 = hash_password_mysql_native("test");
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 20); // SHA1 output

        let hash3 = hash_password_mysql_native("different");
        assert_ne!(hash1, hash3);

        let empty_hash = hash_password_mysql_native("");
        assert!(empty_hash.is_empty());
    }

    #[test]
    fn test_hash_password_caching_sha2() {
        let hash1 = hash_password_caching_sha2("test");
        let hash2 = hash_password_caching_sha2("test");
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 32); // SHA256 output

        let hash3 = hash_password_caching_sha2("different");
        assert_ne!(hash1, hash3);

        let empty_hash = hash_password_caching_sha2("");
        assert!(empty_hash.is_empty());
    }

    #[test]
    fn test_client_auth_mysql_native() {
        let scramble = AuthContext::generate_scramble();
        let response = ClientAuth::mysql_native_password("test", &scramble);
        assert_eq!(response.len(), 20); // SHA1 output

        let empty_response = ClientAuth::mysql_native_password("", &scramble);
        assert!(empty_response.is_empty());
    }

    #[test]
    fn test_client_auth_caching_sha2() {
        let scramble = AuthContext::generate_scramble();
        let response = ClientAuth::caching_sha2_password("test", &scramble);
        assert_eq!(response.len(), 32); // SHA256 output

        let empty_response = ClientAuth::caching_sha2_password("", &scramble);
        assert!(empty_response.is_empty());
    }

    #[test]
    fn test_auth_context_verify_empty_password() {
        let scramble = AuthContext::generate_scramble();
        let mut ctx = AuthContext::new(
            AuthPlugin::MysqlNativePassword,
            "user".to_string(),
            scramble,
        );

        // Empty password with empty stored hash
        let result = ctx.verify(&[], &[]).unwrap();
        assert!(result);
        assert_eq!(ctx.state, AuthState::Success);

        // Empty password with non-empty stored hash
        let mut ctx2 = AuthContext::new(
            AuthPlugin::MysqlNativePassword,
            "user".to_string(),
            AuthContext::generate_scramble(),
        );
        let result = ctx2.verify(&[], &[1, 2, 3]).unwrap();
        assert!(!result);
        assert_eq!(ctx2.state, AuthState::Failed);
    }

    #[test]
    fn test_auth_context_no_auth() {
        let scramble = AuthContext::generate_scramble();
        let mut ctx = AuthContext::new(AuthPlugin::None, "user".to_string(), scramble);

        let result = ctx.verify(&[], &[]).unwrap();
        assert!(result);
        assert_eq!(ctx.state, AuthState::Success);
    }

    #[test]
    fn test_verify_mysql_native_password_roundtrip() {
        let scramble = AuthContext::generate_scramble();
        let stored_hash = hash_password_mysql_native("secretpass");
        let client_response = ClientAuth::mysql_native_password("secretpass", &scramble);

        assert!(verify_mysql_native_password(
            &scramble,
            &stored_hash,
            &client_response
        ));
    }

    #[test]
    fn test_verify_mysql_native_password_wrong_password() {
        let scramble = AuthContext::generate_scramble();
        let stored_hash = hash_password_mysql_native("secretpass");
        let client_response = ClientAuth::mysql_native_password("wrongpass", &scramble);

        assert!(!verify_mysql_native_password(
            &scramble,
            &stored_hash,
            &client_response
        ));
    }

    #[test]
    fn test_verify_caching_sha2_password_roundtrip() {
        let scramble = AuthContext::generate_scramble();
        let stored_hash = hash_password_caching_sha2("secretpass");
        let client_response = ClientAuth::caching_sha2_password("secretpass", &scramble);

        assert!(verify_caching_sha2_password(
            &scramble,
            &stored_hash,
            &client_response
        ));
    }

    #[test]
    fn test_verify_mysql_native_password_both_empty() {
        assert!(verify_mysql_native_password(&[], &[], &[]));
    }

    #[test]
    fn test_verify_mysql_native_password_empty_hash_nonempty_response() {
        let scramble = AuthContext::generate_scramble();
        assert!(!verify_mysql_native_password(&scramble, &[], &[1, 2, 3]));
    }
}
