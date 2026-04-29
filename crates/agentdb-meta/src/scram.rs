//! SCRAM-SHA-256 primitives per RFC 5802 + RFC 7677.
//!
//! The server never stores the plaintext password. At user-create
//! time we derive:
//!
//! ```text
//! SaltedPassword = PBKDF2-HMAC-SHA-256(password, salt, i, 32)
//! ClientKey      = HMAC-SHA-256(SaltedPassword, "Client Key")
//! StoredKey      = SHA-256(ClientKey)
//! ServerKey      = HMAC-SHA-256(SaltedPassword, "Server Key")
//! ```
//!
//! and persist `(iterations, salt, StoredKey, ServerKey)`. The
//! password and `SaltedPassword` / `ClientKey` never touch disk.
//!
//! At auth time the verifier reconstructs `ClientKey` from the
//! client's proof + the server's StoredKey and the agreed
//! AuthMessage, hashes it, and compares against the stored
//! StoredKey in constant time. If equal, the password is correct;
//! we then return ServerSignature so the client can verify the
//! server.
//!
//! This module is the pure-crypto layer — no I/O, no DB, no
//! async. Callers glue it to the users table (see `users.rs`) and
//! the wire / REST handshake (see agentdb-wire + agentdb-api).

use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Default PBKDF2 iteration count. 4096 is the RFC 5802 floor —
/// a reasonable prototype default that keeps per-auth cost under
/// 10 ms on commodity hardware. Deployments that want more can
/// create users with a higher count via `--iterations`.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// Salt length in bytes. 16 bytes = 128 bits of entropy — the
/// MongoDB SCRAM implementation's default. RFC 5802 requires
/// "sufficient entropy"; 128 bits is generous.
pub const SALT_LEN: usize = 16;

/// Output size of SHA-256 in bytes.
pub const HASH_LEN: usize = 32;

/// Stored SCRAM credentials. Everything needed to verify a client
/// proof; nothing that would let an attacker who steals the DB
/// log in as the user (they'd need to also brute-force the PBKDF2
/// to recover the password).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; HASH_LEN],
    pub server_key: [u8; HASH_LEN],
}

/// Generate a fresh set of credentials for a new user. Caller
/// supplies the plaintext password; we own salt generation +
/// key derivation. Returns the tuple to persist.
pub fn derive_credentials(password: &str, iterations: u32) -> Credentials {
    let mut salt = vec![0u8; SALT_LEN];
    rand::thread_rng().fill_bytes(&mut salt);
    derive_credentials_with_salt(password, &salt, iterations)
}

/// Deterministic variant used by tests to pin salt for RFC
/// vector reproduction. Production code always takes the
/// random-salt path above.
pub fn derive_credentials_with_salt(password: &str, salt: &[u8], iterations: u32) -> Credentials {
    let salted = salted_password(password, salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let server_key = hmac_sha256(&salted, b"Server Key");
    let stored_key = sha256(&client_key);
    Credentials {
        iterations,
        salt: salt.to_vec(),
        stored_key,
        server_key,
    }
}

/// Verify a client proof against stored credentials.
///
/// Returns `Ok(ServerSignature)` on success, `Err(VerifyError)`
/// on any failure. The verify path is constant-time w.r.t. the
/// stored key — timing attacks can't distinguish "wrong password"
/// from "wrong auth message" because both take the same code
/// path.
///
/// `auth_message` is the concatenation required by RFC 5802 §3:
/// `client-first-bare + "," + server-first + "," +
/// client-final-without-proof`. The SASL handler assembles this
/// string before calling us.
pub fn verify_client_proof(
    creds: &Credentials,
    auth_message: &[u8],
    client_proof: &[u8],
) -> Result<[u8; HASH_LEN], VerifyError> {
    if client_proof.len() != HASH_LEN {
        return Err(VerifyError::MalformedProof);
    }
    let client_signature = hmac_sha256(&creds.stored_key, auth_message);
    // Reconstruct ClientKey = ClientProof XOR ClientSignature.
    let mut client_key = [0u8; HASH_LEN];
    for i in 0..HASH_LEN {
        client_key[i] = client_proof[i] ^ client_signature[i];
    }
    // Check H(ClientKey) == StoredKey in constant time.
    let candidate = sha256(&client_key);
    if candidate.ct_eq(&creds.stored_key).unwrap_u8() == 0 {
        return Err(VerifyError::BadProof);
    }
    // Return ServerSignature so the caller can send it as the
    // server-final message.
    Ok(hmac_sha256(&creds.server_key, auth_message))
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("client proof is the wrong length")]
    MalformedProof,
    #[error("client proof failed verification")]
    BadProof,
}

fn salted_password(password: &str, salt: &[u8], iter: u32) -> [u8; HASH_LEN] {
    let mut out = [0u8; HASH_LEN];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iter, &mut out);
    out
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; HASH_LEN] {
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC-SHA-256 accepts any key length");
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; HASH_LEN];
    out.copy_from_slice(&bytes);
    out
}

fn sha256(data: &[u8]) -> [u8; HASH_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let bytes = hasher.finalize();
    let mut out = [0u8; HASH_LEN];
    out.copy_from_slice(&bytes);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Base64-decode helper for test constants.
    fn b64(s: &str) -> Vec<u8> {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine;
        B64.decode(s).unwrap()
    }

    /// RFC 7677 §3 / MongoDB SCRAM test vector.
    /// user: "user", password: "pencil",
    /// salt: "W22ZaJ0SNY7soEsUEjb6gQ==" (base64),
    /// iterations: 4096.
    /// Expected:
    ///   StoredKey b64: WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=
    ///   ServerKey b64: wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=
    ///
    /// (Hex form of the proof math sanity-check comes from
    ///  ClientProof = UMWeI+lP1LwzkXJd1cOW2v1D7gQzInc2ExQZVpeW8iE=
    ///  which we exercise in the handshake-level tests; here we
    ///  pin the derivation step alone.)
    #[test]
    fn derive_keys_matches_rfc_7677_vector() {
        let salt = b64("W22ZaJ0SNY7soEsUEjb6gQ==");
        let creds = derive_credentials_with_salt("pencil", &salt, 4096);
        let expected_stored = b64("WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=");
        let expected_server = b64("wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=");
        assert_eq!(
            creds.stored_key.as_slice(),
            expected_stored.as_slice(),
            "StoredKey mismatch vs. RFC 7677 vector"
        );
        assert_eq!(
            creds.server_key.as_slice(),
            expected_server.as_slice(),
            "ServerKey mismatch vs. RFC 7677 vector"
        );
        assert_eq!(creds.iterations, 4096);
        assert_eq!(creds.salt, salt);
    }

    #[test]
    fn derive_credentials_uses_random_salt() {
        let a = derive_credentials("hunter2", 4096);
        let b = derive_credentials("hunter2", 4096);
        assert_ne!(a.salt, b.salt, "two calls must pick distinct salts");
        // Same password + different salt → different keys.
        assert_ne!(a.stored_key, b.stored_key);
    }

    /// Round-trip: derive creds from a password, then simulate a
    /// correct handshake (compute ClientSignature, XOR with ClientKey
    /// to get ClientProof) and verify we accept it. Uses
    /// derive-time intermediates that the real handshake never
    /// exposes, but we need them to synthesize a valid proof in
    /// the test.
    #[test]
    fn verify_accepts_valid_proof() {
        let password = "correct-horse-battery-staple";
        let salt = vec![1u8; 16];
        let creds = derive_credentials_with_salt(password, &salt, 4096);
        let salted = salted_password(password, &salt, 4096);
        let client_key = hmac_sha256(&salted, b"Client Key");

        let auth_message = b"client-first-bare,server-first,client-final-without-proof";
        let client_signature = hmac_sha256(&creds.stored_key, auth_message);
        let mut client_proof = [0u8; HASH_LEN];
        for i in 0..HASH_LEN {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }

        let server_sig = verify_client_proof(&creds, auth_message, &client_proof).unwrap();
        let expected = hmac_sha256(&creds.server_key, auth_message);
        assert_eq!(server_sig, expected);
    }

    #[test]
    fn verify_rejects_tampered_proof() {
        let creds = derive_credentials("hunter2", 4096);
        let bad_proof = [0xFFu8; HASH_LEN];
        let err = verify_client_proof(&creds, b"auth-message", &bad_proof).unwrap_err();
        assert_eq!(err, VerifyError::BadProof);
    }

    #[test]
    fn verify_rejects_short_proof() {
        let creds = derive_credentials("hunter2", 4096);
        let short = [0u8; 10];
        let err = verify_client_proof(&creds, b"auth-message", &short).unwrap_err();
        assert_eq!(err, VerifyError::MalformedProof);
    }
}
