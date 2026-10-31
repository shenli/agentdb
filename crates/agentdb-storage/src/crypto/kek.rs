//! Key-encryption-key (KEK) wrapping — the "envelope" over the data keys.
//!
//! A KEK never leaves its manager; it only *wraps* (encrypts) and *unwraps*
//! (decrypts) per-tenant data-encryption keys (DEKs). AgentDB stores only
//! wrapped DEKs durably (the key catalog), and unwraps a DEK infrequently — on
//! a cache miss or a key operation — so steady-state traffic makes no KEK/KMS
//! calls. This module defines the wrapping seam and a local implementation; a
//! real KMS-backed KEK (AWS KMS, GCP KMS, …) is a drop-in `KekProvider`.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use super::{CryptoError, KeyPurpose};

/// A DEK wrapped under a KEK: the reference to the wrapping key, the nonce, and
/// the ciphertext+tag. Contains no plaintext key material — safe to store in the
/// (plaintext) key catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrappedDek {
    /// Identifies which KEK wrapped this DEK, so a rewrap/rotation can find and
    /// re-wrap DEKs when the KEK changes.
    pub wrapping_key_ref: String,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

/// Wraps and unwraps DEK material under a key-encryption key. The KEK stays
/// inside the implementation (or an external KMS); only wrapped DEKs cross this
/// boundary.
pub trait KekProvider: Send + Sync {
    /// Encrypt a 32-byte DEK under the KEK, bound to (tenant, purpose) so a
    /// wrapped DEK cannot be unwrapped for a different tenant or purpose.
    fn wrap_dek(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        dek: &[u8; 32],
    ) -> Result<WrappedDek, CryptoError>;

    /// Decrypt a wrapped DEK back to its 32 bytes. Fails closed if the KEK ref
    /// is unknown, the (tenant, purpose) does not match, or the tag is invalid.
    fn unwrap_dek(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        wrapped: &WrappedDek,
    ) -> Result<Zeroizing<[u8; 32]>, CryptoError>;

    /// The reference recorded in the catalog for DEKs this provider wraps.
    fn kek_ref(&self) -> &str;
}

/// A local KEK held in process, derived from a configured secret. Suitable for
/// single-operator / development / testing; a production BYOK deployment swaps
/// in a KMS-backed `KekProvider`. Wrapping uses XChaCha20-Poly1305 (24-byte
/// random nonce — collision-safe).
pub struct LocalKekProvider {
    kek: Zeroizing<[u8; 32]>,
    kek_ref: String,
}

impl LocalKekProvider {
    /// Derive the KEK from an arbitrary-length secret. `kek_ref` labels this KEK
    /// in the catalog (e.g. `local:v1`).
    pub fn from_secret(secret: &[u8], kek_ref: impl Into<String>) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(b"agentdb-kek-v1"), secret);
        let mut kek = [0u8; 32];
        hk.expand(b"kek", &mut kek)
            .expect("32 bytes is a valid HKDF length");
        let provider = Self {
            kek: Zeroizing::new(kek),
            kek_ref: kek_ref.into(),
        };
        kek.zeroize();
        provider
    }

    fn aad(tenant_id: &str, purpose: KeyPurpose) -> Vec<u8> {
        let mut aad = Vec::new();
        aad.extend_from_slice(b"AGDB-KEK-AAD-v1");
        for field in [tenant_id.as_bytes(), purpose.as_str().as_bytes()] {
            aad.extend_from_slice(&(field.len() as u32).to_be_bytes());
            aad.extend_from_slice(field);
        }
        aad
    }
}

impl KekProvider for LocalKekProvider {
    fn wrap_dek(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        dek: &[u8; 32],
    ) -> Result<WrappedDek, CryptoError> {
        let cipher = XChaCha20Poly1305::new_from_slice(self.kek.as_slice()).expect("32-byte KEK");
        let mut nonce = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut nonce);
        let aad = Self::aad(tenant_id, purpose);
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: dek.as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::AuthenticationFailed)?;
        Ok(WrappedDek {
            wrapping_key_ref: self.kek_ref.clone(),
            nonce: nonce.to_vec(),
            ciphertext,
        })
    }

    fn unwrap_dek(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        wrapped: &WrappedDek,
    ) -> Result<Zeroizing<[u8; 32]>, CryptoError> {
        if wrapped.wrapping_key_ref != self.kek_ref {
            return Err(CryptoError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                purpose: purpose.as_str(),
                reason: format!(
                    "wrapped under KEK {:?}, but this provider is {:?}",
                    wrapped.wrapping_key_ref, self.kek_ref
                ),
            });
        }
        let cipher = XChaCha20Poly1305::new_from_slice(self.kek.as_slice()).expect("32-byte KEK");
        let aad = Self::aad(tenant_id, purpose);
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(&wrapped.nonce),
                Payload {
                    msg: &wrapped.ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::AuthenticationFailed)?;
        if plaintext.len() != 32 {
            return Err(CryptoError::MalformedEnvelope(
                "unwrapped DEK is not 32 bytes".into(),
            ));
        }
        let mut dek = [0u8; 32];
        dek.copy_from_slice(&plaintext);
        Ok(Zeroizing::new(dek))
    }

    fn kek_ref(&self) -> &str {
        &self.kek_ref
    }
}

/// A KEK backed by an **external command** — the KMS-delegation seam. The KEK
/// itself never enters this process: wrapping and unwrapping a DEK invoke a
/// configured program (e.g. a thin wrapper over `aws kms encrypt|decrypt`,
/// `vault transit`, or an HSM CLI), which performs the crypto and returns the
/// result. AgentDB only ever holds the plaintext DEK momentarily and the wrapped
/// blob.
///
/// **Invocation contract.** The program is run once per operation with:
/// - env `AGENTDB_KEK_OP` = `wrap` | `unwrap`, `AGENTDB_KEK_TENANT`,
///   `AGENTDB_KEK_PURPOSE`, `AGENTDB_KEK_REF` — the wrapper should bind
///   tenant+purpose as the KMS encryption context so a blob cannot be unwrapped
///   for a different tenant/purpose (this provider still rejects a mismatched
///   `wrapping_key_ref` up front, but the authoritative binding is the KMS's);
/// - **stdin** = the raw input bytes (the 32-byte DEK for `wrap`; the wrapped
///   blob for `unwrap`);
/// - **stdout** = the raw output bytes (the wrapped blob for `wrap`; the 32-byte
///   DEK for `unwrap`).
///
/// A non-zero exit fails closed. Because DEKs are cached, this runs only on a
/// cache miss or a key operation, so the per-op process cost is amortized away.
pub struct CommandKekProvider {
    program: String,
    args: Vec<String>,
    kek_ref: String,
}

impl CommandKekProvider {
    pub fn new(program: impl Into<String>, args: Vec<String>, kek_ref: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args,
            kek_ref: kek_ref.into(),
        }
    }

    fn run(
        &self,
        op: &str,
        tenant_id: &str,
        purpose: KeyPurpose,
        input: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        use std::io::Write;
        use std::process::{Command, Stdio};

        let mut child = Command::new(&self.program)
            .args(&self.args)
            .env("AGENTDB_KEK_OP", op)
            .env("AGENTDB_KEK_TENANT", tenant_id)
            .env("AGENTDB_KEK_PURPOSE", purpose.as_str())
            .env("AGENTDB_KEK_REF", &self.kek_ref)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                CryptoError::Config(format!(
                    "KEK command {:?} failed to start: {e}",
                    self.program
                ))
            })?;
        // The payload (a 32-byte DEK or a small wrapped blob) is far below any
        // pipe buffer, so writing it in full before reading stdout cannot
        // deadlock.
        child
            .stdin
            .take()
            .expect("stdin piped")
            .write_all(input)
            .map_err(|e| CryptoError::Config(format!("KEK command stdin write: {e}")))?;
        let output = child.wait_with_output().map_err(|e| {
            CryptoError::Config(format!("KEK command {:?} wait: {e}", self.program))
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CryptoError::Config(format!(
                "KEK command {:?} {op} exited with {}: {}",
                self.program,
                output.status,
                stderr.trim()
            )));
        }
        Ok(output.stdout)
    }
}

impl KekProvider for CommandKekProvider {
    fn wrap_dek(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        dek: &[u8; 32],
    ) -> Result<WrappedDek, CryptoError> {
        let ciphertext = self.run("wrap", tenant_id, purpose, dek.as_slice())?;
        if ciphertext.is_empty() {
            return Err(CryptoError::Config(
                "KEK command produced an empty wrapped DEK".into(),
            ));
        }
        Ok(WrappedDek {
            wrapping_key_ref: self.kek_ref.clone(),
            // The external system's ciphertext is self-describing (it carries its
            // own nonce/metadata), so no separate nonce is stored here.
            nonce: Vec::new(),
            ciphertext,
        })
    }

    fn unwrap_dek(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        wrapped: &WrappedDek,
    ) -> Result<Zeroizing<[u8; 32]>, CryptoError> {
        if wrapped.wrapping_key_ref != self.kek_ref {
            return Err(CryptoError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                purpose: purpose.as_str(),
                reason: format!(
                    "wrapped under KEK {:?}, but this provider is {:?}",
                    wrapped.wrapping_key_ref, self.kek_ref
                ),
            });
        }
        let mut plaintext = self.run("unwrap", tenant_id, purpose, &wrapped.ciphertext)?;
        if plaintext.len() != 32 {
            plaintext.zeroize();
            return Err(CryptoError::MalformedEnvelope(
                "KEK command returned a DEK that is not 32 bytes".into(),
            ));
        }
        let mut dek = [0u8; 32];
        dek.copy_from_slice(&plaintext);
        plaintext.zeroize();
        Ok(Zeroizing::new(dek))
    }

    fn kek_ref(&self) -> &str {
        &self.kek_ref
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_then_unwrap_round_trips() {
        let kek = LocalKekProvider::from_secret(b"kek-secret", "local:v1");
        let dek = [7u8; 32];
        let wrapped = kek.wrap_dek("ten_a", KeyPurpose::Content, &dek).unwrap();
        assert_eq!(wrapped.wrapping_key_ref, "local:v1");
        assert_ne!(wrapped.ciphertext, dek.to_vec(), "DEK stored in the clear");
        let unwrapped = kek
            .unwrap_dek("ten_a", KeyPurpose::Content, &wrapped)
            .unwrap();
        assert_eq!(unwrapped.as_slice(), &dek);
    }

    #[test]
    fn unwrap_for_wrong_tenant_or_purpose_fails() {
        let kek = LocalKekProvider::from_secret(b"kek-secret", "local:v1");
        let dek = [7u8; 32];
        let wrapped = kek.wrap_dek("ten_a", KeyPurpose::Content, &dek).unwrap();
        assert!(kek
            .unwrap_dek("ten_b", KeyPurpose::Content, &wrapped)
            .is_err());
        assert!(kek
            .unwrap_dek("ten_a", KeyPurpose::Metadata, &wrapped)
            .is_err());
    }

    #[test]
    fn unwrap_under_a_different_kek_fails_closed() {
        let kek1 = LocalKekProvider::from_secret(b"secret-1", "local:v1");
        let kek2 = LocalKekProvider::from_secret(b"secret-2", "local:v2");
        let wrapped = kek1
            .wrap_dek("ten_a", KeyPurpose::Content, &[7u8; 32])
            .unwrap();
        // Different ref → rejected before even attempting decryption.
        assert!(kek2
            .unwrap_dek("ten_a", KeyPurpose::Content, &wrapped)
            .is_err());
    }

    #[test]
    fn tampered_wrapped_dek_fails() {
        let kek = LocalKekProvider::from_secret(b"kek-secret", "local:v1");
        let mut wrapped = kek
            .wrap_dek("ten_a", KeyPurpose::Content, &[7u8; 32])
            .unwrap();
        wrapped.ciphertext[0] ^= 0x01;
        assert!(kek
            .unwrap_dek("ten_a", KeyPurpose::Content, &wrapped)
            .is_err());
    }

    // A portable mock "KMS": `wrap` prepends a fixed 4-byte header, `unwrap`
    // strips it (both binary-safe via cat/dd). A real deployment plugs in a
    // wrapper over its KMS/HSM here.
    fn mock_command_kek(kek_ref: &str) -> CommandKekProvider {
        let script = r#"if [ "$AGENTDB_KEK_OP" = wrap ]; then printf 'WRP1'; cat; else dd bs=1 skip=4 2>/dev/null; fi"#;
        CommandKekProvider::new("sh", vec!["-c".into(), script.into()], kek_ref)
    }

    #[test]
    fn command_kek_round_trips_through_an_external_process() {
        let kek = mock_command_kek("cmd:v1");
        // Include a null byte and 0xff to exercise binary-safety of the pipe.
        let dek: [u8; 32] = std::array::from_fn(|i| i as u8);
        let wrapped = kek.wrap_dek("ten_a", KeyPurpose::Content, &dek).unwrap();
        assert_eq!(wrapped.wrapping_key_ref, "cmd:v1");
        assert!(
            wrapped.ciphertext.starts_with(b"WRP1"),
            "the external command actually ran and transformed the DEK"
        );
        assert_ne!(
            wrapped.ciphertext,
            dek.to_vec(),
            "DEK is not stored in the clear"
        );
        let unwrapped = kek
            .unwrap_dek("ten_a", KeyPurpose::Content, &wrapped)
            .unwrap();
        assert_eq!(unwrapped.as_slice(), &dek);
    }

    #[test]
    fn command_kek_rejects_a_foreign_ref_before_spawning() {
        let kek = mock_command_kek("cmd:v1");
        let wrapped = WrappedDek {
            wrapping_key_ref: "cmd:other".into(),
            nonce: vec![],
            ciphertext: b"WRP1anything".to_vec(),
        };
        assert!(kek
            .unwrap_dek("ten_a", KeyPurpose::Content, &wrapped)
            .is_err());
    }

    #[test]
    fn command_kek_fails_closed_on_a_nonzero_exit() {
        let kek = CommandKekProvider::new("sh", vec!["-c".into(), "exit 3".into()], "cmd:v1");
        assert!(kek
            .wrap_dek("ten_a", KeyPurpose::Content, &[9u8; 32])
            .is_err());
    }

    #[test]
    fn command_kek_fails_closed_when_the_program_is_missing() {
        let kek = CommandKekProvider::new("agentdb-nonexistent-kek-helper-xyz", vec![], "cmd:v1");
        assert!(kek
            .wrap_dek("ten_a", KeyPurpose::Content, &[9u8; 32])
            .is_err());
    }
}
