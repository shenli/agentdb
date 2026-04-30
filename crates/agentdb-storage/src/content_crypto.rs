//! Envelope encryption/decryption adapter boundary for artifact content.
//!
//! The concrete crypto/KMS implementation is intentionally outside the
//! storage crate. This module defines the stable boundary that the runtime can
//! call before bytes cross the artifact content backend.

use std::io;

use crate::kms::KeyResolution;

#[derive(Debug, Clone)]
pub struct ContentCryptoContext<'a> {
    pub tenant_id: &'a str,
    pub artifact_id: &'a str,
    pub key: &'a KeyResolution,
}

pub trait ContentCryptoAdapter: Send + Sync {
    fn encrypt(&self, context: &ContentCryptoContext<'_>, plaintext: &[u8]) -> io::Result<Vec<u8>>;

    fn decrypt(&self, context: &ContentCryptoContext<'_>, ciphertext: &[u8])
        -> io::Result<Vec<u8>>;
}

#[derive(Debug, Default)]
pub struct IdentityContentCryptoAdapter;

impl ContentCryptoAdapter for IdentityContentCryptoAdapter {
    fn encrypt(
        &self,
        _context: &ContentCryptoContext<'_>,
        plaintext: &[u8],
    ) -> io::Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(
        &self,
        _context: &ContentCryptoContext<'_>,
        ciphertext: &[u8],
    ) -> io::Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_adapter_round_trips_bytes() {
        let key = KeyResolution {
            key_id: "tenant:ten_a:default".into(),
            key_version: Some("v1".into()),
        };
        let context = ContentCryptoContext {
            tenant_id: "ten_a",
            artifact_id: "art_a",
            key: &key,
        };

        let encrypted = IdentityContentCryptoAdapter
            .encrypt(&context, b"plaintext")
            .unwrap();
        assert_eq!(encrypted, b"plaintext");

        let decrypted = IdentityContentCryptoAdapter
            .decrypt(&context, &encrypted)
            .unwrap();
        assert_eq!(decrypted, b"plaintext");
    }
}
