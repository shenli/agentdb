//! Envelope encryption/decryption adapter boundary for metadata/event rows.
//!
//! This mirrors the artifact content crypto boundary while staying independent
//! from any concrete KMS provider. Runtime persistence code supplies a stable
//! tenant/object context before bytes cross the KV storage boundary.

use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataObjectKind {
    Artifact,
    Event,
}

impl MetadataObjectKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Artifact => "artifact",
            Self::Event => "event",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetadataCryptoContext<'a> {
    pub tenant_id: &'a str,
    pub object_kind: MetadataObjectKind,
    pub object_id: &'a str,
}

pub trait MetadataCryptoAdapter: Send + Sync {
    fn encrypt(&self, context: &MetadataCryptoContext<'_>, plaintext: &[u8])
        -> io::Result<Vec<u8>>;

    fn decrypt(
        &self,
        context: &MetadataCryptoContext<'_>,
        ciphertext: &[u8],
    ) -> io::Result<Vec<u8>>;
}

#[derive(Debug, Default)]
pub struct IdentityMetadataCryptoAdapter;

impl MetadataCryptoAdapter for IdentityMetadataCryptoAdapter {
    fn encrypt(
        &self,
        _context: &MetadataCryptoContext<'_>,
        plaintext: &[u8],
    ) -> io::Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(
        &self,
        _context: &MetadataCryptoContext<'_>,
        ciphertext: &[u8],
    ) -> io::Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_adapter_round_trips_metadata_bytes() {
        let context = MetadataCryptoContext {
            tenant_id: "ten_a",
            object_kind: MetadataObjectKind::Event,
            object_id: "evt_a",
        };

        let encrypted = IdentityMetadataCryptoAdapter
            .encrypt(&context, b"{\"kind\":\"message\"}")
            .unwrap();
        assert_eq!(encrypted, b"{\"kind\":\"message\"}");

        let decrypted = IdentityMetadataCryptoAdapter
            .decrypt(&context, &encrypted)
            .unwrap();
        assert_eq!(decrypted, b"{\"kind\":\"message\"}");
    }
}
