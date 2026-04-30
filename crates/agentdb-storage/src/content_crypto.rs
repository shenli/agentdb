//! Envelope encryption/decryption adapter boundary for artifact content.
//!
//! The concrete crypto/KMS implementation is intentionally outside the
//! storage crate. This module defines the stable boundary that the runtime can
//! call before bytes cross the artifact content backend.

use std::io;

use crate::kms::KeyResolution;

const ENVELOPE_MAGIC: &[u8] = b"AGDB-ENVELOPE-v1\n";
const NONE_LEN: u32 = u32::MAX;

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

pub fn encode_keyed_envelope(key: &KeyResolution, ciphertext: &[u8]) -> Vec<u8> {
    let key_id = key.key_id.as_bytes();
    let key_version = key.key_version.as_deref().map(str::as_bytes);
    let mut out = Vec::with_capacity(
        ENVELOPE_MAGIC.len()
            + 8
            + key_id.len()
            + key_version.map_or(0, <[u8]>::len)
            + ciphertext.len(),
    );
    out.extend_from_slice(ENVELOPE_MAGIC);
    out.extend_from_slice(&(key_id.len() as u32).to_be_bytes());
    out.extend_from_slice(
        &key_version
            .map(|v| v.len() as u32)
            .unwrap_or(NONE_LEN)
            .to_be_bytes(),
    );
    out.extend_from_slice(key_id);
    if let Some(version) = key_version {
        out.extend_from_slice(version);
    }
    out.extend_from_slice(ciphertext);
    out
}

pub fn decode_keyed_envelope(bytes: &[u8]) -> io::Result<Option<(KeyResolution, Vec<u8>)>> {
    if !bytes.starts_with(ENVELOPE_MAGIC) {
        return Ok(None);
    }
    let mut offset = ENVELOPE_MAGIC.len();
    let key_id_len = read_u32(bytes, &mut offset)? as usize;
    let key_version_len = read_u32(bytes, &mut offset)?;
    let key_id = read_utf8(bytes, &mut offset, key_id_len, "key_id")?;
    let key_version = if key_version_len == NONE_LEN {
        None
    } else {
        Some(read_utf8(
            bytes,
            &mut offset,
            key_version_len as usize,
            "key_version",
        )?)
    };
    Ok(Some((
        KeyResolution {
            key_id,
            key_version,
        },
        bytes[offset..].to_vec(),
    )))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> io::Result<u32> {
    let end = offset.saturating_add(4);
    let chunk = bytes.get(*offset..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated keyed envelope length",
        )
    })?;
    *offset = end;
    Ok(u32::from_be_bytes(
        chunk.try_into().expect("slice length checked"),
    ))
}

fn read_utf8(bytes: &[u8], offset: &mut usize, len: usize, field: &str) -> io::Result<String> {
    let end = offset.saturating_add(len);
    let chunk = bytes.get(*offset..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("truncated keyed envelope {field}"),
        )
    })?;
    *offset = end;
    std::str::from_utf8(chunk)
        .map(str::to_string)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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

    #[test]
    fn keyed_envelope_round_trips_key_metadata_and_ciphertext() {
        let key = KeyResolution {
            key_id: "tenant:ten_a:default".into(),
            key_version: Some("v2".into()),
        };
        let encoded = encode_keyed_envelope(&key, b"ciphertext");
        let (decoded_key, ciphertext) = decode_keyed_envelope(&encoded).unwrap().unwrap();
        assert_eq!(decoded_key.key_id, key.key_id);
        assert_eq!(decoded_key.key_version, key.key_version);
        assert_eq!(ciphertext, b"ciphertext");
        assert!(decode_keyed_envelope(b"plaintext").unwrap().is_none());
    }
}
