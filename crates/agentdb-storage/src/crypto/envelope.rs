//! `AGDB-ENVELOPE-v2` — the on-disk framing for an authenticated-encryption
//! payload (see `docs/design/encryption-at-rest.md`).
//!
//! The frame is self-describing (suite, key id, key version, nonce) so a reader
//! can select the right DEK version and algorithm without out-of-band state,
//! and it is deliberately distinct from the legacy `AGDB-ENVELOPE-v1` framing:
//! v1 is *framed plaintext* from the identity adapters and must never be
//! mistaken for authenticated ciphertext.
//!
//! Every field is length-checked; a truncated, over-long, or malformed frame
//! fails closed. The header carries no secrets — the ciphertext and its
//! authentication tag are the only protected bytes, and the security-relevant
//! header fields (suite, key version) are additionally bound into the AEAD AAD
//! by the keyring, so tampering with them makes decryption fail.

use super::{CryptoError, Suite};

/// Magic prefix. The trailing newline and `v2` make it visually distinct and
/// keep it from ever colliding with `AGDB-ENVELOPE-v1\n`.
pub(crate) const ENVELOPE_V2_MAGIC: &[u8] = b"AGDB-ENVELOPE-v2\n";
pub(crate) const ENVELOPE_V2_VERSION: u8 = 2;

/// Defensive upper bounds so a corrupt length can never drive a huge
/// allocation. A DEK id is a short label like `tenant:ten_x:content`.
const MAX_DEK_ID_LEN: usize = 512;
const MAX_NONCE_LEN: usize = 32;
/// 64 MiB per individually sealed value — far above any real event, metadata,
/// or artifact chunk, but a hard ceiling against a malformed length.
const MAX_CIPHERTEXT_LEN: usize = 64 * 1024 * 1024;

/// A parsed envelope header plus a borrow of its ciphertext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeParts<'a> {
    pub suite: Suite,
    pub dek_id: &'a str,
    pub dek_version: u32,
    pub nonce: &'a [u8],
    /// Ciphertext with the trailing AEAD tag appended.
    pub ciphertext: &'a [u8],
}

/// Whether `bytes` is an `AGDB-ENVELOPE-v2` frame (a cheap magic check the
/// mixed-version reader uses to route v2 vs v1/plaintext).
pub fn is_envelope_v2(bytes: &[u8]) -> bool {
    bytes.starts_with(ENVELOPE_V2_MAGIC)
}

/// Encode a v2 envelope.
///
/// Layout (integers big-endian): `AGDB-ENVELOPE-v2\n` | version:u8 | suite:u8 |
/// dek_id_len:u16 | dek_version:u32 | nonce_len:u8 | ciphertext_len:u32 |
/// dek_id | nonce | ciphertext(+tag).
pub fn encode(parts: &EnvelopeParts<'_>) -> Vec<u8> {
    let dek_id = parts.dek_id.as_bytes();
    let mut out = Vec::with_capacity(
        ENVELOPE_V2_MAGIC.len() + 12 + dek_id.len() + parts.nonce.len() + parts.ciphertext.len(),
    );
    out.extend_from_slice(ENVELOPE_V2_MAGIC);
    out.push(ENVELOPE_V2_VERSION);
    out.push(parts.suite as u8);
    out.extend_from_slice(&(dek_id.len() as u16).to_be_bytes());
    out.extend_from_slice(&parts.dek_version.to_be_bytes());
    out.push(parts.nonce.len() as u8);
    out.extend_from_slice(&(parts.ciphertext.len() as u32).to_be_bytes());
    out.extend_from_slice(dek_id);
    out.extend_from_slice(parts.nonce);
    out.extend_from_slice(parts.ciphertext);
    out
}

/// Decode a v2 envelope, or fail closed with an actionable error. Borrows the
/// nonce/ciphertext out of `bytes`.
pub fn decode(bytes: &[u8]) -> Result<EnvelopeParts<'_>, CryptoError> {
    let mut cur = Cursor::new(bytes);
    let magic = cur.take(ENVELOPE_V2_MAGIC.len())?;
    if magic != ENVELOPE_V2_MAGIC {
        return Err(CryptoError::MalformedEnvelope("bad magic".into()));
    }
    let version = cur.u8()?;
    if version != ENVELOPE_V2_VERSION {
        return Err(CryptoError::MalformedEnvelope(format!(
            "unsupported envelope version {version}"
        )));
    }
    let suite = Suite::from_u8(cur.u8()?)
        .ok_or_else(|| CryptoError::MalformedEnvelope("unknown AEAD suite".into()))?;
    let dek_id_len = cur.u16()? as usize;
    let dek_version = cur.u32()?;
    let nonce_len = cur.u8()? as usize;
    let ciphertext_len = cur.u32()? as usize;

    if dek_id_len > MAX_DEK_ID_LEN {
        return Err(CryptoError::MalformedEnvelope("dek_id too long".into()));
    }
    if nonce_len != suite.nonce_len() || nonce_len > MAX_NONCE_LEN {
        return Err(CryptoError::MalformedEnvelope(
            "nonce length does not match suite".into(),
        ));
    }
    if ciphertext_len > MAX_CIPHERTEXT_LEN {
        return Err(CryptoError::MalformedEnvelope("ciphertext too long".into()));
    }

    let dek_id_bytes = cur.take(dek_id_len)?;
    let dek_id = std::str::from_utf8(dek_id_bytes)
        .map_err(|_| CryptoError::MalformedEnvelope("dek_id is not utf-8".into()))?;
    let nonce = cur.take(nonce_len)?;
    let ciphertext = cur.take(ciphertext_len)?;
    if !cur.is_empty() {
        return Err(CryptoError::MalformedEnvelope(
            "trailing bytes after ciphertext".into(),
        ));
    }
    Ok(EnvelopeParts {
        suite,
        dek_id,
        dek_version,
        nonce,
        ciphertext,
    })
}

/// A minimal bounded reader — every read checks the remaining length so a
/// crafted frame can never over-read or drive a large allocation.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], CryptoError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| CryptoError::MalformedEnvelope("length overflow".into()))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| CryptoError::MalformedEnvelope("truncated envelope".into()))?;
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> Result<u8, CryptoError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CryptoError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, CryptoError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn is_empty(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<u8> {
        encode(&EnvelopeParts {
            suite: Suite::XChaCha20Poly1305,
            dek_id: "tenant:ten_a:content",
            dek_version: 7,
            nonce: &[0x11u8; 24],
            ciphertext: b"opaque-ciphertext-and-tag",
        })
    }

    #[test]
    fn round_trips() {
        let bytes = sample();
        assert!(is_envelope_v2(&bytes));
        let parts = decode(&bytes).unwrap();
        assert_eq!(parts.suite, Suite::XChaCha20Poly1305);
        assert_eq!(parts.dek_id, "tenant:ten_a:content");
        assert_eq!(parts.dek_version, 7);
        assert_eq!(parts.nonce, &[0x11u8; 24]);
        assert_eq!(parts.ciphertext, b"opaque-ciphertext-and-tag");
    }

    #[test]
    fn v1_and_plaintext_are_not_v2() {
        assert!(!is_envelope_v2(b"AGDB-ENVELOPE-v1\n....."));
        assert!(!is_envelope_v2(b"just plaintext"));
        assert!(decode(b"AGDB-ENVELOPE-v1\n").is_err());
    }

    #[test]
    fn truncation_fails_closed() {
        let bytes = sample();
        for cut in 0..bytes.len() {
            assert!(decode(&bytes[..cut]).is_err(), "prefix len {cut} decoded");
        }
    }

    #[test]
    fn trailing_bytes_fail_closed() {
        let mut bytes = sample();
        bytes.push(0);
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn nonce_length_must_match_suite() {
        // A GCM suite (12-byte nonce) with a 24-byte nonce is rejected.
        let bytes = encode(&EnvelopeParts {
            suite: Suite::Aes256Gcm,
            dek_id: "k",
            dek_version: 1,
            nonce: &[0u8; 24],
            ciphertext: b"x",
        });
        assert!(decode(&bytes).is_err());
    }

    #[test]
    fn golden_vector_pins_the_wire_format() {
        // A hand-constructed byte sequence the encoder must reproduce exactly.
        // If this test fails, the on-disk envelope format changed — bump the
        // version rather than silently break existing ciphertext.
        let mut expected = Vec::new();
        expected.extend_from_slice(b"AGDB-ENVELOPE-v2\n");
        expected.push(2); // version
        expected.push(2); // suite = XChaCha20-Poly1305
        expected.extend_from_slice(&1u16.to_be_bytes()); // dek_id_len
        expected.extend_from_slice(&1u32.to_be_bytes()); // dek_version
        expected.push(24); // nonce_len
        expected.extend_from_slice(&3u32.to_be_bytes()); // ciphertext_len
        expected.push(b'k'); // dek_id
        expected.extend_from_slice(&[0xAA; 24]); // nonce
        expected.extend_from_slice(&[0xBB; 3]); // ciphertext

        let got = encode(&EnvelopeParts {
            suite: Suite::XChaCha20Poly1305,
            dek_id: "k",
            dek_version: 1,
            nonce: &[0xAA; 24],
            ciphertext: &[0xBB; 3],
        });
        assert_eq!(got, expected);
        let parts = decode(&expected).unwrap();
        assert_eq!(parts.dek_id, "k");
        assert_eq!(parts.dek_version, 1);
    }

    proptest::proptest! {
        // The parser must never panic on arbitrary input — it either decodes a
        // well-formed frame or returns an error. (Fuzz target for the design's
        // "fuzz the parser" requirement.)
        #[test]
        fn decode_never_panics_on_arbitrary_bytes(bytes: Vec<u8>) {
            let _ = decode(&bytes);
        }

        // Any frame that decodes must round-trip back to the same bytes.
        #[test]
        fn decoded_frames_reencode_identically(bytes: Vec<u8>) {
            if let Ok(parts) = decode(&bytes) {
                proptest::prop_assert_eq!(encode(&parts), bytes);
            }
        }
    }
}
