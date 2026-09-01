//! Tenant envelope encryption (`docs/design/encryption-at-rest.md`).
//!
//! This is the cohesive crypto core the design's "Proposed Code Changes" §1–3
//! call for: an [`EnvelopeCrypto`] keyring that seals/opens tenant payloads
//! under per-tenant, per-purpose data-encryption keys (DEKs) with authenticated
//! encryption, the self-describing [`envelope`] v2 frame, a bounded plaintext-DEK
//! cache, and explicit secure/insecure [`config`].
//!
//! Milestone 1 delivers the core and its config **without changing any stored
//! data** — no durable path calls it yet. Wiring it into the artifact-content
//! path (and then events/metadata) follows in later milestones, behind the
//! explicit `envelope` mode so identity/plaintext behavior can never be
//! selected silently.

mod config;
mod envelope;
mod keyring;

pub use config::{CryptoConfig, CryptoMode, DekCacheConfig};
pub use envelope::{is_envelope_v2, EnvelopeParts};
pub use keyring::{
    AeadTenantKeyring, CryptoContext, Dek, DekLease, DisabledEnvelopeCrypto, EnvelopeCrypto,
    KeyMetadata, KeyProvider, LocalKeyProvider,
};

/// AEAD suite selecting the authenticated-encryption algorithm. The chosen
/// suite is recorded in the envelope (so a reader needs no out-of-band state)
/// and bound into the AAD (so it cannot be downgraded without breaking
/// decryption).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Suite {
    /// AES-256-GCM (12-byte nonce). FIPS-friendly and hardware-accelerated;
    /// requires care with nonce uniqueness (random 96-bit nonces have a
    /// birthday bound, so rotate a DEK well before ~2^32 messages).
    Aes256Gcm = 1,
    /// XChaCha20-Poly1305 (24-byte nonce). The extended nonce makes random
    /// nonce generation collision-safe at any realistic message volume, which
    /// is why it is the default suite.
    #[default]
    XChaCha20Poly1305 = 2,
}

impl Suite {
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Suite::Aes256Gcm),
            2 => Some(Suite::XChaCha20Poly1305),
            _ => None,
        }
    }

    /// Nonce length in bytes for this suite.
    pub(crate) fn nonce_len(self) -> usize {
        match self {
            Suite::Aes256Gcm => 12,
            Suite::XChaCha20Poly1305 => 24,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Suite::Aes256Gcm => "aes-256-gcm",
            Suite::XChaCha20Poly1305 => "xchacha20-poly1305",
        }
    }
}

/// The lifecycle/access purpose a DEK serves. Purposes correspond to real
/// isolation and rotation differences, not to Rust types — a new purpose
/// should require a concrete need (design "Key Hierarchy").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyPurpose {
    Metadata,
    Content,
    Index,
    Export,
    Snapshot,
}

impl KeyPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            KeyPurpose::Metadata => "metadata",
            KeyPurpose::Content => "content",
            KeyPurpose::Index => "index",
            KeyPurpose::Export => "export",
            KeyPurpose::Snapshot => "snapshot",
        }
    }
}

/// Failure of a crypto operation. Every variant is a fail-closed error: a
/// caller must treat any of them as "do not expose plaintext" / "do not accept
/// this ciphertext."
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    /// The envelope frame was malformed — bad magic, unknown suite/version,
    /// impossible length, or trailing bytes.
    #[error("malformed envelope: {0}")]
    MalformedEnvelope(String),

    /// The required DEK (active, or a specific historical version) could not be
    /// obtained — the key manager is unavailable, the key was revoked, or the
    /// version is unknown. Writes fail; reads fail closed.
    #[error("key unavailable for tenant {tenant_id} purpose {purpose}: {reason}")]
    KeyUnavailable {
        tenant_id: String,
        purpose: &'static str,
        reason: String,
    },

    /// Authentication failed: the tag, nonce, ciphertext, or AAD (tenant,
    /// object, purpose, key version, suite) did not verify. Moving ciphertext
    /// to another tenant/object, or a one-bit mutation, lands here.
    #[error("authentication failed (wrong key/context or corrupted ciphertext)")]
    AuthenticationFailed,

    /// A crypto operation was requested while encryption is disabled. The
    /// runtime must route around a disabled keyring rather than call it.
    #[error("envelope encryption is disabled")]
    Disabled,

    /// Invalid crypto configuration (missing provider, contradictory mode).
    #[error("invalid encryption configuration: {0}")]
    Config(String),
}
