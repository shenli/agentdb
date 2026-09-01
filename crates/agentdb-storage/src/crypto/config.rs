//! Explicit secure/insecure encryption configuration (design "Proposed Code
//! Changes" §2). Identity/plaintext behavior must be a deliberate `disabled`
//! choice, never a silent default: if a deployment wants envelope encryption it
//! selects `envelope`, and a tenant policy that requires encryption while the
//! keyring is disabled must fail closed at activation.

use super::Suite;

/// Whether tenant envelope encryption is active for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoMode {
    /// No envelope encryption. Durable tenant payloads are stored as the
    /// storage layer produces them (subject to infrastructure disk/object
    /// encryption). This is an explicit choice, not a fallback.
    Disabled,
    /// Tenant payloads are sealed under per-tenant DEKs before they become
    /// durable.
    Envelope,
}

impl CryptoMode {
    pub fn as_str(self) -> &'static str {
        match self {
            CryptoMode::Disabled => "disabled",
            CryptoMode::Envelope => "envelope",
        }
    }
}

/// Bounds on the plaintext-DEK cache. A DEK lease lives at most `ttl_ms` and
/// the cache holds at most `max_entries`; both cap how long revoked key
/// material can still decrypt and how much plaintext key material a process
/// holds. The revocation SLO ultimately sets `ttl_ms` (design "Open
/// Decisions").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DekCacheConfig {
    pub ttl_ms: u64,
    pub max_entries: usize,
}

impl Default for DekCacheConfig {
    fn default() -> Self {
        Self {
            ttl_ms: 300_000, // 5 minutes
            max_entries: 1_000,
        }
    }
}

/// Top-level crypto configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CryptoConfig {
    pub mode: CryptoMode,
    pub suite: Suite,
    pub cache: DekCacheConfig,
}

impl Default for CryptoConfig {
    fn default() -> Self {
        Self {
            mode: CryptoMode::Disabled,
            suite: Suite::default(),
            cache: DekCacheConfig::default(),
        }
    }
}

impl CryptoConfig {
    /// A disabled configuration (the safe default; no stored data changes).
    pub fn disabled() -> Self {
        Self::default()
    }

    /// An envelope configuration with the given suite and default cache bounds.
    pub fn envelope(suite: Suite) -> Self {
        Self {
            mode: CryptoMode::Envelope,
            suite,
            cache: DekCacheConfig::default(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self.mode, CryptoMode::Envelope)
    }
}
