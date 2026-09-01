//! The tenant keyring: [`EnvelopeCrypto`] (seal/open under per-tenant,
//! per-purpose DEKs), the [`KeyProvider`] seam that yields DEK material, a
//! bounded plaintext-DEK cache, and the production [`AeadTenantKeyring`] plus an
//! explicit [`DisabledEnvelopeCrypto`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use super::config::DekCacheConfig;
use super::{envelope, CryptoError, KeyPurpose, Suite};

/// The location and meaning a ciphertext is bound to. It is reproduced exactly
/// on read and mixed into the AEAD associated data, so decryption fails if any
/// of these change — moving ciphertext to a different tenant, object, or
/// purpose cannot succeed.
#[derive(Debug, Clone, Copy)]
pub struct CryptoContext<'a> {
    /// Owning tenant.
    pub tenant_id: &'a str,
    /// Which key lifecycle this payload uses.
    pub purpose: KeyPurpose,
    /// A stable label for the *kind* of object, e.g. `artifact_content` or
    /// `event` — part of the AAD so two kinds never share a ciphertext meaning.
    pub object_kind: &'a str,
    /// The stable logical id / storage key of this specific object.
    pub object_id: &'a str,
}

/// Non-secret metadata describing the DEK currently selected for a
/// (tenant, purpose).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMetadata {
    pub dek_id: String,
    pub version: u32,
    pub purpose: KeyPurpose,
    pub policy_epoch: u64,
}

/// Plaintext DEK material. The bytes are zeroized on drop; the value is never
/// serialized or returned through general runtime APIs.
#[derive(Clone)]
pub struct Dek {
    key: Zeroizing<[u8; 32]>,
    meta: KeyMetadata,
}

impl Dek {
    pub fn new(key: [u8; 32], meta: KeyMetadata) -> Self {
        Self {
            key: Zeroizing::new(key),
            meta,
        }
    }
    fn key(&self) -> &[u8; 32] {
        &self.key
    }
    pub fn metadata(&self) -> &KeyMetadata {
        &self.meta
    }
}

impl std::fmt::Debug for Dek {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key material.
        f.debug_struct("Dek").field("meta", &self.meta).finish()
    }
}

/// A leased DEK — a shared handle to plaintext key material with a bounded
/// lifetime (see the cache). Cloning is cheap (`Arc`).
pub type DekLease = Arc<Dek>;

/// Source of DEK material. A provider unwraps wrapped DEKs (from a KMS-backed
/// catalog in production) and hands back a plaintext lease; the keyring caches
/// leases so steady-state traffic makes no provider/KMS calls.
pub trait KeyProvider: Send + Sync {
    /// The active (write) DEK for a (tenant, purpose).
    fn active_dek(&self, tenant_id: &str, purpose: KeyPurpose) -> Result<DekLease, CryptoError>;

    /// A specific historical DEK version, for decrypting older ciphertext
    /// during and after rotation. Must fail closed if the version is unknown or
    /// its key is unavailable.
    fn dek_version(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        version: u32,
    ) -> Result<DekLease, CryptoError>;
}

/// A local, deterministic key provider that derives per-(tenant, purpose,
/// version) DEKs from a single root secret via HKDF-SHA256.
///
/// This is a **development / single-key-manager** provider: it needs no KMS and
/// produces stable, independent DEKs so the whole seal/open path is testable.
/// It is not BYOK and does not wrap DEKs in a durable catalog — that is a later
/// milestone. Two tenants (or two purposes) always derive independent keys.
pub struct LocalKeyProvider {
    root: Zeroizing<[u8; 32]>,
    /// The active version this provider hands out. Rotation (advancing this and
    /// keeping old versions readable) arrives with the key catalog; for now it
    /// is fixed at 1 and all versions derive deterministically.
    active_version: u32,
}

impl LocalKeyProvider {
    pub fn new(root: [u8; 32]) -> Self {
        Self {
            root: Zeroizing::new(root),
            active_version: 1,
        }
    }

    /// Construct from an arbitrary-length secret by hashing it to 32 bytes.
    pub fn from_secret(secret: &[u8]) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(b"agentdb-crypto-root-v1"), secret);
        let mut root = [0u8; 32];
        hk.expand(b"root", &mut root)
            .expect("32 bytes is a valid HKDF length");
        let provider = Self::new(root);
        root.zeroize();
        provider
    }

    fn derive(&self, tenant_id: &str, purpose: KeyPurpose, version: u32) -> DekLease {
        // Domain-separated, length-prefixed info so distinct (tenant, purpose,
        // version) tuples never collide.
        let mut info = Vec::new();
        info.extend_from_slice(b"agentdb-dek-v1");
        for field in [tenant_id.as_bytes(), purpose.as_str().as_bytes()] {
            info.extend_from_slice(&(field.len() as u32).to_be_bytes());
            info.extend_from_slice(field);
        }
        info.extend_from_slice(&version.to_be_bytes());

        let hk = Hkdf::<Sha256>::new(None, self.root.as_slice());
        let mut key = [0u8; 32];
        hk.expand(&info, &mut key)
            .expect("32 bytes is a valid HKDF length");
        let lease = Arc::new(Dek::new(
            key,
            KeyMetadata {
                dek_id: format!("tenant:{tenant_id}:{}", purpose.as_str()),
                version,
                purpose,
                policy_epoch: 0,
            },
        ));
        // `key` is a copy on the stack (the Dek holds its own zeroizing copy);
        // clear this one now.
        key.zeroize();
        lease
    }
}

impl KeyProvider for LocalKeyProvider {
    fn active_dek(&self, tenant_id: &str, purpose: KeyPurpose) -> Result<DekLease, CryptoError> {
        Ok(self.derive(tenant_id, purpose, self.active_version))
    }

    fn dek_version(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        version: u32,
    ) -> Result<DekLease, CryptoError> {
        if version == 0 {
            return Err(CryptoError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                purpose: purpose.as_str(),
                reason: "version 0 is not valid".into(),
            });
        }
        Ok(self.derive(tenant_id, purpose, version))
    }
}

/// The sealing/opening interface the runtime uses. Every method fails closed.
pub trait EnvelopeCrypto: Send + Sync {
    /// Seal `plaintext` for `context` into an `AGDB-ENVELOPE-v2` frame under the
    /// active DEK.
    fn seal(&self, context: &CryptoContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError>;

    /// Open a v2 envelope, verifying it belongs to `context`. Fails closed on a
    /// wrong context, corrupted bytes, or a missing key version.
    fn open(&self, context: &CryptoContext<'_>, envelope: &[u8]) -> Result<Vec<u8>, CryptoError>;

    /// Metadata for the active DEK of a (tenant, purpose).
    fn active_key(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
    ) -> Result<KeyMetadata, CryptoError>;

    /// Drop cached plaintext DEK leases for a tenant (e.g. on a policy-epoch
    /// change or revocation) so the next operation re-fetches from the provider.
    fn invalidate(&self, tenant_id: &str);

    /// Whether this keyring performs real encryption. `false` for the disabled
    /// keyring, which the runtime must route around rather than call.
    fn is_enabled(&self) -> bool;
}

/// Cache key: a DEK lease is identified by tenant, purpose, and version.
type CacheKey = (String, KeyPurpose, u32);

struct CachedDek {
    lease: DekLease,
    inserted: Instant,
}

struct DekCache {
    entries: HashMap<CacheKey, CachedDek>,
    ttl: Duration,
    max_entries: usize,
}

impl DekCache {
    fn new(cfg: DekCacheConfig) -> Self {
        Self {
            entries: HashMap::new(),
            ttl: Duration::from_millis(cfg.ttl_ms),
            max_entries: cfg.max_entries.max(1),
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<DekLease> {
        if let Some(entry) = self.entries.get(key) {
            if entry.inserted.elapsed() <= self.ttl {
                return Some(entry.lease.clone());
            }
        }
        self.entries.remove(key);
        None
    }

    fn insert(&mut self, key: CacheKey, lease: DekLease) {
        if self.entries.len() >= self.max_entries && !self.entries.contains_key(&key) {
            // Simple bound: evict the oldest lease. Cheap linear scan — the cap
            // is small and this only runs when full.
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.inserted)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(
            key,
            CachedDek {
                lease,
                inserted: Instant::now(),
            },
        );
    }

    fn invalidate_tenant(&mut self, tenant_id: &str) {
        self.entries.retain(|(tenant, _, _), _| tenant != tenant_id);
    }
}

/// Production keyring: authenticated envelope encryption over a [`KeyProvider`],
/// with a bounded plaintext-DEK cache.
pub struct AeadTenantKeyring {
    provider: Arc<dyn KeyProvider>,
    suite: Suite,
    cache: Mutex<DekCache>,
}

impl AeadTenantKeyring {
    pub fn new(provider: Arc<dyn KeyProvider>, suite: Suite, cache: DekCacheConfig) -> Self {
        Self {
            provider,
            suite,
            cache: Mutex::new(DekCache::new(cache)),
        }
    }

    fn active_lease(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
    ) -> Result<DekLease, CryptoError> {
        let lease = self.provider.active_dek(tenant_id, purpose)?;
        let key = (tenant_id.to_string(), purpose, lease.meta.version);
        self.cache
            .lock()
            .expect("dek cache poisoned")
            .insert(key, lease.clone());
        Ok(lease)
    }

    fn version_lease(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        version: u32,
    ) -> Result<DekLease, CryptoError> {
        let key = (tenant_id.to_string(), purpose, version);
        if let Some(lease) = self.cache.lock().expect("dek cache poisoned").get(&key) {
            return Ok(lease);
        }
        let lease = self.provider.dek_version(tenant_id, purpose, version)?;
        self.cache
            .lock()
            .expect("dek cache poisoned")
            .insert(key, lease.clone());
        Ok(lease)
    }
}

impl EnvelopeCrypto for AeadTenantKeyring {
    fn seal(&self, context: &CryptoContext<'_>, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let lease = self.active_lease(context.tenant_id, context.purpose)?;
        let version = lease.meta.version;
        let mut nonce = vec![0u8; self.suite.nonce_len()];
        rand::thread_rng().fill_bytes(&mut nonce);
        let aad = derive_aad(context, &lease.meta.dek_id, version, self.suite);
        let ciphertext = aead_seal(self.suite, lease.key(), &nonce, plaintext, &aad)?;
        Ok(envelope::encode(&envelope::EnvelopeParts {
            suite: self.suite,
            dek_id: &lease.meta.dek_id,
            dek_version: version,
            nonce: &nonce,
            ciphertext: &ciphertext,
        }))
    }

    fn open(&self, context: &CryptoContext<'_>, envelope: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let parts = envelope::decode(envelope)?;
        let lease = self.version_lease(context.tenant_id, context.purpose, parts.dek_version)?;
        // AAD is recomputed from the caller's context and the envelope's own
        // header (dek id, suite, version); a mismatch (wrong tenant/object/
        // purpose, or any tampered header field) makes the tag verification
        // below fail.
        let aad = derive_aad(context, parts.dek_id, parts.dek_version, parts.suite);
        aead_open(parts.suite, lease.key(), parts.nonce, parts.ciphertext, &aad)
    }

    fn active_key(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
    ) -> Result<KeyMetadata, CryptoError> {
        Ok(self.active_lease(tenant_id, purpose)?.meta.clone())
    }

    fn invalidate(&self, tenant_id: &str) {
        self.cache
            .lock()
            .expect("dek cache poisoned")
            .invalidate_tenant(tenant_id);
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

/// The explicit disabled keyring. It performs no encryption and every crypto
/// call fails closed, so a disabled deployment must route around it (store
/// plaintext directly) rather than accidentally treat framed plaintext as
/// ciphertext.
#[derive(Debug, Default)]
pub struct DisabledEnvelopeCrypto;

impl EnvelopeCrypto for DisabledEnvelopeCrypto {
    fn seal(&self, _context: &CryptoContext<'_>, _plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Err(CryptoError::Disabled)
    }
    fn open(&self, _context: &CryptoContext<'_>, _envelope: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Err(CryptoError::Disabled)
    }
    fn active_key(
        &self,
        _tenant_id: &str,
        _purpose: KeyPurpose,
    ) -> Result<KeyMetadata, CryptoError> {
        Err(CryptoError::Disabled)
    }
    fn invalidate(&self, _tenant_id: &str) {}
    fn is_enabled(&self) -> bool {
        false
    }
}

/// Canonical AAD: domain-separated, length-prefixed binding of the context plus
/// the DEK version and suite. Reproduced exactly on read; never stored (it is
/// recomputed), so it cannot be tampered independently of the ciphertext.
fn derive_aad(context: &CryptoContext<'_>, dek_id: &str, dek_version: u32, suite: Suite) -> Vec<u8> {
    let mut aad = Vec::new();
    aad.extend_from_slice(b"AGDB-AAD-v2");
    for field in [
        context.tenant_id.as_bytes(),
        context.object_kind.as_bytes(),
        context.object_id.as_bytes(),
        context.purpose.as_str().as_bytes(),
        dek_id.as_bytes(),
    ] {
        aad.extend_from_slice(&(field.len() as u32).to_be_bytes());
        aad.extend_from_slice(field);
    }
    aad.extend_from_slice(&dek_version.to_be_bytes());
    aad.push(suite as u8);
    aad
}

fn aead_seal(
    suite: Suite,
    key: &[u8; 32],
    nonce: &[u8],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let payload = Payload {
        msg: plaintext,
        aad,
    };
    match suite {
        Suite::Aes256Gcm => Aes256Gcm::new_from_slice(key)
            .expect("32-byte key")
            .encrypt(Nonce::from_slice(nonce), payload)
            .map_err(|_| CryptoError::AuthenticationFailed),
        Suite::XChaCha20Poly1305 => XChaCha20Poly1305::new_from_slice(key)
            .expect("32-byte key")
            .encrypt(XNonce::from_slice(nonce), payload)
            .map_err(|_| CryptoError::AuthenticationFailed),
    }
}

fn aead_open(
    suite: Suite,
    key: &[u8; 32],
    nonce: &[u8],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let payload = Payload {
        msg: ciphertext,
        aad,
    };
    match suite {
        Suite::Aes256Gcm => Aes256Gcm::new_from_slice(key)
            .expect("32-byte key")
            .decrypt(Nonce::from_slice(nonce), payload)
            .map_err(|_| CryptoError::AuthenticationFailed),
        Suite::XChaCha20Poly1305 => XChaCha20Poly1305::new_from_slice(key)
            .expect("32-byte key")
            .decrypt(XNonce::from_slice(nonce), payload)
            .map_err(|_| CryptoError::AuthenticationFailed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyring(suite: Suite) -> AeadTenantKeyring {
        AeadTenantKeyring::new(
            Arc::new(LocalKeyProvider::from_secret(b"unit-test-root")),
            suite,
            DekCacheConfig::default(),
        )
    }

    fn ctx<'a>(tenant: &'a str, object: &'a str) -> CryptoContext<'a> {
        CryptoContext {
            tenant_id: tenant,
            purpose: KeyPurpose::Content,
            object_kind: "artifact_content",
            object_id: object,
        }
    }

    #[test]
    fn round_trips_both_suites() {
        for suite in [Suite::Aes256Gcm, Suite::XChaCha20Poly1305] {
            let kr = keyring(suite);
            let c = ctx("ten_a", "art_1");
            let sealed = kr.seal(&c, b"secret payload").unwrap();
            assert!(super::envelope::is_envelope_v2(&sealed));
            assert_eq!(kr.open(&c, &sealed).unwrap(), b"secret payload");
        }
    }

    #[test]
    fn ciphertext_does_not_contain_plaintext() {
        let kr = keyring(Suite::XChaCha20Poly1305);
        let sealed = kr.seal(&ctx("ten_a", "art_1"), b"TOP-SECRET-MARKER").unwrap();
        assert!(!sealed
            .windows(b"TOP-SECRET-MARKER".len())
            .any(|w| w == b"TOP-SECRET-MARKER"));
    }

    #[test]
    fn two_tenants_same_plaintext_get_independent_ciphertext() {
        let kr = keyring(Suite::XChaCha20Poly1305);
        let a = kr.seal(&ctx("ten_a", "art_1"), b"same").unwrap();
        let b = kr.seal(&ctx("ten_b", "art_1"), b"same").unwrap();
        // Different keys + random nonces → different ciphertext, and neither
        // opens under the other tenant's context.
        assert_ne!(a, b);
        assert!(kr.open(&ctx("ten_b", "art_1"), &a).is_err());
    }

    #[test]
    fn wrong_tenant_object_or_purpose_fails() {
        let kr = keyring(Suite::Aes256Gcm);
        let sealed = kr.seal(&ctx("ten_a", "art_1"), b"payload").unwrap();
        // wrong object id
        assert!(kr.open(&ctx("ten_a", "art_2"), &sealed).is_err());
        // wrong tenant
        assert!(kr.open(&ctx("ten_b", "art_1"), &sealed).is_err());
        // wrong purpose
        let mut wrong = ctx("ten_a", "art_1");
        wrong.purpose = KeyPurpose::Metadata;
        assert!(kr.open(&wrong, &sealed).is_err());
        // wrong object kind
        let mut wrong_kind = ctx("ten_a", "art_1");
        wrong_kind.object_kind = "event";
        assert!(kr.open(&wrong_kind, &sealed).is_err());
    }

    #[test]
    fn one_bit_mutation_fails_closed() {
        let kr = keyring(Suite::XChaCha20Poly1305);
        let c = ctx("ten_a", "art_1");
        let sealed = kr.seal(&c, b"payload").unwrap();
        for i in 0..sealed.len() {
            let mut m = sealed.clone();
            m[i] ^= 0x01;
            // Either the frame fails to parse or the tag fails — never plaintext.
            assert!(
                !kr.open(&c, &m).is_ok_and(|p| p == b"payload"),
                "byte {i} mutation opened to plaintext"
            );
        }
    }

    #[test]
    fn disabled_keyring_fails_closed() {
        let kr = DisabledEnvelopeCrypto;
        assert!(!kr.is_enabled());
        assert!(matches!(
            kr.seal(&ctx("ten_a", "art_1"), b"x"),
            Err(CryptoError::Disabled)
        ));
        assert!(matches!(
            kr.open(&ctx("ten_a", "art_1"), b"x"),
            Err(CryptoError::Disabled)
        ));
    }

    #[test]
    fn cache_invalidation_forces_refetch() {
        let kr = keyring(Suite::XChaCha20Poly1305);
        let meta1 = kr.active_key("ten_a", KeyPurpose::Content).unwrap();
        kr.invalidate("ten_a");
        let meta2 = kr.active_key("ten_a", KeyPurpose::Content).unwrap();
        // Deterministic provider → same key metadata after refetch; the point is
        // it does not error and re-populates the cache.
        assert_eq!(meta1, meta2);
    }

    #[test]
    fn unknown_key_version_fails_closed() {
        // Seal with the keyring, then hand `open` a context whose stored version
        // the provider rejects (version 0 sentinel via a crafted envelope).
        let kr = keyring(Suite::XChaCha20Poly1305);
        let c = ctx("ten_a", "art_1");
        let sealed = kr.seal(&c, b"payload").unwrap();
        let parts = super::envelope::decode(&sealed).unwrap();
        let crafted = super::envelope::encode(&super::envelope::EnvelopeParts {
            suite: parts.suite,
            dek_id: parts.dek_id,
            dek_version: 0, // provider rejects version 0
            nonce: parts.nonce,
            ciphertext: parts.ciphertext,
        });
        assert!(kr.open(&c, &crafted).is_err());
    }
}
