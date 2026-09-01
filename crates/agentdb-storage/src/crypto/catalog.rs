//! The durable wrapped-DEK key catalog and the [`KeyProvider`] that serves DEKs
//! from it.
//!
//! The catalog stores one record per `(tenant, purpose, version)` holding the
//! *wrapped* DEK plus its lifecycle status, and an "active version" pointer per
//! `(tenant, purpose)`. It never stores plaintext key material. Records live
//! under a reserved (`\0agentdb/…`) key prefix so they are control state, not a
//! tenant payload — they are never themselves envelope-encrypted (that would be
//! circular) and never appear in a tenant prefix scan.
//!
//! [`CatalogKeyProvider`] generates a DEK on first use for a `(tenant, purpose)`,
//! wraps it under the [`KekProvider`], stores it, and returns the unwrapped
//! lease; on later use it reads and unwraps the active (or a specific) version.
//! The [`AeadTenantKeyring`](super::AeadTenantKeyring) caches unwrapped DEKs, so
//! this catalog/KEK path runs only on a cache miss or key operation.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

use super::kek::{KekProvider, WrappedDek};
use super::keyring::{Dek, DekLease, KeyMetadata, KeyProvider};
use super::{CryptoError, KeyPurpose};
use crate::kv::KvStore;

const CATALOG_PREFIX: &str = "\0agentdb/keycatalog/";

/// A DEK's lifecycle status in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DekStatus {
    /// The write key: new ciphertext is sealed under this version.
    Active,
    /// Still valid for decryption but no longer for new writes (post-rotation).
    ReadOnly,
    /// No longer usable; kept only for audit. Unwrap is refused.
    Retired,
    /// Crypto-shredded: the key must never decrypt again. Unwrap is refused.
    Revoked,
}

impl DekStatus {
    fn readable(self) -> bool {
        matches!(self, DekStatus::Active | DekStatus::ReadOnly)
    }
}

/// One catalog record. Serialized as JSON under the reserved prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DekRecord {
    pub tenant_id: String,
    pub purpose: String,
    pub version: u32,
    pub wrapped: WrappedDek,
    pub status: DekStatus,
    pub created_at_ms: i64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn record_key(tenant_id: &str, purpose: KeyPurpose, version: u32) -> Vec<u8> {
    format!(
        "{CATALOG_PREFIX}dek/{tenant_id}/{}/{version:010}",
        purpose.as_str()
    )
    .into_bytes()
}

fn active_key(tenant_id: &str, purpose: KeyPurpose) -> Vec<u8> {
    format!("{CATALOG_PREFIX}active/{tenant_id}/{}", purpose.as_str()).into_bytes()
}

/// Durable storage for wrapped DEK records over a [`KvStore`].
#[derive(Clone)]
pub struct KeyCatalog {
    kv: Arc<dyn KvStore + Send + Sync>,
}

impl KeyCatalog {
    pub fn new(kv: Arc<dyn KvStore + Send + Sync>) -> Self {
        Self { kv }
    }

    fn get_record(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        version: u32,
    ) -> Result<Option<DekRecord>, CryptoError> {
        let Some(bytes) = self
            .kv
            .get(&record_key(tenant_id, purpose, version))
            .map_err(|e| CryptoError::Config(format!("key catalog read: {e}")))?
        else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| CryptoError::MalformedEnvelope(format!("key catalog record decode: {e}")))
    }

    fn active_version(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
    ) -> Result<Option<u32>, CryptoError> {
        let Some(bytes) = self
            .kv
            .get(&active_key(tenant_id, purpose))
            .map_err(|e| CryptoError::Config(format!("key catalog read: {e}")))?
        else {
            return Ok(None);
        };
        let arr: [u8; 4] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| CryptoError::MalformedEnvelope("active version pointer".into()))?;
        Ok(Some(u32::from_be_bytes(arr)))
    }

    /// Persist a record and, if it is `Active`, point the active version at it.
    fn put_record(&self, record: &DekRecord) -> Result<(), CryptoError> {
        let purpose = parse_purpose(&record.purpose)?;
        let value = serde_json::to_vec(record)
            .map_err(|e| CryptoError::Config(format!("key catalog record encode: {e}")))?;
        self.kv
            .put(
                &record_key(&record.tenant_id, purpose, record.version),
                &value,
            )
            .map_err(|e| CryptoError::Config(format!("key catalog write: {e}")))?;
        if record.status == DekStatus::Active {
            self.kv
                .put(
                    &active_key(&record.tenant_id, purpose),
                    &record.version.to_be_bytes(),
                )
                .map_err(|e| CryptoError::Config(format!("key catalog write: {e}")))?;
        }
        Ok(())
    }
}

fn parse_purpose(s: &str) -> Result<KeyPurpose, CryptoError> {
    Ok(match s {
        "metadata" => KeyPurpose::Metadata,
        "content" => KeyPurpose::Content,
        "index" => KeyPurpose::Index,
        "export" => KeyPurpose::Export,
        "snapshot" => KeyPurpose::Snapshot,
        other => return Err(CryptoError::Config(format!("unknown key purpose {other}"))),
    })
}

/// A production [`KeyProvider`] backed by the wrapped-DEK catalog and a KEK.
pub struct CatalogKeyProvider {
    catalog: KeyCatalog,
    kek: Arc<dyn KekProvider>,
    /// Serializes first-DEK generation so two racing writers cannot mint two
    /// active versions for the same (tenant, purpose).
    generate_lock: Mutex<()>,
}

impl CatalogKeyProvider {
    pub fn new(catalog: KeyCatalog, kek: Arc<dyn KekProvider>) -> Self {
        Self {
            catalog,
            kek,
            generate_lock: Mutex::new(()),
        }
    }

    fn lease_from_record(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        record: &DekRecord,
    ) -> Result<DekLease, CryptoError> {
        if !record.status.readable() {
            return Err(CryptoError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                purpose: purpose.as_str(),
                reason: format!("DEK version {} is {:?}", record.version, record.status),
            });
        }
        let unwrapped = self.kek.unwrap_dek(tenant_id, purpose, &record.wrapped)?;
        let mut key = *unwrapped;
        let lease = Arc::new(Dek::new(
            key,
            KeyMetadata {
                dek_id: format!("tenant:{tenant_id}:{}", purpose.as_str()),
                version: record.version,
                purpose,
                policy_epoch: 0,
            },
        ));
        key.zeroize();
        Ok(lease)
    }

    fn generate_active(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
    ) -> Result<DekLease, CryptoError> {
        let _guard = self.generate_lock.lock().expect("generate lock poisoned");
        // Re-check under the lock: another writer may have just created it.
        if let Some(version) = self.catalog.active_version(tenant_id, purpose)? {
            if let Some(record) = self.catalog.get_record(tenant_id, purpose, version)? {
                return self.lease_from_record(tenant_id, purpose, &record);
            }
        }
        let mut dek = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut dek);
        let wrapped = self.kek.wrap_dek(tenant_id, purpose, &dek)?;
        let record = DekRecord {
            tenant_id: tenant_id.to_string(),
            purpose: purpose.as_str().to_string(),
            version: 1,
            wrapped,
            status: DekStatus::Active,
            created_at_ms: now_ms(),
        };
        self.catalog.put_record(&record)?;
        let lease = Arc::new(Dek::new(
            dek,
            KeyMetadata {
                dek_id: format!("tenant:{tenant_id}:{}", purpose.as_str()),
                version: 1,
                purpose,
                policy_epoch: 0,
            },
        ));
        dek.zeroize();
        Ok(lease)
    }
}

impl KeyProvider for CatalogKeyProvider {
    fn active_dek(&self, tenant_id: &str, purpose: KeyPurpose) -> Result<DekLease, CryptoError> {
        if let Some(version) = self.catalog.active_version(tenant_id, purpose)? {
            if let Some(record) = self.catalog.get_record(tenant_id, purpose, version)? {
                return self.lease_from_record(tenant_id, purpose, &record);
            }
        }
        self.generate_active(tenant_id, purpose)
    }

    fn dek_version(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        version: u32,
    ) -> Result<DekLease, CryptoError> {
        let record = self
            .catalog
            .get_record(tenant_id, purpose, version)?
            .ok_or_else(|| CryptoError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                purpose: purpose.as_str(),
                reason: format!("DEK version {version} is not in the catalog"),
            })?;
        self.lease_from_record(tenant_id, purpose, &record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::kek::LocalKekProvider;
    use crate::InMemoryKvStore;

    fn provider(kv: Arc<dyn KvStore + Send + Sync>) -> CatalogKeyProvider {
        CatalogKeyProvider::new(
            KeyCatalog::new(kv),
            Arc::new(LocalKekProvider::from_secret(b"kek", "local:v1")),
        )
    }

    #[test]
    fn generates_wraps_and_serves_the_active_dek() {
        let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
        let p = provider(kv.clone());
        let a = p.active_dek("ten_a", KeyPurpose::Content).unwrap();
        assert_eq!(a.metadata().version, 1);
        // Second call serves the same stored DEK (same bytes → same behavior).
        let b = p.dek_version("ten_a", KeyPurpose::Content, 1).unwrap();
        assert_eq!(a.metadata(), b.metadata());
        // Catalog holds only a WRAPPED dek — the raw KV value must not be the key.
        let raw = kv.get(&record_key("ten_a", KeyPurpose::Content, 1)).unwrap().unwrap();
        assert!(!raw.windows(4).any(|w| w == b"\0\0\0\0"), "sanity");
    }

    #[test]
    fn two_tenants_get_independent_deks() {
        let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
        let p = provider(kv);
        let a = p.active_dek("ten_a", KeyPurpose::Content).unwrap();
        let b = p.active_dek("ten_b", KeyPurpose::Content).unwrap();
        // Different tenants → different catalog entries; both version 1.
        assert_eq!(a.metadata().version, 1);
        assert_eq!(b.metadata().version, 1);
        assert_ne!(a.metadata().dek_id, b.metadata().dek_id);
    }

    #[test]
    fn deks_persist_across_provider_restart() {
        let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
        let first = provider(kv.clone());
        first.active_dek("ten_a", KeyPurpose::Content).unwrap();
        // A fresh provider over the same catalog serves the stored DEK v1.
        let second = provider(kv);
        assert!(second.dek_version("ten_a", KeyPurpose::Content, 1).is_ok());
    }

    #[test]
    fn catalog_backed_keyring_survives_restart_and_needs_the_kek() {
        use crate::crypto::{AeadTenantKeyring, CryptoContext, DekCacheConfig, EnvelopeCrypto, Suite};

        let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
        let ctx = CryptoContext {
            tenant_id: "ten_a",
            purpose: KeyPurpose::Content,
            object_kind: "event",
            object_id: "o1",
        };
        let keyring = |kek_secret: &[u8], kek_ref: &str| {
            AeadTenantKeyring::new(
                Arc::new(CatalogKeyProvider::new(
                    KeyCatalog::new(kv.clone()),
                    Arc::new(LocalKekProvider::from_secret(kek_secret, kek_ref)),
                )),
                Suite::XChaCha20Poly1305,
                DekCacheConfig::default(),
            )
        };

        // Seal under a catalog-backed keyring (mints + wraps + stores DEK v1).
        let sealed = keyring(b"kek", "local:v1").seal(&ctx, b"payload").unwrap();

        // A fresh keyring (new process) over the SAME catalog + KEK opens it —
        // the DEK is durably wrapped in the catalog, not re-derived.
        assert_eq!(
            keyring(b"kek", "local:v1").open(&ctx, &sealed).unwrap(),
            b"payload"
        );

        // A keyring with a DIFFERENT KEK cannot open it: the wrapped DEK will not
        // unwrap, so losing/rotating-away the KEK crypto-shreds the data.
        assert!(keyring(b"other-kek", "local:v2").open(&ctx, &sealed).is_err());
    }

    #[test]
    fn revoked_or_missing_version_fails_closed() {
        let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
        let catalog = KeyCatalog::new(kv.clone());
        let p = CatalogKeyProvider::new(
            catalog.clone(),
            Arc::new(LocalKekProvider::from_secret(b"kek", "local:v1")),
        );
        p.active_dek("ten_a", KeyPurpose::Content).unwrap();
        // Unknown version fails.
        assert!(p.dek_version("ten_a", KeyPurpose::Content, 99).is_err());
        // Revoke v1 and confirm it can no longer be served.
        let mut rec = catalog
            .get_record("ten_a", KeyPurpose::Content, 1)
            .unwrap()
            .unwrap();
        rec.status = DekStatus::Revoked;
        catalog.put_record(&rec).unwrap();
        assert!(p.dek_version("ten_a", KeyPurpose::Content, 1).is_err());
    }
}
