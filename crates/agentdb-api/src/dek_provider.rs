//! Cluster key provider: serves per-tenant DEKs from the shared meta-DB key
//! catalog (`agentdb_meta::dek_catalog`) with a KEK.
//!
//! This is the multi-node counterpart to the single-node `CatalogKeyProvider`.
//! The catalog is async (a database) but the seal/open hot path is sync, so the
//! provider **pre-warms** every wrapped DEK into an in-memory map at startup
//! (`load`) and unwraps them under the KEK; the sync [`KeyProvider`] serve path
//! then reads that map and never touches the database. Minting is **explicit
//! and async** — [`provision_all`] is called on tenant creation (the leader),
//! writing through the race-safe `provision_if_absent` so every node converges
//! on one DEK per tenant. The serve path never mints, so it fails closed on an
//! unprovisioned tenant rather than risk two nodes minting different keys.
//!
//! Followers pick up a newly-provisioned tenant's key on the next [`refresh`].

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::RngCore;
use sqlx::AnyPool;

use agentdb_meta::dek_catalog::{self, DekCatalogRow};
use agentdb_storage::{
    CryptoError, Dek, DekLease, KekProvider, KeyMetadata, KeyProvider, KeyPurpose, WrappedDek,
};

/// The purposes a tenant gets a DEK for. Matches the durable paths that seal
/// today (content = artifacts/segments/…, metadata = events/artifact rows).
const PROVISIONED_PURPOSES: [KeyPurpose; 2] = [KeyPurpose::Content, KeyPurpose::Metadata];

type ActiveMap = HashMap<(String, KeyPurpose), DekLease>;
type VersionMap = HashMap<(String, KeyPurpose, u32), DekLease>;

pub struct MetaCatalogKeyProvider {
    pool: AnyPool,
    kek: Arc<dyn KekProvider>,
    active: RwLock<ActiveMap>,
    by_version: RwLock<VersionMap>,
}

impl MetaCatalogKeyProvider {
    /// Build the provider and pre-warm every DEK from the shared catalog.
    pub async fn load(pool: AnyPool, kek: Arc<dyn KekProvider>) -> Result<Self, CryptoError> {
        let provider = Self {
            pool,
            kek,
            active: RwLock::new(HashMap::new()),
            by_version: RwLock::new(HashMap::new()),
        };
        provider.refresh().await?;
        Ok(provider)
    }

    /// Reload the in-memory key maps from the catalog — call after another node
    /// provisions a tenant so this node can serve it.
    pub async fn refresh(&self) -> Result<(), CryptoError> {
        let rows = dek_catalog::list_all(&self.pool)
            .await
            .map_err(|e| CryptoError::Config(format!("key catalog list: {e}")))?;
        let mut active = HashMap::new();
        let mut by_version = HashMap::new();
        for row in &rows {
            // Only *readable* statuses are ever served. Skipping revoked/retired
            // rows entirely means a soft revoke (status → revoked) makes the key
            // unavailable even for decryption after a reload, and the periodic
            // refresh never resurrects a revoked key.
            if row.status != "active" && row.status != "read_only" {
                continue;
            }
            let purpose = parse_purpose(&row.purpose)?;
            let lease = self.lease_from_row(row, purpose)?;
            by_version.insert(
                (row.tenant_id.clone(), purpose, row.version as u32),
                lease.clone(),
            );
            if row.status == "active" {
                active.insert((row.tenant_id.clone(), purpose), lease);
            }
        }
        *self.active.write().expect("dek map poisoned") = active;
        *self.by_version.write().expect("dek map poisoned") = by_version;
        Ok(())
    }

    /// Ensure a tenant's DEKs exist (leader-coordinated, at tenant creation),
    /// then add them to this node's map. Idempotent and race-safe across nodes.
    pub async fn provision_all(&self, tenant_id: &str) -> Result<(), CryptoError> {
        for purpose in PROVISIONED_PURPOSES {
            self.provision(tenant_id, purpose).await?;
        }
        Ok(())
    }

    /// Ensure one `(tenant, purpose)` DEK exists and cache it locally.
    pub async fn provision(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
    ) -> Result<KeyMetadata, CryptoError> {
        // Already provisioned (in this node's map)? Nothing to do.
        if let Some(lease) = self
            .active
            .read()
            .expect("dek map poisoned")
            .get(&(tenant_id.to_string(), purpose))
        {
            return Ok(lease.metadata().clone());
        }
        // Mint a candidate, wrap it, and race it into the shared catalog. The
        // returned row is authoritative — it may be another node's DEK.
        let mut dek = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut dek);
        let wrapped = self.kek.wrap_dek(tenant_id, purpose, &dek)?;
        dek.fill(0);
        let candidate = DekCatalogRow {
            tenant_id: tenant_id.to_string(),
            purpose: purpose.as_str().to_string(),
            version: 1,
            wrapping_key_ref: wrapped.wrapping_key_ref,
            nonce: wrapped.nonce,
            ciphertext: wrapped.ciphertext,
            status: "active".to_string(),
            created_at_ms: now_ms(),
        };
        let winner = dek_catalog::provision_if_absent(&self.pool, &candidate)
            .await
            .map_err(|e| CryptoError::Config(format!("key catalog provision: {e}")))?;
        let lease = self.lease_from_row(&winner, purpose)?;
        let meta = lease.metadata().clone();
        self.active
            .write()
            .expect("dek map poisoned")
            .insert((tenant_id.to_string(), purpose), lease.clone());
        self.by_version.write().expect("dek map poisoned").insert(
            (tenant_id.to_string(), purpose, winner.version as u32),
            lease,
        );
        Ok(meta)
    }

    /// Rotate the active DEK for one `(tenant, purpose)`: mint a fresh key at the
    /// next version, race it into the shared catalog, demote the previous active
    /// version to `read_only`, and update this node's maps. Returns the new
    /// active key's metadata. Leader-coordinated (like [`provision`]); other
    /// nodes pick up the new active version on their next [`refresh`].
    ///
    /// Race-safe across nodes at the storage layer: the new version is written
    /// with `provision_if_absent`, so two nodes rotating concurrently converge on
    /// one winning DEK for the new version (the loser adopts the winner's row).
    pub async fn rotate(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
    ) -> Result<KeyMetadata, CryptoError> {
        // Determine the version to advance from the authoritative catalog, not
        // this node's (possibly stale) map.
        let current = dek_catalog::get_active(&self.pool, tenant_id, purpose.as_str())
            .await
            .map_err(|e| CryptoError::Config(format!("key catalog active read: {e}")))?;
        let Some(current) = current else {
            // Never provisioned: minting version 1 is the rotation.
            return self.provision(tenant_id, purpose).await;
        };
        let next = current.version + 1;

        let mut dek = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut dek);
        let wrapped = self.kek.wrap_dek(tenant_id, purpose, &dek)?;
        dek.fill(0);
        let candidate = DekCatalogRow {
            tenant_id: tenant_id.to_string(),
            purpose: purpose.as_str().to_string(),
            version: next,
            wrapping_key_ref: wrapped.wrapping_key_ref,
            nonce: wrapped.nonce,
            ciphertext: wrapped.ciphertext,
            status: "active".to_string(),
            created_at_ms: now_ms(),
        };
        let winner = dek_catalog::provision_if_absent(&self.pool, &candidate)
            .await
            .map_err(|e| CryptoError::Config(format!("key catalog rotate: {e}")))?;
        // Demote the previous active version (idempotent; a racing rotator may
        // have already done it).
        dek_catalog::set_status(
            &self.pool,
            tenant_id,
            purpose.as_str(),
            current.version,
            "read_only",
        )
        .await
        .map_err(|e| CryptoError::Config(format!("key catalog demote: {e}")))?;

        let lease = self.lease_from_row(&winner, purpose)?;
        let meta = lease.metadata().clone();
        self.active
            .write()
            .expect("dek map poisoned")
            .insert((tenant_id.to_string(), purpose), lease.clone());
        self.by_version.write().expect("dek map poisoned").insert(
            (tenant_id.to_string(), purpose, winner.version as u32),
            lease,
        );
        Ok(meta)
    }

    /// Rotate every provisioned purpose for a tenant. Returns the new active
    /// metadata per purpose.
    pub async fn rotate_all(&self, tenant_id: &str) -> Result<Vec<KeyMetadata>, CryptoError> {
        let mut out = Vec::with_capacity(PROVISIONED_PURPOSES.len());
        for purpose in PROVISIONED_PURPOSES {
            out.push(self.rotate(tenant_id, purpose).await?);
        }
        Ok(out)
    }

    /// Revoke (or crypto-shred, when `destroy`) all of a tenant's DEKs in the
    /// shared catalog and drop them from this node's maps. Soft revoke marks
    /// every row `revoked` (serve refuses them; reversible); `destroy` deletes
    /// the rows, destroying the wrapped key material (irreversible). Other nodes
    /// drop the keys on their next [`refresh`]. Returns the number of rows
    /// affected in the catalog.
    pub async fn revoke_tenant(&self, tenant_id: &str, destroy: bool) -> Result<u64, CryptoError> {
        let affected = dek_catalog::revoke_tenant(&self.pool, tenant_id, destroy)
            .await
            .map_err(|e| CryptoError::Config(format!("key catalog revoke: {e}")))?;
        // Drop this node's cached leases for the tenant so it stops serving them
        // immediately (soft revoke) / for good (shred).
        self.active
            .write()
            .expect("dek map poisoned")
            .retain(|(t, _), _| t != tenant_id);
        self.by_version
            .write()
            .expect("dek map poisoned")
            .retain(|(t, _, _), _| t != tenant_id);
        Ok(affected)
    }

    /// Retire the read-only DEK versions of `(tenant, purpose)` not in
    /// `versions_in_use` (see the storage-crate counterpart's safety note), then
    /// reload this node's maps so it stops serving the retired versions. Returns
    /// the count retired.
    pub async fn retire_drained_versions(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        versions_in_use: &[u32],
    ) -> Result<usize, CryptoError> {
        let in_use: Vec<i64> = versions_in_use.iter().map(|&v| v as i64).collect();
        let retired =
            dek_catalog::retire_drained_versions(&self.pool, tenant_id, purpose.as_str(), &in_use)
                .await
                .map_err(|e| CryptoError::Config(format!("key catalog retire: {e}")))?;
        // Reload so the retired versions (now non-readable) leave the maps.
        self.refresh().await?;
        Ok(retired as usize)
    }

    fn lease_from_row(
        &self,
        row: &DekCatalogRow,
        purpose: KeyPurpose,
    ) -> Result<DekLease, CryptoError> {
        let wrapped = WrappedDek {
            wrapping_key_ref: row.wrapping_key_ref.clone(),
            nonce: row.nonce.clone(),
            ciphertext: row.ciphertext.clone(),
        };
        let unwrapped = self.kek.unwrap_dek(&row.tenant_id, purpose, &wrapped)?;
        Ok(Arc::new(Dek::new(
            *unwrapped,
            KeyMetadata {
                dek_id: format!("tenant:{}:{}", row.tenant_id, purpose.as_str()),
                version: row.version as u32,
                purpose,
                policy_epoch: 0,
            },
        )))
    }
}

/// Lets the runtime call the provider at tenant creation (the cluster
/// provisioning hook), mapping the crypto error into a runtime error.
#[async_trait::async_trait]
impl agentdb_core::TenantKeyProvisioner for MetaCatalogKeyProvider {
    async fn provision_tenant(&self, tenant_id: &str) -> Result<(), agentdb_core::errors::Error> {
        self.provision_all(tenant_id).await.map_err(|e| {
            agentdb_core::errors::Error::new(
                agentdb_core::errors::ErrorCode::Internal,
                format!("tenant key provisioning failed: {e}"),
            )
        })
    }
}

/// Lets the runtime rotate a tenant's DEKs on the admin path (cluster shape).
#[async_trait::async_trait]
impl agentdb_core::TenantKeyRotator for MetaCatalogKeyProvider {
    async fn rotate_tenant(&self, tenant_id: &str) -> Result<(), agentdb_core::errors::Error> {
        self.rotate_all(tenant_id).await.map(|_| ()).map_err(|e| {
            agentdb_core::errors::Error::new(
                agentdb_core::errors::ErrorCode::Internal,
                format!("tenant key rotation failed: {e}"),
            )
        })
    }
}

/// Lets the runtime revoke / crypto-shred a tenant's DEKs (cluster shape).
#[async_trait::async_trait]
impl agentdb_core::TenantKeyRevoker for MetaCatalogKeyProvider {
    async fn revoke_tenant(
        &self,
        tenant_id: &str,
        destroy: bool,
    ) -> Result<(), agentdb_core::errors::Error> {
        self.revoke_tenant(tenant_id, destroy)
            .await
            .map(|_| ())
            .map_err(|e| {
                agentdb_core::errors::Error::new(
                    agentdb_core::errors::ErrorCode::Internal,
                    format!("tenant key revocation failed: {e}"),
                )
            })
    }
}

/// Lets the runtime retire a tenant's drained DEK versions (cluster shape).
#[async_trait::async_trait]
impl agentdb_core::TenantKeyRetirer for MetaCatalogKeyProvider {
    async fn retire_drained_versions(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        versions_in_use: &[u32],
    ) -> Result<usize, agentdb_core::errors::Error> {
        self.retire_drained_versions(tenant_id, purpose, versions_in_use)
            .await
            .map_err(|e| {
                agentdb_core::errors::Error::new(
                    agentdb_core::errors::ErrorCode::Internal,
                    format!("tenant key retirement failed: {e}"),
                )
            })
    }
}

/// Single-node key admin: adapts the storage-crate [`CatalogKeyProvider`]
/// (whose `rotate` / `revoke_tenant` / `retire_drained_versions` are
/// synchronous) to the runtime's async
/// [`TenantKeyRotator`](agentdb_core::TenantKeyRotator),
/// [`TenantKeyRevoker`](agentdb_core::TenantKeyRevoker), and
/// [`TenantKeyRetirer`](agentdb_core::TenantKeyRetirer) hooks. Lives here
/// because the traits are defined in `agentdb-core`, which the storage crate
/// does not depend on.
pub struct CatalogKeyAdmin {
    provider: Arc<agentdb_storage::CatalogKeyProvider>,
}

impl CatalogKeyAdmin {
    pub fn new(provider: Arc<agentdb_storage::CatalogKeyProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait::async_trait]
impl agentdb_core::TenantKeyRotator for CatalogKeyAdmin {
    async fn rotate_tenant(&self, tenant_id: &str) -> Result<(), agentdb_core::errors::Error> {
        for purpose in PROVISIONED_PURPOSES {
            self.provider.rotate(tenant_id, purpose).map_err(|e| {
                agentdb_core::errors::Error::new(
                    agentdb_core::errors::ErrorCode::Internal,
                    format!("tenant key rotation failed: {e}"),
                )
            })?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl agentdb_core::TenantKeyRevoker for CatalogKeyAdmin {
    async fn revoke_tenant(
        &self,
        tenant_id: &str,
        destroy: bool,
    ) -> Result<(), agentdb_core::errors::Error> {
        self.provider
            .revoke_tenant(tenant_id, destroy)
            .map_err(|e| {
                agentdb_core::errors::Error::new(
                    agentdb_core::errors::ErrorCode::Internal,
                    format!("tenant key revocation failed: {e}"),
                )
            })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl agentdb_core::TenantKeyRetirer for CatalogKeyAdmin {
    async fn retire_drained_versions(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        versions_in_use: &[u32],
    ) -> Result<usize, agentdb_core::errors::Error> {
        self.provider
            .retire_drained_versions(tenant_id, purpose, versions_in_use)
            .map_err(|e| {
                agentdb_core::errors::Error::new(
                    agentdb_core::errors::ErrorCode::Internal,
                    format!("tenant key retirement failed: {e}"),
                )
            })
    }
}

impl KeyProvider for MetaCatalogKeyProvider {
    fn active_dek(&self, tenant_id: &str, purpose: KeyPurpose) -> Result<DekLease, CryptoError> {
        self.active
            .read()
            .expect("dek map poisoned")
            .get(&(tenant_id.to_string(), purpose))
            .cloned()
            .ok_or_else(|| CryptoError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                purpose: purpose.as_str(),
                reason: "no active DEK provisioned (provision it at tenant creation on the \
                         leader, then refresh this node)"
                    .into(),
            })
    }

    fn dek_version(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        version: u32,
    ) -> Result<DekLease, CryptoError> {
        self.by_version
            .read()
            .expect("dek map poisoned")
            .get(&(tenant_id.to_string(), purpose, version))
            .cloned()
            .ok_or_else(|| CryptoError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                purpose: purpose.as_str(),
                reason: format!("DEK version {version} is not cached (refresh from the catalog)"),
            })
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::{
        AeadTenantKeyring, CryptoContext, DekCacheConfig, EnvelopeCrypto, LocalKekProvider, Suite,
    };

    async fn pool() -> AnyPool {
        let meta = agentdb_meta::MetaStore::open_sqlite(":memory:")
            .await
            .unwrap();
        meta.pool().clone()
    }

    fn kek() -> Arc<dyn KekProvider> {
        Arc::new(LocalKekProvider::from_secret(b"cluster-kek", "local:v1"))
    }

    #[tokio::test]
    async fn serve_fails_closed_until_provisioned_then_round_trips() {
        let pool = pool().await;
        let provider = Arc::new(MetaCatalogKeyProvider::load(pool, kek()).await.unwrap());
        // Serve path never mints.
        assert!(provider.active_dek("ten_a", KeyPurpose::Content).is_err());

        provider.provision_all("ten_a").await.unwrap();

        let keyring = AeadTenantKeyring::new(
            provider.clone(),
            Suite::XChaCha20Poly1305,
            DekCacheConfig::default(),
        );
        let ctx = CryptoContext {
            tenant_id: "ten_a",
            purpose: KeyPurpose::Content,
            object_kind: "event",
            object_id: "o1",
        };
        let sealed = keyring.seal(&ctx, b"payload").unwrap();
        assert_eq!(keyring.open(&ctx, &sealed).unwrap(), b"payload");
    }

    #[tokio::test]
    async fn a_second_node_serves_the_same_dek_after_refresh() {
        // Two providers over one shared meta pool = two cluster nodes sharing the
        // catalog. Node A provisions + seals; node B, after a refresh, opens the
        // very same ciphertext.
        let pool = pool().await;
        let node_a = Arc::new(
            MetaCatalogKeyProvider::load(pool.clone(), kek())
                .await
                .unwrap(),
        );
        let node_b = Arc::new(
            MetaCatalogKeyProvider::load(pool.clone(), kek())
                .await
                .unwrap(),
        );
        let ctx = CryptoContext {
            tenant_id: "ten_a",
            purpose: KeyPurpose::Content,
            object_kind: "event",
            object_id: "o1",
        };

        node_a.provision_all("ten_a").await.unwrap();
        let sealed = AeadTenantKeyring::new(
            node_a.clone(),
            Suite::XChaCha20Poly1305,
            DekCacheConfig::default(),
        )
        .seal(&ctx, b"payload")
        .unwrap();

        // Node B hasn't refreshed yet → fails closed.
        assert!(node_b.active_dek("ten_a", KeyPurpose::Content).is_err());
        node_b.refresh().await.unwrap();
        // After refresh, node B opens node A's ciphertext under the shared DEK.
        let opened =
            AeadTenantKeyring::new(node_b, Suite::XChaCha20Poly1305, DekCacheConfig::default())
                .open(&ctx, &sealed)
                .unwrap();
        assert_eq!(opened, b"payload");
    }

    #[tokio::test]
    async fn rotate_advances_active_and_other_nodes_pick_it_up_after_refresh() {
        use agentdb_core::TenantKeyRotator;

        let pool = pool().await;
        let node_a = Arc::new(
            MetaCatalogKeyProvider::load(pool.clone(), kek())
                .await
                .unwrap(),
        );
        let node_b = Arc::new(
            MetaCatalogKeyProvider::load(pool.clone(), kek())
                .await
                .unwrap(),
        );
        let ctx = CryptoContext {
            tenant_id: "ten_a",
            purpose: KeyPurpose::Content,
            object_kind: "event",
            object_id: "o1",
        };

        node_a.provision_all("ten_a").await.unwrap();
        node_b.refresh().await.unwrap();
        // Seal under v1 on node A.
        let sealed_v1 = AeadTenantKeyring::new(
            node_a.clone(),
            Suite::XChaCha20Poly1305,
            DekCacheConfig::default(),
        )
        .seal(&ctx, b"v1-payload")
        .unwrap();
        assert_eq!(
            node_a
                .active_dek("ten_a", KeyPurpose::Content)
                .unwrap()
                .metadata()
                .version,
            1
        );

        // Rotate on node A (the leader would). Active advances to v2.
        node_a.rotate_tenant("ten_a").await.unwrap();
        assert_eq!(
            node_a
                .active_dek("ten_a", KeyPurpose::Content)
                .unwrap()
                .metadata()
                .version,
            2
        );
        // The demoted v1 is still fetchable by version for decryption.
        assert!(node_a.dek_version("ten_a", KeyPurpose::Content, 1).is_ok());

        // A fresh keyring on node A seals under the new active key now (its
        // provider serves v2), and still opens the v1 blob.
        let kr_a = AeadTenantKeyring::new(
            node_a.clone(),
            Suite::XChaCha20Poly1305,
            DekCacheConfig::default(),
        );
        let sealed_v2 = kr_a.seal(&ctx, b"v2-payload").unwrap();
        assert_eq!(kr_a.open(&ctx, &sealed_v1).unwrap(), b"v1-payload");

        // Node B still has only v1 until it refreshes.
        assert_eq!(
            node_b
                .active_dek("ten_a", KeyPurpose::Content)
                .unwrap()
                .metadata()
                .version,
            1
        );
        node_b.refresh().await.unwrap();
        assert_eq!(
            node_b
                .active_dek("ten_a", KeyPurpose::Content)
                .unwrap()
                .metadata()
                .version,
            2
        );
        // After refresh node B opens BOTH the v1 and v2 ciphertext.
        let kr_b =
            AeadTenantKeyring::new(node_b, Suite::XChaCha20Poly1305, DekCacheConfig::default());
        assert_eq!(kr_b.open(&ctx, &sealed_v1).unwrap(), b"v1-payload");
        assert_eq!(kr_b.open(&ctx, &sealed_v2).unwrap(), b"v2-payload");
    }

    #[tokio::test]
    async fn revoke_tenant_soft_then_shred_stops_serving_and_reloads_empty() {
        let pool = pool().await;
        let provider = Arc::new(
            MetaCatalogKeyProvider::load(pool.clone(), kek())
                .await
                .unwrap(),
        );
        provider.provision_all("ten_a").await.unwrap();
        assert!(provider.active_dek("ten_a", KeyPurpose::Content).is_ok());

        // Soft revoke: this node stops serving immediately (maps dropped)...
        let affected = provider.revoke_tenant("ten_a", false).await.unwrap();
        assert_eq!(affected, 2); // content + metadata
        assert!(provider.active_dek("ten_a", KeyPurpose::Content).is_err());
        // ...and a fresh node loading the catalog does NOT serve the revoked keys
        // (rows are marked revoked, so they never enter the active map).
        let reloaded = Arc::new(
            MetaCatalogKeyProvider::load(pool.clone(), kek())
                .await
                .unwrap(),
        );
        assert!(reloaded.active_dek("ten_a", KeyPurpose::Content).is_err());

        // Crypto-shred: the rows are deleted outright.
        assert_eq!(provider.revoke_tenant("ten_a", true).await.unwrap(), 2);
        assert!(dek_catalog::get_version(&pool, "ten_a", "content", 1)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn survives_restart_by_reloading_from_the_catalog() {
        let pool = pool().await;
        let ctx = CryptoContext {
            tenant_id: "ten_a",
            purpose: KeyPurpose::Content,
            object_kind: "event",
            object_id: "o1",
        };
        let first = Arc::new(
            MetaCatalogKeyProvider::load(pool.clone(), kek())
                .await
                .unwrap(),
        );
        first.provision_all("ten_a").await.unwrap();
        let sealed =
            AeadTenantKeyring::new(first, Suite::XChaCha20Poly1305, DekCacheConfig::default())
                .seal(&ctx, b"payload")
                .unwrap();

        // A fresh provider (process restart) pre-warms from the catalog and opens it.
        let restarted = Arc::new(MetaCatalogKeyProvider::load(pool, kek()).await.unwrap());
        let opened = AeadTenantKeyring::new(
            restarted,
            Suite::XChaCha20Poly1305,
            DekCacheConfig::default(),
        )
        .open(&ctx, &sealed)
        .unwrap();
        assert_eq!(opened, b"payload");
    }
}
