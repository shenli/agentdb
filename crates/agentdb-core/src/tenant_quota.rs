//! In-memory global per-tenant admission control.
//!
//! Persistent source of truth is `agentdb_meta::tenant_quotas`.
//! This module is the runtime enforcement path: one token
//! bucket per tenant id, keyed by the external string id
//! (which is what operators configure and what appears in the
//! request URL).
//!
//! Scope choice: this is "aggregate across every box on this
//! node." Cluster-global coordination is out of scope — for
//! multi-node deployments operators divide the quota by node
//! count. See `docs/ARCHITECTURE.md § Per-tenant rate limits`.
//!
//! A denied request increments a per-tenant counter
//! (`deny_counts`) that the metrics exposition surface turns
//! into `agentdb_tenant_quota_denies_total{tenant="..."}`.
//! Per-tenant labels stay bounded because buckets only exist
//! when an operator has deliberately created a quota —
//! cardinality is operator-controlled.
//!
//! This module does NOT record `agentdb_quota_hits_total` on
//! deny; that metric is per-box by design. A tenant-global
//! deny is its own thing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::admission::{AdmissionDenied, Scope, TokenBucket};

/// Runtime configuration of a tenant's bucket. Mirrors
/// [`agentdb_meta::tenant_quotas::TenantQuota`] sans
/// timestamp / tenant_id (those live outside the bucket).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantQuotaConfig {
    pub max_qps: u64,
    pub burst: u64,
}

/// Snapshot row for admin GET responses. Does not include the
/// live token count — bucket state is ephemeral and coupling
/// to it would tempt callers into wrong-shaped retry logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQuotaView {
    pub tenant_id: String,
    pub max_qps: u64,
    pub burst: u64,
    pub deny_count: u64,
}

/// Return of [`TenantQuotaRegistry::hydrate`]. Enumerates the
/// tenants that flipped state in the reload. Useful for the
/// audit log so operators can see "reload added quotas for
/// {A, B} and removed {C}" in one place.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReloadDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

#[derive(Default)]
pub struct TenantQuotaRegistry {
    buckets: Mutex<HashMap<String, BucketEntry>>,
    deny_counts: Mutex<HashMap<String, AtomicU64>>,
}

struct BucketEntry {
    config: TenantQuotaConfig,
    bucket: TokenBucket,
}

impl TenantQuotaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace (or create) the bucket for `tenant_id`. Called
    /// by the admin "set quota" handler after the meta write
    /// lands. Any outstanding bucket state is discarded; the
    /// new bucket starts full at `burst`.
    pub fn set(&self, tenant_id: &str, cfg: TenantQuotaConfig) {
        let mut m = self.buckets.lock();
        m.insert(
            tenant_id.to_string(),
            BucketEntry {
                config: cfg,
                bucket: TokenBucket::new(cfg.max_qps, cfg.burst),
            },
        );
    }

    /// Drop the bucket. Subsequent `admit` calls pass through.
    /// The deny counter is preserved — operators still want to
    /// see how often the quota fired before it was removed.
    pub fn clear(&self, tenant_id: &str) -> bool {
        self.buckets.lock().remove(tenant_id).is_some()
    }

    /// Replace the in-memory bucket set with exactly the
    /// tenants listed in `rows`, reading from the persistent
    /// meta table. Existing buckets for tenants no longer in
    /// `rows` are dropped (matching the "meta is the source
    /// of truth" contract). Deny counters survive.
    ///
    /// Called on server startup (initial hydration) AND on
    /// `POST /api/admin/reload` — the reload path lets an
    /// operator edit the `tenant_quotas` table out-of-band
    /// and pick up the new config without a restart.
    ///
    /// Returns `(added, removed)` — populated with tenant
    /// ids that flipped state across the reload. Useful for
    /// audit messages.
    pub fn hydrate(&self, rows: &[(String, TenantQuotaConfig)]) -> ReloadDiff {
        let incoming: std::collections::HashSet<&str> =
            rows.iter().map(|(tid, _)| tid.as_str()).collect();

        let mut buckets = self.buckets.lock();
        // Drop tenants no longer present.
        let removed: Vec<String> = buckets
            .keys()
            .filter(|tid| !incoming.contains(tid.as_str()))
            .cloned()
            .collect();
        for tid in &removed {
            buckets.remove(tid);
        }
        // Upsert every row. New tenants: fresh bucket (full
        // burst). Existing tenants whose config is unchanged:
        // preserved as-is (no bucket reset). Changed config:
        // replace with a fresh bucket.
        let mut added: Vec<String> = Vec::new();
        for (tid, cfg) in rows {
            let entry = buckets.get(tid);
            let unchanged = entry.map(|e| e.config == *cfg).unwrap_or(false);
            if unchanged {
                continue;
            }
            if entry.is_none() {
                added.push(tid.clone());
            }
            buckets.insert(
                tid.clone(),
                BucketEntry {
                    config: *cfg,
                    bucket: TokenBucket::new(cfg.max_qps, cfg.burst),
                },
            );
        }
        ReloadDiff { added, removed }
    }

    /// Check if a request from `tenant_id` is admitted. Tenants
    /// without a configured quota are always admitted; this is
    /// a fast `HashMap::get` + early return.
    ///
    /// On denial, increments the per-tenant counter visible via
    /// [`snapshot`] / the metrics surface.
    pub fn admit(&self, tenant_id: &str) -> std::result::Result<(), AdmissionDenied> {
        // Fast path: no bucket → admit.
        let mut m = self.buckets.lock();
        let Some(entry) = m.get_mut(tenant_id) else {
            return Ok(());
        };
        if entry.bucket.admit() {
            return Ok(());
        }
        // Drop the buckets lock before taking the deny_counts
        // lock to keep lock order consistent and ensure a busy
        // admit path never blocks on counter writes.
        drop(m);
        self.record_deny(tenant_id);
        Err(AdmissionDenied {
            scope: Scope::TenantGlobal,
        })
    }

    /// Increment the per-tenant deny counter. Exposed for tests
    /// that want to pre-seed state; normal code paths go
    /// through [`admit`].
    pub fn record_deny(&self, tenant_id: &str) {
        // Read-first: if the counter already exists, no write
        // lock is needed. Only missing tenants take the write
        // path. Most denies hit the read path because a tenant
        // that denies once usually denies repeatedly.
        let counts = self.deny_counts.lock();
        if let Some(c) = counts.get(tenant_id) {
            c.fetch_add(1, Ordering::Relaxed);
            return;
        }
        drop(counts);
        let mut counts = self.deny_counts.lock();
        counts
            .entry(tenant_id.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot every configured tenant's quota. Sorted by
    /// tenant id for stable output. Tenants whose bucket was
    /// cleared but that had accumulated denies appear in
    /// [`deny_snapshot`] but not here.
    pub fn snapshot(&self) -> Vec<TenantQuotaView> {
        let buckets = self.buckets.lock();
        let denies = self.deny_counts.lock();
        let mut rows: Vec<TenantQuotaView> = buckets
            .iter()
            .map(|(tid, entry)| TenantQuotaView {
                tenant_id: tid.clone(),
                max_qps: entry.config.max_qps,
                burst: entry.config.burst,
                deny_count: denies
                    .get(tid)
                    .map(|a| a.load(Ordering::Relaxed))
                    .unwrap_or(0),
            })
            .collect();
        rows.sort_by(|a, b| a.tenant_id.cmp(&b.tenant_id));
        rows
    }

    /// Every tenant that has ever denied, whether or not a
    /// bucket is currently configured. Used by the metrics
    /// exposition surface to emit
    /// `agentdb_tenant_quota_denies_total{tenant="..."}`.
    pub fn deny_snapshot(&self) -> Vec<(String, u64)> {
        let counts = self.deny_counts.lock();
        let mut rows: Vec<(String, u64)> = counts
            .iter()
            .map(|(tid, a)| (tid.clone(), a.load(Ordering::Relaxed)))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unconfigured_tenant_is_always_admitted() {
        let r = TenantQuotaRegistry::new();
        for _ in 0..100 {
            assert!(r.admit("unconfigured").is_ok());
        }
    }

    #[test]
    fn configured_tenant_denies_after_burst() {
        let r = TenantQuotaRegistry::new();
        r.set(
            "acme",
            TenantQuotaConfig {
                max_qps: 1,
                burst: 3,
            },
        );
        assert!(r.admit("acme").is_ok());
        assert!(r.admit("acme").is_ok());
        assert!(r.admit("acme").is_ok());
        let err = r.admit("acme").unwrap_err();
        assert_eq!(err.scope, Scope::TenantGlobal);
    }

    #[test]
    fn deny_counter_increments_per_tenant() {
        let r = TenantQuotaRegistry::new();
        r.set(
            "acme",
            TenantQuotaConfig {
                max_qps: 1,
                burst: 1,
            },
        );
        let _ = r.admit("acme");
        assert!(r.admit("acme").is_err());
        assert!(r.admit("acme").is_err());
        let denies: std::collections::HashMap<_, _> = r.deny_snapshot().into_iter().collect();
        assert_eq!(denies.get("acme").copied(), Some(2));
    }

    #[test]
    fn set_replaces_existing_bucket() {
        let r = TenantQuotaRegistry::new();
        r.set(
            "acme",
            TenantQuotaConfig {
                max_qps: 1,
                burst: 1,
            },
        );
        let _ = r.admit("acme");
        assert!(r.admit("acme").is_err());
        // Reset with a larger burst — fresh full bucket admits.
        r.set(
            "acme",
            TenantQuotaConfig {
                max_qps: 10,
                burst: 5,
            },
        );
        for _ in 0..5 {
            assert!(r.admit("acme").is_ok());
        }
    }

    #[test]
    fn clear_drops_bucket_but_preserves_deny_count() {
        let r = TenantQuotaRegistry::new();
        r.set(
            "acme",
            TenantQuotaConfig {
                max_qps: 1,
                burst: 1,
            },
        );
        let _ = r.admit("acme");
        assert!(r.admit("acme").is_err()); // one deny
        assert!(r.clear("acme"));
        // With no bucket, subsequent calls pass through.
        for _ in 0..10 {
            assert!(r.admit("acme").is_ok());
        }
        let denies: std::collections::HashMap<_, _> = r.deny_snapshot().into_iter().collect();
        assert_eq!(
            denies.get("acme").copied(),
            Some(1),
            "deny count survives clear",
        );
    }

    #[test]
    fn clear_returns_false_for_unknown_tenant() {
        let r = TenantQuotaRegistry::new();
        assert!(!r.clear("never-configured"));
    }

    #[test]
    fn snapshot_is_sorted_by_tenant_id() {
        let r = TenantQuotaRegistry::new();
        r.set(
            "zeta",
            TenantQuotaConfig {
                max_qps: 1,
                burst: 1,
            },
        );
        r.set(
            "alpha",
            TenantQuotaConfig {
                max_qps: 2,
                burst: 2,
            },
        );
        r.set(
            "mid",
            TenantQuotaConfig {
                max_qps: 3,
                burst: 3,
            },
        );
        let ids: Vec<String> = r.snapshot().into_iter().map(|q| q.tenant_id).collect();
        assert_eq!(ids, vec!["alpha", "mid", "zeta"]);
    }

    #[test]
    fn hydrate_adds_removes_and_preserves_unchanged() {
        let r = TenantQuotaRegistry::new();
        // Initial hydration: two tenants, one of them later
        // kept unchanged, one later replaced.
        let diff = r.hydrate(&[
            ("alpha".into(), TenantQuotaConfig { max_qps: 1, burst: 1 }),
            ("bravo".into(), TenantQuotaConfig { max_qps: 2, burst: 2 }),
        ]);
        assert_eq!(diff.added.len(), 2);
        assert!(diff.removed.is_empty());
        // Simulate some denies on `bravo` so we can test that
        // an unchanged reload doesn't reset counters.
        r.record_deny("bravo");
        r.record_deny("bravo");

        // Second hydration: alpha unchanged, bravo's config
        // changes, charlie is new, old "bravo" stays but
        // fresh bucket, no removal.
        let diff = r.hydrate(&[
            ("alpha".into(), TenantQuotaConfig { max_qps: 1, burst: 1 }), // same
            ("bravo".into(), TenantQuotaConfig { max_qps: 5, burst: 10 }), // changed
            ("charlie".into(), TenantQuotaConfig { max_qps: 9, burst: 9 }), // added
        ]);
        assert_eq!(diff.added, vec!["charlie".to_string()]);
        assert!(diff.removed.is_empty());

        // Deny counter survives reload (we intentionally
        // don't reset it; operators want continuity).
        let denies: std::collections::HashMap<_, _> = r.deny_snapshot().into_iter().collect();
        assert_eq!(denies.get("bravo").copied(), Some(2));

        // Third hydration: bravo and charlie dropped.
        let diff = r.hydrate(&[
            ("alpha".into(), TenantQuotaConfig { max_qps: 1, burst: 1 }),
        ]);
        let mut removed = diff.removed.clone();
        removed.sort();
        assert_eq!(removed, vec!["bravo".to_string(), "charlie".to_string()]);
        assert!(diff.added.is_empty());

        // Deny counter still survives the removal.
        let denies: std::collections::HashMap<_, _> = r.deny_snapshot().into_iter().collect();
        assert_eq!(denies.get("bravo").copied(), Some(2));
    }
}
