//! Shard-level routing primitives (M20 foundation).
//!
//! The design calls for a fixed `S = 16384` shard space (see
//! `docs/agent-db-v2.md` §2.4). A client's session id hashes to a
//! shard; the shard-to-tablet assignment in the control plane's
//! `shards` table maps to the tablet that owns it. This module
//! ships the *client-side* half — the hash function — plus the
//! [`LocationProxy`] that caches the shard→tablet assignment and
//! refreshes it periodically from meta.
//!
//! This PR only lights up the lookup surface: an admin endpoint can
//! ask "which tablet would session X route to?", and operators can
//! override a shard's assignment. Actual *routing* (the write path
//! using `tablet_for_session` to pick a box / tablet) lands in the
//! next slice when the Box_/tablet_id layout refactor happens.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agentdb_meta::{shards as meta_shards, MetaStore};
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};

/// Shard space size. Fixed at cluster-init (see
/// `agentdb_meta::shards::DEFAULT_SHARD_COUNT`). The value is
/// reproduced here as a `u16`-friendly constant so the hash output
/// type can be a `u16` without runtime casts.
pub const SHARD_COUNT: u32 = 16384;

/// Stable shard id for `session_external`. FNV-1a (64-bit) on the
/// UTF-8 bytes, modulo [`SHARD_COUNT`]. Same algorithm the existing
/// tenant-to-box router uses, so operators who have mental models of
/// the hash distribution don't need a second one.
///
/// Returns `u16` because `SHARD_COUNT <= u16::MAX + 1`.
pub fn shard_id(session_external: &str) -> u16 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in session_external.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as u32 % SHARD_COUNT) as u16
}

/// Live shard-to-tablet lookup. Backed by a cached table refreshed
/// periodically from the control plane's `shards` table. Lookups
/// are lock-free in the hot path (a read-side RwLock + indexed
/// access); refresh takes the write side once per interval.
///
/// `None` for a shard means "no override — use default routing"
/// (the row is present but `tablet_id IS NULL`). Callers decide
/// what default routing means; today the fallback is
/// `shard_id % num_boxes`, matching the existing tenant→box
/// distribution.
pub struct LocationProxy {
    meta: Arc<MetaStore>,
    /// Point-in-time snapshot. Indexed by shard_id. `None` = no
    /// override. Sized to [`SHARD_COUNT`] at construction.
    cache: RwLock<Vec<Option<i64>>>,
    /// Count of successful refreshes since start. Used by admin
    /// endpoints and tests to observe liveness.
    refresh_count: AtomicU64,
    /// Last successful refresh, as ms since UNIX epoch. Zero before
    /// the first refresh completes.
    last_refresh_ms: AtomicI64,
}

impl LocationProxy {
    /// Build a proxy and populate it via an initial full refresh.
    /// Fails if the initial refresh errors — the refresh background
    /// task can later recover from transient errors, but start-up
    /// shouldn't silently run with an empty cache.
    pub async fn new(meta: Arc<MetaStore>) -> Result<Arc<Self>> {
        let proxy = Arc::new(Self {
            meta,
            cache: RwLock::new(vec![None; SHARD_COUNT as usize]),
            refresh_count: AtomicU64::new(0),
            last_refresh_ms: AtomicI64::new(0),
        });
        proxy.refresh().await?;
        Ok(proxy)
    }

    /// Return the tablet id for `session_external`, if the control
    /// plane has an explicit override; otherwise `None` (caller
    /// should fall back to default routing).
    pub fn tablet_for_session(&self, session_external: &str) -> Option<i64> {
        self.tablet_for_shard(shard_id(session_external))
    }

    /// Return the tablet id for `shard`, if any.
    pub fn tablet_for_shard(&self, shard: u16) -> Option<i64> {
        let cache = self.cache.read();
        cache.get(shard as usize).copied().flatten()
    }

    /// Pull the full shard assignment from meta and swap it into the
    /// cache. Called once at startup, periodically by the refresh
    /// task, and explicitly by admin endpoints that need a
    /// strongly-consistent view right after an override.
    pub async fn refresh(&self) -> Result<()> {
        let mut next: Vec<Option<i64>> = vec![None; SHARD_COUNT as usize];
        // Pull every row. At prototype scale this is 16384 rows; a
        // single scan is fast enough that we don't need incremental
        // sync yet. Follow-up PR can add per-shard change
        // notifications on Postgres LISTEN/NOTIFY.
        let rows = meta_shards::list_all(self.meta.pool())
            .await
            .map_err(|e| Error::Invalid {
                field: None,
                message: format!("location_proxy refresh: {e}"),
            })?;
        for row in rows {
            let idx = row.shard_id as usize;
            if idx < next.len() {
                next[idx] = row.tablet_id;
            }
        }
        *self.cache.write() = next;
        self.refresh_count.fetch_add(1, Ordering::Release);
        self.last_refresh_ms.store(now_ms(), Ordering::Release);
        Ok(())
    }

    pub fn refresh_count(&self) -> u64 {
        self.refresh_count.load(Ordering::Acquire)
    }

    pub fn last_refresh_ms(&self) -> i64 {
        self.last_refresh_ms.load(Ordering::Acquire)
    }
}

/// RAII handle for the periodic refresh task. Drop cancels the loop.
pub struct LocationProxyRefreshJob {
    _handle: JoinHandle<()>,
}

impl LocationProxyRefreshJob {
    /// Spawn the refresh loop. Runs every `interval` after an initial
    /// tick delay. Refresh failures log at WARN and retry on the next
    /// interval — a transient meta outage should not stop the
    /// loop entirely.
    pub fn start(proxy: Arc<LocationProxy>, interval: Duration) -> Self {
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // First tick fires immediately; skip it so we align with
            // "refresh every N seconds after start" rather than
            // "refresh on entry".
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(err) = proxy.refresh().await {
                    tracing::warn!(%err, "location proxy refresh failed; will retry next tick");
                }
            }
        });
        Self { _handle: handle }
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_meta::migrations;

    #[test]
    fn shard_id_is_stable_for_the_same_input() {
        let a1 = shard_id("session-42");
        let a2 = shard_id("session-42");
        assert_eq!(a1, a2);
    }

    #[test]
    fn shard_id_fits_in_the_shard_space() {
        for s in ["", "s-1", "very-long-session-id-12345", "日本語"] {
            let id = shard_id(s);
            assert!(
                (id as u32) < SHARD_COUNT,
                "shard_id({s:?}) = {id} should be < {SHARD_COUNT}"
            );
        }
    }

    #[test]
    fn shard_id_distributes_across_the_full_space() {
        // Generate enough keys that every bucket on a 64-key sample
        // modulus is hit at least once. Not a rigorous statistical
        // test — just a smoke check that the hash isn't degenerate.
        let mut buckets = [0u32; 256];
        for i in 0..10_000u32 {
            let s = format!("session-{i}");
            let id = shard_id(&s);
            buckets[(id as usize) % 256] += 1;
        }
        // Every bucket in 256 must see at least 10 entries.
        for (i, count) in buckets.iter().enumerate() {
            assert!(
                *count >= 10,
                "bucket {i} got only {count} hits — hash degenerate?"
            );
        }
    }

    async fn fresh_meta() -> Arc<MetaStore> {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        migrations::apply(meta.pool()).await.unwrap();
        // Seed the shards table so list_all returns full rows.
        meta_shards::seed_shards(meta.pool(), SHARD_COUNT as i64, 0)
            .await
            .unwrap();
        Arc::new(meta)
    }

    #[tokio::test]
    async fn new_initializes_an_empty_cache_via_refresh() {
        let meta = fresh_meta().await;
        let proxy = LocationProxy::new(meta.clone()).await.unwrap();
        assert_eq!(proxy.refresh_count(), 1);
        // Nothing was assigned — every lookup is None.
        assert_eq!(proxy.tablet_for_session("anything"), None);
        assert_eq!(proxy.tablet_for_shard(0), None);
        assert_eq!(proxy.tablet_for_shard(SHARD_COUNT as u16 - 1), None);
    }

    #[tokio::test]
    async fn refresh_picks_up_assignments_set_in_meta() {
        let meta = fresh_meta().await;
        let proxy = LocationProxy::new(meta.clone()).await.unwrap();
        // Set shard 7 to tablet 99.
        meta_shards::set_assignment(meta.pool(), 7, Some(99), 1)
            .await
            .unwrap();
        // Before refresh — still empty.
        assert_eq!(proxy.tablet_for_shard(7), None);
        // After refresh — reflects the override.
        proxy.refresh().await.unwrap();
        assert_eq!(proxy.tablet_for_shard(7), Some(99));
        assert_eq!(proxy.refresh_count(), 2);
    }

    #[tokio::test]
    async fn tablet_for_session_goes_through_shard_hash() {
        let meta = fresh_meta().await;
        let proxy = LocationProxy::new(meta.clone()).await.unwrap();
        let s = "session-route-me";
        let shard = shard_id(s);
        meta_shards::set_assignment(meta.pool(), shard as i64, Some(42), 0)
            .await
            .unwrap();
        proxy.refresh().await.unwrap();
        assert_eq!(proxy.tablet_for_session(s), Some(42));
        // A different session lands on a different shard — still None
        // unless it happens to hash-collide onto shard 7.
        let other_shard = shard_id("session-other");
        if other_shard != shard {
            assert_eq!(proxy.tablet_for_session("session-other"), None);
        }
    }

    #[tokio::test]
    async fn last_refresh_ms_advances_on_refresh() {
        let meta = fresh_meta().await;
        let proxy = LocationProxy::new(meta.clone()).await.unwrap();
        let first = proxy.last_refresh_ms();
        assert!(first > 0);
        tokio::time::sleep(Duration::from_millis(5)).await;
        proxy.refresh().await.unwrap();
        let second = proxy.last_refresh_ms();
        assert!(second > first, "last_refresh_ms should advance");
    }

    #[tokio::test]
    async fn refresh_job_ticks_and_updates_the_cache() {
        let meta = fresh_meta().await;
        let proxy = LocationProxy::new(meta.clone()).await.unwrap();
        assert_eq!(proxy.refresh_count(), 1);
        let _job = LocationProxyRefreshJob::start(proxy.clone(), Duration::from_millis(50));
        // Sleep well past the first-tick-after-start window. The
        // spawned task skips the immediate tick then fires every
        // 50ms. Give 400ms to be robust under load, and only assert
        // at least one extra refresh beyond the initial one — the
        // signal is "the job is alive and calling refresh", not the
        // exact tick count.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let after = proxy.refresh_count();
        assert!(
            after >= 2,
            "expected at least one extra refresh from the job; saw {after}"
        );
    }
}
