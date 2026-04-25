//! Tablet lease management (M21, scoped).
//!
//! Acquires a lease on a tablet via `agentdb-meta::tablets`, renews
//! it periodically, and exposes an `is_live()` check the write path
//! uses to fail fast when the lease is lost. This is the ownership-
//! coordination half of the fencing-token pattern committed to in
//! `docs/agent-db-v2.md` §5.5; the storage-path fencing half (epoch-
//! tagged WAL/segment paths) is a follow-up.
//!
//! Design invariants:
//!
//! 1. **Lease is read-through an atomic timestamp**, not a method
//!    call. `is_live()` is a single atomic load + wall-clock compare
//!    — safe for every append path without contention.
//!
//! 2. **Epoch only goes up**. Each `assume_ownership` increments it
//!    on the server (M18); we store the most recent epoch locally so
//!    writers can tag it onto future storage-path writes.
//!
//! 3. **When renewal fails**, we do **not** immediately clear the
//!    lease — we let it expire naturally so writers keep accepting
//!    appends until `lease_expires_at_ms` actually passes. Matches
//!    the design doc's "stop writing when lease expires" semantics;
//!    a transient meta outage shorter than the renewal margin does
//!    not kill writes.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentdb_meta::{tablets, MetaStore};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};

/// Lease configuration — tunable at `MetaStore` attach time.
#[derive(Debug, Clone, Copy)]
pub struct LeaseConfig {
    /// How often we try to renew. Must be materially less than
    /// `duration` to leave room for transient meta slowness.
    pub renew: Duration,
    /// Lease lifetime per renewal. Lease expires at
    /// `now() + duration` after a successful renewal.
    pub duration: Duration,
    /// Safety margin the scheduler will wait beyond lease expiry
    /// before reassigning. We store it for callers that want to
    /// read the expected takeover window; we don't enforce it here.
    pub safety_margin: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            renew: Duration::from_secs(10),
            duration: Duration::from_secs(30),
            safety_margin: Duration::from_secs(5),
        }
    }
}

impl LeaseConfig {
    /// Read from env:
    /// - `AGENTDB_LEASE_RENEW_SECS`   (default 10)
    /// - `AGENTDB_LEASE_DURATION_SECS` (default 30)
    /// - `AGENTDB_LEASE_SAFETY_MARGIN_SECS` (default 5)
    pub fn from_env() -> Self {
        let default = Self::default();
        let renew = std::env::var("AGENTDB_LEASE_RENEW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(default.renew);
        let duration = std::env::var("AGENTDB_LEASE_DURATION_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(default.duration);
        let safety_margin = std::env::var("AGENTDB_LEASE_SAFETY_MARGIN_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(default.safety_margin);
        Self {
            renew,
            duration,
            safety_margin,
        }
    }
}

/// Live lease state for one tablet. Exposed via `Box_::lease` (an
/// `Option<Arc<LeaseManager>>`) so when absent, no lease enforcement
/// applies and legacy tests keep working unchanged.
pub struct LeaseManager {
    meta: Arc<MetaStore>,
    tablet_id: i64,
    node_id: String,
    config: LeaseConfig,
    /// Most recent epoch observed via `assume_ownership`. Used by
    /// follow-up PRs to tag WAL/segment writes.
    epoch: AtomicI64,
    /// Unix ms at which the current lease expires. `is_live` compares
    /// this against wall clock; writer-path hot-path hits this atomic
    /// with zero contention.
    lease_expires_at_ms: AtomicI64,
    metrics: Arc<crate::metrics::BoxMetrics>,
}

impl LeaseManager {
    /// Acquire an initial lease for `tablet_id`. Side effect:
    /// `tablets` row is created if absent, and `epoch` is bumped by
    /// one as part of `assume_ownership`.
    #[tracing::instrument(
        name = "lease.acquire",
        skip(meta, config, metrics),
        fields(tablet_id = tablet_id, node_id = %node_id),
    )]
    pub async fn acquire(
        meta: Arc<MetaStore>,
        tablet_id: i64,
        node_id: String,
        config: LeaseConfig,
        metrics: Arc<crate::metrics::BoxMetrics>,
    ) -> Result<Arc<Self>> {
        let now_ms = now_ms();
        tablets::get_or_create(meta.pool(), tablet_id, now_ms)
            .await
            .map_err(lease_meta_err)?;
        let row = tablets::get(meta.pool(), tablet_id)
            .await
            .map_err(lease_meta_err)?
            .ok_or_else(|| Error::Invalid {
                field: None,
                message: format!("tablet {tablet_id} disappeared between create and get"),
            })?;
        // Claim or renew. The current row's epoch is used as the
        // expected epoch; if two processes race, the loser gets
        // Conflict and will retry on next tick.
        let new_expiry = now_ms + config.duration.as_millis() as i64;
        let updated =
            tablets::assume_ownership(meta.pool(), tablet_id, row.epoch, &node_id, new_expiry)
                .await
                .map_err(lease_meta_err)?;

        metrics.record_lease_renewal();
        metrics.set_lease_epoch(updated.epoch);
        Ok(Arc::new(Self {
            meta,
            tablet_id,
            node_id,
            config,
            epoch: AtomicI64::new(updated.epoch),
            lease_expires_at_ms: AtomicI64::new(updated.lease_expires_at_ms),
            metrics,
        }))
    }

    /// Attempt to renew. On success, advance `lease_expires_at_ms`
    /// and return new expiry. On failure (meta unreachable, epoch
    /// stolen, etc.), leave local state unchanged so `is_live` still
    /// reports true until the old lease expires naturally.
    #[tracing::instrument(
        name = "lease.renew",
        skip(self),
        fields(tablet_id = self.tablet_id, node_id = %self.node_id),
    )]
    pub async fn renew(&self) -> Result<i64> {
        let now_ms = now_ms();
        let expected_epoch = self.epoch.load(Ordering::Acquire);
        let new_expiry = now_ms + self.config.duration.as_millis() as i64;
        match tablets::assume_ownership(
            self.meta.pool(),
            self.tablet_id,
            expected_epoch,
            &self.node_id,
            new_expiry,
        )
        .await
        {
            Ok(updated) => {
                // assume_ownership always bumps epoch by one on
                // success. Store the new value so subsequent renewals
                // compare against it.
                self.epoch.store(updated.epoch, Ordering::Release);
                self.lease_expires_at_ms
                    .store(updated.lease_expires_at_ms, Ordering::Release);
                self.metrics.record_lease_renewal();
                self.metrics.set_lease_epoch(updated.epoch);
                Ok(updated.lease_expires_at_ms)
            }
            Err(e) => {
                self.metrics.record_lease_renewal_failure();
                Err(lease_meta_err(e))
            }
        }
    }

    /// Is the lease still live per wall-clock? Hot-path, atomic-only.
    pub fn is_live(&self) -> bool {
        let expiry = self.lease_expires_at_ms.load(Ordering::Acquire);
        now_ms() < expiry
    }

    pub fn tablet_id(&self) -> i64 {
        self.tablet_id
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn current_epoch(&self) -> i64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn expires_at_ms(&self) -> i64 {
        self.lease_expires_at_ms.load(Ordering::Acquire)
    }

    pub fn config(&self) -> LeaseConfig {
        self.config
    }

    /// Test hook: force the lease to appear expired to `is_live()` by
    /// writing a past timestamp into the expiry atomic. Does **not**
    /// touch the control-plane row; the next successful `renew()` will
    /// re-populate the atomic and restore liveness. Kept `pub` so
    /// integration tests in `tests/` can reach it; production callers
    /// have no reason to use it.
    #[doc(hidden)]
    pub fn expire_for_test(&self) {
        self.lease_expires_at_ms
            .store(now_ms() - 1, Ordering::Release);
    }
}

/// RAII handle for the per-tablet lease-renewal background task.
/// Drops cancel the task.
pub struct LeaseRenewalJob {
    _handle: JoinHandle<()>,
}

impl LeaseRenewalJob {
    /// Spawn the renewal loop. `lease.renew()` runs every
    /// `lease.config().renew`; failures are logged but do not abort
    /// the loop — a transient meta outage shorter than the renewal
    /// margin should not kill writes.
    ///
    /// This overload exists for tests and legacy paths that don't
    /// have a drain flag to wire up. Production callers should
    /// prefer [`Self::start_with_drain`].
    pub fn start(lease: Arc<LeaseManager>) -> Self {
        Self::start_with_drain(lease, Arc::new(std::sync::atomic::AtomicBool::new(false)))
    }

    /// Spawn the renewal loop, tied to a process-level drain flag
    /// (typically `BoxRegistry::draining_flag()`). When the flag
    /// flips to true (via `POST /api/admin/drain`), the next tick
    /// skips the `lease.renew()` call. The lease expires naturally
    /// after `lease.config().duration` and the stale-lease reaper
    /// on another node clears the tablet row — without needing the
    /// operator to SIGTERM first.
    ///
    /// Counter behavior under drain: `lease_renewals` stops
    /// advancing; `lease_renewal_failures` also stops. The lease's
    /// `is_live()` will return false once wall clock passes
    /// `lease_expires_at_ms`, at which point the write path returns
    /// `LeaseLost` — but the drain check in
    /// `append_and_fetch_with_actor` fires first, so the client
    /// actually sees `NodeDraining`.
    pub fn start_with_drain(
        lease: Arc<LeaseManager>,
        drain_flag: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(lease.config().renew);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // First tick fires immediately; skip to align with "renew
            // every N seconds after the initial acquire".
            interval.tick().await;
            let mut drain_logged = false;
            loop {
                interval.tick().await;
                if drain_flag.load(std::sync::atomic::Ordering::Acquire) {
                    if !drain_logged {
                        tracing::info!(
                            tablet_id = lease.tablet_id(),
                            node_id = %lease.node_id(),
                            "lease renewal halted: node is draining; lease will expire naturally"
                        );
                        drain_logged = true;
                    }
                    continue;
                }
                if let Err(err) = lease.renew().await {
                    tracing::warn!(
                        tablet_id = lease.tablet_id(),
                        node_id = %lease.node_id(),
                        error = %err,
                        "lease renewal failed; lease will expire naturally unless recovered"
                    );
                    // If the lease has already expired by wall clock,
                    // there's nothing this loop can do — lose writes
                    // happens at the write-path is_live check.
                    if !lease.is_live() {
                        lease.metrics.record_lease_lost();
                    }
                }
            }
        });
        Self { _handle: handle }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn lease_meta_err(e: agentdb_meta::MetaError) -> Error {
    Error::Invalid {
        field: None,
        message: format!("lease meta operation failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn meta_in_memory() -> Arc<MetaStore> {
        Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap())
    }

    #[tokio::test]
    async fn acquire_creates_tablet_and_grants_lease() {
        let meta = meta_in_memory().await;
        let metrics = Arc::new(crate::metrics::BoxMetrics::new());
        let lease = LeaseManager::acquire(
            meta.clone(),
            1,
            "node-a".into(),
            LeaseConfig::default(),
            metrics.clone(),
        )
        .await
        .unwrap();
        assert_eq!(lease.tablet_id(), 1);
        assert_eq!(lease.node_id(), "node-a");
        assert!(lease.current_epoch() >= 1, "epoch bumped on claim");
        assert!(lease.is_live(), "fresh lease must be live");

        let snap = metrics.snapshot();
        assert_eq!(snap.lease_renewals, 1);
        assert_eq!(snap.lease_epoch as i64, lease.current_epoch());
    }

    #[tokio::test]
    async fn renew_extends_expiry_and_bumps_epoch() {
        let meta = meta_in_memory().await;
        let metrics = Arc::new(crate::metrics::BoxMetrics::new());
        let lease = LeaseManager::acquire(
            meta.clone(),
            2,
            "node-a".into(),
            LeaseConfig::default(),
            metrics.clone(),
        )
        .await
        .unwrap();
        let epoch_before = lease.current_epoch();
        let expiry_before = lease.expires_at_ms();
        // Tiny sleep so a subsequent renew picks up a later `now_ms`.
        tokio::time::sleep(Duration::from_millis(5)).await;
        lease.renew().await.unwrap();
        assert_eq!(
            lease.current_epoch(),
            epoch_before + 1,
            "renew bumps epoch by 1"
        );
        assert!(
            lease.expires_at_ms() > expiry_before,
            "renew extends expiry"
        );
        assert_eq!(metrics.snapshot().lease_renewals, 2);
    }

    #[tokio::test]
    async fn stolen_lease_renew_fails_and_records_metric() {
        let meta = meta_in_memory().await;
        let metrics_a = Arc::new(crate::metrics::BoxMetrics::new());
        let lease_a = LeaseManager::acquire(
            meta.clone(),
            3,
            "node-a".into(),
            LeaseConfig::default(),
            metrics_a.clone(),
        )
        .await
        .unwrap();

        // A different node's acquire reads the current epoch and
        // claims ownership. Now lease_a's cached epoch is stale.
        let metrics_b = Arc::new(crate::metrics::BoxMetrics::new());
        let _lease_b = LeaseManager::acquire(
            meta.clone(),
            3,
            "node-b".into(),
            LeaseConfig::default(),
            metrics_b,
        )
        .await
        .unwrap();

        let err = lease_a.renew().await.unwrap_err();
        assert!(
            err.to_string().contains("conflict") || err.to_string().contains("epoch"),
            "expected conflict-related error; got: {err}"
        );
        assert_eq!(metrics_a.snapshot().lease_renewal_failures, 1);
    }

    #[tokio::test]
    async fn is_live_flips_to_false_when_expiry_is_in_the_past() {
        // `is_live` compares the stored lease-expiry atomic against
        // wall clock (not tokio's clock), so simulate expiration by
        // writing a past timestamp directly. Using `start_paused` +
        // `advance` would be cleaner but also pauses sqlx's pool
        // keep-alive timers and hangs the connection.
        let meta = meta_in_memory().await;
        let metrics = Arc::new(crate::metrics::BoxMetrics::new());
        let lease =
            LeaseManager::acquire(meta, 4, "node-a".into(), LeaseConfig::default(), metrics)
                .await
                .unwrap();
        assert!(lease.is_live());
        lease
            .lease_expires_at_ms
            .store(now_ms() - 1, Ordering::Release);
        assert!(!lease.is_live());
    }

    #[tokio::test]
    async fn env_config_parses() {
        std::env::remove_var("AGENTDB_LEASE_RENEW_SECS");
        std::env::remove_var("AGENTDB_LEASE_DURATION_SECS");
        std::env::remove_var("AGENTDB_LEASE_SAFETY_MARGIN_SECS");
        let c = LeaseConfig::from_env();
        assert_eq!(c.renew, Duration::from_secs(10));
        assert_eq!(c.duration, Duration::from_secs(30));

        std::env::set_var("AGENTDB_LEASE_RENEW_SECS", "4");
        std::env::set_var("AGENTDB_LEASE_DURATION_SECS", "12");
        let c = LeaseConfig::from_env();
        assert_eq!(c.renew, Duration::from_secs(4));
        assert_eq!(c.duration, Duration::from_secs(12));
        std::env::remove_var("AGENTDB_LEASE_RENEW_SECS");
        std::env::remove_var("AGENTDB_LEASE_DURATION_SECS");
    }

    #[tokio::test]
    async fn renewal_loop_stops_when_drain_flag_is_set_before_start() {
        // Drain already set at spawn time: the renewal loop must
        // skip every tick and never call lease.renew(). We verify by
        // watching lease_renewals — it starts at 1 (from the initial
        // acquire) and must remain 1.
        let meta = meta_in_memory().await;
        let metrics = Arc::new(crate::metrics::BoxMetrics::new());
        let lease = LeaseManager::acquire(
            meta,
            5,
            "node-a".into(),
            LeaseConfig {
                renew: Duration::from_millis(20),
                duration: Duration::from_secs(30),
                safety_margin: Duration::from_secs(5),
            },
            metrics.clone(),
        )
        .await
        .unwrap();
        assert_eq!(metrics.snapshot().lease_renewals, 1, "initial acquire");

        let drain = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let _job = LeaseRenewalJob::start_with_drain(lease, drain);
        // Let several tick intervals pass.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            metrics.snapshot().lease_renewals,
            1,
            "drained loop must not renew"
        );
    }

    #[tokio::test]
    async fn renewal_loop_stops_after_drain_flag_flips_mid_run() {
        // Drain flips after one or more successful renewals: the
        // counter advances for a bit and then stops.
        let meta = meta_in_memory().await;
        let metrics = Arc::new(crate::metrics::BoxMetrics::new());
        let lease = LeaseManager::acquire(
            meta,
            6,
            "node-a".into(),
            LeaseConfig {
                renew: Duration::from_millis(20),
                duration: Duration::from_secs(30),
                safety_margin: Duration::from_secs(5),
            },
            metrics.clone(),
        )
        .await
        .unwrap();

        let drain = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let _job = LeaseRenewalJob::start_with_drain(lease, drain.clone());
        // Run for a while so renewals accumulate.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let renewals_before = metrics.snapshot().lease_renewals;
        assert!(
            renewals_before > 1,
            "expected at least one renewal before drain; got {renewals_before}"
        );

        // Flip drain; wait well past several tick periods.
        drain.store(true, std::sync::atomic::Ordering::Release);
        tokio::time::sleep(Duration::from_millis(120)).await;
        let renewals_after_drain = metrics.snapshot().lease_renewals;

        // Give yet more time to confirm the counter has frozen.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let renewals_final = metrics.snapshot().lease_renewals;
        assert_eq!(
            renewals_after_drain, renewals_final,
            "after drain, renewals must stop advancing"
        );
    }
}
