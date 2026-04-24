//! Stale-lease tablet reaper. The one thing the scheduler leader
//! (see `scheduler::LeaderElection`) does today: periodically sweep
//! `tablets` rows whose lease has been dead longer than
//! `safety_margin` and clear their owner. Follow-up PRs layer on
//! reassignment policy (cross-AZ preference, anti-affinity) and
//! the `agentdb drain` orchestration.
//!
//! Design invariants:
//!
//! 1. **Only the leader reaps.** Followers call `is_leader()` every
//!    tick and no-op when false. This means two nodes can't race to
//!    clear the same row (which would be harmless via the meta
//!    layer's atomic UPDATE, but would double the audit-log noise
//!    and confuse operators).
//!
//! 2. **Safety margin protects slow renewals.** A node whose
//!    renewal is a few seconds late — not dead, just slow — must
//!    not lose its tablets. The meta-layer SQL predicate is
//!    `expires_at + safety_margin < now`, which moves the cliff out
//!    by `safety_margin_ms`.
//!
//! 3. **Transient meta errors don't count as work done.** If the
//!    sweep errors (meta unreachable), we log WARN and try again
//!    next tick. `reassignments_total` does not advance on failure
//!    so operators see the real count.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentdb_meta::{shards as meta_shards, tablets, MetaStore};
use tokio::task::JoinHandle;

use crate::scheduler::LeaderElection;
use crate::shard::LocationProxy;

/// Tunable timings for the reaper loop.
#[derive(Debug, Clone, Copy)]
pub struct ReaperConfig {
    /// How often to sweep. Defaults to 10s. Lower = faster reaction
    /// to dead nodes; higher = less meta load. The reaper does a
    /// single bulk UPDATE per tick, so meta load is modest.
    pub tick: Duration,
    /// Extra grace period beyond nominal lease expiry before the
    /// reaper intervenes. Defaults to 5s. Must be larger than the
    /// typical jitter in lease renewals to avoid reaping slow-but-
    /// alive owners.
    pub safety_margin: Duration,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(10),
            safety_margin: Duration::from_secs(5),
        }
    }
}

impl ReaperConfig {
    /// Env:
    /// - `AGENTDB_REAPER_TICK_SECS`          (default 10)
    /// - `AGENTDB_REAPER_SAFETY_MARGIN_SECS` (default 5)
    pub fn from_env() -> Self {
        let d = Self::default();
        let tick = std::env::var("AGENTDB_REAPER_TICK_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.tick);
        let safety_margin = std::env::var("AGENTDB_REAPER_SAFETY_MARGIN_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.safety_margin);
        Self {
            tick,
            safety_margin,
        }
    }
}

/// Background task that reaps expired tablet leases while this
/// process is the scheduler leader.
pub struct StaleTabletReaper {
    reassignments_total: Arc<AtomicU64>,
    shard_reassignments_total: Arc<AtomicU64>,
    config: ReaperConfig,
    _handle: JoinHandle<()>,
}

impl StaleTabletReaper {
    /// Spawn the reap loop. Cheap; the loop is idle on followers
    /// except for a tick-rate atomic load. Returns `Arc<Self>` for
    /// admin handlers to read the counter.
    ///
    /// `location` is optional: when provided, after the reaper
    /// reassigns shards it force-refreshes the proxy so this node's
    /// routing decisions pick up the new assignment on the next
    /// write (rather than waiting for the periodic refresh tick).
    /// The scheduler-leader node is always also a node that accepts
    /// writes today, so keeping the local proxy current matters.
    pub fn start(
        meta: Arc<MetaStore>,
        election: Arc<LeaderElection>,
        config: ReaperConfig,
        location: Option<Arc<LocationProxy>>,
    ) -> Arc<Self> {
        let reassignments_total = Arc::new(AtomicU64::new(0));
        let shard_reassignments_total = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            meta,
            election,
            config,
            reassignments_total.clone(),
            shard_reassignments_total.clone(),
            location,
        ));
        Arc::new(Self {
            reassignments_total,
            shard_reassignments_total,
            config,
            _handle: handle,
        })
    }

    /// Total number of tablet leases this process has reaped across
    /// its lifetime as leader. Followers always report 0 (they
    /// never hit the reap path). Reads are atomic and lock-free.
    pub fn reassignments_total(&self) -> u64 {
        self.reassignments_total.load(Ordering::Acquire)
    }

    /// Total number of shard overrides this reaper has rewritten
    /// while reassigning away from a reaped tablet. One shard move
    /// counts as one here; a tablet that owned 500 shards at reap
    /// time contributes 500. Distinct from
    /// `reassignments_total`, which counts tablet-clears.
    pub fn shard_reassignments_total(&self) -> u64 {
        self.shard_reassignments_total.load(Ordering::Acquire)
    }

    /// The configuration this reaper is running with. Useful for
    /// admin endpoints that want to surface tick / safety-margin to
    /// operators alongside the counter.
    pub fn config(&self) -> ReaperConfig {
        self.config
    }
}

async fn run_loop(
    meta: Arc<MetaStore>,
    election: Arc<LeaderElection>,
    config: ReaperConfig,
    reassignments_total: Arc<AtomicU64>,
    shard_reassignments_total: Arc<AtomicU64>,
    location: Option<Arc<LocationProxy>>,
) {
    let safety_margin_ms = config.safety_margin.as_millis() as i64;
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // First tick fires immediately; skip it so we align with "reap
    // every N seconds after startup" rather than "reap on boot".
    interval.tick().await;
    loop {
        interval.tick().await;
        if !election.is_leader() {
            continue;
        }
        let now = now_ms();
        match tablets::clear_expired_ownership(meta.pool(), safety_margin_ms, now).await {
            Ok(ids) if ids.is_empty() => {
                // Most common case on a healthy cluster — nothing to do.
            }
            Ok(ids) => {
                let n = ids.len();
                reassignments_total.fetch_add(n as u64, Ordering::Release);
                tracing::info!(
                    reaped = n,
                    ?ids,
                    safety_margin_ms,
                    "stale-lease reaper cleared tablet ownership"
                );
                // M22: reassign shards that pointed at the reaped
                // tablets. Failures here are logged but don't stop
                // the loop — next tick will retry.
                if let Err(err) = reassign_orphaned_shards(
                    &meta,
                    &ids,
                    now,
                    &shard_reassignments_total,
                    location.as_ref(),
                )
                .await
                {
                    tracing::warn!(
                        %err,
                        "shard reassignment after tablet reap failed; will retry next tick"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    %err,
                    "stale-lease reaper sweep failed; will retry on next tick"
                );
            }
        }
    }
}

/// For each reaped tablet, move its shard overrides to a live
/// tablet, round-robin across the live set. Returns early (no
/// error) when the live set is empty — there's no one to reassign
/// to, the shards stay orphaned, and operators see the warning in
/// the log. Refreshes the local `LocationProxy` at the end when
/// one is attached, so the write path picks up the new routing on
/// the next request instead of waiting for the periodic refresh.
async fn reassign_orphaned_shards(
    meta: &MetaStore,
    reaped_tablets: &[i64],
    now_ms: i64,
    shard_reassignments_total: &AtomicU64,
    location: Option<&Arc<LocationProxy>>,
) -> crate::error::Result<()> {
    // Live tablets = still owned + lease hasn't expired. We just
    // cleared the dead ones in clear_expired_ownership, so the
    // remaining owned rows are all live by construction.
    let all_tablets = tablets::list_all(meta.pool())
        .await
        .map_err(|e| crate::error::Error::Invalid {
            field: None,
            message: format!("reaper: list tablets: {e}"),
        })?;
    let live_ids: Vec<i64> = all_tablets
        .iter()
        .filter(|t| t.owner_node_id.is_some() && t.lease_expires_at_ms > now_ms)
        .map(|t| t.tablet_id)
        .collect();
    if live_ids.is_empty() {
        tracing::warn!(
            reaped_tablets = ?reaped_tablets,
            "no live tablet to reassign orphaned shards to; overrides left in place"
        );
        return Ok(());
    }

    let mut cursor = 0usize;
    let mut moved = 0u64;
    for &dead_tablet in reaped_tablets {
        let shards = meta_shards::list_by_tablet(meta.pool(), dead_tablet)
            .await
            .map_err(|e| crate::error::Error::Invalid {
                field: None,
                message: format!("reaper: list shards for tablet {dead_tablet}: {e}"),
            })?;
        for shard_id in shards {
            let new_tablet = live_ids[cursor % live_ids.len()];
            cursor = cursor.wrapping_add(1);
            meta_shards::set_assignment(meta.pool(), shard_id, Some(new_tablet), now_ms)
                .await
                .map_err(|e| crate::error::Error::Invalid {
                    field: None,
                    message: format!("reaper: reassign shard {shard_id}: {e}"),
                })?;
            moved += 1;
            tracing::info!(
                shard_id,
                old_tablet = dead_tablet,
                new_tablet,
                "reaper reassigned orphaned shard"
            );
        }
    }
    if moved > 0 {
        shard_reassignments_total.fetch_add(moved, Ordering::Release);
        // Force a local proxy refresh so this node's write path
        // sees the new routing on the next request. A periodic
        // refresh would pick it up too, but potentially after
        // seconds of stale routing.
        if let Some(loc) = location {
            if let Err(err) = loc.refresh().await {
                tracing::warn!(
                    %err,
                    "reaper: post-reassign location proxy refresh failed"
                );
            }
        }
    }
    Ok(())
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
    use crate::scheduler::{LeaderElection, LeaderElectionConfig};
    use agentdb_meta::tablets as meta_tablets;

    async fn file_backed_meta() -> (tempfile::NamedTempFile, Arc<MetaStore>) {
        // Shared file so the election loop and the reaper see each
        // other's writes (`:memory:` is per-connection).
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let meta = Arc::new(MetaStore::open_sqlite(&path).await.unwrap());
        (file, meta)
    }

    #[tokio::test]
    async fn env_config_parses_with_defaults_when_unset() {
        std::env::remove_var("AGENTDB_REAPER_TICK_SECS");
        std::env::remove_var("AGENTDB_REAPER_SAFETY_MARGIN_SECS");
        let c = ReaperConfig::from_env();
        assert_eq!(c.tick, Duration::from_secs(10));
        assert_eq!(c.safety_margin, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn reaper_clears_expired_ownership_when_it_is_leader() {
        let (_file, meta) = file_backed_meta().await;
        // Seed a tablet with an ancient, expired lease.
        meta_tablets::get_or_create(meta.pool(), 7, 0)
            .await
            .unwrap();
        meta_tablets::assume_ownership(meta.pool(), 7, 0, "dead-node", 1)
            .await
            .unwrap();

        // Spawn election; wait for it to win.
        let election = LeaderElection::start(
            meta.clone(),
            "reaper-node".into(),
            LeaderElectionConfig {
                lease_duration: Duration::from_secs(10),
                renew_interval: Duration::from_millis(100),
                attempt_interval: Duration::from_millis(50),
            },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(election.is_leader(), "election must have converged");

        // Spawn reaper with a tiny tick so we don't have to wait 10s.
        let reaper = StaleTabletReaper::start(
            meta.clone(),
            election.clone(),
            ReaperConfig {
                tick: Duration::from_millis(100),
                safety_margin: Duration::from_millis(0),
            },
            None,
        );
        // Give the reaper time to run at least one sweep (first tick
        // is skipped, so we need at least 2× tick = 200ms).
        tokio::time::sleep(Duration::from_millis(400)).await;

        let row = meta_tablets::get(meta.pool(), 7).await.unwrap().unwrap();
        assert_eq!(row.owner_node_id, None, "stale owner must be cleared");
        assert!(reaper.reassignments_total() >= 1);
    }

    #[tokio::test]
    async fn reaper_does_nothing_when_this_node_is_not_leader() {
        // Two competing election loops → only one wins. The loser
        // spawns a reaper; the reaper sees is_leader()=false every
        // tick and does nothing.
        let (_file, meta) = file_backed_meta().await;
        meta_tablets::get_or_create(meta.pool(), 1, 0)
            .await
            .unwrap();
        meta_tablets::assume_ownership(meta.pool(), 1, 0, "dead-node", 1)
            .await
            .unwrap();

        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let winner = LeaderElection::start(meta.clone(), "winner".into(), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(winner.is_leader());

        let loser = LeaderElection::start(meta.clone(), "loser".into(), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!loser.is_leader(), "second election must stay follower");

        // Reaper on the loser must never reap.
        let reaper_on_loser = StaleTabletReaper::start(
            meta.clone(),
            loser.clone(),
            ReaperConfig {
                tick: Duration::from_millis(100),
                safety_margin: Duration::from_millis(0),
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(400)).await;

        // The winner's reaper would have reaped it (no reaper here,
        // so the row stays) — this proves the loser is inert, not
        // that the reaper doesn't work.
        assert_eq!(
            reaper_on_loser.reassignments_total(),
            0,
            "follower reaper must not count reassignments"
        );
    }

    #[tokio::test]
    async fn reaper_leaves_live_leases_alone_even_as_leader() {
        let (_file, meta) = file_backed_meta().await;
        // Live lease: expires far in the future.
        meta_tablets::get_or_create(meta.pool(), 99, 0)
            .await
            .unwrap();
        let future = now_ms() + 10 * 60 * 1000; // 10 minutes
        meta_tablets::assume_ownership(meta.pool(), 99, 0, "healthy-owner", future)
            .await
            .unwrap();

        let election = LeaderElection::start(
            meta.clone(),
            "reaper-node".into(),
            LeaderElectionConfig {
                lease_duration: Duration::from_secs(10),
                renew_interval: Duration::from_millis(100),
                attempt_interval: Duration::from_millis(50),
            },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;

        let reaper = StaleTabletReaper::start(
            meta.clone(),
            election.clone(),
            ReaperConfig {
                tick: Duration::from_millis(100),
                safety_margin: Duration::from_millis(0),
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(400)).await;

        let row = meta_tablets::get(meta.pool(), 99).await.unwrap().unwrap();
        assert_eq!(
            row.owner_node_id.as_deref(),
            Some("healthy-owner"),
            "live lease must not be reaped"
        );
        assert_eq!(reaper.reassignments_total(), 0);
    }

    /// Helper: build a meta-store-backed scenario with N tablets,
    /// seed the shards table, and return the meta store.
    async fn meta_with_tablets_and_shards(
        num_tablets: i64,
    ) -> (tempfile::NamedTempFile, Arc<MetaStore>) {
        let (file, meta) = file_backed_meta().await;
        agentdb_meta::migrations::apply(meta.pool(), agentdb_meta::MetaDialect::Sqlite).await.unwrap();
        agentdb_meta::shards::seed_shards(meta.pool(), 64, 0)
            .await
            .unwrap();
        for t in 0..num_tablets {
            meta_tablets::get_or_create(meta.pool(), t, 0)
                .await
                .unwrap();
        }
        (file, meta)
    }

    #[tokio::test]
    async fn reaper_reassigns_orphaned_shards_after_clearing_tablet() {
        let (_file, meta) = meta_with_tablets_and_shards(2).await;
        // Tablet 0 has an expired lease; tablet 1 is healthy.
        meta_tablets::assume_ownership(meta.pool(), 0, 0, "dead-node", 1)
            .await
            .unwrap();
        meta_tablets::assume_ownership(
            meta.pool(),
            1,
            0,
            "live-node",
            now_ms() + 10 * 60 * 1000,
        )
        .await
        .unwrap();
        // Shard 5 is overridden to tablet 0.
        agentdb_meta::shards::set_assignment(meta.pool(), 5, Some(0), 100)
            .await
            .unwrap();

        let election = LeaderElection::start(
            meta.clone(),
            "reaper-node".into(),
            LeaderElectionConfig {
                lease_duration: Duration::from_secs(10),
                renew_interval: Duration::from_millis(100),
                attempt_interval: Duration::from_millis(50),
            },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(election.is_leader());

        let reaper = StaleTabletReaper::start(
            meta.clone(),
            election.clone(),
            ReaperConfig {
                tick: Duration::from_millis(100),
                safety_margin: Duration::from_millis(0),
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Tablet 0 cleared, shard 5 moved to tablet 1.
        let t0 = meta_tablets::get(meta.pool(), 0).await.unwrap().unwrap();
        assert_eq!(t0.owner_node_id, None, "dead tablet owner cleared");
        let shard5 = agentdb_meta::shards::get_assignment(meta.pool(), 5)
            .await
            .unwrap();
        assert_eq!(
            shard5,
            Some(1),
            "orphaned shard 5 reassigned to live tablet 1"
        );
        assert!(reaper.shard_reassignments_total() >= 1);
    }

    #[tokio::test]
    async fn reaper_round_robins_across_live_tablets() {
        let (_file, meta) = meta_with_tablets_and_shards(3).await;
        // Tablet 0 is expired; 1 and 2 are healthy.
        meta_tablets::assume_ownership(meta.pool(), 0, 0, "dead", 1)
            .await
            .unwrap();
        meta_tablets::assume_ownership(meta.pool(), 1, 0, "live-1", now_ms() + 600_000)
            .await
            .unwrap();
        meta_tablets::assume_ownership(meta.pool(), 2, 0, "live-2", now_ms() + 600_000)
            .await
            .unwrap();
        // Four shards on tablet 0.
        for s in [3, 7, 11, 15] {
            agentdb_meta::shards::set_assignment(meta.pool(), s, Some(0), 0)
                .await
                .unwrap();
        }

        let election = LeaderElection::start(
            meta.clone(),
            "reaper".into(),
            LeaderElectionConfig {
                lease_duration: Duration::from_secs(10),
                renew_interval: Duration::from_millis(100),
                attempt_interval: Duration::from_millis(50),
            },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;

        let _reaper = StaleTabletReaper::start(
            meta.clone(),
            election.clone(),
            ReaperConfig {
                tick: Duration::from_millis(100),
                safety_margin: Duration::from_millis(0),
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(400)).await;

        // All four shards reassigned, split roughly evenly across
        // the two live tablets (round-robin: 1, 2, 1, 2).
        let mut by_tablet: std::collections::HashMap<i64, usize> = Default::default();
        for s in [3, 7, 11, 15] {
            let t = agentdb_meta::shards::get_assignment(meta.pool(), s)
                .await
                .unwrap()
                .expect("shard reassigned");
            *by_tablet.entry(t).or_default() += 1;
        }
        // Both live tablets must have received at least one shard.
        assert_eq!(by_tablet.len(), 2, "round-robin must hit both live tablets");
        for count in by_tablet.values() {
            assert_eq!(*count, 2, "even split across live tablets");
        }
    }

    #[tokio::test]
    async fn reaper_leaves_shards_alone_when_no_live_tablet() {
        let (_file, meta) = meta_with_tablets_and_shards(1).await;
        meta_tablets::assume_ownership(meta.pool(), 0, 0, "dead", 1)
            .await
            .unwrap();
        agentdb_meta::shards::set_assignment(meta.pool(), 9, Some(0), 0)
            .await
            .unwrap();

        let election = LeaderElection::start(
            meta.clone(),
            "reaper".into(),
            LeaderElectionConfig {
                lease_duration: Duration::from_secs(10),
                renew_interval: Duration::from_millis(100),
                attempt_interval: Duration::from_millis(50),
            },
        );
        tokio::time::sleep(Duration::from_millis(200)).await;

        let reaper = StaleTabletReaper::start(
            meta.clone(),
            election.clone(),
            ReaperConfig {
                tick: Duration::from_millis(100),
                safety_margin: Duration::from_millis(0),
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Tablet 0 cleared, but no live tablet to reassign to —
        // shard 9's override stays in place (pointing at the now-
        // orphaned tablet). Write path will fall back to tenant
        // hash via route_session's safety fallback.
        let shard9 = agentdb_meta::shards::get_assignment(meta.pool(), 9)
            .await
            .unwrap();
        assert_eq!(shard9, Some(0), "no live tablet → shard override left alone");
        assert_eq!(reaper.shard_reassignments_total(), 0);
        // Tablet clear still counted.
        assert!(reaper.reassignments_total() >= 1);
    }
}
