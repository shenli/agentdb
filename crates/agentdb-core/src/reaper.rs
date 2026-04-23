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

use agentdb_meta::{tablets, MetaStore};
use tokio::task::JoinHandle;

use crate::scheduler::LeaderElection;

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
    config: ReaperConfig,
    _handle: JoinHandle<()>,
}

impl StaleTabletReaper {
    /// Spawn the reap loop. Cheap; the loop is idle on followers
    /// except for a tick-rate atomic load. Returns `Arc<Self>` for
    /// admin handlers to read the counter.
    pub fn start(
        meta: Arc<MetaStore>,
        election: Arc<LeaderElection>,
        config: ReaperConfig,
    ) -> Arc<Self> {
        let reassignments_total = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            meta,
            election,
            config,
            reassignments_total.clone(),
        ));
        Arc::new(Self {
            reassignments_total,
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
}
