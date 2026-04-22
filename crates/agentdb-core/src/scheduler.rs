//! Scheduler leader election — the runtime loop that keeps the
//! singleton `scheduler_leader` row in the control plane fresh.
//!
//! The meta layer (`agentdb_meta::scheduler`) owns the actual lease
//! semantics — CAS on epoch, takeover of expired leases, etc. This
//! module hosts the tokio task that alternates between
//! "try to acquire / try to renew" and exposes the result via two
//! cheap accessors (`is_leader`, `current_epoch`) that any admin
//! endpoint or future scheduler-guarded work can read without
//! round-tripping to meta.
//!
//! Design invariants (M22, foundation-only scope):
//!
//! 1. **Election is side-effect-free.** Winning a term doesn't do
//!    anything yet — no tablet reassignment, no drain orchestration.
//!    That's the next PR. This loop just ensures exactly one process
//!    in the cluster will be ready to do it.
//!
//! 2. **Loss of leadership is detected, not hidden.** When
//!    `renew` comes back with `Conflict`, the loop flips
//!    `is_leader` to false and drops back into the acquire phase
//!    with the shared `attempt_interval` cadence. Callers reading
//!    `is_leader()` after a renewal failure see `false` before
//!    they could have issued a stale write.
//!
//! 3. **Transient meta outages do not immediately release
//!    leadership.** A renewal that errors for a reason *other* than
//!    Conflict (meta unreachable, timeout) is logged and retried on
//!    the next tick — identical to the lease-renewal philosophy in
//!    `agentdb_core::lease`. The lease expires naturally if the
//!    outage persists past `lease_duration`.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentdb_meta::scheduler as meta_scheduler;
use agentdb_meta::MetaStore;
use tokio::task::JoinHandle;

/// Tunable timings for the election loop.
#[derive(Debug, Clone, Copy)]
pub struct LeaderElectionConfig {
    /// Lifetime of each successful acquire/renew. The DB row's
    /// `expires_at_ms` is set to `now + lease_duration`. Other nodes
    /// must wait for this to pass before they can take over.
    pub lease_duration: Duration,
    /// How often the *leader* renews. Must be materially less than
    /// `lease_duration` to tolerate transient meta slowness without
    /// losing leadership.
    pub renew_interval: Duration,
    /// How often a *non-leader* attempts to acquire. Shorter →
    /// faster takeover after a failed leader; longer → less meta
    /// load. Independent of `renew_interval`.
    pub attempt_interval: Duration,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(30),
            renew_interval: Duration::from_secs(5),
            attempt_interval: Duration::from_secs(2),
        }
    }
}

impl LeaderElectionConfig {
    /// Read from env:
    /// - `AGENTDB_LEADER_LEASE_SECS`    (default 30)
    /// - `AGENTDB_LEADER_RENEW_SECS`    (default 5)
    /// - `AGENTDB_LEADER_ATTEMPT_SECS`  (default 2)
    pub fn from_env() -> Self {
        let d = Self::default();
        let lease_duration = std::env::var("AGENTDB_LEADER_LEASE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.lease_duration);
        let renew_interval = std::env::var("AGENTDB_LEADER_RENEW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.renew_interval);
        let attempt_interval = std::env::var("AGENTDB_LEADER_ATTEMPT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.attempt_interval);
        Self {
            lease_duration,
            renew_interval,
            attempt_interval,
        }
    }
}

/// Live leader-election state for this process.
pub struct LeaderElection {
    node_id: String,
    /// Shared with the background loop — reads from admin endpoints
    /// see the loop's latest store. Arc-wrapped so the loop and the
    /// accessor can point at the same atomic.
    is_leader: Arc<AtomicBool>,
    current_epoch: Arc<AtomicI64>,
    _handle: JoinHandle<()>,
}

impl LeaderElection {
    /// Spawn the election loop in the background. The returned
    /// `Arc<LeaderElection>` is clonable into admin handlers; the
    /// JoinHandle lives for the process (drop-to-cancel semantics
    /// match the existing `LeaseRenewalJob` pattern in M21).
    pub fn start(meta: Arc<MetaStore>, node_id: String, config: LeaderElectionConfig) -> Arc<Self> {
        let is_leader = Arc::new(AtomicBool::new(false));
        let current_epoch = Arc::new(AtomicI64::new(0));
        let handle = tokio::spawn(run_loop(
            meta,
            node_id.clone(),
            config,
            is_leader.clone(),
            current_epoch.clone(),
        ));
        Arc::new(Self {
            node_id,
            is_leader,
            current_epoch,
            _handle: handle,
        })
    }

    /// Is this process the leader right now? Hot path, atomic load.
    /// Safe to call from any thread / any async context.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }

    /// Current epoch observed by this process. Zero before the
    /// first acquire attempt completes.
    pub fn current_epoch(&self) -> i64 {
        self.current_epoch.load(Ordering::Acquire)
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

async fn run_loop(
    meta: Arc<MetaStore>,
    node_id: String,
    config: LeaderElectionConfig,
    is_leader: Arc<AtomicBool>,
    current_epoch: Arc<AtomicI64>,
) {
    let lease_ms = config.lease_duration.as_millis() as i64;
    loop {
        if is_leader.load(Ordering::Acquire) {
            // Leader path: renew at `renew_interval`.
            tokio::time::sleep(config.renew_interval).await;
            let epoch = current_epoch.load(Ordering::Acquire);
            let new_expiry = now_ms() + lease_ms;
            match meta_scheduler::renew(meta.pool(), &node_id, epoch, new_expiry).await {
                Ok(row) => {
                    current_epoch.store(row.epoch, Ordering::Release);
                }
                Err(agentdb_meta::MetaError::Conflict(_)) => {
                    tracing::warn!(
                        %node_id,
                        old_epoch = epoch,
                        "leader lease stolen — dropping leadership"
                    );
                    is_leader.store(false, Ordering::Release);
                }
                Err(err) => {
                    tracing::warn!(
                        %node_id,
                        %err,
                        "leader-lease renew failed (transient); will retry on next tick"
                    );
                }
            }
        } else {
            // Non-leader path: try to acquire every `attempt_interval`.
            tokio::time::sleep(config.attempt_interval).await;
            match meta_scheduler::try_acquire(meta.pool(), &node_id, lease_ms, now_ms()).await {
                Ok(Some(row)) => {
                    tracing::info!(%node_id, epoch = row.epoch, "acquired scheduler leadership");
                    current_epoch.store(row.epoch, Ordering::Release);
                    is_leader.store(true, Ordering::Release);
                }
                Ok(None) => {
                    // Live leader exists. Nothing to do.
                }
                Err(err) => {
                    tracing::warn!(
                        %node_id,
                        %err,
                        "scheduler leader acquire attempt errored; will retry"
                    );
                }
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

    async fn file_backed_meta() -> (tempfile::NamedTempFile, Arc<MetaStore>) {
        // File-backed SQLite so multiple MetaStore handles opened
        // against the same path see each other's writes (in-memory
        // stores are per-connection and can't simulate two processes).
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let meta = Arc::new(MetaStore::open_sqlite(&path).await.unwrap());
        (file, meta)
    }

    #[tokio::test]
    async fn env_config_parses_with_defaults_when_unset() {
        std::env::remove_var("AGENTDB_LEADER_LEASE_SECS");
        std::env::remove_var("AGENTDB_LEADER_RENEW_SECS");
        std::env::remove_var("AGENTDB_LEADER_ATTEMPT_SECS");
        let c = LeaderElectionConfig::from_env();
        assert_eq!(c.lease_duration, Duration::from_secs(30));
        assert_eq!(c.renew_interval, Duration::from_secs(5));
        assert_eq!(c.attempt_interval, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn single_node_acquires_leadership_within_attempt_interval() {
        let (_file, meta) = file_backed_meta().await;
        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let node = LeaderElection::start(meta.clone(), "node-a".into(), cfg);
        // Wait past the first attempt_interval tick.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(node.is_leader(), "single node must become leader");
        assert!(node.current_epoch() >= 1);
    }

    #[tokio::test]
    async fn second_process_sees_the_first_as_leader_and_stays_follower() {
        let (_file, meta) = file_backed_meta().await;
        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let a = LeaderElection::start(meta.clone(), "node-a".into(), cfg);
        // Let a acquire first.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(a.is_leader());

        let b = LeaderElection::start(meta.clone(), "node-b".into(), cfg);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!b.is_leader(), "second node must not steal live lease");
        assert!(a.is_leader(), "first node must still be leader");

        // The row in meta shows node-a.
        let row = meta_scheduler::get(meta.pool())
            .await
            .unwrap()
            .expect("row");
        assert_eq!(row.node_id, "node-a");
    }
}
