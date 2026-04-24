//! Leader-owned node-lifecycle reaper.
//!
//! Periodically sweeps the `nodes` meta table and handles two
//! transitions that no single process can own on its own:
//!
//! 1. `alive -> dead`. When a node's last heartbeat is older than
//!    [`NodeReaperConfig::dead_after`], flip its `state` column
//!    to `dead`. The owning node either crashed or lost its meta
//!    connection; either way, peers should route around it.
//!
//! 2. `dead -> removed`. After a node has been `dead` for
//!    [`NodeReaperConfig::gc_after`], drop the row entirely. Keeps
//!    the control-plane table bounded across deployments and
//!    reduces clutter in `GET /api/admin/nodes`.
//!
//! Like the tablet-lease reaper, only the scheduler-leader runs
//! the sweep — followers call `is_leader()` every tick and no-op.
//! Transient meta errors are logged and retried on the next tick;
//! they do not advance the observability counters.
//!
//! Design invariants:
//!
//! 1. **No tablet-ownership churn here.** Reaping a node does NOT
//!    touch its tablet leases. The existing [`StaleTabletReaper`]
//!    handles stale leases via `expires_at + safety_margin < now`,
//!    which fires whether the node is `alive`, `dead`, or missing.
//!    Keeping the two loops decoupled means a flaky heartbeat
//!    doesn't cascade into tablet reassignments.
//!
//! 2. **Timestamp preserved through state change.** `alive -> dead`
//!    leaves `last_heartbeat_ms` untouched so operators and the GC
//!    pass can see *how long* the node has been gone.
//!
//! 3. **Graceful-shutdown rows are respected.** A node that
//!    announced `state = "leaving"` during shutdown is never
//!    flipped to `dead` here — the predicate only fires on
//!    `state = 'alive'`. The dead-GC predicate only fires on
//!    `state = 'dead'`. `leaving`/`draining` rows persist until an
//!    operator removes them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentdb_meta::{nodes as meta_nodes, MetaStore};
use tokio::task::JoinHandle;

use crate::scheduler::LeaderElection;

/// Tunable timings for the node-lifecycle reaper.
#[derive(Debug, Clone, Copy)]
pub struct NodeReaperConfig {
    /// How often to sweep. Defaults to 10s.
    pub tick: Duration,
    /// A node whose last heartbeat is older than this is flipped
    /// from `alive` to `dead`. Defaults to 30s — six times the 5s
    /// default heartbeat period. Set this substantially larger
    /// than the heartbeat interval so a single slow renewal
    /// doesn't prematurely mark a node dead.
    pub dead_after: Duration,
    /// A `dead` row whose last heartbeat is older than this gets
    /// removed from the table. Defaults to 1h so operators have
    /// time to observe/debug before the row disappears. Must be
    /// larger than `dead_after`.
    pub gc_after: Duration,
}

impl Default for NodeReaperConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(10),
            dead_after: Duration::from_secs(30),
            gc_after: Duration::from_secs(3_600),
        }
    }
}

impl NodeReaperConfig {
    /// Env overrides:
    /// - `AGENTDB_NODE_REAPER_TICK_SECS`      (default 10)
    /// - `AGENTDB_NODE_DEAD_AFTER_SECS`       (default 30)
    /// - `AGENTDB_NODE_GC_AFTER_SECS`         (default 3600)
    pub fn from_env() -> Self {
        let d = Self::default();
        let parse = |k: &str, fallback: Duration| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(fallback)
        };
        Self {
            tick: parse("AGENTDB_NODE_REAPER_TICK_SECS", d.tick),
            dead_after: parse("AGENTDB_NODE_DEAD_AFTER_SECS", d.dead_after),
            gc_after: parse("AGENTDB_NODE_GC_AFTER_SECS", d.gc_after),
        }
    }
}

/// Background task owner. Hold it alive; drop cancels the loop.
pub struct NodeReaper {
    marked_dead_total: Arc<AtomicU64>,
    gc_removed_total: Arc<AtomicU64>,
    config: NodeReaperConfig,
    _handle: JoinHandle<()>,
}

impl NodeReaper {
    pub fn start(
        meta: Arc<MetaStore>,
        election: Arc<LeaderElection>,
        config: NodeReaperConfig,
    ) -> Arc<Self> {
        let marked_dead_total = Arc::new(AtomicU64::new(0));
        let gc_removed_total = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            meta,
            election,
            config,
            marked_dead_total.clone(),
            gc_removed_total.clone(),
        ));
        Arc::new(Self {
            marked_dead_total,
            gc_removed_total,
            config,
            _handle: handle,
        })
    }

    /// Total `alive -> dead` transitions this process has made as
    /// leader. Followers always report 0.
    pub fn marked_dead_total(&self) -> u64 {
        self.marked_dead_total.load(Ordering::Acquire)
    }

    /// Total rows removed by the dead-GC pass on this process.
    pub fn gc_removed_total(&self) -> u64 {
        self.gc_removed_total.load(Ordering::Acquire)
    }

    pub fn config(&self) -> NodeReaperConfig {
        self.config
    }
}

async fn run_loop(
    meta: Arc<MetaStore>,
    election: Arc<LeaderElection>,
    config: NodeReaperConfig,
    marked_dead_total: Arc<AtomicU64>,
    gc_removed_total: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // First tick fires immediately; skip to align with "sweep
    // every N seconds after startup" rather than "sweep on boot".
    interval.tick().await;
    loop {
        interval.tick().await;
        if !election.is_leader() {
            continue;
        }
        let now = now_ms();
        sweep_once(
            &meta,
            now,
            config,
            &marked_dead_total,
            &gc_removed_total,
        )
        .await;
    }
}

async fn sweep_once(
    meta: &MetaStore,
    now_ms: i64,
    config: NodeReaperConfig,
    marked_dead_total: &AtomicU64,
    gc_removed_total: &AtomicU64,
) {
    let dead_deadline = now_ms.saturating_sub(config.dead_after.as_millis() as i64);
    match meta_nodes::mark_dead_if_stale(meta.pool(), dead_deadline).await {
        Ok(ids) if ids.is_empty() => {}
        Ok(ids) => {
            marked_dead_total.fetch_add(ids.len() as u64, Ordering::Release);
            tracing::info!(
                count = ids.len(),
                nodes = ?ids,
                dead_after_ms = config.dead_after.as_millis() as i64,
                "node reaper marked stale nodes as dead"
            );
        }
        Err(err) => {
            tracing::warn!(
                %err,
                "node reaper: mark_dead_if_stale failed; will retry on next tick"
            );
        }
    }

    let gc_deadline = now_ms.saturating_sub(config.gc_after.as_millis() as i64);
    match meta_nodes::gc_dead(meta.pool(), gc_deadline).await {
        Ok(ids) if ids.is_empty() => {}
        Ok(ids) => {
            gc_removed_total.fetch_add(ids.len() as u64, Ordering::Release);
            tracing::info!(
                count = ids.len(),
                nodes = ?ids,
                gc_after_ms = config.gc_after.as_millis() as i64,
                "node reaper removed long-dead node rows"
            );
        }
        Err(err) => {
            tracing::warn!(
                %err,
                "node reaper: gc_dead failed; will retry on next tick"
            );
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
    use agentdb_meta::{migrations, MetaDialect};

    async fn file_backed_meta() -> (tempfile::NamedTempFile, Arc<MetaStore>) {
        // File-backed SQLite so the election loop's writes are
        // observable to this test. `:memory:` with max_connections=1
        // would serialize every await and stall the loop.
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        let meta = MetaStore::open_sqlite(&path).await.unwrap();
        (file, Arc::new(meta))
    }

    #[tokio::test]
    async fn sweep_once_marks_and_gcs_in_a_single_pass() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        migrations::apply(meta.pool(), MetaDialect::Sqlite)
            .await
            .unwrap();
        // Three nodes with different heartbeat ages at now=2000:
        //   fresh   hb=1800 (recent — stays alive)
        //   stale   hb=500  (old enough to die, but recent enough to
        //                    survive the GC pass; see cfg below)
        //   ancient hb=0    (pre-flipped to dead below; GC'd)
        meta_nodes::upsert(meta.pool(), "fresh", "a", None, "alive", 1_800)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "stale", "b", None, "alive", 500)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "ancient", "c", None, "alive", 0)
            .await
            .unwrap();
        // Flip "ancient" to dead directly so the GC pass has a
        // candidate without waiting. Deadline=50 only catches
        // ancient (hb=0); the other rows have hb >= 500.
        meta_nodes::mark_dead_if_stale(meta.pool(), 50).await.unwrap();

        let marked = Arc::new(AtomicU64::new(0));
        let gced = Arc::new(AtomicU64::new(0));
        // now = 2000.
        //   dead_after = 500ms → rows with hb < 1500 flip to dead.
        //   gc_after = 1900ms → rows dead-and-hb < 100 are removed.
        // "stale" (hb=100) transitions but its hb is NOT < 100 so
        // it survives the GC pass. "ancient" (hb=0) was already
        // dead and is below the GC deadline, so it drops.
        let cfg = NodeReaperConfig {
            tick: Duration::from_secs(1),
            dead_after: Duration::from_millis(1_000),
            gc_after: Duration::from_millis(1_900),
        };
        sweep_once(&meta, 2_000, cfg, &marked, &gced).await;

        // "stale" (hb=500) < 2000-1000=1000 → marked dead, not GC'd.
        assert_eq!(
            meta_nodes::get(meta.pool(), "stale")
                .await
                .unwrap()
                .unwrap()
                .state,
            "dead"
        );
        // "fresh" (hb=1800) is newer than 1000 → still alive.
        assert_eq!(
            meta_nodes::get(meta.pool(), "fresh")
                .await
                .unwrap()
                .unwrap()
                .state,
            "alive"
        );
        // "ancient" was already dead with hb=0 < 2000-1900=100 → GC'd.
        assert!(meta_nodes::get(meta.pool(), "ancient")
            .await
            .unwrap()
            .is_none());

        // Counters include stale (marked) and ancient (gc'd).
        assert_eq!(marked.load(Ordering::Acquire), 1);
        assert_eq!(gced.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn sweep_once_skips_leaving_and_draining() {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        migrations::apply(meta.pool(), MetaDialect::Sqlite)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "bye", "a", None, "leaving", 0)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "drain", "b", None, "draining", 0)
            .await
            .unwrap();

        let marked = Arc::new(AtomicU64::new(0));
        let gced = Arc::new(AtomicU64::new(0));
        let cfg = NodeReaperConfig {
            tick: Duration::from_secs(1),
            dead_after: Duration::from_millis(1),
            gc_after: Duration::from_millis(1),
        };
        sweep_once(&meta, 10_000, cfg, &marked, &gced).await;

        // Neither row is `alive` or `dead`, so both pass through
        // untouched regardless of how aggressive the thresholds are.
        assert_eq!(
            meta_nodes::get(meta.pool(), "bye")
                .await
                .unwrap()
                .unwrap()
                .state,
            "leaving"
        );
        assert_eq!(
            meta_nodes::get(meta.pool(), "drain")
                .await
                .unwrap()
                .unwrap()
                .state,
            "draining"
        );
        assert_eq!(marked.load(Ordering::Acquire), 0);
        assert_eq!(gced.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn reaper_no_ops_on_follower() {
        // Two election loops on the same meta. The second never
        // wins leadership (first's lease is long, renewal tight),
        // so its reaper must never mark anything dead — even
        // though the stale node would qualify.
        let (_guard, meta) = file_backed_meta().await;
        meta_nodes::upsert(meta.pool(), "stale", "a", None, "alive", 0)
            .await
            .unwrap();
        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(30),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let leader = LeaderElection::start(meta.clone(), "leader".into(), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(leader.is_leader(), "first elector should have won");

        let follower = LeaderElection::start(meta.clone(), "follower".into(), cfg);
        // Aggressive reaper cfg so, if it did run, it would
        // definitely touch the stale row.
        let reaper = NodeReaper::start(
            meta.clone(),
            follower.clone(),
            NodeReaperConfig {
                tick: Duration::from_millis(100),
                dead_after: Duration::from_millis(1),
                gc_after: Duration::from_millis(1),
            },
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!follower.is_leader(), "follower must not have won");
        assert_eq!(
            reaper.marked_dead_total(),
            0,
            "follower-side reaper must not advance counters"
        );
        assert_eq!(
            meta_nodes::get(meta.pool(), "stale")
                .await
                .unwrap()
                .unwrap()
                .state,
            "alive",
            "follower must not have touched the row"
        );
    }

    #[tokio::test]
    async fn reaper_on_leader_eventually_marks_stale_rows_dead() {
        let (_guard, meta) = file_backed_meta().await;
        // Heartbeat is "60s ago" on the wall clock — stale enough
        // to die under dead_after=1ms, but fresh enough that
        // gc_after=3600s doesn't also drop the row on the very
        // next tick. This mirrors the intended production shape
        // where dead nodes persist long enough for operators to
        // notice them before GC.
        let hb = now_ms() - 60_000;
        meta_nodes::upsert(meta.pool(), "stale", "a", None, "alive", hb)
            .await
            .unwrap();
        let cfg = LeaderElectionConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_millis(100),
            attempt_interval: Duration::from_millis(50),
        };
        let election = LeaderElection::start(meta.clone(), "solo".into(), cfg);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(election.is_leader());
        let reaper = NodeReaper::start(
            meta.clone(),
            election,
            NodeReaperConfig {
                tick: Duration::from_millis(100),
                dead_after: Duration::from_millis(1),
                gc_after: Duration::from_secs(3_600),
            },
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            meta_nodes::get(meta.pool(), "stale")
                .await
                .unwrap()
                .unwrap()
                .state,
            "dead"
        );
        assert!(reaper.marked_dead_total() >= 1);
    }
}
