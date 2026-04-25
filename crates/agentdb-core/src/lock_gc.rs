//! Periodic cleanup of idle entries from `Box_::session_locks`
//! and `tenant_locks`.
//!
//! Both maps grow monotonically as new sessions and tenants
//! get touched: every distinct external id ever seen leaves
//! an entry behind, which is fine in tests but a slow memory
//! leak in production where the working set churns. This
//! background loop walks every box's lock maps every
//! `tick_secs` and drops entries whose `Arc<Mutex>` has
//! `strong_count == 1` — i.e., the only holder is the map
//! itself, no in-flight writer is waiting on the lock.
//!
//! Re-acquisition after a prune is identical to first-touch
//! acquisition: the next caller of `session_write_lock`
//! reinserts a fresh entry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::box_runtime::BoxRegistry;

/// Tunable timings.
#[derive(Debug, Clone, Copy)]
pub struct LockGcConfig {
    /// How often to sweep. Defaults to 60s — slow enough that
    /// the prune cost is negligible, fast enough that a
    /// machine under steady-state churn doesn't grow unbounded
    /// memory between sweeps.
    pub tick: Duration,
}

impl Default for LockGcConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(60),
        }
    }
}

impl LockGcConfig {
    /// Env: `AGENTDB_LOCK_GC_TICK_SECS` (default 60).
    pub fn from_env() -> Self {
        let d = Self::default();
        let tick = std::env::var("AGENTDB_LOCK_GC_TICK_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.tick);
        Self { tick }
    }
}

/// RAII handle. Drop cancels the loop.
pub struct LockGc {
    config: LockGcConfig,
    session_locks_pruned_total: Arc<AtomicU64>,
    tenant_locks_pruned_total: Arc<AtomicU64>,
    _handle: JoinHandle<()>,
}

impl LockGc {
    pub fn start(registry: Arc<BoxRegistry>, config: LockGcConfig) -> Arc<Self> {
        let session_locks_pruned_total = Arc::new(AtomicU64::new(0));
        let tenant_locks_pruned_total = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            registry,
            config,
            session_locks_pruned_total.clone(),
            tenant_locks_pruned_total.clone(),
        ));
        Arc::new(Self {
            config,
            session_locks_pruned_total,
            tenant_locks_pruned_total,
            _handle: handle,
        })
    }

    /// Total session-lock entries this process has pruned
    /// across every box, across the lifetime of the loop.
    /// Operators can graph this against per-box
    /// `session_lock_count` to detect a leak (steady growth
    /// in count + zero pruning means lock holders aren't
    /// dropping their guards).
    pub fn session_locks_pruned_total(&self) -> u64 {
        self.session_locks_pruned_total.load(Ordering::Acquire)
    }

    /// Total tenant-lock entries pruned across all boxes.
    pub fn tenant_locks_pruned_total(&self) -> u64 {
        self.tenant_locks_pruned_total.load(Ordering::Acquire)
    }

    pub fn config(&self) -> LockGcConfig {
        self.config
    }
}

async fn run_loop(
    registry: Arc<BoxRegistry>,
    config: LockGcConfig,
    session_locks_pruned_total: Arc<AtomicU64>,
    tenant_locks_pruned_total: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        interval.tick().await;
        let (sp, tp) = registry.prune_idle_locks();
        if sp > 0 {
            session_locks_pruned_total.fetch_add(sp as u64, Ordering::Release);
        }
        if tp > 0 {
            tenant_locks_pruned_total.fetch_add(tp as u64, Ordering::Release);
        }
        if sp > 0 || tp > 0 {
            tracing::debug!(
                session_locks_pruned = sp,
                tenant_locks_pruned = tp,
                "lock_gc: idle entries pruned"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HlcClock;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use tempfile::TempDir;

    fn fresh_registry(num_boxes: u16) -> (TempDir, TempDir, Arc<BoxRegistry>) {
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(
            BoxRegistry::new_with_hlc(kv, store, num_boxes, Arc::new(HlcClock::default()))
                .expect("registry"),
        );
        (dir, obj_dir, reg)
    }

    #[tokio::test]
    async fn prune_drops_entries_whose_only_holder_is_the_map() {
        let (_d, _o, reg) = fresh_registry(1);
        let box_ = reg.box_by_id(0).unwrap();
        // Touch 3 distinct sessions to grow the map.
        let _ = box_.session_write_lock("s-a");
        let _ = box_.session_write_lock("s-b");
        let _ = box_.session_write_lock("s-c");
        assert_eq!(box_.session_lock_count(), 3);
        // Nothing held — every Arc has strong_count==1
        // (the map's). A prune must clear all three.
        let (sp, tp) = reg.prune_idle_locks();
        assert_eq!(sp, 3);
        assert_eq!(tp, 0);
        assert_eq!(box_.session_lock_count(), 0);
    }

    #[tokio::test]
    async fn prune_keeps_entries_with_an_active_guard() {
        let (_d, _o, reg) = fresh_registry(1);
        let box_ = reg.box_by_id(0).unwrap();
        let lock = box_.session_write_lock("s-held");
        // The caller holds a clone of the Arc → strong_count
        // is 2 (caller + map). Prune must NOT remove it.
        let _guard = lock.clone().lock_owned().await;
        let (sp, _) = reg.prune_idle_locks();
        assert_eq!(sp, 0);
        assert_eq!(box_.session_lock_count(), 1);
        // Drop the guard + the local Arc → strong_count==1.
        drop(_guard);
        drop(lock);
        let (sp, _) = reg.prune_idle_locks();
        assert_eq!(sp, 1);
        assert_eq!(box_.session_lock_count(), 0);
    }

    #[tokio::test]
    async fn prune_handles_tenant_locks_independently() {
        let (_d, _o, reg) = fresh_registry(1);
        let box_ = reg.box_by_id(0).unwrap();
        // Grow tenant_locks, leave session_locks empty.
        let _ = box_.tenant_write_lock("t-a");
        let _ = box_.tenant_write_lock("t-b");
        let (sp, tp) = reg.prune_idle_locks();
        assert_eq!(sp, 0);
        assert_eq!(tp, 2);
    }

    #[tokio::test]
    async fn re_acquisition_after_prune_works() {
        // After pruning, the next caller for the same key
        // gets a fresh Arc — equivalent to first-touch
        // semantics. No correctness regression.
        let (_d, _o, reg) = fresh_registry(1);
        let box_ = reg.box_by_id(0).unwrap();
        let _ = box_.session_write_lock("s-1");
        reg.prune_idle_locks();
        assert_eq!(box_.session_lock_count(), 0);
        // Re-acquire: serializes against any other writer
        // for "s-1" exactly as before.
        let l1 = box_.session_write_lock("s-1");
        let l2 = box_.session_write_lock("s-1");
        assert!(Arc::ptr_eq(&l1, &l2));
        assert_eq!(box_.session_lock_count(), 1);
    }

    #[tokio::test]
    async fn loop_advances_pruned_counter() {
        let (_d, _o, reg) = fresh_registry(1);
        let box_ = reg.box_by_id(0).unwrap();
        let _ = box_.session_write_lock("s-x");
        let _ = box_.tenant_write_lock("t-y");
        let gc = LockGc::start(
            reg.clone(),
            LockGcConfig {
                tick: Duration::from_millis(50),
            },
        );
        // Wait for at least one tick.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(gc.session_locks_pruned_total() >= 1);
        assert!(gc.tenant_locks_pruned_total() >= 1);
    }
}
