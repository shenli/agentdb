//! Per-node tablet reconciler.
//!
//! Sits in the gap between `TabletPlacement` (the leader writes
//! "node X owns tablet 7" into the meta) and `BoxRegistry`
//! (this process needs a live `Box_` + `LeaseManager` for
//! tablet 7 before it can serve writes for it).
//!
//! Periodically:
//!
//! 1. Query `tablets WHERE owner_node_id = self_node_id`.
//! 2. For each tablet I'm not already running (`registry.has_box`
//!    is `false`), call `BoxRegistry::add_box`. That creates the
//!    `Box_`, acquires the lease (epoch-bumping
//!    `assume_ownership`), and exposes the box on the hot path.
//!
//! Single-direction for now: this PR does **not** drop boxes
//! when ownership shifts away. The dominant failure mode in
//! a multi-node cluster is "node A goes down, B picks up A's
//! tablets" — never "B was running tablet 7 and the leader
//! reassigned it to C while B is still alive". Voluntary
//! handoff is explicit follow-up; until then a reassignment
//! away from a running node is resolved by a process restart.
//!
//! Design invariants:
//!
//! 1. **Idempotent.** A tick that finds nothing new to add is
//!    a one-row meta query and a Vec scan; cheap to run every
//!    few seconds.
//!
//! 2. **Errors don't stop the loop.** Add-box failures are
//!    logged + counted in `errors_total` and the next tick
//!    will retry. A tablet whose ownership is "promised" but
//!    can't be acquired (e.g., the previous owner is still
//!    holding the lease past the safety margin) just gets
//!    retried on the next tick.
//!
//! 3. **Race-tolerant against the placement loop.** Placement
//!    sets `owner_node_id` without bumping epoch. The
//!    reconciler's `add_box` call goes through
//!    `LeaseManager::acquire`, which reads the current epoch
//!    and CAS's it. If a concurrent renewal from a still-alive
//!    previous owner moves the epoch, our acquire returns
//!    `Conflict` and the next tick retries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agentdb_meta::tablets as meta_tablets;
use tokio::task::JoinHandle;

use crate::box_runtime::BoxRegistry;

#[derive(Debug, Clone, Copy)]
pub struct ReconcilerConfig {
    /// How often to sweep. Defaults to 3s — fast enough that a
    /// new placement assignment becomes a serving box within
    /// ~3s, slow enough that the meta query is a rounding
    /// error in the load profile.
    pub tick: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(3),
        }
    }
}

impl ReconcilerConfig {
    /// Env: `AGENTDB_RECONCILER_TICK_SECS` (default 3).
    pub fn from_env() -> Self {
        let d = Self::default();
        let tick = std::env::var("AGENTDB_RECONCILER_TICK_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(d.tick);
        Self { tick }
    }
}

pub struct TabletReconciler {
    config: ReconcilerConfig,
    boxes_added_total: Arc<AtomicU64>,
    errors_total: Arc<AtomicU64>,
    _handle: JoinHandle<()>,
}

impl TabletReconciler {
    /// Spawn the reconcile loop. Cheap; does no I/O until the
    /// first tick fires.
    pub fn start(registry: Arc<BoxRegistry>, config: ReconcilerConfig) -> Arc<Self> {
        let boxes_added_total = Arc::new(AtomicU64::new(0));
        let errors_total = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(run_loop(
            registry,
            config,
            boxes_added_total.clone(),
            errors_total.clone(),
        ));
        Arc::new(Self {
            config,
            boxes_added_total,
            errors_total,
            _handle: handle,
        })
    }

    /// Total `add_box` calls this reconciler has made
    /// successfully. Each successful call corresponds to one
    /// tablet this process started running because the
    /// control-plane assigned it here.
    pub fn boxes_added_total(&self) -> u64 {
        self.boxes_added_total.load(Ordering::Acquire)
    }

    /// Total tick-level errors (meta query failed, or every
    /// `add_box` for the tick failed). Useful to alert on a
    /// reconciler that's stuck.
    pub fn errors_total(&self) -> u64 {
        self.errors_total.load(Ordering::Acquire)
    }

    pub fn config(&self) -> ReconcilerConfig {
        self.config
    }
}

async fn run_loop(
    registry: Arc<BoxRegistry>,
    config: ReconcilerConfig,
    boxes_added_total: Arc<AtomicU64>,
    errors_total: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        interval.tick().await;
        if let Err(()) = reconcile_once(&registry, &boxes_added_total, &errors_total).await {
            // reconcile_once already logged; just count.
            errors_total.fetch_add(1, Ordering::Release);
        }
    }
}

/// One reconcile pass. Returns `Err(())` for tick-level errors
/// (couldn't pull state from meta); per-tablet errors are
/// logged and counted but don't fail the tick.
///
/// Public so the server boot path can run an initial pass
/// synchronously before binding the HTTP listener — that way
/// `/health/ready` reports the post-reconcile box set, not a
/// transient empty registry.
pub async fn reconcile_once(
    registry: &Arc<BoxRegistry>,
    boxes_added_total: &AtomicU64,
    errors_total: &AtomicU64,
) -> Result<(), ()> {
    let Some(meta) = registry.meta_store() else {
        // Reconciler must not run on a meta-less registry. If
        // it's been spawned anyway it's a programmer error;
        // log loudly and treat as a hard error.
        tracing::error!("tablet reconciler: registry has no MetaStore; loop will be a no-op");
        return Err(());
    };
    let Some(node_id) = registry.self_node_id().map(|s| s.to_string()) else {
        tracing::error!("tablet reconciler: registry has no node_id; loop will be a no-op");
        return Err(());
    };
    let owned = match meta_tablets::list_owned_by(meta.pool(), &node_id).await {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(
                %err,
                node_id = %node_id,
                "tablet reconciler: list_owned_by failed; will retry next tick"
            );
            return Err(());
        }
    };
    let mut added = 0u64;
    let mut errs = 0u64;
    for t in owned {
        let id_u16 = match u16::try_from(t.tablet_id) {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(
                    tablet_id = t.tablet_id,
                    "tablet reconciler: tablet_id out of u16 range; skipping"
                );
                continue;
            }
        };
        if registry.has_box(id_u16) {
            continue;
        }
        match registry.add_box(id_u16).await {
            Ok(_) => {
                added += 1;
                tracing::info!(
                    tablet_id = t.tablet_id,
                    node_id = %node_id,
                    "tablet reconciler: started serving newly-assigned tablet"
                );
            }
            Err(err) => {
                errs += 1;
                tracing::warn!(
                    %err,
                    tablet_id = t.tablet_id,
                    node_id = %node_id,
                    "tablet reconciler: add_box failed; will retry next tick"
                );
            }
        }
    }
    if added > 0 {
        boxes_added_total.fetch_add(added, Ordering::Release);
    }
    if errs > 0 {
        errors_total.fetch_add(errs, Ordering::Release);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::box_runtime::BoxRegistry;
    use crate::hlc::HlcClock;
    use agentdb_meta::{nodes as meta_nodes, MetaStore};
    use agentdb_storage::{KvStore, RocksKvStore};

    /// A single-node registry wired to an on-disk SQLite (so
    /// that a separately-opened MetaStore handle in the same
    /// test sees the same rows). Returns the registry plus the
    /// shared meta path so the test can mutate the same DB
    /// from outside.
    async fn registry_with_shared_meta() -> (
        Arc<BoxRegistry>,
        Arc<MetaStore>,
        tempfile::TempDir,
        tempfile::NamedTempFile,
    ) {
        let data = tempfile::tempdir().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(
            RocksKvStore::open(data.path()).expect("open rocks"),
        );
        let object_dir = tempfile::tempdir().unwrap();
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(object_dir.path()).unwrap(),
        );
        std::mem::forget(object_dir); // outlive the test
        let meta_file = tempfile::NamedTempFile::new().unwrap();
        let meta_path = meta_file.path().to_string_lossy().to_string();
        let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
        meta_nodes::upsert(meta.pool(), "n1", "addr", None, "alive", 0)
            .await
            .unwrap();
        let registry = Arc::new(
            BoxRegistry::new_with_meta_and_capacity(
                kv,
                object_store,
                // Start with NO boxes — the reconciler is the
                // only path that adds them in this test.
                1,
                Arc::new(HlcClock::default()),
                meta.clone(),
                "n1".into(),
                64,
            )
            .await
            .unwrap(),
        );
        // The constructor still claims `0..num_boxes`; for this
        // test we only care about box 1, 2, 3 being added by
        // the reconciler. So num_boxes=1 above lets box 0 stay
        // claimed and 1+ get reconciler-added.
        (registry, meta, data, meta_file)
    }

    #[tokio::test]
    async fn reconcile_once_adds_newly_assigned_tablets() {
        let (registry, meta, _data, _meta_file) = registry_with_shared_meta().await;
        // Start: registry has box 0 (from constructor).
        assert_eq!(registry.num_boxes(), 1);

        // External: leader places tablet 1 on us.
        meta_tablets::get_or_create(meta.pool(), 1, 0).await.unwrap();
        meta_tablets::assign_to(meta.pool(), 1, "n1").await.unwrap();

        let added = Arc::new(AtomicU64::new(0));
        let errs = Arc::new(AtomicU64::new(0));
        reconcile_once(&registry, &added, &errs).await.unwrap();
        assert_eq!(added.load(Ordering::Acquire), 1);
        assert_eq!(errs.load(Ordering::Acquire), 0);
        assert!(registry.has_box(1));
        assert_eq!(registry.num_boxes(), 2);
    }

    #[tokio::test]
    async fn reconcile_once_skips_tablets_already_running() {
        let (registry, _meta, _data, _meta_file) = registry_with_shared_meta().await;
        // Box 0 was claimed at construction time. The tablets
        // table has a row for tablet 0 with owner=n1 from that
        // boot-time acquire. A reconcile must NOT try to
        // re-add it.
        let added = Arc::new(AtomicU64::new(0));
        let errs = Arc::new(AtomicU64::new(0));
        reconcile_once(&registry, &added, &errs).await.unwrap();
        assert_eq!(added.load(Ordering::Acquire), 0);
        assert_eq!(registry.num_boxes(), 1);
    }

    #[tokio::test]
    async fn reconcile_once_ignores_tablets_owned_by_other_nodes() {
        let (registry, meta, _data, _meta_file) = registry_with_shared_meta().await;
        meta_tablets::get_or_create(meta.pool(), 5, 0).await.unwrap();
        meta_tablets::assign_to(meta.pool(), 5, "other-node")
            .await
            .unwrap();

        let added = Arc::new(AtomicU64::new(0));
        let errs = Arc::new(AtomicU64::new(0));
        reconcile_once(&registry, &added, &errs).await.unwrap();
        assert_eq!(added.load(Ordering::Acquire), 0);
        assert!(!registry.has_box(5));
    }

    #[tokio::test]
    async fn reconcile_loop_picks_up_late_assignments() {
        let (registry, meta, _data, _meta_file) = registry_with_shared_meta().await;
        let reconciler = TabletReconciler::start(
            registry.clone(),
            ReconcilerConfig {
                tick: Duration::from_millis(100),
            },
        );

        // Put a new tablet in our column AFTER the loop is
        // running.
        meta_tablets::get_or_create(meta.pool(), 7, 0).await.unwrap();
        meta_tablets::assign_to(meta.pool(), 7, "n1").await.unwrap();

        // Wait for at least one tick.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(registry.has_box(7));
        assert!(reconciler.boxes_added_total() >= 1);
    }
}
