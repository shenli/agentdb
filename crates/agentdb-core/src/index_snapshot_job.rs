//! M16 Phase F — periodic index-snapshot job.
//!
//! One task per box. Every `tick` (default 1 hour), capture the live
//! `PerBoxIndexes` as a snapshot and upload it to object storage, then
//! prune snapshots older than the retention bar.
//!
//! Pattern follows `SegmentGc` — one `tokio::spawn` per box, held via
//! a `JoinHandle` inside the RAII `IndexSnapshotJob` struct. The task
//! never exits; dropping the struct aborts the task.

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use tokio::task::JoinHandle;

use crate::box_runtime::Box_;
use crate::error::Result;

/// Configuration for the per-box periodic snapshot job.
#[derive(Debug, Clone, Copy)]
pub struct IndexSnapshotJobConfig {
    /// Gap between snapshot attempts.
    pub tick: Duration,
    /// How many snapshots to retain on object storage. Older ones are
    /// pruned after each successful upload.
    pub retain: usize,
}

impl Default for IndexSnapshotJobConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(3600),
            retain: 3,
        }
    }
}

impl IndexSnapshotJobConfig {
    /// Read config from env. Defaults: 1-hour tick, retain 3.
    ///
    /// `AGENTDB_INDEX_SNAPSHOT_SECS`  — integer seconds.
    /// `AGENTDB_INDEX_SNAPSHOT_RETAIN` — integer count.
    pub fn from_env() -> Self {
        let default = Self::default();
        let tick = std::env::var("AGENTDB_INDEX_SNAPSHOT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(default.tick);
        let retain = std::env::var("AGENTDB_INDEX_SNAPSHOT_RETAIN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default.retain);
        Self { tick, retain }
    }
}

/// RAII handle for one box's periodic snapshot task.
pub struct IndexSnapshotJob {
    _handle: JoinHandle<()>,
}

impl IndexSnapshotJob {
    /// Spawn the periodic job for one box. Returns immediately; the
    /// task runs in the background until this handle is dropped.
    pub fn start(
        store: Arc<dyn ObjectStore>,
        box_: Arc<Box_>,
        config: IndexSnapshotJobConfig,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate-first-tick behavior of `Interval` — we
            // want the first snapshot ~1 hour into the run, not at t=0
            // when the indexes are likely still empty or unbuilt.
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(err) = snapshot_once(&store, &box_, config).await {
                    tracing::warn!(box_id = box_.id, ?err, "index_snapshot_job errored");
                }
            }
        });
        Self { _handle: handle }
    }
}

/// Run one snapshot-and-prune cycle against the given box. Exposed for
/// tests and for the drain path (Phase F follow-up); in normal
/// operation it's called by the spawned loop.
pub async fn snapshot_once(
    store: &Arc<dyn ObjectStore>,
    box_: &Arc<Box_>,
    config: IndexSnapshotJobConfig,
) -> Result<()> {
    // Capture the HLC cutoff *before* taking the snapshot. Any event
    // appended after this point has an HLC strictly greater than
    // `hlc_cutoff` and will be picked up by replay on the next
    // recovery path.
    let hlc_cutoff = box_.hlc().current();
    let written = box_.snapshot_indexes_to_store(store, hlc_cutoff).await?;
    if written.is_none() {
        // Indexes were never built (no queries or writes on this box
        // yet). Nothing to snapshot; not an error.
        return Ok(());
    }
    let pruned = crate::index_snapshot::prune_old_snapshots(store, box_.id, config.retain).await?;
    if pruned > 0 {
        tracing::debug!(box_id = box_.id, pruned, "index_snapshot_job pruned");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{append_and_fetch, BoxRegistry, SessionRegistry, TenantRegistry};
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use tempfile::TempDir;

    async fn ctx() -> (
        TempDir,
        TempDir,
        Arc<dyn KvStore>,
        Arc<dyn ObjectStore>,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store.clone(), 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        (kv_dir, obj_dir, kv, store, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn snapshot_once_writes_and_prunes() {
        let (_kd, _od, kv, store, reg, tenants, sessions) = ctx().await;
        // Seed + build indexes so snapshot has non-empty state.
        append_and_fetch(&reg, &tenants, &sessions, "t-1", "s-1", json!({"text": "x"}), 0, 0)
            .await
            .unwrap();
        let box_ = reg.route_tenant("t-1");
        drop(
            box_.indexes_built(&kv, &store, &tenants, &sessions)
                .await
                .unwrap(),
        );

        // Run 4 ticks of snapshot_once manually with retain=2. After
        // the 4th, only 2 snapshots should remain on the store.
        let config = IndexSnapshotJobConfig {
            tick: Duration::from_millis(1),
            retain: 2,
        };
        for _ in 0..4 {
            // Advance HLC between snapshots so paths differ.
            reg.hlc().now();
            snapshot_once(&store, &box_, config).await.unwrap();
        }
        let kept = crate::index_snapshot::list_snapshots(&store, box_.id)
            .await
            .unwrap();
        assert_eq!(kept.len(), 2, "retain=2 must keep exactly 2 snapshots");

        let snap = box_.metrics.snapshot();
        assert!(
            snap.index_snapshots_written >= 4,
            "each snapshot_once should record a successful write; got {}",
            snap.index_snapshots_written
        );
    }

    #[tokio::test]
    async fn snapshot_once_is_a_noop_when_indexes_not_built() {
        let (_kd, _od, _kv, store, reg, _tenants, _sessions) = ctx().await;
        let box_ = reg.route_tenant("t-empty");
        // Do not call indexes_built — indexes are None.
        let config = IndexSnapshotJobConfig {
            tick: Duration::from_millis(1),
            retain: 3,
        };
        snapshot_once(&store, &box_, config).await.unwrap();
        // No snapshots should exist.
        let kept = crate::index_snapshot::list_snapshots(&store, box_.id)
            .await
            .unwrap();
        assert!(
            kept.is_empty(),
            "no snapshot expected when indexes not built; got {kept:?}"
        );
    }

    #[tokio::test]
    async fn env_config_parses_with_defaults() {
        std::env::remove_var("AGENTDB_INDEX_SNAPSHOT_SECS");
        std::env::remove_var("AGENTDB_INDEX_SNAPSHOT_RETAIN");
        let c = IndexSnapshotJobConfig::from_env();
        assert_eq!(c.tick, Duration::from_secs(3600));
        assert_eq!(c.retain, 3);

        std::env::set_var("AGENTDB_INDEX_SNAPSHOT_SECS", "120");
        std::env::set_var("AGENTDB_INDEX_SNAPSHOT_RETAIN", "5");
        let c = IndexSnapshotJobConfig::from_env();
        assert_eq!(c.tick, Duration::from_secs(120));
        assert_eq!(c.retain, 5);

        std::env::remove_var("AGENTDB_INDEX_SNAPSHOT_SECS");
        std::env::remove_var("AGENTDB_INDEX_SNAPSHOT_RETAIN");
    }
}
