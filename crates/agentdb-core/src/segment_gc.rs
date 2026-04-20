use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use tokio::task::JoinHandle;

use crate::box_runtime::Box_;
use crate::error::Result;
use crate::registry::{SessionRegistry, TenantRegistry};

#[derive(Debug, Clone, Copy)]
pub struct SegmentGcConfig {
    pub tick: Duration,
    pub grace: Duration,
}

impl Default for SegmentGcConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(5),
            grace: Duration::from_secs(30),
        }
    }
}

/// Called after each sweep that reclaimed at least one segment, with
/// `(box_id, reclaimed, skipped_pinned)` deltas for that sweep.
/// Wired from the server binary to the SSE stream bus; core stays
/// independent of the API crate.
pub type GcRunReporter = Arc<dyn Fn(u16, u64, u64) + Send + Sync>;

pub struct SegmentGc {
    _handle: JoinHandle<()>,
}

impl SegmentGc {
    pub fn start(
        store: Arc<dyn ObjectStore>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
        box_: Arc<Box_>,
        config: SegmentGcConfig,
        reporter: Option<GcRunReporter>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let before_reclaimed = box_.metrics.snapshot().segment_gc_reclaimed;
                let before_skipped = box_.metrics.snapshot().segment_gc_skipped_pinned;
                if let Err(err) = sweep(&store, &tenants, &sessions, &box_, config).await {
                    tracing::warn!(box_id = box_.id, ?err, "segment_gc sweep errored");
                }
                let after = box_.metrics.snapshot();
                let reclaimed = after.segment_gc_reclaimed.saturating_sub(before_reclaimed);
                let skipped = after.segment_gc_skipped_pinned.saturating_sub(before_skipped);
                if reclaimed > 0 {
                    if let Some(rep) = reporter.as_ref() {
                        rep(box_.id, reclaimed, skipped);
                    }
                }
            }
        });
        Self { _handle: handle }
    }
}

pub(crate) async fn sweep(
    store: &Arc<dyn ObjectStore>,
    tenants: &Arc<TenantRegistry>,
    sessions: &Arc<SessionRegistry>,
    box_: &Arc<Box_>,
    config: SegmentGcConfig,
) -> Result<()> {
    // (1) Live references: scan current session records in this box.
    let mut referenced: HashSet<String> = HashSet::new();
    for tenant_ext in tenants.list_in_box(box_.id)? {
        let Some(tenant) = tenants.lookup(box_.id, &tenant_ext)? else {
            continue;
        };
        for session_ext in sessions.list_in_tenant(box_.id, tenant.internal_id)? {
            let Some(s) = sessions.lookup(box_.id, tenant.internal_id, &session_ext)? else {
                continue;
            };
            if let Some(p) = s.segment_path {
                referenced.insert(p);
            }
        }
    }

    // (2) Pinned by open snapshots.
    let pinned = box_.segment_pins.pinned_paths();
    for p in &pinned {
        referenced.insert(p.clone());
    }

    // (3) List + reap. The prefix matches `segment::segment_path()`:
    // `boxes/{box_id:04x}/cold/...`.
    let prefix = format!("boxes/{:04x}/cold/", box_.id);
    let obj_prefix = ObjPath::from(prefix.as_str());
    let mut stream = store.list(Some(&obj_prefix));
    let grace_ms = config.grace.as_millis() as i64;
    let now_ms = now_millis() as i64;
    while let Some(obj) = stream.next().await {
        let obj = match obj {
            Ok(o) => o,
            Err(err) => {
                tracing::warn!(?err, "segment_gc list errored; aborting sweep");
                return Ok(());
            }
        };
        let path = obj.location.to_string();
        if referenced.contains(&path) {
            // Could be live-referenced or pinned. Record the pin case separately.
            if pinned.contains(&path) {
                box_.metrics.record_segment_gc_skipped_pinned();
            }
            continue;
        }
        let age_ms = now_ms.saturating_sub(obj.last_modified.timestamp_millis());
        if age_ms < grace_ms {
            continue;
        }
        match store.delete(&obj.location).await {
            Ok(()) => {
                box_.metrics.record_segment_gc_reclaimed();
            }
            Err(err) => tracing::warn!(?err, %path, "segment_gc delete failed"),
        }
    }
    Ok(())
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::append_and_fetch;
    use crate::read_ctx::ReadCtx;
    use crate::tiering::cold_transition;
    use crate::BoxRegistry;
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use tempfile::TempDir;

    async fn setup() -> (
        TempDir,
        TempDir,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 1).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        (kv_dir, obj_dir, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn sweep_skips_referenced_segment() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"x": 1}),
            0,
            10,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await
                .unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_millis(0),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config)
            .await
            .unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path)
                .await
                .is_ok(),
            "live-referenced segment must not be reaped"
        );
        let m = box_.metrics.snapshot();
        assert_eq!(m.segment_gc_reclaimed, 0);
    }

    #[tokio::test]
    async fn sweep_skips_snapshot_pinned_segment() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"x": 1}),
            0,
            10,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await
                .unwrap();
        }
        // Open a ReadCtx that pins the segment.
        let ctx = ReadCtx::open(&box_, reg.kv()).unwrap();
        let _s = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap();
        assert_eq!(box_.segment_pins.pinned_len(), 1);

        // Reactivate so the session's current record no longer references the
        // segment — it's only referenced by the pin.
        {
            let guard = box_.write_lock.lock().await;
            let mut fresh = sessions
                .lookup(box_.id, tenant.internal_id, "s-1")
                .unwrap()
                .unwrap();
            crate::tiering::reactivate(reg.kv(), reg.object_store(), &sessions, &guard, &mut fresh)
                .await
                .unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_millis(0),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config)
            .await
            .unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path)
                .await
                .is_ok(),
            "pinned segment must not be reaped"
        );
        let m = box_.metrics.snapshot();
        assert_eq!(m.segment_gc_skipped_pinned, 1);
        drop(ctx);
    }

    #[tokio::test]
    async fn sweep_reclaims_orphan_past_grace() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"x": 1}),
            0,
            10,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await
                .unwrap();
        }
        // Reactivate, nobody pins the segment: orphan.
        {
            let guard = box_.write_lock.lock().await;
            let mut fresh = sessions
                .lookup(box_.id, tenant.internal_id, "s-1")
                .unwrap()
                .unwrap();
            crate::tiering::reactivate(reg.kv(), reg.object_store(), &sessions, &guard, &mut fresh)
                .await
                .unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        // Grace 0 — the orphan is eligible immediately.
        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_millis(0),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config)
            .await
            .unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path)
                .await
                .is_err(),
            "orphan segment past grace must be reaped"
        );
        let m = box_.metrics.snapshot();
        assert_eq!(m.segment_gc_reclaimed, 1);
    }

    #[tokio::test]
    async fn sweep_respects_grace_window() {
        let (_k, _o, reg, tenants, sessions) = setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"x": 1}),
            0,
            10,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        {
            let guard = box_.write_lock.lock().await;
            cold_transition(reg.kv(), reg.object_store(), &sessions, &guard, &mut session)
                .await
                .unwrap();
        }
        {
            let guard = box_.write_lock.lock().await;
            let mut fresh = sessions
                .lookup(box_.id, tenant.internal_id, "s-1")
                .unwrap()
                .unwrap();
            crate::tiering::reactivate(reg.kv(), reg.object_store(), &sessions, &guard, &mut fresh)
                .await
                .unwrap();
        }
        let path = session.segment_path.clone().unwrap();

        // Grace 60s — the orphan is NOT yet eligible.
        let config = SegmentGcConfig {
            tick: Duration::from_millis(10),
            grace: Duration::from_secs(60),
        };
        sweep(reg.object_store(), &tenants, &sessions, &box_, config)
            .await
            .unwrap();
        assert!(
            crate::segment::read_segment(reg.object_store(), &path)
                .await
                .is_ok(),
            "segment younger than grace must not be reaped"
        );
        assert_eq!(box_.metrics.snapshot().segment_gc_reclaimed, 0);
    }
}
