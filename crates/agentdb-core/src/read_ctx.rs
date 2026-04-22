use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use agentdb_storage::{BoxId, KvIter, KvSnapshot, KvStore, TenantId};
use parking_lot::Mutex;

use crate::box_runtime::Box_;
use crate::error::Result;
use crate::hlc::HlcTimestamp;
use crate::registry::SessionRegistry;
use crate::segment_pin::SegmentPinRegistry;
use crate::types::{Session, Tier};

/// A request-scoped point-in-time view over one box. Holds a pinned KV
/// snapshot and a set of segment paths pinned in the box's
/// `SegmentPinRegistry`. Drops release all pins and the snapshot.
///
/// Not persisted, not durable — a process restart loses all ReadCtxs
/// and their pins simultaneously, which is safe because all readers
/// would be gone too.
pub struct ReadCtx {
    box_id: BoxId,
    snapshot: Arc<dyn KvSnapshot>,
    registry: Arc<SegmentPinRegistry>,
    metrics: Arc<crate::metrics::BoxMetrics>,
    pinned_segments: Mutex<BTreeSet<String>>,
    // `box_id` is deliberately omitted from the key because a ReadCtx is
    // bound to a single box (see `open`); all entries share that box.
    session_cache: Mutex<HashMap<(TenantId, u64), Session>>,
    external_cache: Mutex<HashMap<(TenantId, String), u64>>,
    /// HLC cutoff captured at `open()` time. Read paths filter out events
    /// whose `hlc_ts` is greater than this cutoff — the HLC analogue of the
    /// RocksDB snapshot, and the only isolation mechanism for segment-backed
    /// (cold / archive / fork-parent) reads where RocksDB snapshots do not
    /// apply. See `docs/agent-db-v2.md` §4.2.
    hlc_cutoff: HlcTimestamp,
}

impl ReadCtx {
    /// Open a new ReadCtx against `box_`'s KV snapshot.
    pub fn open(box_: &Box_, kv: &Arc<dyn KvStore>) -> Result<Self> {
        let snapshot = Arc::clone(kv).snapshot_owned()?;
        box_.metrics.bump_open_snapshots();
        let hlc_cutoff = box_.hlc().current();
        Ok(Self {
            box_id: box_.id,
            snapshot,
            registry: box_.segment_pins.clone(),
            metrics: box_.metrics.clone(),
            pinned_segments: Mutex::new(BTreeSet::new()),
            session_cache: Mutex::new(HashMap::new()),
            external_cache: Mutex::new(HashMap::new()),
            hlc_cutoff,
        })
    }

    pub fn box_id(&self) -> BoxId {
        self.box_id
    }

    /// The HLC cutoff captured at open time. Events with `hlc_ts > cutoff`
    /// are not visible through this ReadCtx.
    pub fn hlc_cutoff(&self) -> HlcTimestamp {
        self.hlc_cutoff
    }

    /// Scan a key prefix against the pinned snapshot.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIter> {
        Ok(self.snapshot.scan_prefix(prefix)?)
    }

    /// Point lookup against the pinned snapshot.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snapshot.get(key)?)
    }

    /// Resolve a session at snapshot time by external id. If the session
    /// is Cold, its `segment_path` is pinned in the registry so GC
    /// cannot delete it while this ReadCtx is live. Results are cached.
    pub fn session(
        &self,
        sessions: &SessionRegistry,
        tenant_internal_id: TenantId,
        session_external: &str,
    ) -> Result<Option<Session>> {
        if let Some(&internal_id) = self
            .external_cache
            .lock()
            .get(&(tenant_internal_id, session_external.to_string()))
        {
            return self.session_by_internal(sessions, tenant_internal_id, internal_id);
        }
        let session = sessions.lookup_at(
            self.snapshot.as_ref(),
            self.box_id,
            tenant_internal_id,
            session_external,
        )?;
        if let Some(ref s) = session {
            self.external_cache.lock().insert(
                (tenant_internal_id, session_external.to_string()),
                s.internal_id,
            );
            self.cache_and_pin(s.clone());
        }
        Ok(session)
    }

    /// Resolve a session at snapshot time by internal id.
    pub fn session_by_internal(
        &self,
        sessions: &SessionRegistry,
        tenant_internal_id: TenantId,
        session_internal_id: u64,
    ) -> Result<Option<Session>> {
        let key = (tenant_internal_id, session_internal_id);
        if let Some(cached) = self.session_cache.lock().get(&key).cloned() {
            return Ok(Some(cached));
        }
        let session = sessions.lookup_by_internal_at(
            self.snapshot.as_ref(),
            self.box_id,
            tenant_internal_id,
            session_internal_id,
        )?;
        if let Some(ref s) = session {
            self.cache_and_pin(s.clone());
        }
        Ok(session)
    }

    fn cache_and_pin(&self, s: Session) {
        debug_assert_eq!(
            s.box_id, self.box_id,
            "ReadCtx is bound to one box; session must match"
        );
        // Lock order (for future contributors): pinned_segments Mutex, THEN
        // registry.counts Mutex. Never invert. `Drop` takes pinned_segments
        // first and releases it (via mem::take) before calling registry.release,
        // which keeps the order consistent there too.
        if s.tier == Tier::Cold {
            if let Some(path) = s.segment_path.as_deref() {
                let mut pins = self.pinned_segments.lock();
                if pins.insert(path.to_string()) {
                    self.registry.pin(path);
                    self.metrics.set_segments_pinned(self.registry.pinned_len());
                }
            }
        }
        let key = (s.tenant_internal_id, s.internal_id);
        self.session_cache.lock().insert(key, s);
    }
}

impl Drop for ReadCtx {
    fn drop(&mut self) {
        let pins = std::mem::take(&mut *self.pinned_segments.lock());
        self.registry.release(pins);
        self.metrics.set_segments_pinned(self.registry.pinned_len());
        self.metrics.drop_open_snapshot();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::box_runtime::BoxRegistry;
    use crate::primitives::append_and_fetch;
    use crate::registry::{SessionRegistry, TenantRegistry};
    use agentdb_storage::RocksKvStore;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use serde_json::json;
    use tempfile::TempDir;

    async fn ctx_setup() -> (
        TempDir,
        TempDir,
        Arc<dyn KvStore>,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv.clone()));
        (kv_dir, obj_dir, kv, reg, tenants, sessions)
    }

    #[tokio::test]
    async fn open_bumps_gauge_and_drop_decrements() {
        let (_k, _o, kv, reg, _t, _s) = ctx_setup().await;
        let box_ = reg.route_tenant("t-1");
        {
            let ctx = ReadCtx::open(&box_, &kv).unwrap();
            let snap = box_.metrics.snapshot();
            assert_eq!(snap.open_snapshots, 1);
            drop(ctx);
        }
        let snap = box_.metrics.snapshot();
        assert_eq!(snap.open_snapshots, 0);
    }

    #[tokio::test]
    async fn session_lookup_pins_cold_segment() {
        let (_k, _o, kv, reg, tenants, sessions) = ctx_setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"x": 1}),
            0,
            100,
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
            crate::tiering::cold_transition(
                &kv,
                reg.object_store(),
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        assert_eq!(session.tier, Tier::Cold);

        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let _s = ctx.session(&sessions, tenant.internal_id, "s-1").unwrap();
        assert_eq!(box_.segment_pins.pinned_len(), 1);

        drop(ctx);
        assert_eq!(box_.segment_pins.pinned_len(), 0);
    }

    #[tokio::test]
    async fn session_cache_returns_same_value_on_second_call() {
        let (_k, _o, kv, reg, tenants, sessions) = ctx_setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"x": 1}),
            0,
            100,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        let a = ctx
            .session(&sessions, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        let b = ctx
            .session_by_internal(&sessions, tenant.internal_id, a.internal_id)
            .unwrap()
            .unwrap();
        assert_eq!(a.external_id, b.external_id);
        assert_eq!(a.next_seq, b.next_seq);
    }

    #[tokio::test]
    async fn hlc_cutoff_captures_current_clock_at_open() {
        // ReadCtx::open captures the HLC at the moment of call. Subsequent
        // `hlc.now()` invocations (from a later append) must not affect
        // this ctx's cutoff.
        let (_k, _o, kv, reg, _t, _s) = ctx_setup().await;
        let box_ = reg.route_tenant("t-1");
        // Advance the clock once.
        let before = reg.hlc().now();
        let ctx = ReadCtx::open(&box_, &kv).unwrap();
        assert!(
            ctx.hlc_cutoff() >= before,
            "cutoff {:?} must be >= pre-open clock {:?}",
            ctx.hlc_cutoff(),
            before
        );
        let cutoff_at_open = ctx.hlc_cutoff();
        // Advance the clock more times.
        for _ in 0..5 {
            reg.hlc().now();
        }
        assert_eq!(
            ctx.hlc_cutoff(),
            cutoff_at_open,
            "ctx.hlc_cutoff() must not change after later hlc.now() calls"
        );
    }

    #[tokio::test]
    async fn hlc_cutoff_hides_events_appended_after_open() {
        // Write 2 events, then open a ReadCtx, then write 3 more events.
        // The ReadCtx should see only the first 2. This is the HLC-cutoff
        // version of snapshot isolation; it matches the RocksDB-snapshot
        // story but also extends to any segment-sourced reads.
        let (_k, _o, kv, reg, tenants, sessions) = ctx_setup().await;
        for i in 1..=2u64 {
            append_and_fetch(
                &reg,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"i": i}),
                0,
                i * 10,
            )
            .await
            .unwrap();
        }
        let box_ = reg.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let ctx = ReadCtx::open(&box_, &kv).unwrap();

        // Write more events after ctx opens.
        for i in 3..=5u64 {
            append_and_fetch(
                &reg,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"i": i}),
                0,
                i * 10,
            )
            .await
            .unwrap();
        }

        let session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        let seen = crate::primitives::fetch_last_n(&ctx, &session, 100).unwrap();
        assert_eq!(
            seen.len(),
            2,
            "ReadCtx should see only pre-open events; saw {}",
            seen.len()
        );
        assert_eq!(seen[0].seq, 1);
        assert_eq!(seen[1].seq, 2);
        for ev in &seen {
            assert!(
                ev.hlc_ts <= ctx.hlc_cutoff(),
                "event hlc_ts {:?} leaked past cutoff {:?}",
                ev.hlc_ts,
                ctx.hlc_cutoff()
            );
        }
    }

    #[tokio::test]
    async fn two_ctxs_pin_same_cold_segment_independently() {
        let (_k, _o, kv, reg, tenants, sessions) = ctx_setup().await;
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"x": 1}),
            0,
            100,
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
            crate::tiering::cold_transition(
                &kv,
                reg.object_store(),
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }

        let ctx_a = ReadCtx::open(&box_, &kv).unwrap();
        let ctx_b = ReadCtx::open(&box_, &kv).unwrap();
        ctx_a.session(&sessions, tenant.internal_id, "s-1").unwrap();
        ctx_b.session(&sessions, tenant.internal_id, "s-1").unwrap();
        assert_eq!(
            box_.segment_pins.pinned_len(),
            1,
            "same path, one distinct entry"
        );

        drop(ctx_a);
        assert_eq!(box_.segment_pins.pinned_len(), 1, "ctx_b still holds pin");

        drop(ctx_b);
        assert_eq!(box_.segment_pins.pinned_len(), 0, "all dropped");
    }
}
