use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use agentdb_storage::{BoxId, KvStore};
use object_store::ObjectStore;
use parking_lot::RwLock;
use tokio::sync::Mutex;

use crate::admission::{AdmissionDenied, Scope, TokenBucket};
use crate::error::{Error, Result};
use crate::hlc::HlcClock;
use crate::metrics::BoxMetrics;
use crate::segment_pin::SegmentPinRegistry;

#[derive(Debug, Clone, Copy)]
pub enum FaultKind {
    Latency { ms: u64 },
    Error,
}

#[derive(Debug, Clone, Copy)]
pub struct InjectedFault {
    pub kind: FaultKind,
    pub expires_at: Instant,
}

impl InjectedFault {
    pub fn is_active(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Quota {
    pub max_warm_sessions: Option<u64>,
    pub max_qps: Option<u64>,
    pub max_tenant_qps: Option<u64>,
    pub burst: Option<u64>,
}

/// One logical box. Owns a mutex that serializes the hot-path writes
/// (append, metadata updates) for tenants it hosts.
pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
    indexes: tokio::sync::RwLock<Option<crate::indexes::PerBoxIndexes>>,
    pub metrics: Arc<BoxMetrics>,
    pub fault: RwLock<Option<InjectedFault>>,
    pub quota: RwLock<Quota>,
    pub webhook: RwLock<Option<String>>,
    pub segment_pins: Arc<SegmentPinRegistry>,
    box_bucket: parking_lot::Mutex<Option<TokenBucket>>,
    tenant_buckets: parking_lot::Mutex<HashMap<u64, TokenBucket>>,
    hlc: Arc<HlcClock>,
}

impl Box_ {
    fn new(id: BoxId, hlc: Arc<HlcClock>) -> Self {
        Self {
            id,
            write_lock: Mutex::new(()),
            indexes: tokio::sync::RwLock::new(None),
            metrics: Arc::new(BoxMetrics::new()),
            fault: RwLock::new(None),
            quota: RwLock::new(Quota::default()),
            webhook: RwLock::new(None),
            segment_pins: Arc::new(SegmentPinRegistry::new()),
            box_bucket: parking_lot::Mutex::new(None),
            tenant_buckets: parking_lot::Mutex::new(HashMap::new()),
            hlc,
        }
    }

    /// The process-global hybrid logical clock shared by all boxes. Cheap
    /// Arc clone; always returns the same clock instance.
    pub fn hlc(&self) -> &Arc<HlcClock> {
        &self.hlc
    }

    /// If a fault is active, apply its effect. For Latency, sleep. For
    /// Error, return true so the caller can short-circuit.
    pub async fn apply_fault(&self) -> bool {
        let f = { *self.fault.read() };
        if let Some(f) = f {
            if f.is_active() {
                match f.kind {
                    FaultKind::Latency { ms } => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
                        false
                    }
                    FaultKind::Error => true,
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Rebuild the box_bucket and drop any cached tenant_buckets so
    /// the next `admit` call for each tenant constructs a fresh bucket
    /// at the current `quota` settings. Called by the admin quota
    /// endpoint after a live config change.
    pub fn rebuild_buckets(&self) {
        let q = *self.quota.read();
        *self.box_bucket.lock() = q
            .max_qps
            .map(|qps| TokenBucket::new(qps, q.burst.unwrap_or(qps)));
        self.tenant_buckets.lock().clear();
    }

    /// Returns `Ok(())` if both the per-box and per-tenant buckets
    /// grant a token. Returns `AdmissionDenied` with the scope of the
    /// first refusing gate. Records a `quota_hit` metric on denial.
    ///
    /// `tenant_internal_id == 0` is a sentinel meaning "tenant not
    /// yet minted" (callers use it for the first-ever request from a
    /// new tenant, where calling `get_or_mint` before the write_lock
    /// would race). In that case the per-tenant gate is skipped; the
    /// per-box gate still runs.
    ///
    /// Both buckets must grant. The per-box gate runs first so one
    /// tenant can't repeatedly drain the per-tenant bucket check while
    /// the per-box gate is already saturated.
    pub fn admit(&self, tenant_internal_id: u64) -> std::result::Result<(), AdmissionDenied> {
        {
            let mut b = self.box_bucket.lock();
            if let Some(bucket) = b.as_mut() {
                if !bucket.admit() {
                    self.metrics.record_quota_hit();
                    return Err(AdmissionDenied { scope: Scope::Box });
                }
            }
        }
        if tenant_internal_id == 0 {
            return Ok(());
        }
        let q = *self.quota.read();
        if let Some(qps) = q.max_tenant_qps {
            let burst = q.burst.unwrap_or(qps);
            let mut map = self.tenant_buckets.lock();
            let bucket = map
                .entry(tenant_internal_id)
                .or_insert_with(|| TokenBucket::new(qps, burst));
            if !bucket.admit() {
                self.metrics.record_quota_hit();
                return Err(AdmissionDenied {
                    scope: Scope::Tenant,
                });
            }
        }
        Ok(())
    }

    /// Build (on first call) and return a read guard over the box's indexes.
    /// Rebuilds are skipped — subsequent calls reuse the existing instance.
    ///
    /// M16 Phase E: the "first call" path first tries to load the most
    /// recent index snapshot from object storage and replay only
    /// events appended after the snapshot's HLC cutoff. Falls back to a
    /// full KV scan if no snapshot is found or if loading fails.
    pub async fn indexes_built<'a>(
        &'a self,
        kv: &Arc<dyn KvStore>,
        store: &Arc<dyn ObjectStore>,
        tenants: &crate::registry::TenantRegistry,
        sessions: &crate::registry::SessionRegistry,
    ) -> Result<tokio::sync::RwLockReadGuard<'a, Option<crate::indexes::PerBoxIndexes>>> {
        // Check fast path under a read lock.
        {
            let r = self.indexes.read().await;
            if r.is_some() {
                return Ok(r);
            }
        }
        // Build under an exclusive lock. Double-check inside in case of races.
        {
            let mut w = self.indexes.write().await;
            if w.is_none() {
                let mut built = match self.try_load_snapshot(store).await {
                    Ok(Some((snap_hlc, indexes))) => {
                        tracing::info!(
                            box_id = self.id,
                            cutoff = snap_hlc.physical_ns,
                            "index snapshot loaded; replaying events after cutoff"
                        );
                        let mut idx = indexes;
                        let replayed = idx
                            .replay_events_after(
                                kv, store, tenants, sessions, self.id, snap_hlc,
                            )
                            .await?;
                        tracing::info!(box_id = self.id, replayed, "snapshot replay complete");
                        idx
                    }
                    Ok(None) => {
                        // No snapshot — fall back to full rebuild.
                        let mut idx = crate::indexes::PerBoxIndexes::new();
                        idx.build(kv, store, tenants, sessions, self.id).await?;
                        idx
                    }
                    Err(err) => {
                        // Snapshot exists but we couldn't load it —
                        // log and fall back rather than wedging startup.
                        tracing::warn!(
                            box_id = self.id,
                            error = %err,
                            "failed to load index snapshot; falling back to full rebuild"
                        );
                        let mut idx = crate::indexes::PerBoxIndexes::new();
                        idx.build(kv, store, tenants, sessions, self.id).await?;
                        idx
                    }
                };
                // Prevent "unused mut" from triggering when we assign
                // without later modifying — `built` is already populated
                // above but kept mutable for symmetry.
                let _ = &mut built;
                *w = Some(built);
            }
        }
        Ok(self.indexes.read().await)
    }

    /// Look for an existing snapshot in object storage and, if present,
    /// deserialize it. Returns `Ok(None)` if there are no snapshots.
    /// The caller is responsible for replaying post-cutoff events.
    async fn try_load_snapshot(
        &self,
        store: &Arc<dyn ObjectStore>,
    ) -> Result<Option<(crate::hlc::HlcTimestamp, crate::indexes::PerBoxIndexes)>> {
        let Some(path) = crate::index_snapshot::latest_snapshot_path(store, self.id).await? else {
            return Ok(None);
        };
        let snap = crate::index_snapshot::read_snapshot(store, &path).await?;
        let indexes = crate::index_snapshot::restore_indexes(&snap)?;
        Ok(Some((snap.hlc_ts_at_snapshot, indexes)))
    }

    /// Capture the current state of this box's indexes to object
    /// storage. Triggered by the periodic job (M16 Phase D) and by
    /// `drain` (M16 Phase F). No-op if the indexes have not yet been
    /// built (never queried, never written).
    pub async fn snapshot_indexes_to_store(
        &self,
        store: &Arc<dyn ObjectStore>,
        hlc: crate::hlc::HlcTimestamp,
    ) -> Result<Option<String>> {
        let guard = self.indexes.read().await;
        let Some(indexes) = guard.as_ref() else {
            return Ok(None);
        };
        let snap = crate::index_snapshot::snapshot_indexes(indexes, self.id, hlc);
        drop(guard);
        let path = match crate::index_snapshot::write_snapshot(store, &snap).await {
            Ok(p) => {
                self.metrics.record_index_snapshot_written();
                p
            }
            Err(e) => {
                self.metrics.record_index_snapshot_failure();
                return Err(e);
            }
        };
        Ok(Some(path))
    }

    /// Incrementally index one newly-appended event (M16 Phase C).
    ///
    /// If the box's indexes have not yet been built, this triggers the
    /// full initial build from KV+object-store first — the same lazy
    /// pattern as `indexes_built`. In that case the newly-appended
    /// event is already present in KV and gets picked up by the full
    /// build, so the explicit incremental insert is skipped to avoid
    /// double-indexing.
    ///
    /// When the indexes are already built, the event is inserted
    /// incrementally with no rebuild (the M16 improvement).
    ///
    /// Callers hold `Box_::write_lock` while invoking this; the lock
    /// order is always `write_lock → indexes.write`, never inverted.
    pub async fn index_event(
        &self,
        kv: &Arc<dyn KvStore>,
        store: &Arc<dyn ObjectStore>,
        tenants: &crate::registry::TenantRegistry,
        sessions: &crate::registry::SessionRegistry,
        session_internal_id: u64,
        event: &crate::types::Event,
    ) -> Result<()> {
        let mut w = self.indexes.write().await;
        let was_built = w.is_some();
        if !was_built {
            let mut built = crate::indexes::PerBoxIndexes::new();
            built.build(kv, store, tenants, sessions, self.id).await?;
            *w = Some(built);
            // The full build already includes the event (it's been
            // persisted to KV by the time we reach here).
            self.metrics.record_event_indexed();
            return Ok(());
        }
        if let Some(indexes) = w.as_mut() {
            indexes.index_event(session_internal_id, event);
            self.metrics.record_event_indexed();
        }
        Ok(())
    }
}

/// Owns the pool of boxes and the shared KV store, plus the tenant→box
/// routing function and the process-global HLC.
pub struct BoxRegistry {
    boxes: Vec<Arc<Box_>>,
    kv: Arc<dyn KvStore>,
    object_store: Arc<dyn ObjectStore>,
    hlc: Arc<HlcClock>,
}

impl BoxRegistry {
    /// Build a registry with a default HLC (10s max-offset). The server
    /// binary should prefer `new_with_hlc` to honor the
    /// `AGENTDB_HLC_MAX_OFFSET_SECS` env var.
    pub fn new(
        kv: Arc<dyn KvStore>,
        object_store: Arc<dyn ObjectStore>,
        num_boxes: u16,
    ) -> Result<Self> {
        Self::new_with_hlc(kv, object_store, num_boxes, Arc::new(HlcClock::default()))
    }

    /// Build a registry with a caller-provided HLC. There is exactly one
    /// `HlcClock` per process; all boxes share it.
    pub fn new_with_hlc(
        kv: Arc<dyn KvStore>,
        object_store: Arc<dyn ObjectStore>,
        num_boxes: u16,
        hlc: Arc<HlcClock>,
    ) -> Result<Self> {
        if num_boxes == 0 {
            return Err(Error::Invalid {
                field: Some("num_boxes".into()),
                message: "num_boxes must be > 0".into(),
            });
        }
        let boxes = (0..num_boxes)
            .map(|i| Arc::new(Box_::new(i, hlc.clone())))
            .collect();
        Ok(Self {
            boxes,
            kv,
            object_store,
            hlc,
        })
    }

    pub fn kv(&self) -> &Arc<dyn KvStore> {
        &self.kv
    }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }

    /// The process-global hybrid logical clock. Every event append assigns
    /// its `hlc_ts` from this, and every RPC-layer receive updates it via
    /// `HlcClock::update`.
    pub fn hlc(&self) -> &Arc<HlcClock> {
        &self.hlc
    }

    pub fn num_boxes(&self) -> u16 {
        self.boxes.len() as u16
    }

    /// Route a tenant external id to a box using FNV-1a hash mod num_boxes.
    pub fn route_tenant(&self, external: &str) -> Arc<Box_> {
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in external.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let idx = (hash as usize) % self.boxes.len();
        self.boxes[idx].clone()
    }

    /// Look up an `Arc<Box_>` by index. Used by the server binary to start
    /// one `LifecycleEngine` per box at process startup.
    pub fn box_by_id(&self, id: BoxId) -> Option<Arc<Box_>> {
        self.boxes.get(id as usize).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;
    use object_store::local::LocalFileSystem;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn registry(num: u16) -> (TempDir, TempDir, BoxRegistry) {
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        (dir, obj_dir, BoxRegistry::new(kv, store, num).unwrap())
    }

    #[test]
    fn zero_boxes_is_rejected() {
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        assert!(BoxRegistry::new(kv, store, 0).is_err());
    }

    #[test]
    fn same_tenant_always_routes_to_same_box() {
        let (_dir, _obj_dir, reg) = registry(8);
        let a = reg.route_tenant("t-42").id;
        let b = reg.route_tenant("t-42").id;
        assert_eq!(a, b);
    }

    #[test]
    fn different_tenants_eventually_hit_multiple_boxes() {
        let (_dir, _obj_dir, reg) = registry(4);
        let mut hit = HashSet::new();
        for i in 0..200 {
            hit.insert(reg.route_tenant(&format!("t-{i}")).id);
        }
        assert!(
            hit.len() > 1,
            "expected distribution across boxes, got {hit:?}"
        );
    }

    #[test]
    fn num_boxes_reports_correct_count() {
        let (_dir, _obj_dir, reg) = registry(5);
        assert_eq!(reg.num_boxes(), 5);
    }

    #[tokio::test]
    async fn indexes_built_is_idempotent_and_reflects_events() {
        use crate::primitives::append_and_fetch;
        use crate::registry::{SessionRegistry, TenantRegistry};
        use serde_json::json;

        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        append_and_fetch(
            &reg,
            &tenants,
            &sessions,
            "t-1",
            "s-1",
            json!({"text": "hybrid retrieval"}),
            0,
            0,
        )
        .await
        .unwrap();
        let box_ = reg.route_tenant("t-1");
        let built1 = box_
            .indexes_built(reg.kv(), reg.object_store(), &tenants, &sessions)
            .await
            .unwrap();
        assert_eq!(built1.as_ref().expect("built").indexed, 1);
        drop(built1);
        let built2 = box_
            .indexes_built(reg.kv(), reg.object_store(), &tenants, &sessions)
            .await
            .unwrap();
        assert_eq!(built2.as_ref().expect("built").indexed, 1);
    }

    #[test]
    fn box_has_segment_pin_registry() {
        let b = Box_::new(0, Arc::new(HlcClock::default()));
        b.segment_pins.pin("seg1");
        assert_eq!(b.segment_pins.pinned_len(), 1);
    }
}

#[cfg(test)]
mod admit_tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn make_box() -> Arc<Box_> {
        Arc::new(Box_::new(0, Arc::new(HlcClock::default())))
    }

    #[test]
    fn admit_unlimited_when_no_quota_set() {
        let b = make_box();
        for _ in 0..1000 {
            b.admit(42).expect("unlimited by default");
        }
    }

    #[test]
    fn admit_denies_per_box_when_bucket_drained() {
        let b = make_box();
        {
            let mut q = b.quota.write();
            q.max_qps = Some(5);
            q.burst = Some(5);
        }
        b.rebuild_buckets();
        for _ in 0..5 {
            b.admit(1).expect("first 5 admit");
        }
        let denied = b.admit(1).unwrap_err();
        assert_eq!(denied.scope, Scope::Box);
    }

    #[test]
    fn admit_denies_per_tenant_when_bucket_drained() {
        let b = make_box();
        {
            let mut q = b.quota.write();
            q.max_qps = Some(100); // per-box generous
            q.max_tenant_qps = Some(3);
            // Do NOT set burst, so per-tenant bucket defaults to burst=qps=3
        }
        b.rebuild_buckets();
        for _ in 0..3 {
            b.admit(7).expect("first 3 admit");
        }
        let denied = b.admit(7).unwrap_err();
        assert_eq!(denied.scope, Scope::Tenant);
    }

    #[test]
    fn admit_refills_after_wait() {
        let b = make_box();
        {
            let mut q = b.quota.write();
            q.max_qps = Some(100);
            q.burst = Some(1);
        }
        b.rebuild_buckets();
        b.admit(1).expect("first admit");
        assert!(b.admit(1).is_err());
        thread::sleep(Duration::from_millis(50));
        b.admit(1).expect("refilled after wait");
    }

    #[test]
    fn rebuild_buckets_resets_per_tenant_state() {
        let b = make_box();
        {
            let mut q = b.quota.write();
            q.max_tenant_qps = Some(1);
            q.burst = Some(1);
        }
        b.rebuild_buckets();
        b.admit(1).expect("first admit");
        assert!(b.admit(1).is_err());
        b.rebuild_buckets(); // new config → fresh buckets
        b.admit(1).expect("fresh bucket admits again");
    }

    #[test]
    fn admit_with_sentinel_zero_skips_per_tenant_gate() {
        let b = make_box();
        {
            let mut q = b.quota.write();
            q.max_qps = Some(1000);
            q.max_tenant_qps = Some(1);
            // Do NOT set burst, so per-tenant bucket defaults to burst=qps=1
        }
        b.rebuild_buckets();
        // Real tenant gets rate-limited after 1.
        b.admit(42).expect("real tenant: first admit");
        assert!(b.admit(42).is_err());
        // Sentinel 0 bypasses per-tenant; per-box has plenty left.
        for _ in 0..5 {
            b.admit(0).expect("sentinel bypasses per-tenant gate");
        }
    }

    #[test]
    fn quota_hit_counter_increments_on_denial() {
        let b = make_box();
        {
            let mut q = b.quota.write();
            q.max_qps = Some(1);
            q.burst = Some(1);
        }
        b.rebuild_buckets();
        b.admit(1).unwrap();
        let before = b.metrics.snapshot().quota_hits;
        let _ = b.admit(1);
        let after = b.metrics.snapshot().quota_hits;
        assert_eq!(after, before + 1);
    }
}
