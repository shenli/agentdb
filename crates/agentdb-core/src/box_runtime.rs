use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use agentdb_storage::{BoxId, KvStore};
use object_store::ObjectStore;
use parking_lot::RwLock;
use tokio::sync::{Mutex, Semaphore};

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

/// Default admission semaphore capacity when no override is supplied.
/// Matches the v2 design's "bounded tablet admission" recommendation
/// (§5.2) at the prototype-scale default; operators tune upward for
/// higher-concurrency workloads via `BoxRegistry::new_with_capacity`.
pub const DEFAULT_ADMISSION_CAPACITY: u32 = 64;

/// One logical box. Owns per-tenant write locks so writes to
/// different tenants on the same box run in parallel (M23), plus a
/// per-box admission semaphore that bounds in-flight write
/// concurrency so a burst across many tenants can't drive unbounded
/// IO / memory pressure.
pub struct Box_ {
    pub id: BoxId,
    /// Per-tenant write lock. Keyed by the external tenant id.
    /// Entries are created lazily on first acquire and retained for
    /// the box's lifetime — LRU eviction is a follow-up PR. Two
    /// callers targeting the same tenant share the `Arc<Mutex<()>>`
    /// and serialize; two callers targeting different tenants get
    /// distinct locks and proceed in parallel. Replaced the box-wide
    /// `write_lock: Mutex<()>` that serialized every tenant.
    tenant_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    /// Bounded-concurrency admission gate. Complements the QPS
    /// token bucket (`admit`): token bucket controls sustained rate,
    /// semaphore controls instantaneous concurrency. Write-path
    /// callers acquire a permit before proceeding; burst beyond
    /// capacity waits rather than rejects (waiting clients count
    /// under `admission_in_flight` and surface in
    /// `admission_waits_total`).
    admission: Arc<Semaphore>,
    admission_capacity: u32,
    indexes: tokio::sync::RwLock<Option<crate::indexes::PerBoxIndexes>>,
    pub metrics: Arc<BoxMetrics>,
    pub fault: RwLock<Option<InjectedFault>>,
    pub quota: RwLock<Quota>,
    pub webhook: RwLock<Option<String>>,
    pub segment_pins: Arc<SegmentPinRegistry>,
    box_bucket: parking_lot::Mutex<Option<TokenBucket>>,
    tenant_buckets: parking_lot::Mutex<HashMap<u64, TokenBucket>>,
    hlc: Arc<HlcClock>,
    /// Optional lease manager (M21). `None` when the registry was
    /// built without a `MetaStore`; in that mode the write path
    /// skips the lease check entirely — every pre-M21 test keeps
    /// working unchanged.
    lease: Option<Arc<crate::lease::LeaseManager>>,
}

impl Box_ {
    #[cfg(test)]
    fn new(id: BoxId, hlc: Arc<HlcClock>) -> Self {
        Self::new_with_lease_and_capacity(id, hlc, None, DEFAULT_ADMISSION_CAPACITY)
    }

    fn new_with_lease_and_capacity(
        id: BoxId,
        hlc: Arc<HlcClock>,
        lease: Option<Arc<crate::lease::LeaseManager>>,
        admission_capacity: u32,
    ) -> Self {
        let cap = admission_capacity.max(1);
        Self {
            id,
            tenant_locks: RwLock::new(HashMap::new()),
            admission: Arc::new(Semaphore::new(cap as usize)),
            admission_capacity: cap,
            indexes: tokio::sync::RwLock::new(None),
            metrics: Arc::new(BoxMetrics::new()),
            fault: RwLock::new(None),
            quota: RwLock::new(Quota::default()),
            webhook: RwLock::new(None),
            segment_pins: Arc::new(SegmentPinRegistry::new()),
            box_bucket: parking_lot::Mutex::new(None),
            tenant_buckets: parking_lot::Mutex::new(HashMap::new()),
            hlc,
            lease,
        }
    }

    /// Configured admission semaphore capacity. Operators need this
    /// to compute `admission_in_flight = capacity - available_permits`
    /// in external dashboards.
    pub fn admission_capacity(&self) -> u32 {
        self.admission_capacity
    }

    /// Snapshot of permits currently held — i.e. writes currently
    /// past the admission gate. Read by the metrics snapshotter to
    /// expose `admission_in_flight`; never acquire on this method's
    /// return value since it's just a counter read.
    pub fn admission_in_flight(&self) -> u32 {
        self.admission_capacity
            .saturating_sub(self.admission.available_permits() as u32)
    }

    /// Acquire one admission permit. Returns an owned permit whose
    /// Drop releases capacity back to the semaphore. Used by the
    /// write path; operators reading this code should note that
    /// admission is *blocking* (waits for capacity) rather than
    /// *rejecting* — the QPS token bucket via [`Box_::admit`] is the
    /// rejecting gate. The `admission_waits_total` counter bumps
    /// when the permit wasn't available immediately.
    pub async fn acquire_admission(&self) -> tokio::sync::OwnedSemaphorePermit {
        // Fast path: permit available right now.
        match self.admission.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_closed_or_full) => {
                // Either the semaphore is at capacity (common) or was
                // closed (impossible — we never close it). Count the
                // wait and fall through to the blocking path.
                self.metrics.record_admission_wait();
                self.admission
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("admission semaphore is never closed")
            }
        }
    }

    /// Get (or lazily create) the write lock for `tenant_external`.
    /// Two callers for the same tenant share the same `Arc<Mutex<()>>`
    /// and serialize; two callers for different tenants get distinct
    /// locks and run in parallel. Use with `lock_owned().await` to get
    /// an `OwnedMutexGuard<()>` suitable for passing to helper
    /// functions.
    pub fn tenant_write_lock(&self, tenant_external: &str) -> Arc<Mutex<()>> {
        // Fast path: lock already exists — upgrade-free read.
        if let Some(lock) = self.tenant_locks.read().get(tenant_external) {
            return lock.clone();
        }
        // Slow path: first caller for this tenant on this box. Take
        // the write lock and re-check to handle the race where two
        // threads both passed the read-side miss.
        let mut map = self.tenant_locks.write();
        map.entry(tenant_external.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Lease manager for this box, if one was configured at registry
    /// build time (see `BoxRegistry::new_with_meta`). Returns `None`
    /// when running without a control-plane connection.
    pub fn lease(&self) -> Option<&Arc<crate::lease::LeaseManager>> {
        self.lease.as_ref()
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
    /// new tenant, where calling `get_or_mint` before the tenant
    /// write lock would race). In that case the per-tenant gate is
    /// skipped; the per-box gate still runs.
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
                            .replay_events_after(kv, store, tenants, sessions, self.id, snap_hlc)
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
    /// Node id of the process hosting this registry. `None` under
    /// the legacy `new` / `new_with_hlc` / `new_with_capacity`
    /// paths that don't involve a control plane; `Some` when built
    /// via `new_with_meta*`. Surfaced in the `NodeDraining` error so
    /// clients know which specific process refused the write.
    node_id: Option<String>,
    /// Process-level drain flag (M22). Flipped via
    /// [`Self::drain`] from the `POST /api/admin/drain` admin
    /// endpoint. Once set, the write path returns
    /// [`Error::NodeDraining`] for every subsequent request. Reset
    /// only by restarting the process — un-draining via the API
    /// invites load-balancer state to drift from node state and has
    /// no clear use case.
    draining: Arc<std::sync::atomic::AtomicBool>,
    /// Wall-clock ms at which `drain()` was first called, or `None`
    /// before the first call. Captured in an `OnceLock` so repeated
    /// drain calls remain idempotent (second+ calls return the
    /// original `drained_at_ms` in admin responses).
    drained_at_ms: Arc<std::sync::OnceLock<i64>>,
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
    /// `HlcClock` per process; all boxes share it. Admission capacity
    /// defaults to [`DEFAULT_ADMISSION_CAPACITY`]; use
    /// [`Self::new_with_capacity`] to override.
    pub fn new_with_hlc(
        kv: Arc<dyn KvStore>,
        object_store: Arc<dyn ObjectStore>,
        num_boxes: u16,
        hlc: Arc<HlcClock>,
    ) -> Result<Self> {
        Self::new_with_capacity(kv, object_store, num_boxes, hlc, DEFAULT_ADMISSION_CAPACITY)
    }

    /// Build a registry with a caller-provided HLC and admission
    /// capacity. Every box in the registry is initialized with the
    /// same capacity (heterogeneous per-box caps aren't useful at
    /// prototype scale). `capacity` is clamped to `>= 1` — the server
    /// binary reads `AGENTDB_ADMISSION_CAPACITY` and passes it here.
    pub fn new_with_capacity(
        kv: Arc<dyn KvStore>,
        object_store: Arc<dyn ObjectStore>,
        num_boxes: u16,
        hlc: Arc<HlcClock>,
        capacity: u32,
    ) -> Result<Self> {
        if num_boxes == 0 {
            return Err(Error::Invalid {
                field: Some("num_boxes".into()),
                message: "num_boxes must be > 0".into(),
            });
        }
        let boxes = (0..num_boxes)
            .map(|i| {
                Arc::new(Box_::new_with_lease_and_capacity(
                    i,
                    hlc.clone(),
                    None,
                    capacity,
                ))
            })
            .collect();
        Ok(Self {
            boxes,
            kv,
            object_store,
            hlc,
            node_id: None,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drained_at_ms: Arc::new(std::sync::OnceLock::new()),
        })
    }

    /// Build a registry whose boxes each hold a lease acquired via the
    /// given `MetaStore` (M21). The node id identifies this process
    /// to the control plane — used as the `owner_node_id` on every
    /// `tablets::assume_ownership` call. Lease config is read from
    /// env via `LeaseConfig::from_env`.
    ///
    /// **Acquires the initial lease for each box before returning.**
    /// If any box fails to acquire (meta unreachable, epoch race
    /// with a peer), the error propagates — the caller should treat
    /// this as fatal for the process because the scheduler has
    /// presumably assigned this tablet elsewhere.
    ///
    /// **Does not start the periodic renewal task**; that's the
    /// caller's responsibility via `LeaseRenewalJob::start` per
    /// box. Separating spawn from acquire keeps tests able to
    /// observe the acquired state without a live renewal loop.
    pub async fn new_with_meta(
        kv: Arc<dyn KvStore>,
        object_store: Arc<dyn ObjectStore>,
        num_boxes: u16,
        hlc: Arc<HlcClock>,
        meta: Arc<agentdb_meta::MetaStore>,
        node_id: String,
    ) -> Result<Self> {
        Self::new_with_meta_and_capacity(
            kv,
            object_store,
            num_boxes,
            hlc,
            meta,
            node_id,
            DEFAULT_ADMISSION_CAPACITY,
        )
        .await
    }

    /// Like [`Self::new_with_meta`] but with an explicit admission
    /// capacity. The server binary uses this path so operators can
    /// tune `AGENTDB_ADMISSION_CAPACITY` alongside the control-plane
    /// envs.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_meta_and_capacity(
        kv: Arc<dyn KvStore>,
        object_store: Arc<dyn ObjectStore>,
        num_boxes: u16,
        hlc: Arc<HlcClock>,
        meta: Arc<agentdb_meta::MetaStore>,
        node_id: String,
        capacity: u32,
    ) -> Result<Self> {
        if num_boxes == 0 {
            return Err(Error::Invalid {
                field: Some("num_boxes".into()),
                message: "num_boxes must be > 0".into(),
            });
        }
        let config = crate::lease::LeaseConfig::from_env();
        let mut boxes: Vec<Arc<Box_>> = Vec::with_capacity(num_boxes as usize);
        for i in 0..num_boxes {
            // Metrics is created inside Box_::new; but LeaseManager::acquire
            // needs it before the Box_ exists. Build it here, attach to the
            // Box_ via new_with_lease_and_capacity. Same Arc shared with
            // the Box_'s BoxMetrics slot so counters increment as expected.
            let metrics = Arc::new(BoxMetrics::new());
            let lease = crate::lease::LeaseManager::acquire(
                meta.clone(),
                i as i64,
                node_id.clone(),
                config,
                metrics.clone(),
            )
            .await?;
            let mut b = Box_::new_with_lease_and_capacity(i, hlc.clone(), Some(lease), capacity);
            // Overwrite the Box_'s own metrics with the one the lease
            // is already wired to. (new_with_lease creates a fresh
            // BoxMetrics; replace with the lease-connected one.)
            b.metrics = metrics;
            boxes.push(Arc::new(b));
        }
        Ok(Self {
            boxes,
            kv,
            object_store,
            hlc,
            node_id: Some(node_id),
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drained_at_ms: Arc::new(std::sync::OnceLock::new()),
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

    /// Mark this process as draining. Subsequent writes fail fast with
    /// [`Error::NodeDraining`]. Idempotent — the first call records
    /// `now_ms` in an `OnceLock`, later calls leave the recorded time
    /// alone so admin views stay stable.
    ///
    /// There is no `undrain()` — operators restart the process to
    /// reverse a drain. Allowing un-drain invites load-balancer
    /// state to drift from node state and has no clear use case.
    pub fn drain(&self, now_ms: i64) {
        // Initialize `drained_at_ms` before flipping the flag so
        // concurrent readers that see `is_draining() == true` are
        // guaranteed to see a populated timestamp. The ordering
        // between these two stores doesn't need SeqCst because each
        // call is idempotent and the first-writer-wins semantics on
        // the OnceLock are atomic by construction.
        let _ = self.drained_at_ms.set(now_ms);
        self.draining
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Is this process currently draining? Atomic load; safe from
    /// any task. Hot-path callers (the write path) check this before
    /// any other work.
    pub fn is_draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Wall-clock ms of the first `drain()` call on this process, or
    /// `None` if drain has never been called. Admin handlers surface
    /// this so operators can verify drain has been in effect long
    /// enough for in-flight writes to have drained.
    pub fn drained_at_ms(&self) -> Option<i64> {
        self.drained_at_ms.get().copied()
    }

    /// Shared handle to the process-level drain flag. Cheap clone
    /// (just an Arc bump). Consumed by the lease-renewal loop so it
    /// can stop renewing when drain flips — without needing a
    /// back-reference to the registry itself. Any caller that wants
    /// to *react* to drain (rather than merely *read* its current
    /// value) should take this handle and poll it.
    pub fn draining_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.draining.clone()
    }

    /// Node id of the process hosting this registry, if it was built
    /// via `new_with_meta*`. Returns `None` for legacy-path registries
    /// (which don't have a control plane and so can't be drained).
    pub fn node_id(&self) -> Option<&str> {
        self.node_id.as_deref()
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
