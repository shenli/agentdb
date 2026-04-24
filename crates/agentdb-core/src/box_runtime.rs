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

/// Result of a write-path routing decision. `Local` is the
/// fast path — the caller hands off to an `Arc<Box_>` and runs
/// the append/fetch inline. `Remote` means the tablet is owned
/// by another node; the caller must forward the request via
/// HTTP rather than serving it locally.
#[derive(Clone)]
pub enum RouteDecision {
    /// Write this locally on the returned box.
    Local(Arc<Box_>),
    /// Forward the write to another node. `target` carries
    /// the node id + advertised address; `tablet_id` is the
    /// specific tablet the shard currently maps to (used for
    /// tracing and error reporting, not for the HTTP path).
    Remote {
        target: crate::forwarding::NodeTarget,
        tablet_id: i64,
    },
}

impl std::fmt::Debug for RouteDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteDecision::Local(b) => f.debug_tuple("Local").field(&b.id).finish(),
            RouteDecision::Remote { target, tablet_id } => f
                .debug_struct("Remote")
                .field("node_id", &target.node_id)
                .field("address", &target.address)
                .field("tablet_id", tablet_id)
                .finish(),
        }
    }
}

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
                let started = std::time::Instant::now();
                let permit = self
                    .admission
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("admission semaphore is never closed");
                // Record the wait latency so operators can
                // see p50/p95/p99 of how long the semaphore
                // was contended. Only records when we
                // actually waited — fast path is excluded
                // so the histogram reflects pressure, not
                // uncontended writes.
                let ns = started.elapsed().as_nanos() as u64;
                self.metrics.admission_wait_latency.record(ns);
                permit
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
///
/// The `boxes` vector is behind an `RwLock` so [`Self::add_box`] can
/// grow the registry at runtime. Hot-path readers take a read
/// guard, clone the `Arc<Box_>` they need, and drop the guard
/// immediately. Writes only happen when an operator calls the
/// dynamic-tablet admin endpoint — no contention in steady state.
pub struct BoxRegistry {
    boxes: RwLock<Vec<Arc<Box_>>>,
    kv: Arc<dyn KvStore>,
    object_store: Arc<dyn ObjectStore>,
    hlc: Arc<HlcClock>,
    /// Node id of the process hosting this registry. `None` under
    /// the legacy `new` / `new_with_hlc` / `new_with_capacity`
    /// paths that don't involve a control plane; `Some` when built
    /// via `new_with_meta*`. Surfaced in the `NodeDraining` error so
    /// clients know which specific process refused the write.
    node_id: Option<String>,
    /// Meta store + lease config + admission capacity stashed so
    /// `add_box` can acquire a lease without the caller threading
    /// them through. `None` on the legacy / test construction
    /// paths that never call `add_box`.
    meta: Option<Arc<agentdb_meta::MetaStore>>,
    lease_config: crate::lease::LeaseConfig,
    admission_capacity: u32,
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
    /// Shard→tablet lookup (M20). `None` on the legacy `new*`
    /// paths that don't attach a meta store; `Some` when the server
    /// binary calls [`Self::attach_location`] after building the
    /// proxy. When set, [`Self::route_session`] consults it for
    /// shard overrides; when unset, `route_session` degenerates to
    /// tenant-hash routing. Tests that don't care about shard
    /// overrides leave it `None`.
    location: std::sync::OnceLock<Arc<crate::shard::LocationProxy>>,
    /// Tablet→node address lookup (M20 multi-node). When set,
    /// [`Self::route_session_for_io`] returns
    /// [`RouteDecision::Remote`] for tablets owned by other
    /// nodes; when unset, every routed decision degrades to
    /// Local (the single-node path).
    node_addr: std::sync::OnceLock<Arc<crate::forwarding::NodeAddressProxy>>,
    /// Global per-tenant rate-limit registry. `None` on the
    /// legacy / test paths that don't attach one. When `Some`,
    /// [`crate::primitives::append_and_fetch_with_actor`]
    /// consults it before the per-box admit; denial surfaces as
    /// [`Error::TooManyRequests`] with
    /// [`Scope::TenantGlobal`](crate::admission::Scope::TenantGlobal).
    tenant_quotas: std::sync::OnceLock<Arc<crate::TenantQuotaRegistry>>,
    /// Slow-query ring buffer. `None` on the legacy / test
    /// paths; when `Some`, the write path and wire read
    /// handlers record completed requests whose latency
    /// exceeds the configured threshold.
    slow_queries: std::sync::OnceLock<Arc<crate::SlowQueryBuffer>>,
    /// Backup-scheduler status handle. `None` when no
    /// scheduler is configured. Attached once, at startup,
    /// right after spawning the scheduler loop.
    backup_status: std::sync::OnceLock<Arc<crate::BackupSchedulerHandle>>,
    /// Node-level wire-forwarding counters. Mongo wire
    /// requests that the local node forwards to a peer
    /// increment these — kept at the BoxRegistry level
    /// because wire forwarding is a node-scoped concept
    /// (per-box counters already cover the REST path).
    wire_forwarded_total: std::sync::atomic::AtomicU64,
    wire_forward_failures_total: std::sync::atomic::AtomicU64,
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
        let boxes: Vec<Arc<Box_>> = (0..num_boxes)
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
            boxes: RwLock::new(boxes),
            kv,
            object_store,
            hlc,
            node_id: None,
            meta: None,
            lease_config: crate::lease::LeaseConfig::default(),
            admission_capacity: capacity,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drained_at_ms: Arc::new(std::sync::OnceLock::new()),
            location: std::sync::OnceLock::new(),
            node_addr: std::sync::OnceLock::new(),
            tenant_quotas: std::sync::OnceLock::new(),
            slow_queries: std::sync::OnceLock::new(),
            backup_status: std::sync::OnceLock::new(),
            wire_forwarded_total: std::sync::atomic::AtomicU64::new(0),
            wire_forward_failures_total: std::sync::atomic::AtomicU64::new(0),
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
            boxes: RwLock::new(boxes),
            kv,
            object_store,
            hlc,
            node_id: Some(node_id),
            meta: Some(meta),
            lease_config: config,
            admission_capacity: capacity,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drained_at_ms: Arc::new(std::sync::OnceLock::new()),
            location: std::sync::OnceLock::new(),
            node_addr: std::sync::OnceLock::new(),
            tenant_quotas: std::sync::OnceLock::new(),
            slow_queries: std::sync::OnceLock::new(),
            backup_status: std::sync::OnceLock::new(),
            wire_forwarded_total: std::sync::atomic::AtomicU64::new(0),
            wire_forward_failures_total: std::sync::atomic::AtomicU64::new(0),
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
        self.boxes.read().len() as u16
    }

    /// Route a tenant external id to a box using FNV-1a hash mod num_boxes.
    pub fn route_tenant(&self, external: &str) -> Arc<Box_> {
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in external.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let boxes = self.boxes.read();
        let idx = (hash as usize) % boxes.len();
        boxes[idx].clone()
    }

    /// Route a (tenant, session) pair to a box. When the control
    /// plane has an explicit shard override (set via `POST
    /// /api/admin/shards/:id`) and this registry has a proxy
    /// attached via [`Self::attach_location`], the write lands on
    /// the box backing that override's tablet. Otherwise falls
    /// back to tenant-based hash routing — the pre-M20 behavior.
    ///
    /// **Fallback on unreachable tablet.** `LocationProxy` can
    /// legitimately return a `tablet_id` that this node doesn't own
    /// (shard pointed at a different node) or that's out of this
    /// node's box range. We return the tenant-hash box in both cases
    /// rather than panicking — writes still land *somewhere* durable
    /// even if the operator's override is stale or points at a
    /// remote tablet. Cross-node forwarding is future work; see
    /// `docs/superpowers/plans/2026-04-22-m20-write-path-m22-reassignment.md`.
    pub fn route_session(&self, tenant_external: &str, session_external: &str) -> Arc<Box_> {
        if let Some(location) = self.location.get() {
            if let Some(tablet_id) = location.tablet_for_session(session_external) {
                // Under M25 dynamic-tablet, a box's id equals
                // its tablet_id — so a simple scan of the
                // current snapshot finds the right one. The
                // linear scan is fine at prototype scale
                // (dozens of boxes); move to a HashMap if
                // callers ever grow it past hundreds.
                let boxes = self.boxes.read();
                if let Some(b) = boxes.iter().find(|b| b.id as i64 == tablet_id) {
                    return b.clone();
                }
            }
        }
        self.route_tenant(tenant_external)
    }

    /// Attach a [`LocationProxy`] so [`Self::route_session`] starts
    /// consulting it. Idempotent at the OnceLock level — calling
    /// twice returns `Err` on the second call. The server binary
    /// calls this exactly once, right after building the proxy.
    pub fn attach_location(
        &self,
        location: Arc<crate::shard::LocationProxy>,
    ) -> std::result::Result<(), Arc<crate::shard::LocationProxy>> {
        self.location.set(location)
    }

    /// Currently-attached location proxy, if any. Exposed for the
    /// reaper + admin endpoints that need to force a refresh after
    /// writing an override.
    pub fn location(&self) -> Option<Arc<crate::shard::LocationProxy>> {
        self.location.get().cloned()
    }

    /// Attach the global per-tenant rate-limit registry so the
    /// write path enforces the quotas configured in the meta
    /// store's `tenant_quotas` table. Idempotent at the
    /// OnceLock level — second call returns the input back to
    /// the caller. The server binary calls this exactly once
    /// after hydrating the registry from meta.
    pub fn attach_tenant_quotas(
        &self,
        quotas: Arc<crate::TenantQuotaRegistry>,
    ) -> std::result::Result<(), Arc<crate::TenantQuotaRegistry>> {
        self.tenant_quotas.set(quotas)
    }

    /// Currently-attached global per-tenant rate-limit registry,
    /// if any. `None` means "no enforcement" — tests and legacy
    /// construction paths operate this way.
    pub fn tenant_quotas(&self) -> Option<Arc<crate::TenantQuotaRegistry>> {
        self.tenant_quotas.get().cloned()
    }

    /// Attach the slow-query ring buffer. Idempotent at the
    /// OnceLock level. The server binary calls this once
    /// after constructing the buffer from env vars; tests
    /// can attach a smaller buffer to exercise eviction.
    pub fn attach_slow_queries(
        &self,
        buffer: Arc<crate::SlowQueryBuffer>,
    ) -> std::result::Result<(), Arc<crate::SlowQueryBuffer>> {
        self.slow_queries.set(buffer)
    }

    /// Currently-attached slow-query buffer, if any.
    /// `None` means "no recording" — every `maybe_record` is
    /// a no-op.
    pub fn slow_queries(&self) -> Option<Arc<crate::SlowQueryBuffer>> {
        self.slow_queries.get().cloned()
    }

    /// Attach the backup-scheduler status handle so admin
    /// endpoints + `/metrics` can read recent activity.
    /// Idempotent at the OnceLock level.
    pub fn attach_backup_status(
        &self,
        handle: Arc<crate::BackupSchedulerHandle>,
    ) -> std::result::Result<(), Arc<crate::BackupSchedulerHandle>> {
        self.backup_status.set(handle)
    }

    /// Currently-attached backup-scheduler status handle,
    /// if any. `None` means "no scheduled backups configured
    /// on this process."
    pub fn backup_status(&self) -> Option<Arc<crate::BackupSchedulerHandle>> {
        self.backup_status.get().cloned()
    }

    /// Increment the node-level successful-wire-forward
    /// counter. Exposed on `/metrics` as
    /// `agentdb_wire_forwarded_total`.
    pub fn record_wire_forwarded(&self) {
        self.wire_forwarded_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increment the failed-wire-forward counter.
    pub fn record_wire_forward_failure(&self) {
        self.wire_forward_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn wire_forwarded_total(&self) -> u64 {
        self.wire_forwarded_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn wire_forward_failures_total(&self) -> u64 {
        self.wire_forward_failures_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Attach a [`NodeAddressProxy`] so
    /// [`Self::route_session_for_io`] can differentiate
    /// local-tablet vs. remote-tablet targets. Idempotent at
    /// the OnceLock level.
    pub fn attach_node_address(
        &self,
        node_addr: Arc<crate::forwarding::NodeAddressProxy>,
    ) -> std::result::Result<(), Arc<crate::forwarding::NodeAddressProxy>> {
        self.node_addr.set(node_addr)
    }

    pub fn node_address_proxy(&self) -> Option<Arc<crate::forwarding::NodeAddressProxy>> {
        self.node_addr.get().cloned()
    }

    /// Route a (tenant, session) write with multi-node awareness.
    ///
    /// - **No override / no location proxy** → `Local(route_tenant(tenant))`.
    ///   Matches pre-M20 behavior.
    /// - **Override points at a tablet this node owns locally** →
    ///   `Local(boxes[tablet_id])`.
    /// - **Override points at a tablet owned by another live
    ///   node** (requires `attach_node_address`) →
    ///   `Remote { target, tablet_id }`.
    /// - **Override points at a tablet with no resolvable owner
    ///   (unowned / just reaped / owner absent from nodes
    ///   table)** → `Local(route_tenant(tenant))` + warn.
    ///
    /// The "resolvable" case trumps local box-index check —
    /// when the node-address proxy knows the tablet is owned
    /// by a *different* node, we forward even if the
    /// `tablet_id` happens to be in `0..num_boxes` (which is
    /// the normal case in single-node clusters where
    /// tablet_id == box_id and the prefix happens to match
    /// the peer's too).
    pub fn route_session_for_io(
        &self,
        tenant_external: &str,
        session_external: &str,
    ) -> RouteDecision {
        let Some(location) = self.location.get() else {
            return RouteDecision::Local(self.route_tenant(tenant_external));
        };
        let Some(tablet_id) = location.tablet_for_session(session_external) else {
            return RouteDecision::Local(self.route_tenant(tenant_external));
        };

        // If we have a node-address proxy, consult it first.
        // The proxy's answer is authoritative when present:
        // it tells us which node currently owns the tablet,
        // which is exactly what a multi-node write needs.
        if let Some(addr_proxy) = self.node_addr.get() {
            if let Some(target) = addr_proxy.target_for_tablet(tablet_id) {
                if addr_proxy.is_self(&target) {
                    // This node owns the tablet. Fall through
                    // to the local box lookup below.
                } else {
                    return RouteDecision::Remote { target, tablet_id };
                }
            } else {
                // Tablet has no resolvable owner — unowned,
                // reaped, or address unknown. Safety fallback.
                tracing::warn!(
                    tablet_id,
                    session = session_external,
                    "shard override points at tablet with no resolvable owner; falling back to tenant-hash"
                );
                return RouteDecision::Local(self.route_tenant(tenant_external));
            }
        }

        // No address proxy attached (single-node path), or
        // proxy confirmed we own this tablet. Resolve to a
        // local box via id scan (see note on route_session).
        let boxes = self.boxes.read();
        if let Some(b) = boxes.iter().find(|b| b.id as i64 == tablet_id) {
            RouteDecision::Local(b.clone())
        } else {
            drop(boxes);
            // Override points at a tablet we don't host and
            // no address proxy told us where it is. Fall back
            // for safety.
            RouteDecision::Local(self.route_tenant(tenant_external))
        }
    }

    /// Look up an `Arc<Box_>` by index. Used by the server binary to start
    /// one `LifecycleEngine` per box at process startup.
    pub fn box_by_id(&self, id: BoxId) -> Option<Arc<Box_>> {
        // Scan: the vector is typically dozens of entries,
        // and box_id == tablet_id but isn't the same as the
        // vector index once runtime-added boxes exist (they
        // get ids past the initial 0..num_boxes range).
        let boxes = self.boxes.read();
        boxes.iter().find(|b| b.id == id).cloned()
    }

    /// Snapshot of all boxes in this registry. Returns a
    /// cloned `Vec<Arc<Box_>>` — cheap because boxes are
    /// Arc'd; callers can iterate without holding the lock
    /// (important for anything that awaits inside the
    /// iteration). The snapshot is a point-in-time view: if
    /// [`Self::add_box`] fires mid-iteration, the new box
    /// is missed and picked up on the next call.
    pub fn boxes_snapshot(&self) -> Vec<Arc<Box_>> {
        self.boxes.read().clone()
    }

    /// Dynamically grow the registry with a new tablet/box at
    /// runtime (M25 continuation).
    ///
    /// Only available on meta-backed registries built via
    /// `new_with_meta*` — legacy paths lack the lease
    /// acquisition plumbing and return `Error::Invalid`.
    ///
    /// Atomic at the visibility level: the new `Arc<Box_>`
    /// isn't exposed via `box_by_id` or `num_boxes` until
    /// after the meta row is created and the lease is
    /// acquired. A caller who sees the tablet in
    /// `/api/admin/tablets` can rely on the lease already
    /// being live.
    pub async fn add_box(&self, tablet_id: BoxId) -> Result<Arc<Box_>> {
        let Some(meta) = self.meta.clone() else {
            return Err(Error::Invalid {
                field: None,
                message: "cannot add_box on legacy (meta-less) registry".into(),
            });
        };
        let Some(node_id) = self.node_id.clone() else {
            return Err(Error::Invalid {
                field: None,
                message: "cannot add_box: node_id not configured".into(),
            });
        };

        // Reject duplicates with a clear error.
        {
            let boxes = self.boxes.read();
            if boxes.iter().any(|b| b.id == tablet_id) {
                return Err(Error::Invalid {
                    field: Some("tablet_id".into()),
                    message: format!("tablet {tablet_id} already exists on this node"),
                });
            }
        }

        // Create the meta row (idempotent) and acquire the lease.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        agentdb_meta::tablets::get_or_create(meta.pool(), tablet_id as i64, now)
            .await
            .map_err(|e| Error::Invalid {
                field: None,
                message: format!("add_box: meta tablets::get_or_create: {e}"),
            })?;
        let metrics = Arc::new(BoxMetrics::new());
        let lease = crate::lease::LeaseManager::acquire(
            meta.clone(),
            tablet_id as i64,
            node_id,
            self.lease_config,
            metrics.clone(),
        )
        .await?;
        let mut b = Box_::new_with_lease_and_capacity(
            tablet_id,
            self.hlc.clone(),
            Some(lease),
            self.admission_capacity,
        );
        b.metrics = metrics;
        let arc = Arc::new(b);
        // Publish under the write lock. Hot-path readers that
        // held a read lock during scan will drop it and pick
        // up the new box on their next iteration.
        self.boxes.write().push(arc.clone());
        Ok(arc)
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

    // M20 route_session coverage. LocationProxy needs a meta
    // pool; build it with migrations + seed so route_session can
    // be exercised end-to-end without spinning up the full
    // registry-with-meta code path.
    async fn location_with(
        session: &str,
        tablet_override: Option<i64>,
    ) -> Arc<crate::shard::LocationProxy> {
        use agentdb_meta::{migrations, shards as meta_shards, MetaStore};
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        migrations::apply(meta.pool()).await.unwrap();
        meta_shards::seed_shards(meta.pool(), crate::shard::SHARD_COUNT as i64, 0)
            .await
            .unwrap();
        if let Some(t) = tablet_override {
            let sid = crate::shard::shard_id(session) as i64;
            meta_shards::set_assignment(meta.pool(), sid, Some(t), 0)
                .await
                .unwrap();
        }
        crate::shard::LocationProxy::new(meta).await.unwrap()
    }

    #[tokio::test]
    async fn route_session_with_no_proxy_falls_back_to_tenant() {
        let (_dir, _obj_dir, reg) = registry(4);
        // No attach_location call — route_session must behave like route_tenant.
        let routed = reg.route_session("t-1", "s-1").id;
        let fallback = reg.route_tenant("t-1").id;
        assert_eq!(routed, fallback);
    }

    #[tokio::test]
    async fn route_session_falls_back_to_tenant_when_no_override() {
        let (_dir, _obj_dir, reg) = registry(4);
        let loc = location_with("s-1", None).await;
        reg.attach_location(loc).ok();
        let routed = reg.route_session("t-1", "s-1").id;
        let fallback = reg.route_tenant("t-1").id;
        assert_eq!(routed, fallback);
    }

    #[tokio::test]
    async fn route_session_uses_override_when_set() {
        let (_dir, _obj_dir, reg) = registry(4);
        let fallback = reg.route_tenant("t-1").id;
        // Pick a target that differs from the fallback.
        let target = ((fallback as i64) + 1) % 4;
        let loc = location_with("s-1", Some(target)).await;
        reg.attach_location(loc).ok();
        let routed = reg.route_session("t-1", "s-1").id;
        assert_eq!(routed as i64, target);
        assert_ne!(
            routed, fallback,
            "override must redirect away from hash target"
        );
    }

    #[tokio::test]
    async fn route_session_falls_back_on_out_of_range_tablet() {
        let (_dir, _obj_dir, reg) = registry(4);
        // Tablet 99 is outside 0..4 — a cross-node or stale override.
        let loc = location_with("s-1", Some(99)).await;
        reg.attach_location(loc).ok();
        let routed = reg.route_session("t-1", "s-1").id;
        let fallback = reg.route_tenant("t-1").id;
        assert_eq!(
            routed, fallback,
            "out-of-range tablet must fall back to tenant hash"
        );
    }

    // M20 multi-node: route_session_for_io coverage.
    // Builds a meta store with two nodes registered, assigns
    // tablets explicitly to one or the other, and verifies
    // the RouteDecision the registry returns.
    async fn meta_with_nodes_and_tablets(
        self_tablets: &[i64],
        other_node_tablets: &[i64],
    ) -> Arc<agentdb_meta::MetaStore> {
        use agentdb_meta::{migrations, nodes as meta_nodes, tablets as meta_tablets, MetaStore};
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        migrations::apply(meta.pool()).await.unwrap();
        meta_nodes::upsert(meta.pool(), "self", "127.0.0.1:7000", None, "alive", 100)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "other", "127.0.0.1:7001", None, "alive", 100)
            .await
            .unwrap();
        for &t in self_tablets {
            meta_tablets::get_or_create(meta.pool(), t, 100).await.unwrap();
            meta_tablets::assume_ownership(meta.pool(), t, 0, "self", i64::MAX)
                .await
                .unwrap();
        }
        for &t in other_node_tablets {
            meta_tablets::get_or_create(meta.pool(), t, 100).await.unwrap();
            meta_tablets::assume_ownership(meta.pool(), t, 0, "other", i64::MAX)
                .await
                .unwrap();
        }
        Arc::new(meta)
    }

    #[tokio::test]
    async fn route_for_write_without_proxies_is_local_tenant_hash() {
        let (_dir, _obj_dir, reg) = registry(4);
        match reg.route_session_for_io("t-1", "s-1") {
            RouteDecision::Local(box_) => {
                assert_eq!(box_.id, reg.route_tenant("t-1").id);
            }
            RouteDecision::Remote { .. } => panic!("expected Local without proxies"),
        }
    }

    #[tokio::test]
    async fn route_for_write_locally_owned_tablet_is_local() {
        use agentdb_meta::shards as meta_shards;
        let (_dir, _obj_dir, reg) = registry(4);
        let meta = meta_with_nodes_and_tablets(&[0, 1, 2, 3], &[]).await;
        meta_shards::seed_shards(meta.pool(), crate::shard::SHARD_COUNT as i64, 0)
            .await
            .unwrap();
        let sid = crate::shard::shard_id("s-1") as i64;
        // Point the shard at tablet 2 (locally owned).
        meta_shards::set_assignment(meta.pool(), sid, Some(2), 0)
            .await
            .unwrap();
        let loc = crate::shard::LocationProxy::new(meta.clone())
            .await
            .unwrap();
        reg.attach_location(loc).ok();
        let addr_proxy = crate::forwarding::NodeAddressProxy::new(meta, "self".into())
            .await
            .unwrap();
        reg.attach_node_address(addr_proxy).ok();

        match reg.route_session_for_io("t-1", "s-1") {
            RouteDecision::Local(box_) => assert_eq!(box_.id, 2),
            RouteDecision::Remote { .. } => panic!("expected Local for self-owned tablet"),
        }
    }

    #[tokio::test]
    async fn route_for_write_remote_tablet_is_remote() {
        use agentdb_meta::shards as meta_shards;
        let (_dir, _obj_dir, reg) = registry(4);
        let meta = meta_with_nodes_and_tablets(&[0, 1], &[2, 3]).await;
        meta_shards::seed_shards(meta.pool(), crate::shard::SHARD_COUNT as i64, 0)
            .await
            .unwrap();
        let sid = crate::shard::shard_id("s-remote") as i64;
        meta_shards::set_assignment(meta.pool(), sid, Some(3), 0)
            .await
            .unwrap();
        let loc = crate::shard::LocationProxy::new(meta.clone())
            .await
            .unwrap();
        reg.attach_location(loc).ok();
        let addr_proxy = crate::forwarding::NodeAddressProxy::new(meta, "self".into())
            .await
            .unwrap();
        reg.attach_node_address(addr_proxy).ok();

        match reg.route_session_for_io("t-1", "s-remote") {
            RouteDecision::Local(_) => panic!("expected Remote for peer-owned tablet"),
            RouteDecision::Remote { target, tablet_id } => {
                assert_eq!(target.node_id, "other");
                assert_eq!(target.address, "127.0.0.1:7001");
                assert_eq!(tablet_id, 3);
            }
        }
    }

    #[tokio::test]
    async fn route_for_write_unresolvable_owner_falls_back_to_local() {
        use agentdb_meta::{migrations, nodes as meta_nodes, shards as meta_shards, tablets as meta_tablets, MetaStore};
        let (_dir, _obj_dir, reg) = registry(4);
        let meta = Arc::new(MetaStore::open_sqlite(":memory:").await.unwrap());
        migrations::apply(meta.pool()).await.unwrap();
        meta_nodes::upsert(meta.pool(), "self", "127.0.0.1:7000", None, "alive", 100)
            .await
            .unwrap();
        // tablet 5 claims a ghost owner that isn't in the nodes table.
        meta_tablets::get_or_create(meta.pool(), 5, 100).await.unwrap();
        meta_tablets::assume_ownership(meta.pool(), 5, 0, "ghost", i64::MAX)
            .await
            .unwrap();
        meta_shards::seed_shards(meta.pool(), crate::shard::SHARD_COUNT as i64, 0)
            .await
            .unwrap();
        let sid = crate::shard::shard_id("s-orphan") as i64;
        meta_shards::set_assignment(meta.pool(), sid, Some(5), 0)
            .await
            .unwrap();
        let loc = crate::shard::LocationProxy::new(meta.clone())
            .await
            .unwrap();
        reg.attach_location(loc).ok();
        let addr_proxy = crate::forwarding::NodeAddressProxy::new(meta, "self".into())
            .await
            .unwrap();
        reg.attach_node_address(addr_proxy).ok();

        match reg.route_session_for_io("t-1", "s-orphan") {
            RouteDecision::Local(_) => {} // expected — fallback
            RouteDecision::Remote { .. } => panic!("unresolvable owner must not forward"),
        }
    }

    /// Helper for dynamic-tablet (add_box) tests: build a
    /// meta-backed registry with capacity=1 so lease
    /// acquisition works.
    async fn meta_registry(num_boxes: u16) -> (TempDir, TempDir, tempfile::NamedTempFile, Arc<BoxRegistry>) {
        let meta_file = tempfile::NamedTempFile::new().unwrap();
        let meta_path = meta_file.path().to_string_lossy().to_string();
        let meta = Arc::new(agentdb_meta::MetaStore::open_sqlite(&meta_path).await.unwrap());
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(
            BoxRegistry::new_with_meta(
                kv,
                store,
                num_boxes,
                Arc::new(HlcClock::default()),
                meta,
                "add-box-test".into(),
            )
            .await
            .unwrap(),
        );
        (dir, obj_dir, meta_file, reg)
    }

    #[tokio::test]
    async fn add_box_grows_the_registry_and_exposes_new_box() {
        let (_d, _o, _m, reg) = meta_registry(2).await;
        assert_eq!(reg.num_boxes(), 2);
        let new_box = reg.add_box(2).await.unwrap();
        assert_eq!(new_box.id, 2);
        assert_eq!(reg.num_boxes(), 3);
        // box_by_id finds the new box by id.
        let via_lookup = reg.box_by_id(2).expect("box 2 should be reachable");
        assert!(Arc::ptr_eq(&new_box, &via_lookup));
    }

    #[tokio::test]
    async fn add_box_rejects_duplicate_tablet_id() {
        let (_d, _o, _m, reg) = meta_registry(2).await;
        let result = reg.add_box(1).await;
        match result {
            Err(Error::Invalid { field, .. }) => {
                assert_eq!(field.as_deref(), Some("tablet_id"));
            }
            Ok(_) => panic!("duplicate add_box should fail"),
            Err(other) => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn add_box_errors_on_legacy_registry() {
        let (_dir, _obj_dir, reg) = registry(2);
        let result = reg.add_box(2).await;
        assert!(
            matches!(result, Err(Error::Invalid { .. })),
            "legacy registry must reject add_box"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn add_box_survives_route_tenant_under_concurrent_readers() {
        // Hammer route_tenant while add_box grows the vec,
        // to catch any RwLock misuse or ABA-style bug. The
        // number of boxes grows from 2 → 6 under a load of
        // route_tenant calls. Uses multi_thread flavor so
        // the reader doesn't starve the add_box futures; on
        // current_thread the tight loop below would monopolize
        // the single worker.
        let (_d, _o, _m, reg) = meta_registry(2).await;
        let reg2 = reg.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = stop.clone();
        let reader = tokio::spawn(async move {
            let mut hits = 0u64;
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                let b = reg2.route_tenant("t-concurrent");
                assert!(b.id < 64, "box id stays bounded");
                hits += 1;
                if hits % 1000 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            hits
        });
        for i in 2..6 {
            let _ = reg.add_box(i).await.unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let hits = reader.await.unwrap();
        assert!(hits > 100, "reader should have run many loops");
        assert_eq!(reg.num_boxes(), 6);
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
