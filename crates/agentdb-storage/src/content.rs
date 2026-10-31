//! Artifact content backend — abstraction over where artifact bytes live.
//!
//! The runtime metadata/event store (`KvStore`) holds artifact metadata; the
//! `ContentBackend` holds the bytes. V2.1 ships two impls:
//!
//! - [`LocalFsContentBackend`]: writes blobs to a directory rooted on the
//!   local filesystem under `artifact_content/{tenant_id}/{artifact_id}/blob`.
//!   Used by the persistent runtime and exercised by the restart tests.
//! - [`InMemoryContentBackend`]: holds blobs in a process-local map. Used by
//!   `Runtime::in_memory()` and unit tests.
//! - [`ObjectStoreContentBackend`]: stores blobs in any `object_store`
//!   backend. S3 is the primary target today; the same implementation also
//!   works with local/object-store adapters used in tests.
//!
//! ## Atomicity
//!
//! `LocalFsContentBackend::put` writes to a tmp path under the same parent
//! directory and renames into place. The rename is atomic on POSIX, so a
//! reader either sees the old (or absent) blob or the new one — never a
//! partial write. The tmp file is fsynced before the rename so a crash
//! between rename and the next write doesn't leave an unflushed blob.
//!
//! The parent directory is fsynced after rename so a successful `put` also
//! makes the new directory entry durable across power loss.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

/// Result of a successful put: the byte count written and the sha256 we
/// computed while writing. Callers compare `sha256_hex` against any
/// caller-provided expectation; the metadata layer stores it on the
/// `Artifact` record so future reads can verify integrity.
#[derive(Debug, Clone)]
pub struct PutResult {
    pub bytes: u64,
    pub sha256_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentObject {
    pub tenant_id: String,
    pub artifact_id: String,
    /// Last-modified time of the stored object, when the backend can supply
    /// it cheaply. Drives managed-cache eviction in the async mirror.
    pub last_modified_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentListing {
    pub objects: Vec<ContentObject>,
    pub temporary_objects_deleted: usize,
}

/// Abstraction over where artifact bytes live.
///
/// All methods are tenant- and artifact-scoped. The trait does not surface a
/// concept of "current tenant" — every call passes both ids — so the runtime
/// can multiplex many tenants over one backend without per-tenant state.
#[async_trait]
pub trait ContentBackend: Send + Sync {
    /// Atomically write `bytes` at the logical path `tenant_id/artifact_id`,
    /// returning the byte count and the sha256 hex digest computed while
    /// writing. Implementations MUST guarantee that on a successful return
    /// the bytes are durable (or the in-memory equivalent) and a concurrent
    /// reader sees either the new content or no content at that path.
    async fn put(&self, tenant_id: &str, artifact_id: &str, bytes: &[u8]) -> io::Result<PutResult>;

    /// Read the entire blob into memory. V2.1 does not stream; large blobs
    /// are memory-bound. TODO(V2.4): expose a streaming reader.
    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>>;

    /// Existence check. Returns `Ok(false)` if the blob is absent;
    /// propagates other I/O errors.
    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool>;

    /// Best-effort delete. Removing a missing blob is NOT an error — the
    /// V2.4 GC reaper relies on idempotent delete to clean up orphans.
    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()>;

    /// Best-effort "this object was just used" hint so managed caches can
    /// keep recently read objects local. Backends without a cheap way to
    /// record it treat it as a no-op.
    async fn touch(&self, _tenant_id: &str, _artifact_id: &str) -> io::Result<()> {
        Ok(())
    }

    /// Remove crash-left temporary files (and empty artifact directories)
    /// from the store. DESTRUCTIVE toward in-flight writes: a live put's
    /// tmp file is indistinguishable from a crash-left one, so call this
    /// only before the backend accepts writes (startup reconciliation),
    /// never from a periodic task. Returns the number of temporaries removed.
    async fn sweep_temporary_objects(&self, _limit: usize) -> io::Result<usize> {
        Ok(0)
    }

    /// Enumerate stored blobs. Read-only — safe to run concurrently with
    /// writes (the periodic cache sweep depends on this). Backends that
    /// cannot provide a bounded namespace listing return `Unsupported`.
    async fn list_objects(&self, _limit: usize) -> io::Result<ContentListing> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "content backend does not support object listing",
        ))
    }
}

// ----- AsyncMirrorContentBackend ---------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AsyncMirrorStats {
    pub queued: u64,
    pub completed: u64,
    pub failed: u64,
    /// Pending mirror jobs (count).
    pub backlog: u64,
    /// Pending mirror payload (bytes) — the debt that grows when the mirror
    /// (object store) drains slower than writes arrive. This is the signal
    /// backpressure gates on.
    pub backlog_bytes: u64,
    /// Configured byte ceiling; `Some(0)`/`None` means unbounded.
    pub max_backlog_bytes: Option<u64>,
    /// Whether the backlog is currently over the ceiling (writes throttled).
    pub over_limit: bool,
    /// Tenant currently holding the most pending bytes, and how many — the
    /// "responsible tenant" a throttle is attributed to.
    pub top_tenant: Option<String>,
    pub top_tenant_backlog_bytes: u64,
    /// ms since a mirror job last completed; large + non-empty backlog means
    /// the resource has stopped draining.
    pub ms_since_last_drain: Option<i64>,
    pub evicted: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MirrorCacheSweepReport {
    pub scanned: u64,
    pub evicted: u64,
    pub retained_unmirrored: u64,
    pub skipped_no_mtime: u64,
    /// Un-mirrored local objects re-queued for upload this pass (a subset of
    /// `retained_unmirrored`; the rest were left for a later pass because the
    /// backlog ceiling was reached or a read failed).
    pub requeued: u64,
    pub requeued_bytes: u64,
}

/// Result of a re-drive pass over un-mirrored local objects
/// ([`AsyncMirrorContentBackend::reconcile_unmirrored`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MirrorReconcileReport {
    pub scanned: u64,
    pub already_mirrored: u64,
    pub requeued: u64,
    pub requeued_bytes: u64,
    /// Objects left for a later pass because the backlog ceiling was reached.
    pub throttled: u64,
    /// Objects whose mirror-head or primary read failed; retained for retry.
    pub errors: u64,
}

/// Outcome of attempting to re-queue one un-mirrored local object.
enum RequeueOutcome {
    /// Re-queued for upload; carries the payload byte count.
    Requeued(u64),
    /// The backlog ceiling was reached; left for a later pass.
    Throttled,
    /// The primary read or enqueue failed; the local copy is retained.
    Error,
}

#[derive(Default)]
struct AsyncMirrorCounters {
    queued: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    backlog: AtomicU64,
    backlog_bytes: AtomicU64,
    /// Wall-clock ms a mirror job last completed (drained). 0 = never.
    last_drain_ms: AtomicI64,
    evicted: AtomicU64,
}

#[derive(Clone)]
enum MirrorJob {
    Put {
        tenant_id: String,
        artifact_id: String,
        bytes: Vec<u8>,
    },
    Delete {
        tenant_id: String,
        artifact_id: String,
    },
}

impl MirrorJob {
    /// Pending payload this job represents. Deletes carry no payload; the
    /// count backlog still bounds them.
    fn byte_len(&self) -> u64 {
        match self {
            MirrorJob::Put { bytes, .. } => bytes.len() as u64,
            MirrorJob::Delete { .. } => 0,
        }
    }

    fn tenant_id(&self) -> &str {
        match self {
            MirrorJob::Put { tenant_id, .. } | MirrorJob::Delete { tenant_id, .. } => tenant_id,
        }
    }
}

fn now_ms_wall() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// Typed error the async mirror returns when its byte backlog is over the
/// configured ceiling, so the write path can map it to a retryable throttle
/// (HTTP 429 + Retry-After) rather than blocking the hot path on slow
/// object storage. The API layer recognizes this marker.
pub const MIRROR_BACKPRESSURE_MARKER: &str = "MIRROR_UPLOAD_BACKPRESSURE";

/// Stages content in a primary backend and mirrors to another backend
/// asynchronously.
///
/// This is the storage primitive behind the "do not block hot writes on S3"
/// design: configure the primary as local durable storage and the mirror as
/// object storage. `put` returns after the primary write succeeds and the mirror
/// job is queued.
pub struct AsyncMirrorContentBackend {
    primary: Arc<dyn ContentBackend>,
    mirror: Arc<dyn ContentBackend>,
    sender: mpsc::Sender<MirrorJob>,
    counters: Arc<AsyncMirrorCounters>,
    /// Per-tenant pending mirror bytes, for attributing backpressure to the
    /// tenant producing the debt.
    per_tenant_backlog: Arc<Mutex<HashMap<String, u64>>>,
    /// Byte ceiling on the pending backlog. When exceeded, `put`/`delete`
    /// return `MIRROR_BACKPRESSURE_MARKER` instead of enqueuing, throttling
    /// the writer. `None` = unbounded (blocking-on-full channel only).
    max_backlog_bytes: Option<u64>,
    /// When set, the primary is a managed cache over the mirror: local
    /// copies older than this TTL whose mirror copy exists are evicted by
    /// the cache sweep, and local read hits refresh the object's age.
    local_cache_ttl_ms: Option<u64>,
}

impl AsyncMirrorContentBackend {
    pub fn new(
        primary: Arc<dyn ContentBackend>,
        mirror: Arc<dyn ContentBackend>,
        queue_capacity: usize,
    ) -> Self {
        Self::new_with_retries(primary, mirror, queue_capacity, 3)
    }

    pub fn new_with_retries(
        primary: Arc<dyn ContentBackend>,
        mirror: Arc<dyn ContentBackend>,
        queue_capacity: usize,
        max_attempts: usize,
    ) -> Self {
        let capacity = queue_capacity.max(1);
        let max_attempts = max_attempts.max(1);
        let (sender, mut receiver) = mpsc::channel::<MirrorJob>(capacity);
        let counters = Arc::new(AsyncMirrorCounters::default());
        let worker_counters = counters.clone();
        let worker_mirror = mirror.clone();
        let per_tenant_backlog: Arc<Mutex<HashMap<String, u64>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let worker_per_tenant = per_tenant_backlog.clone();
        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                let job_bytes = job.byte_len();
                let job_tenant = job.tenant_id().to_string();
                let result =
                    execute_mirror_job_with_retry(worker_mirror.as_ref(), &job, max_attempts).await;
                worker_counters.backlog.fetch_sub(1, Ordering::Relaxed);
                worker_counters
                    .backlog_bytes
                    .fetch_sub(job_bytes, Ordering::Relaxed);
                worker_counters
                    .last_drain_ms
                    .store(now_ms_wall(), Ordering::Relaxed);
                if job_bytes > 0 {
                    if let Ok(mut map) = worker_per_tenant.lock() {
                        if let Some(v) = map.get_mut(&job_tenant) {
                            *v = v.saturating_sub(job_bytes);
                            if *v == 0 {
                                map.remove(&job_tenant);
                            }
                        }
                    }
                }
                match result {
                    Ok(()) => {
                        worker_counters.completed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        worker_counters.failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(?err, "async content mirror job failed");
                    }
                }
            }
        });
        Self {
            primary,
            mirror,
            sender,
            counters,
            per_tenant_backlog,
            max_backlog_bytes: None,
            local_cache_ttl_ms: None,
        }
    }

    /// Bound the pending mirror backlog to `max_bytes`. Beyond it, `put` and
    /// `delete` throttle the writer (`MIRROR_BACKPRESSURE_MARKER`) instead of
    /// enqueuing, so a slow object store cannot grow the backlog without
    /// bound. `0` disables the ceiling.
    pub fn with_max_backlog_bytes(mut self, max_bytes: u64) -> Self {
        self.max_backlog_bytes = (max_bytes > 0).then_some(max_bytes);
        self
    }

    /// Turn the primary into a managed cache: local objects unread for
    /// `ttl_ms` are evicted once the mirror holds them. Cold data then lives
    /// only in the mirror; a later read pulls it back through and the cycle
    /// repeats.
    pub fn with_local_cache_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.local_cache_ttl_ms = Some(ttl_ms);
        self
    }

    pub fn local_cache_ttl_ms(&self) -> Option<u64> {
        self.local_cache_ttl_ms
    }

    /// One managed-cache sweep pass. Evicts local objects whose age exceeds
    /// the TTL **and** whose mirror copy exists — never the only copy. The
    /// upload path already verified content (atomic put of the same bytes),
    /// so mirror existence is the eviction precondition.
    pub async fn run_cache_sweep_once(
        &self,
        now_ms: i64,
        list_limit: usize,
    ) -> io::Result<MirrorCacheSweepReport> {
        let Some(ttl_ms) = self.local_cache_ttl_ms else {
            return Ok(MirrorCacheSweepReport::default());
        };
        let listing = self.primary.list_objects(list_limit).await?;
        let mut report = MirrorCacheSweepReport {
            scanned: listing.objects.len() as u64,
            ..MirrorCacheSweepReport::default()
        };
        for object in listing.objects {
            let Some(modified_ms) = object.last_modified_ms else {
                report.skipped_no_mtime += 1;
                continue;
            };
            if now_ms.saturating_sub(modified_ms) < ttl_ms as i64 {
                continue;
            }
            match self
                .mirror
                .head(&object.tenant_id, &object.artifact_id)
                .await
            {
                Ok(true) => {
                    self.primary
                        .delete(&object.tenant_id, &object.artifact_id)
                        .await?;
                    self.counters.evicted.fetch_add(1, Ordering::Relaxed);
                    report.evicted += 1;
                }
                Ok(false) => {
                    // Mirror does not have it (upload lost to a crash, an
                    // exhausted retry budget, or an outage past the worker's
                    // retries): the local copy is the only copy. Never evict
                    // it, and re-drive the upload so the object still reaches
                    // the mirror instead of accumulating as permanent debt.
                    report.retained_unmirrored += 1;
                    match self
                        .requeue_local_object(&object.tenant_id, &object.artifact_id)
                        .await
                    {
                        RequeueOutcome::Requeued(bytes) => {
                            report.requeued += 1;
                            report.requeued_bytes += bytes;
                        }
                        RequeueOutcome::Throttled | RequeueOutcome::Error => {}
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        tenant_id = %object.tenant_id,
                        artifact_id = %object.artifact_id,
                        "cache sweep mirror head failed; retaining local copy"
                    );
                    report.retained_unmirrored += 1;
                }
            }
        }
        Ok(report)
    }

    /// Re-drive every local object the mirror is missing back onto the upload
    /// queue. The local primary is the durable record of what must be
    /// mirrored, so this recovers uploads lost to a process crash, an
    /// exhausted retry budget, or an object-store outage that outlived the
    /// worker's retries — without any persisted queue. Safe to run repeatedly:
    /// an object the mirror already holds is skipped, and a re-uploaded object
    /// is an idempotent create of identical bytes.
    ///
    /// Requires the primary to support `list_objects`; other backends return
    /// `Unsupported` and this is a no-op.
    pub async fn reconcile_unmirrored(
        &self,
        list_limit: usize,
    ) -> io::Result<MirrorReconcileReport> {
        let listing = self.primary.list_objects(list_limit).await?;
        let mut report = MirrorReconcileReport {
            scanned: listing.objects.len() as u64,
            ..MirrorReconcileReport::default()
        };
        for object in listing.objects {
            match self
                .mirror
                .head(&object.tenant_id, &object.artifact_id)
                .await
            {
                Ok(true) => report.already_mirrored += 1,
                Ok(false) => match self
                    .requeue_local_object(&object.tenant_id, &object.artifact_id)
                    .await
                {
                    RequeueOutcome::Requeued(bytes) => {
                        report.requeued += 1;
                        report.requeued_bytes += bytes;
                    }
                    RequeueOutcome::Throttled => report.throttled += 1,
                    RequeueOutcome::Error => report.errors += 1,
                },
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        tenant_id = %object.tenant_id,
                        artifact_id = %object.artifact_id,
                        "mirror reconcile head failed; retaining local copy for retry"
                    );
                    report.errors += 1;
                }
            }
        }
        Ok(report)
    }

    /// Read an un-mirrored local object and re-enqueue its upload, honoring the
    /// same backlog ceiling as a fresh `put` so reconciliation cannot push the
    /// backlog past its bound. Reads the primary backend directly so it does
    /// not refresh managed-cache age.
    async fn requeue_local_object(&self, tenant_id: &str, artifact_id: &str) -> RequeueOutcome {
        let bytes = match self.primary.get(tenant_id, artifact_id).await {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    tenant_id,
                    artifact_id,
                    "mirror reconcile primary read failed; retaining local copy for retry"
                );
                return RequeueOutcome::Error;
            }
        };
        if self.over_backlog_limit(bytes.len() as u64) {
            return RequeueOutcome::Throttled;
        }
        let byte_len = bytes.len() as u64;
        match self
            .enqueue(MirrorJob::Put {
                tenant_id: tenant_id.into(),
                artifact_id: artifact_id.into(),
                bytes,
            })
            .await
        {
            Ok(()) => RequeueOutcome::Requeued(byte_len),
            Err(error) => {
                tracing::warn!(
                    ?error,
                    tenant_id,
                    artifact_id,
                    "mirror reconcile enqueue failed"
                );
                RequeueOutcome::Error
            }
        }
    }

    pub fn stats(&self) -> AsyncMirrorStats {
        let backlog = self.counters.backlog.load(Ordering::Relaxed);
        let backlog_bytes = self.counters.backlog_bytes.load(Ordering::Relaxed);
        let (top_tenant, top_tenant_backlog_bytes) = self
            .per_tenant_backlog
            .lock()
            .ok()
            .and_then(|map| {
                map.iter()
                    .max_by_key(|(_, bytes)| **bytes)
                    .map(|(t, b)| (Some(t.clone()), *b))
            })
            .unwrap_or((None, 0));
        let last_drain = self.counters.last_drain_ms.load(Ordering::Relaxed);
        let ms_since_last_drain =
            (backlog > 0 && last_drain > 0).then(|| now_ms_wall().saturating_sub(last_drain));
        AsyncMirrorStats {
            queued: self.counters.queued.load(Ordering::Relaxed),
            completed: self.counters.completed.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
            backlog,
            backlog_bytes,
            max_backlog_bytes: self.max_backlog_bytes,
            over_limit: self
                .max_backlog_bytes
                .is_some_and(|limit| backlog_bytes >= limit),
            top_tenant,
            top_tenant_backlog_bytes,
            ms_since_last_drain,
            evicted: self.counters.evicted.load(Ordering::Relaxed),
        }
    }

    /// Whether a new upload of `incoming_bytes` would exceed the backlog
    /// ceiling. When it would, the caller throttles instead of enqueuing.
    fn over_backlog_limit(&self, incoming_bytes: u64) -> bool {
        self.max_backlog_bytes.is_some_and(|limit| {
            self.counters
                .backlog_bytes
                .load(Ordering::Relaxed)
                .saturating_add(incoming_bytes)
                > limit
        })
    }

    fn backpressure_error(&self, tenant_id: &str) -> io::Error {
        let stats = self.stats();
        io::Error::other(format!(
            "{MIRROR_BACKPRESSURE_MARKER}: object-store upload backlog {} bytes exceeds limit {} \
             (top tenant {:?}); throttling writes for tenant {tenant_id}",
            stats.backlog_bytes,
            self.max_backlog_bytes.unwrap_or(0),
            stats.top_tenant.as_deref().unwrap_or("-"),
        ))
    }

    async fn enqueue(&self, job: MirrorJob) -> io::Result<()> {
        let bytes = job.byte_len();
        let tenant = job.tenant_id().to_string();
        self.counters.queued.fetch_add(1, Ordering::Relaxed);
        self.counters.backlog.fetch_add(1, Ordering::Relaxed);
        self.counters
            .backlog_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        if bytes > 0 {
            if let Ok(mut map) = self.per_tenant_backlog.lock() {
                *map.entry(tenant).or_insert(0) += bytes;
            }
        }
        if let Err(err) = self.sender.send(job).await {
            self.counters.backlog.fetch_sub(1, Ordering::Relaxed);
            self.counters
                .backlog_bytes
                .fetch_sub(bytes, Ordering::Relaxed);
            self.counters.failed.fetch_add(1, Ordering::Relaxed);
            return Err(io::Error::other(format!(
                "async content mirror queue closed: {err}"
            )));
        }
        Ok(())
    }
}

/// Wraps a content backend and delays every operation by a fixed duration.
/// Used to demonstrate pipeline backpressure ("inject 500ms object-store
/// latency," hardening §5): put it in front of the mirror backend and the
/// upload backlog grows until the ceiling throttles writers.
pub struct LatencyContentBackend {
    inner: Arc<dyn ContentBackend>,
    delay: std::time::Duration,
}

impl LatencyContentBackend {
    pub fn new(inner: Arc<dyn ContentBackend>, delay_ms: u64) -> Self {
        Self {
            inner,
            delay: std::time::Duration::from_millis(delay_ms),
        }
    }

    async fn sleep(&self) {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
    }
}

#[async_trait]
impl ContentBackend for LatencyContentBackend {
    async fn put(&self, tenant_id: &str, artifact_id: &str, bytes: &[u8]) -> io::Result<PutResult> {
        self.sleep().await;
        self.inner.put(tenant_id, artifact_id, bytes).await
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        self.sleep().await;
        self.inner.get(tenant_id, artifact_id).await
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        self.sleep().await;
        self.inner.head(tenant_id, artifact_id).await
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        self.sleep().await;
        self.inner.delete(tenant_id, artifact_id).await
    }

    async fn touch(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        self.inner.touch(tenant_id, artifact_id).await
    }

    async fn sweep_temporary_objects(&self, limit: usize) -> io::Result<usize> {
        self.inner.sweep_temporary_objects(limit).await
    }

    async fn list_objects(&self, limit: usize) -> io::Result<ContentListing> {
        self.inner.list_objects(limit).await
    }
}

async fn execute_mirror_job_with_retry(
    mirror: &(dyn ContentBackend + Send + Sync),
    job: &MirrorJob,
    max_attempts: usize,
) -> io::Result<()> {
    let mut attempt = 1;
    loop {
        match execute_mirror_job(mirror, job).await {
            Ok(()) => return Ok(()),
            Err(err) if attempt < max_attempts => {
                tracing::warn!(
                    ?err,
                    attempt,
                    max_attempts,
                    "async content mirror job failed; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(25 * attempt as u64)).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn execute_mirror_job(
    mirror: &(dyn ContentBackend + Send + Sync),
    job: &MirrorJob,
) -> io::Result<()> {
    match job {
        MirrorJob::Put {
            tenant_id,
            artifact_id,
            bytes,
        } => mirror.put(tenant_id, artifact_id, bytes).await.map(|_| ()),
        MirrorJob::Delete {
            tenant_id,
            artifact_id,
        } => mirror.delete(tenant_id, artifact_id).await,
    }
}

#[async_trait]
impl ContentBackend for AsyncMirrorContentBackend {
    async fn put(&self, tenant_id: &str, artifact_id: &str, bytes: &[u8]) -> io::Result<PutResult> {
        // Throttle BEFORE the primary write: a rejected write leaves no
        // half-mirrored state, and the writer retries after the backlog
        // drains. This is what keeps the mirror backlog bounded under slow
        // object storage instead of blocking the hot path indefinitely.
        if self.over_backlog_limit(bytes.len() as u64) {
            return Err(self.backpressure_error(tenant_id));
        }
        let result = self.primary.put(tenant_id, artifact_id, bytes).await?;
        self.enqueue(MirrorJob::Put {
            tenant_id: tenant_id.into(),
            artifact_id: artifact_id.into(),
            bytes: bytes.to_vec(),
        })
        .await?;
        Ok(result)
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        match self.primary.get(tenant_id, artifact_id).await {
            Ok(bytes) => {
                if self.local_cache_ttl_ms.is_some() {
                    if let Err(error) = self.primary.touch(tenant_id, artifact_id).await {
                        tracing::debug!(?error, "content cache touch failed");
                    }
                }
                Ok(bytes)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let bytes = self.mirror.get(tenant_id, artifact_id).await?;
                if let Err(backfill_err) = self.primary.put(tenant_id, artifact_id, &bytes).await {
                    tracing::warn!(
                        ?backfill_err,
                        tenant_id,
                        artifact_id,
                        "async content mirror read-through backfill failed"
                    );
                }
                Ok(bytes)
            }
            Err(err) => Err(err),
        }
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        if self.primary.head(tenant_id, artifact_id).await? {
            return Ok(true);
        }
        self.mirror.head(tenant_id, artifact_id).await
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        self.primary.delete(tenant_id, artifact_id).await?;
        self.enqueue(MirrorJob::Delete {
            tenant_id: tenant_id.into(),
            artifact_id: artifact_id.into(),
        })
        .await
    }

    async fn sweep_temporary_objects(&self, limit: usize) -> io::Result<usize> {
        self.primary.sweep_temporary_objects(limit).await
    }

    async fn list_objects(&self, limit: usize) -> io::Result<ContentListing> {
        self.primary.list_objects(limit).await
    }
}

// ----- LocalFsContentBackend --------------------------------------------------

/// Filesystem-backed content store. Writes blobs under
/// `{root}/artifact_content/{tenant_id}/{artifact_id}/blob`.
pub struct LocalFsContentBackend {
    root: PathBuf,
}

static LOCAL_FS_TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

impl LocalFsContentBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn dir_for(&self, tenant_id: &str, artifact_id: &str) -> PathBuf {
        self.root
            .join("artifact_content")
            .join(tenant_id)
            .join(artifact_id)
    }

    fn path_for(&self, tenant_id: &str, artifact_id: &str) -> PathBuf {
        self.dir_for(tenant_id, artifact_id).join("blob")
    }

    fn tmp_path_for(&self, tenant_id: &str, artifact_id: &str) -> PathBuf {
        // Random suffix so concurrent puts of the same artifact_id (which
        // shouldn't normally happen — artifact_ids are UUIDv7 and unique
        // per call — but defensive) don't collide on the tmp path. We use a
        // monotonic-ish nanosecond timestamp combined with a process-local
        // counter; that's enough to avoid same-process collisions.
        use std::time::{SystemTime, UNIX_EPOCH};
        let ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let sequence = LOCAL_FS_TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let suffix = format!(".tmp.{ns}.{sequence}");
        self.dir_for(tenant_id, artifact_id)
            .join(format!("blob{suffix}"))
    }
}

#[async_trait]
impl ContentBackend for LocalFsContentBackend {
    async fn touch(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        let path = self.path_for(tenant_id, artifact_id);
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            file.set_modified(std::time::SystemTime::now())
        })
        .await
        .map_err(|error| io::Error::other(format!("touch task failed: {error}")))?
    }

    async fn put(&self, tenant_id: &str, artifact_id: &str, bytes: &[u8]) -> io::Result<PutResult> {
        let dir = self.dir_for(tenant_id, artifact_id);
        tokio::fs::create_dir_all(&dir).await?;

        let final_path = self.path_for(tenant_id, artifact_id);
        let tmp_path = self.tmp_path_for(tenant_id, artifact_id);

        let sha256_hex = sha256_hex(bytes);

        // Write tmp + fsync + rename. tokio::fs::rename is atomic on POSIX.
        let write_result = async {
            let mut file = tokio::fs::File::create(&tmp_path).await?;
            file.write_all(bytes).await?;
            file.sync_data().await?;
            drop(file);
            tokio::fs::rename(&tmp_path, &final_path).await
        }
        .await;
        if let Err(error) = write_result {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(error);
        }
        if let Err(error) = sync_directory_chain(dir.clone(), self.root.clone()).await {
            // A failed durability barrier is a failed put. Remove the visible
            // object so callers never mistake an error return for a committed
            // blob; the runtime also performs a best-effort cleanup.
            let _ = tokio::fs::remove_file(&final_path).await;
            return Err(error);
        }

        Ok(PutResult {
            bytes: bytes.len() as u64,
            sha256_hex,
        })
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        let p = self.path_for(tenant_id, artifact_id);
        tokio::fs::read(&p).await
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        let p = self.path_for(tenant_id, artifact_id);
        match tokio::fs::metadata(&p).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        let p = self.path_for(tenant_id, artifact_id);
        let removed = match tokio::fs::remove_file(&p).await {
            Ok(()) => true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => return Err(e),
        };
        if !removed {
            return Ok(());
        }
        let dir = self.dir_for(tenant_id, artifact_id);
        sync_directory_chain(dir.clone(), self.root.clone()).await?;
        // Best-effort cleanup of the empty artifact directory. A non-empty
        // directory can contain an in-flight temporary write.
        if tokio::fs::remove_dir(&dir).await.is_ok() {
            if let Some(parent) = dir.parent() {
                sync_directory_chain(parent.to_path_buf(), self.root.clone()).await?;
            }
        }
        Ok(())
    }

    async fn sweep_temporary_objects(&self, limit: usize) -> io::Result<usize> {
        let root = self.root.join("artifact_content");
        let mut tenants = match tokio::fs::read_dir(&root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        };
        let mut removed = 0_usize;
        let mut scanned = 0_usize;
        while let Some(tenant) = tenants.next_entry().await? {
            if !tenant.file_type().await?.is_dir() {
                continue;
            }
            let mut artifacts = tokio::fs::read_dir(tenant.path()).await?;
            while let Some(artifact) = artifacts.next_entry().await? {
                if !artifact.file_type().await?.is_dir() {
                    continue;
                }
                scanned += 1;
                if scanned > limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("temporary sweep exceeds configured limit of {limit} objects"),
                    ));
                }
                let artifact_path = artifact.path();
                let mut entries = tokio::fs::read_dir(&artifact_path).await?;
                let mut has_blob = false;
                let mut removed_temporary_file = false;
                while let Some(entry) = entries.next_entry().await? {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name == "blob" && entry.file_type().await?.is_file() {
                        has_blob = true;
                    } else if name.starts_with("blob.tmp.") && entry.file_type().await?.is_file() {
                        tokio::fs::remove_file(entry.path()).await?;
                        removed = removed.saturating_add(1);
                        removed_temporary_file = true;
                    }
                }
                if removed_temporary_file {
                    sync_directory_chain(artifact_path.clone(), self.root.clone()).await?;
                }
                if !has_blob && tokio::fs::remove_dir(&artifact_path).await.is_ok() {
                    sync_directory_chain(tenant.path(), self.root.clone()).await?;
                }
            }
        }
        Ok(removed)
    }

    async fn list_objects(&self, limit: usize) -> io::Result<ContentListing> {
        let root = self.root.join("artifact_content");
        let mut tenants = match tokio::fs::read_dir(&root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(ContentListing {
                    objects: Vec::new(),
                    temporary_objects_deleted: 0,
                })
            }
            Err(error) => return Err(error),
        };
        let mut objects = Vec::new();
        while let Some(tenant) = tenants.next_entry().await? {
            if !tenant.file_type().await?.is_dir() {
                continue;
            }
            let tenant_id = tenant.file_name().to_string_lossy().to_string();
            let mut artifacts = tokio::fs::read_dir(tenant.path()).await?;
            while let Some(artifact) = artifacts.next_entry().await? {
                if !artifact.file_type().await?.is_dir() {
                    continue;
                }
                let artifact_path = artifact.path();
                let blob_path = artifact_path.join("blob");
                let Ok(metadata) = tokio::fs::metadata(&blob_path).await else {
                    // No committed blob (in-flight put or crash leftover):
                    // not listable, and cleanup belongs to
                    // `sweep_temporary_objects`, never a read-only listing.
                    continue;
                };
                if objects.len() >= limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "content object listing exceeds configured limit of {limit} objects"
                        ),
                    ));
                }
                let last_modified_ms = metadata
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as i64);
                objects.push(ContentObject {
                    tenant_id: tenant_id.clone(),
                    artifact_id: artifact.file_name().to_string_lossy().to_string(),
                    last_modified_ms,
                });
            }
        }
        Ok(ContentListing {
            objects,
            temporary_objects_deleted: 0,
        })
    }
}

#[cfg(unix)]
async fn sync_directory(path: PathBuf) -> io::Result<()> {
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
        .await
        .map_err(|error| io::Error::other(format!("directory fsync task failed: {error}")))?
}

#[cfg(not(unix))]
async fn sync_directory(_path: PathBuf) -> io::Result<()> {
    Ok(())
}

async fn sync_directory_chain(mut path: PathBuf, stop_at: PathBuf) -> io::Result<()> {
    loop {
        sync_directory(path.clone()).await?;
        if path == stop_at {
            return Ok(());
        }
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        let parent = parent.to_path_buf();
        if !parent.starts_with(&stop_at) {
            return Ok(());
        }
        path = parent;
    }
}

// ----- InMemoryContentBackend -------------------------------------------------

/// Process-local in-memory blob store. Used by `Runtime::in_memory()` and by
/// tests that don't exercise filesystem persistence. Goes alongside
/// `LocalFsContentBackend` so callers that want a content backend but no
/// disk I/O don't have to roll their own.
#[derive(Default)]
pub struct InMemoryContentBackend {
    blobs: Mutex<HashMap<(String, String), Vec<u8>>>,
}

#[async_trait]
impl ContentBackend for InMemoryContentBackend {
    async fn put(&self, tenant_id: &str, artifact_id: &str, bytes: &[u8]) -> io::Result<PutResult> {
        let sha256_hex = sha256_hex(bytes);

        let mut g = self.blobs.lock().expect("blobs mutex poisoned");
        g.insert(
            (tenant_id.to_string(), artifact_id.to_string()),
            bytes.to_vec(),
        );
        Ok(PutResult {
            bytes: bytes.len() as u64,
            sha256_hex,
        })
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        let g = self.blobs.lock().expect("blobs mutex poisoned");
        g.get(&(tenant_id.to_string(), artifact_id.to_string()))
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "artifact blob not found"))
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        let g = self.blobs.lock().expect("blobs mutex poisoned");
        Ok(g.contains_key(&(tenant_id.to_string(), artifact_id.to_string())))
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        let mut g = self.blobs.lock().expect("blobs mutex poisoned");
        g.remove(&(tenant_id.to_string(), artifact_id.to_string()));
        Ok(())
    }

    async fn list_objects(&self, limit: usize) -> io::Result<ContentListing> {
        let blobs = self.blobs.lock().expect("blobs mutex poisoned");
        if blobs.len() > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("content object listing exceeds configured limit of {limit} objects"),
            ));
        }
        Ok(ContentListing {
            objects: blobs
                .keys()
                .map(|(tenant_id, artifact_id)| ContentObject {
                    tenant_id: tenant_id.clone(),
                    artifact_id: artifact_id.clone(),
                    last_modified_ms: None,
                })
                .collect(),
            temporary_objects_deleted: 0,
        })
    }
}

// ----- ObjectStoreContentBackend ----------------------------------------------

/// Object-store-backed content store. Stores blobs under
/// `{prefix}/artifact_content/{tenant_id}/{artifact_id}/blob`.
pub struct ObjectStoreContentBackend {
    store: std::sync::Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStoreContentBackend {
    pub fn new(store: std::sync::Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into().trim_matches('/').to_string();
        Self { store, prefix }
    }

    fn path_for(&self, tenant_id: &str, artifact_id: &str) -> ObjectPath {
        let mut path = String::new();
        if !self.prefix.is_empty() {
            path.push_str(&self.prefix);
            path.push('/');
        }
        path.push_str("artifact_content/");
        path.push_str(tenant_id);
        path.push('/');
        path.push_str(artifact_id);
        path.push_str("/blob");
        ObjectPath::from(path)
    }
}

#[async_trait]
impl ContentBackend for ObjectStoreContentBackend {
    async fn put(&self, tenant_id: &str, artifact_id: &str, bytes: &[u8]) -> io::Result<PutResult> {
        let sha256_hex = sha256_hex(bytes);
        self.store
            .put(
                &self.path_for(tenant_id, artifact_id),
                bytes.to_vec().into(),
            )
            .await
            .map_err(|err| io::Error::other(format!("object store put failed: {err}")))?;
        Ok(PutResult {
            bytes: bytes.len() as u64,
            sha256_hex,
        })
    }

    async fn get(&self, tenant_id: &str, artifact_id: &str) -> io::Result<Vec<u8>> {
        let path = self.path_for(tenant_id, artifact_id);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(map_object_store_get_error)?;
        let bytes = result
            .bytes()
            .await
            .map_err(|err| io::Error::other(format!("object store read failed: {err}")))?;
        Ok(bytes.to_vec())
    }

    async fn head(&self, tenant_id: &str, artifact_id: &str) -> io::Result<bool> {
        let path = self.path_for(tenant_id, artifact_id);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(io::Error::other(format!("object store head failed: {err}"))),
        }
    }

    async fn delete(&self, tenant_id: &str, artifact_id: &str) -> io::Result<()> {
        let path = self.path_for(tenant_id, artifact_id);
        match self.store.delete(&path).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(io::Error::other(format!(
                "object store delete failed: {err}"
            ))),
        }
    }

    async fn list_objects(&self, limit: usize) -> io::Result<ContentListing> {
        use futures::StreamExt;

        let mut list_prefix = String::new();
        if !self.prefix.is_empty() {
            list_prefix.push_str(&self.prefix);
            list_prefix.push('/');
        }
        list_prefix.push_str("artifact_content");
        let list_path = ObjectPath::from(list_prefix.clone());

        let mut objects = Vec::new();
        let mut stream = self.store.list(Some(&list_path));
        while let Some(entry) = stream.next().await {
            let meta = entry
                .map_err(|err| io::Error::other(format!("object store list failed: {err}")))?;
            let location = meta.location.as_ref();
            let relative = location
                .strip_prefix(&list_prefix)
                .unwrap_or(location)
                .trim_start_matches('/');
            // Expected layout: {tenant_id}/{artifact_id}/blob. Anything else
            // under the content prefix is not an artifact blob and is skipped
            // rather than treated as corruption.
            let mut parts = relative.split('/');
            let (Some(tenant_id), Some(artifact_id), Some("blob"), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            if objects.len() >= limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("content object listing exceeds configured limit of {limit} objects"),
                ));
            }
            objects.push(ContentObject {
                tenant_id: tenant_id.to_string(),
                artifact_id: artifact_id.to_string(),
                last_modified_ms: Some(meta.last_modified.timestamp_millis()),
            });
        }
        Ok(ContentListing {
            objects,
            temporary_objects_deleted: 0,
        })
    }
}

fn map_object_store_get_error(err: object_store::Error) -> io::Error {
    match err {
        object_store::Error::NotFound { .. } => {
            io::Error::new(io::ErrorKind::NotFound, "artifact blob not found")
        }
        other => io::Error::other(format!("object store get failed: {other}")),
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
