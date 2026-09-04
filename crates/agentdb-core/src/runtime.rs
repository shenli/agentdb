//! V2 runtime — atomic append, idempotency, in-memory state with optional
//! KV-backed persistence (Task 11).
//!
//! `Runtime::in_memory()` keeps the original ephemeral behavior (used by every
//! pre-Task-11 unit/integration test). `Runtime::with_kv(kv)` mirrors every
//! mutation into the supplied `KvStore` and hydrates from it on startup, so
//! sessions, events, and idempotency records survive restarts.
//!
//! Atomicity contract for `append` (per Task 9 fix): the event row, the
//! updated session row, and the idempotency record (if present) commit
//! together. With KV this is implemented via `KvStore::write_batch`; with
//! in-memory it's the state mutex.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};

use agentdb_index::doc_id::DocId;
use agentdb_index::text::{InMemoryInverted, TextIndex};
use agentdb_index::vector::{HnswVectorIndex, VectorIndex};
use agentdb_storage::content::{sha256_hex, ContentBackend, InMemoryContentBackend};
use agentdb_storage::content_crypto::{decode_keyed_envelope, encode_keyed_envelope};
use agentdb_storage::{
    envelope_dek_version, is_envelope_v2, AllowAllKmsKeyResolver, ContentCryptoAdapter,
    ContentCryptoContext,
    CryptoContext, DisabledEnvelopeCrypto, EnvelopeCrypto, IdentityContentCryptoAdapter,
    IdentityMetadataCryptoAdapter, KeyOperation, KeyPurpose, KeyResolution, KmsError,
    KmsKeyResolver, KvIter, KvSnapshot, KvStore, MetadataCryptoAdapter, PrefixScopedTabletStore,
    ReplicatedLog, TabletKvStore, TabletStoreLifecycle, TabletStoreManifest, WriteOp,
};

use crate::agent::{Agent, NewAgent};
use crate::artifact::{Artifact, NewArtifact};
use crate::errors::{Error, ErrorCode, Result};
use crate::event::{ArtifactRef, CredentialRef, EventEnvelope, NewEvent, SourceRef};
use crate::global_index::{
    GlobalIndexMutation, GlobalIndexMutationOp, GlobalIndexQuery, GlobalRecordIndex,
    InMemoryGlobalRecordIndex,
};
use crate::id::{Id, IdKind};
use crate::idempotency::{hash_json, hash_request, IdempotencyRecord, IdempotencyStore};
use crate::persist::{Persist, RecordIndexMutationOp};
use crate::policy::{Priority, RuntimePolicy};
use crate::record::{IndexingMode, NewRecord, RecordStatus, SearchableRecord};
use crate::segment::{
    ReplayPrefixFile, ReplayPrefixManifest, SessionArchiveFile, SessionArchiveManifest,
    SessionSegment, SessionSegmentFile,
};
use crate::session::{ForkRef, Lifecycle, NewSession, Session, SnapshotLayer, SnapshotRef};
use crate::tablet_state::{
    decode_sst_snapshot_payload, encode_sst_snapshot_payload, is_sst_snapshot_payload,
    LoggedTabletStateMachine, TabletCommand, TabletSnapshotShardFilter, TabletStateMachine,
    TabletStateSnapshot, TabletStorageStats, TabletWriteOp,
};
use crate::tenant::{NewTenant, Tenant};

pub const RUNTIME_PRIMARY_TABLET_ID: i64 = 1;

/// Reserved tenant that hosts self-managed metadata (stage 2 of
/// docs/design/metadata-self-hosting.md): admin/tenant tokens, session
/// locators, and the cluster tenant registry live as records under it.
/// It is invisible to and unwritable through the public API.
pub const SYSTEM_TENANT_ID: &str = "ten_00000000-0000-0000-0000-000000000001";
/// Reserved external key for the system tenant.
pub const SYSTEM_TENANT_EXTERNAL_KEY: &str = "__agentdb_system";

pub fn is_system_tenant(tenant_id: &str) -> bool {
    tenant_id == SYSTEM_TENANT_ID
}
const DEFAULT_PERSISTED_HOT_EVENT_CACHE_PER_SESSION: u64 = 2_048;
static PERSISTED_HOT_EVENT_CACHE_PER_SESSION: OnceLock<u64> = OnceLock::new();

/// Upper bound on events scanned by a single `search_events` request. Event
/// search is an unindexed scan, so this caps worst-case work; a tenant-wide
/// query over a larger history is best-effort over the newest events.
const EVENT_SEARCH_MAX_SCAN: usize = 10_000;

static APPEND_PATH_PROFILE: OnceLock<Option<AppendPathProfile>> = OnceLock::new();
static SESSION_APPEND_COALESCE_PROFILE: OnceLock<Option<SessionAppendCoalesceProfile>> =
    OnceLock::new();

/// Per-tenant metadata aggregates used by quota admission and backpressure.
/// Computed from authoritative enumerations (drift-free) and cached briefly
/// for the advisory hot path.
#[derive(Debug, Clone, Copy, Default)]
pub struct TenantAggregates {
    pub active_sessions: usize,
    pub live_agents: usize,
    pub live_artifacts: usize,
    pub live_artifact_bytes: u64,
    pub live_records: usize,
}

tokio::task_local! {
    static CURRENT_RUNTIME_TABLET_ID: i64;
    static CURRENT_RUNTIME_ROUTE_CONTEXT: RuntimeRouteContext;
}

/// Tablet routing rides a task-local, which does not follow work handed to
/// other threads. Any component that moves a write off the submitting task
/// (the adaptive write batcher) must capture the id at submit time with this
/// helper and re-enter the scope at flush time with
/// [`with_runtime_tablet_scope_sync`].
pub(crate) fn current_runtime_tablet_id() -> Option<i64> {
    CURRENT_RUNTIME_TABLET_ID
        .try_with(|tablet_id| *tablet_id)
        .ok()
}

pub(crate) fn with_runtime_tablet_scope_sync<R>(
    tablet_id: Option<i64>,
    f: impl FnOnce() -> R,
) -> R {
    match tablet_id {
        Some(tablet_id) => CURRENT_RUNTIME_TABLET_ID.sync_scope(tablet_id, f),
        None => f(),
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeRouteContext {
    pub tablet_id: Option<i64>,
    pub cell_id: Option<String>,
    pub placement_scope_id: Option<String>,
    pub tablet_epoch: Option<i64>,
}

#[derive(Default)]
struct AppendPathSample {
    refresh_nanos: u128,
    session_lock_wait_nanos: u128,
    initial_state_lock_wait_nanos: u128,
    initial_state_work_nanos: u128,
    durable_write_nanos: u128,
    update_state_lock_wait_nanos: u128,
    update_state_work_nanos: u128,
    total_nanos: u128,
}

struct AppendPathProfile {
    interval_ms: u64,
    last_log_ms: AtomicU64,
    calls: AtomicU64,
    refresh_nanos: AtomicU64,
    session_lock_wait_nanos: AtomicU64,
    initial_state_lock_wait_nanos: AtomicU64,
    initial_state_work_nanos: AtomicU64,
    durable_write_nanos: AtomicU64,
    update_state_lock_wait_nanos: AtomicU64,
    update_state_work_nanos: AtomicU64,
    total_nanos: AtomicU64,
}

#[derive(Default)]
struct SessionAppendCoalesceSample {
    jobs: usize,
    events: usize,
    lock_wait_nanos: u128,
    prepare_lock_wait_nanos: u128,
    prepare_work_nanos: u128,
    durable_write_nanos: u128,
    update_state_lock_wait_nanos: u128,
    update_state_work_nanos: u128,
    total_nanos: u128,
}

struct SessionAppendCoalesceProfile {
    interval_ms: u64,
    last_log_ms: AtomicU64,
    batches: AtomicU64,
    jobs: AtomicU64,
    events: AtomicU64,
    lock_wait_nanos: AtomicU64,
    prepare_lock_wait_nanos: AtomicU64,
    prepare_work_nanos: AtomicU64,
    durable_write_nanos: AtomicU64,
    update_state_lock_wait_nanos: AtomicU64,
    update_state_work_nanos: AtomicU64,
    total_nanos: AtomicU64,
}

impl AppendPathProfile {
    fn from_env() -> Option<Self> {
        let interval_ms = std::env::var("AGENTDB_APPEND_PATH_PROFILE_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)?;
        Some(Self {
            interval_ms,
            last_log_ms: AtomicU64::new(crate::now_ms()),
            calls: AtomicU64::new(0),
            refresh_nanos: AtomicU64::new(0),
            session_lock_wait_nanos: AtomicU64::new(0),
            initial_state_lock_wait_nanos: AtomicU64::new(0),
            initial_state_work_nanos: AtomicU64::new(0),
            durable_write_nanos: AtomicU64::new(0),
            update_state_lock_wait_nanos: AtomicU64::new(0),
            update_state_work_nanos: AtomicU64::new(0),
            total_nanos: AtomicU64::new(0),
        })
    }

    fn record(&self, sample: AppendPathSample) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        add_nanos(&self.refresh_nanos, sample.refresh_nanos);
        add_nanos(
            &self.session_lock_wait_nanos,
            sample.session_lock_wait_nanos,
        );
        add_nanos(
            &self.initial_state_lock_wait_nanos,
            sample.initial_state_lock_wait_nanos,
        );
        add_nanos(
            &self.initial_state_work_nanos,
            sample.initial_state_work_nanos,
        );
        add_nanos(&self.durable_write_nanos, sample.durable_write_nanos);
        add_nanos(
            &self.update_state_lock_wait_nanos,
            sample.update_state_lock_wait_nanos,
        );
        add_nanos(
            &self.update_state_work_nanos,
            sample.update_state_work_nanos,
        );
        add_nanos(&self.total_nanos, sample.total_nanos);

        let now = crate::now_ms();
        let last = self.last_log_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < self.interval_ms {
            return;
        }
        if self
            .last_log_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let calls = self.calls.swap(0, Ordering::Relaxed);
        if calls == 0 {
            return;
        }
        let refresh_nanos = self.refresh_nanos.swap(0, Ordering::Relaxed);
        let session_lock_wait_nanos = self.session_lock_wait_nanos.swap(0, Ordering::Relaxed);
        let initial_state_lock_wait_nanos = self
            .initial_state_lock_wait_nanos
            .swap(0, Ordering::Relaxed);
        let initial_state_work_nanos = self.initial_state_work_nanos.swap(0, Ordering::Relaxed);
        let durable_write_nanos = self.durable_write_nanos.swap(0, Ordering::Relaxed);
        let update_state_lock_wait_nanos =
            self.update_state_lock_wait_nanos.swap(0, Ordering::Relaxed);
        let update_state_work_nanos = self.update_state_work_nanos.swap(0, Ordering::Relaxed);
        let total_nanos = self.total_nanos.swap(0, Ordering::Relaxed);
        tracing::info!(
            calls,
            avg_refresh_us = avg_us(refresh_nanos, calls),
            avg_session_lock_wait_us = avg_us(session_lock_wait_nanos, calls),
            avg_initial_state_lock_wait_us = avg_us(initial_state_lock_wait_nanos, calls),
            avg_initial_state_work_us = avg_us(initial_state_work_nanos, calls),
            avg_durable_write_us = avg_us(durable_write_nanos, calls),
            avg_update_state_lock_wait_us = avg_us(update_state_lock_wait_nanos, calls),
            avg_update_state_work_us = avg_us(update_state_work_nanos, calls),
            avg_total_us = avg_us(total_nanos, calls),
            "append path profile"
        );
    }
}

impl SessionAppendCoalesceProfile {
    fn from_env() -> Option<Self> {
        let interval_ms = std::env::var("AGENTDB_APPEND_PATH_PROFILE_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)?;
        Some(Self {
            interval_ms,
            last_log_ms: AtomicU64::new(crate::now_ms()),
            batches: AtomicU64::new(0),
            jobs: AtomicU64::new(0),
            events: AtomicU64::new(0),
            lock_wait_nanos: AtomicU64::new(0),
            prepare_lock_wait_nanos: AtomicU64::new(0),
            prepare_work_nanos: AtomicU64::new(0),
            durable_write_nanos: AtomicU64::new(0),
            update_state_lock_wait_nanos: AtomicU64::new(0),
            update_state_work_nanos: AtomicU64::new(0),
            total_nanos: AtomicU64::new(0),
        })
    }

    fn record(&self, sample: SessionAppendCoalesceSample) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.jobs.fetch_add(sample.jobs as u64, Ordering::Relaxed);
        self.events
            .fetch_add(sample.events as u64, Ordering::Relaxed);
        add_nanos(&self.lock_wait_nanos, sample.lock_wait_nanos);
        add_nanos(
            &self.prepare_lock_wait_nanos,
            sample.prepare_lock_wait_nanos,
        );
        add_nanos(&self.prepare_work_nanos, sample.prepare_work_nanos);
        add_nanos(&self.durable_write_nanos, sample.durable_write_nanos);
        add_nanos(
            &self.update_state_lock_wait_nanos,
            sample.update_state_lock_wait_nanos,
        );
        add_nanos(
            &self.update_state_work_nanos,
            sample.update_state_work_nanos,
        );
        add_nanos(&self.total_nanos, sample.total_nanos);

        let now = crate::now_ms();
        let last = self.last_log_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < self.interval_ms {
            return;
        }
        if self
            .last_log_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let batches = self.batches.swap(0, Ordering::Relaxed);
        if batches == 0 {
            return;
        }
        let jobs = self.jobs.swap(0, Ordering::Relaxed);
        let events = self.events.swap(0, Ordering::Relaxed);
        let lock_wait_nanos = self.lock_wait_nanos.swap(0, Ordering::Relaxed);
        let prepare_lock_wait_nanos = self.prepare_lock_wait_nanos.swap(0, Ordering::Relaxed);
        let prepare_work_nanos = self.prepare_work_nanos.swap(0, Ordering::Relaxed);
        let durable_write_nanos = self.durable_write_nanos.swap(0, Ordering::Relaxed);
        let update_state_lock_wait_nanos =
            self.update_state_lock_wait_nanos.swap(0, Ordering::Relaxed);
        let update_state_work_nanos = self.update_state_work_nanos.swap(0, Ordering::Relaxed);
        let total_nanos = self.total_nanos.swap(0, Ordering::Relaxed);
        tracing::info!(
            batches,
            jobs,
            events,
            avg_jobs_per_batch = jobs as f64 / batches as f64,
            avg_events_per_batch = events as f64 / batches as f64,
            avg_lock_wait_us = avg_us(lock_wait_nanos, batches),
            avg_prepare_lock_wait_us = avg_us(prepare_lock_wait_nanos, batches),
            avg_prepare_work_us = avg_us(prepare_work_nanos, batches),
            avg_durable_write_us = avg_us(durable_write_nanos, batches),
            avg_update_state_lock_wait_us = avg_us(update_state_lock_wait_nanos, batches),
            avg_update_state_work_us = avg_us(update_state_work_nanos, batches),
            avg_total_us = avg_us(total_nanos, batches),
            "session append coalesce profile"
        );
    }
}

fn append_path_profile() -> Option<&'static AppendPathProfile> {
    APPEND_PATH_PROFILE
        .get_or_init(AppendPathProfile::from_env)
        .as_ref()
}

fn session_append_coalesce_profile() -> Option<&'static SessionAppendCoalesceProfile> {
    SESSION_APPEND_COALESCE_PROFILE
        .get_or_init(SessionAppendCoalesceProfile::from_env)
        .as_ref()
}

fn add_nanos(counter: &AtomicU64, nanos: u128) {
    counter.fetch_add(nanos.min(u128::from(u64::MAX)) as u64, Ordering::Relaxed);
}

fn avg_us(total_nanos: u64, calls: u64) -> f64 {
    total_nanos as f64 / calls.max(1) as f64 / 1_000.0
}

fn env_flag_disabled_runtime(name: &str) -> bool {
    std::env::var(name)
        .map(|raw| {
            let raw = raw.trim().to_ascii_lowercase();
            matches!(raw.as_str(), "0" | "false" | "off" | "disabled")
        })
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
pub struct AppendRequest {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub session_id: Id,
    pub kind: String,
    pub payload: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub source_refs: Vec<SourceRef>,
    pub credential_refs: Vec<CredentialRef>,
    pub idempotency_key: Option<String>,
}

impl AppendRequest {
    /// Convenience for tests and simple call sites that already have references
    /// to the live tenant/agent/session.
    pub fn simple(
        t: &Tenant,
        a: &Agent,
        s: &Session,
        kind: &str,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            session_id: s.session_id.clone(),
            kind: kind.into(),
            payload,
            artifact_refs: vec![],
            source_refs: vec![],
            credential_refs: vec![],
            idempotency_key: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub tenant_id: Id,
    pub session_id: Id,
    pub from_seq: u64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendResponse {
    pub event_id: Id,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendAndFetchResponse {
    pub event_id: Id,
    pub seq: u64,
    pub recent: Vec<EventEnvelope>,
}

struct AppendCommitResponse {
    appended: AppendResponse,
    recent: Vec<EventEnvelope>,
    recent_complete: bool,
    backpressure: BackpressureReport,
    backpressure_complete: bool,
}

#[derive(Debug, Clone)]
pub struct ForkSessionRequest {
    pub tenant_id: Id,
    pub session_id: Id,
    pub at_seq: u64,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResponse {
    pub session_id: Id,
    pub events: Vec<EventEnvelope>,
    #[serde(default)]
    pub fork_depth: usize,
    #[serde(default)]
    pub segments_scanned: usize,
    #[serde(default)]
    pub events_scanned: usize,
    #[serde(default)]
    pub materialized_prefix: bool,
}

/// Request to record a sandbox snapshot reference at a session `seq`. AgentDB
/// stores the reference only; the bytes live in the sandbox filesystem / object
/// store. See `docs/reference/sandbox-snapshot-contract.md`.
#[derive(Debug, Clone)]
pub struct NewSnapshotRef {
    pub tenant_id: Id,
    pub session_id: Id,
    pub at_seq: u64,
    pub layers: Vec<SnapshotLayer>,
    pub expires_at_ms: Option<u64>,
}

/// Result of resolving which snapshot to restore when resuming a session.
/// `replay_from_seq` is the leaf-session seq from which to replay the event tail
/// after restoring `snapshot`: when the snapshot belongs to the leaf session it
/// is the snapshot's `at_seq`; when it is inherited from an ancestor across a
/// fork boundary it is `0` (restore the ancestor base, then replay the whole
/// child branch). `dangling_skipped` is true when a newer snapshot existed but
/// was expired and we fell back to an older one (or to a clean start).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotResolution {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owning_session_id: Option<Id>,
    pub replay_from_seq: u64,
    pub dangling_skipped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStorageSummary {
    pub session_id: Id,
    pub lifecycle: Lifecycle,
    pub next_seq: u64,
    pub warm_head_from_seq: u64,
    pub sealed_segment_count: usize,
    pub sealed_segments: Vec<SessionSegment>,
    pub archive_manifest: Option<SessionArchiveManifest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_prefix: Option<ReplayPrefixManifest>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleCounts {
    pub warm: usize,
    pub cold: usize,
    pub archive: usize,
    pub derivative_only: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantStorageSummary {
    pub tenant_id: Id,
    pub session_count: usize,
    pub active_session_count: usize,
    pub lifecycle_counts: LifecycleCounts,
    pub sealed_segment_count: usize,
    pub archive_manifest_count: usize,
    pub sealed_bytes: u64,
    pub archived_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FleetStorageSummary {
    pub tenant_count: usize,
    pub session_count: usize,
    pub active_session_count: usize,
    pub lifecycle_counts: LifecycleCounts,
    pub sealed_segment_count: usize,
    pub archive_manifest_count: usize,
    pub sealed_bytes: u64,
    pub archived_bytes: u64,
    pub tenants: Vec<TenantStorageSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextInclude {
    RecentEvents,
    Records,
    Policy,
}

#[derive(Debug, Clone)]
pub struct ContextRequest {
    pub tenant_id: Id,
    pub session_id: Id,
    pub max_events: usize,
    pub max_bytes: usize,
    pub include: Vec<ContextInclude>,
    pub record_query: Option<SearchRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextResponse {
    pub session_id: Id,
    pub events: Vec<EventEnvelope>,
    pub records: Vec<SearchableRecord>,
    pub policy: Option<RuntimePolicy>,
    pub bytes_used: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchScope {
    Session,
    Tenant,
    Agent,
}

#[derive(Debug, Clone)]
pub struct ExportWatchRequest {
    pub tenant_id: Id,
    pub scope: WatchScope,
    pub session_id: Option<Id>,
    pub agent_id: Option<Id>,
    pub include: Vec<String>,
    pub from_cursor: Option<String>,
    pub limit: usize,
}

/// A fork-edge control item in the changefeed. It announces that `child_session_id`
/// was forked from `parent_session_id` at `fork_point_seq`, so a consumer
/// following the parent can discover the child sub-stream. It carries no events:
/// fork is a pointer, not a re-emission of ancestor history. See
/// `docs/design/design.md` (fork-aware feeds).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkEdge {
    pub parent_session_id: Id,
    pub child_session_id: Id,
    pub fork_point_seq: u64,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportLedgerItem {
    pub op: String,
    pub object_type: String,
    pub tenant_id: Id,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Id>,
    pub object_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub created_at_ms: u64,
    pub data: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFeedItem {
    pub cursor: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cell_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement_scope_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablet_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tablet_epoch: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_index: Option<u64>,
    pub item: ExportLedgerItem,
}

#[derive(Debug, Clone)]
struct ChangefeedCursorContext {
    cell_id: Option<String>,
    placement_scope_id: Option<String>,
    tablet_id: Option<i64>,
    tablet_epoch: Option<i64>,
    applied_index: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct LifecycleSweepReport {
    pub sessions_transitioned: usize,
    pub artifacts_raw_purged: usize,
    pub seal_segment_actions: usize,
    pub move_to_cold_actions: usize,
    pub archive_actions: usize,
    pub hydrate_actions: usize,
    pub work_units: usize,
    pub soft_work_limit_exceeded: bool,
    pub hard_work_limit_hit: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct GcSweepReport {
    pub tenants_purged: usize,
    pub agents_purged: usize,
    pub sessions_purged: usize,
    pub events_purged: usize,
    pub idempotency_purged: usize,
    pub records_purged: usize,
    pub artifacts_purged: usize,
    pub changefeed_delete_events: usize,
    pub changefeed_purge_events: usize,
    pub work_units: usize,
    pub soft_work_limit_exceeded: bool,
    pub hard_work_limit_hit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactLifecycleIssue {
    pub artifact_id: Id,
    pub issue: String,
    pub message: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactLifecycleVerificationReport {
    pub tenant_id: Option<Id>,
    pub artifacts_checked: usize,
    pub raw_present: usize,
    pub raw_purged: usize,
    pub missing_raw: usize,
    pub hash_mismatch: usize,
    pub unexpected_raw_after_purge: usize,
    pub read_errors: usize,
    pub issues: Vec<ArtifactLifecycleIssue>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactContentReconciliationReport {
    pub listing_supported: bool,
    pub objects_scanned: usize,
    pub referenced_objects: usize,
    pub orphan_objects_deleted: usize,
    pub temporary_objects_deleted: usize,
    pub delete_errors: usize,
}

impl ArtifactLifecycleVerificationReport {
    pub fn ok(&self) -> bool {
        self.issues.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BackpressureSignal {
    pub bucket: String,
    pub percent: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct BackpressureReport {
    pub signals: Vec<BackpressureSignal>,
}

impl BackpressureReport {
    pub fn is_empty(&self) -> bool {
        self.signals.is_empty()
    }

    pub fn header_value(&self) -> Option<String> {
        if self.signals.is_empty() {
            return None;
        }
        Some(
            self.signals
                .iter()
                .map(|s| format!("{}={}", s.bucket, s.percent))
                .collect::<Vec<_>>()
                .join(","),
        )
    }

    fn push_if_over(&mut self, bucket: impl Into<String>, usage: u64, soft_limit: u64) {
        if let Some(percent) = usage_percent(usage, soft_limit) {
            self.signals.push(BackpressureSignal {
                bucket: bucket.into(),
                percent,
            });
        }
    }

    pub fn merge(&mut self, other: BackpressureReport) {
        self.signals.extend(other.signals);
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TenantWorkerReport {
    pub tenant_id: Id,
    pub priority: Priority,
    pub lifecycle: LifecycleSweepReport,
    pub gc: GcSweepReport,
}

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct PolicyWorkerReport {
    pub tenants_considered: usize,
    pub tenants_scheduled: usize,
    pub lifecycle: LifecycleSweepReport,
    pub gc: GcSweepReport,
    pub tenants: Vec<TenantWorkerReport>,
}

#[derive(Debug, Clone)]
pub struct RecordUpdate {
    pub tenant_id: Id,
    pub record_id: Id,
    pub expected_version: Option<u64>,
    pub namespace: Option<Vec<String>>,
    pub scope: Option<serde_json::Value>,
    pub text: Option<Option<String>>,
    pub metadata: Option<serde_json::Value>,
    pub tags: Option<Vec<String>>,
    pub source: Option<Option<SourceRef>>,
    pub embedding: Option<Option<Vec<f32>>>,
    pub policy: Option<crate::record::IndexingPolicy>,
    pub status: Option<RecordStatus>,
    pub expires_at_ms: Option<Option<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TenantMetaEntry {
    pub tenant_id: Id,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at_ms: u64,
}

#[derive(Default, Debug)]
struct State {
    /// Aggregate cache for backpressure/quota: value + computed-at instant.
    tenant_aggregate_cache: BTreeMap<String, (TenantAggregates, std::time::Instant)>,
    tenants: BTreeMap<String, Tenant>,
    agents: BTreeMap<String, Agent>,
    sessions: BTreeMap<String, Session>,
    /// Keyed by artifact_id_str. Hydrated at startup from
    /// `Persist::scan_all_artifacts`; updated on every `create_artifact`.
    artifacts: BTreeMap<String, Artifact>,
    /// Keyed by (session_id, seq) for ordered scan. In-memory runtimes keep all
    /// events here. Persisted runtimes keep a bounded hot tail per session so
    /// append+fetch can avoid a storage read while older reads fall back to
    /// RocksDB.
    events: BTreeMap<(String, u64), EventEnvelope>,
    /// Maps `event_id -> (session_id_str, seq)` for in-memory `event_by_id`
    /// lookup. Only populated in the in-memory backend (the KV backend
    /// resolves through the `event_by_id` secondary index in storage).
    /// Reset on every restart, which is fine — the in-memory backend
    /// doesn't persist anyway.
    event_by_id: BTreeMap<String, (String, u64)>,
    /// In-memory export ledger, keyed `tenant_id -> (durable-order seq -> item)`.
    /// Only populated for in-memory runtimes; persisted runtimes keep the
    /// authoritative ledger in the KV store. Both backends emit the same
    /// `ledger:` cursor format so the export/changefeed surface is uniform.
    export_ledger: BTreeMap<String, BTreeMap<u64, ExportLedgerItem>>,
    /// Next per-tenant in-memory export ledger sequence. Starts at 1 to match
    /// the persist sequencer (first committed item gets seq 1).
    export_ledger_next_seq: BTreeMap<String, u64>,
    /// Alias / external_key uniqueness maps. Each entry is the canonical
    /// "is this alias claimed?" lookup; on `create_*` we check the map under
    /// the state lock and reject collisions with `AliasConflict`.
    ///
    /// Tenant alias scope is global (external_key is a global identifier).
    /// Agent and session alias scopes are tenant-local — encoded as
    /// "{tenant_id}::{alias}" so we can use a flat BTreeMap.
    tenant_aliases: BTreeMap<String, String>,
    agent_aliases: BTreeMap<String, String>,
    session_aliases: BTreeMap<String, String>,
    /// V2.2 searchable records, keyed by `record_id` string. Hydrated at
    /// startup from `Persist::scan_all_records`; updated on every
    /// `write_record` / `delete_record`.
    records: BTreeMap<String, SearchableRecord>,
    /// Tenant-local uniqueness map for caller-stable record external ids.
    record_external_ids: BTreeMap<String, String>,
    /// Tenant-scoped small metadata KV, keyed as `{tenant_id}::{key}`.
    tenant_meta: BTreeMap<String, TenantMetaEntry>,
    /// Reverse mapping `DocId -> record_id_str`. Search hits come back
    /// from the text + vector indexes as `DocId`s; we walk this map to
    /// recover the record_id and post-filter from `records`. The
    /// `DocId` itself is minted by [`mint_doc_id`] from a per-runtime
    /// counter, so it is unique and stable for the runtime's lifetime
    /// (rebuilt on hydration).
    doc_to_record: BTreeMap<DocId, String>,
    /// Forward mapping `record_id_str -> DocId`. Used on
    /// `delete_record` to remove the index entry and the reverse
    /// mapping in one shot.
    record_to_doc: BTreeMap<String, DocId>,
    /// Tenant-scoped searchable-record commit sequence by record id. Used by
    /// read-after-write repair when the local index watermark lags behind a
    /// caller's requested visibility token.
    record_commit_seq: BTreeMap<String, u64>,
    /// Next tenant-scoped searchable-record commit sequence.
    next_record_commit_seq: u64,
    /// Highest searchable-record commit sequence fully reflected in the local
    /// text/vector indexes.
    local_record_index_watermark: u64,
    /// Highest searchable-record commit sequence fully reflected in the
    /// GSI-like global placement/discovery index.
    global_record_index_watermark: u64,
    record_index_repairs_total: u64,
    record_index_repair_candidates_total: u64,
    record_index_worker_failures_total: u64,
    global_record_index_apply_failures_total: u64,
    /// Outbox mutations skipped because the referenced record row is missing
    /// from both state.records and the persist layer (typically GC purged
    /// it). Incrementing this avoids head-of-line-blocking the entire queue
    /// behind one unreparable mutation; operators should alert on non-zero
    /// values to detect the underlying state divergence.
    record_index_outbox_skipped_total: u64,
    /// Counter behind [`mint_doc_id`]. Lives in `State` so the same
    /// state mutex serializes counter increments alongside record
    /// inserts.
    next_doc_seq: u64,
    /// Watermark of the last record-index snapshot written (or restored)
    /// by this runtime. The snapshot writer skips work until the live
    /// watermark advances far enough past this.
    last_record_index_snapshot_watermark: u64,
    record_index_snapshot_writes_total: u64,
    /// True when startup restored the indexes from a persisted snapshot
    /// (plus delta replay) instead of the full record scan.
    record_index_snapshot_loaded: bool,
}

fn insert_runtime_event_cache(
    state: &mut State,
    persist_enabled: bool,
    session_id: &str,
    event: EventEnvelope,
) {
    let seq = event.seq;
    if !persist_enabled {
        // In-memory runtimes carry their own append-ordered export ledger so
        // the changefeed emits `ledger:` cursors identical in shape to the
        // persisted backend. The persisted backend writes its ledger through
        // the durable KV sequencer instead (see `persist.rs`).
        push_in_memory_export_ledger_item(state, export_ledger_item_from_event(event.clone()));
    }
    state.events.insert((session_id.to_string(), seq), event);

    if !persist_enabled {
        return;
    }

    let hot_event_cache_per_session = *PERSISTED_HOT_EVENT_CACHE_PER_SESSION.get_or_init(|| {
        std::env::var("AGENTDB_PERSISTED_HOT_EVENT_CACHE_PER_SESSION")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_PERSISTED_HOT_EVENT_CACHE_PER_SESSION)
    });
    if hot_event_cache_per_session == 0 {
        state.events.remove(&(session_id.to_string(), seq));
        return;
    }

    if seq < hot_event_cache_per_session {
        return;
    }

    let stale_seq = seq - hot_event_cache_per_session;
    state.events.remove(&(session_id.to_string(), stale_seq));
}

/// Public `ledger:` cursor for an in-memory ledger entry. Mirrors the persist
/// ledger key layout (`export_ledger_item_cursor_from_key` in `persist.rs`):
/// events key on `event/{session}/{event_id}`, every other object type keys on
/// `object/{object_type}/{object_id}`. The 20-digit zero-padded sequence makes
/// lexical cursor order equal to durable commit order.
fn in_memory_export_ledger_cursor(tenant_id: &str, seq: u64, item: &ExportLedgerItem) -> String {
    if item.object_type == "event" {
        let session_id = item
            .session_id
            .as_ref()
            .map(|id| id.as_str())
            .unwrap_or_default();
        format!(
            "ledger:{tenant_id}/v2/{seq:020}/event/{session_id}/{}",
            item.object_id
        )
    } else {
        format!(
            "ledger:{tenant_id}/v2/{seq:020}/object/{}/{}",
            item.object_type, item.object_id
        )
    }
}

/// Append `item` to the in-memory export ledger for its tenant, assigning the
/// next per-tenant sequence. Caller holds the state lock, so sequence
/// assignment is serialized (the same guarantee the persist sequencer gives).
fn push_in_memory_export_ledger_item(state: &mut State, item: ExportLedgerItem) {
    let tenant_id = item.tenant_id.as_str().to_string();
    let seq = state.export_ledger_next_seq.entry(tenant_id.clone()).or_insert(1);
    let assigned = *seq;
    *seq = seq.saturating_add(1);
    state
        .export_ledger
        .entry(tenant_id)
        .or_default()
        .insert(assigned, item);
}

fn scoped_alias_key(tenant_id: &str, alias: &str) -> String {
    format!("{tenant_id}::{alias}")
}

fn policy_for_tenant_in_state(s: &State, tenant_id: &Id) -> RuntimePolicy {
    s.tenants
        .get(tenant_id.as_str())
        .map(|t| RuntimePolicy::from_json(&t.policy))
        .unwrap_or_default()
}

fn active_tenant_policy_for_id(s: &State, tenant_id: &Id) -> Option<RuntimePolicy> {
    s.tenants
        .get(tenant_id.as_str())
        .filter(|t| t.deleted_at_ms.is_none())
        .map(|t| RuntimePolicy::from_json(&t.policy))
}

fn lifecycle_rank(l: Lifecycle) -> u8 {
    match l {
        Lifecycle::Warm => 0,
        Lifecycle::Cold => 1,
        Lifecycle::Archive => 2,
        Lifecycle::DerivativeOnly => 3,
    }
}

fn lifecycle_report_action(report: &mut LifecycleSweepReport, from: Lifecycle, to: Lifecycle) {
    if lifecycle_rank(to) <= lifecycle_rank(from) {
        return;
    }
    if from == Lifecycle::Warm && lifecycle_rank(to) >= lifecycle_rank(Lifecycle::Cold) {
        report.seal_segment_actions += 1;
        report.move_to_cold_actions += 1;
    }
    if lifecycle_rank(to) >= lifecycle_rank(Lifecycle::Archive) {
        report.archive_actions += 1;
    }
}

fn gc_work_allowed(report: &mut GcSweepReport, policy: &RuntimePolicy) -> bool {
    if let Some(soft) = policy.quota.soft_max_gc_work_per_sweep {
        if report.work_units >= soft {
            report.soft_work_limit_exceeded = true;
        }
    }
    if let Some(max) = policy.quota.max_gc_work_per_sweep {
        if report.work_units >= max {
            report.hard_work_limit_hit = true;
            return false;
        }
    }
    true
}

fn lifecycle_work_allowed(report: &mut LifecycleSweepReport, policy: &RuntimePolicy) -> bool {
    if let Some(soft) = policy.quota.soft_max_lifecycle_work_per_sweep {
        if report.work_units >= soft {
            report.soft_work_limit_exceeded = true;
        }
    }
    if let Some(max) = policy.quota.max_lifecycle_work_per_sweep {
        if report.work_units >= max {
            report.hard_work_limit_hit = true;
            return false;
        }
    }
    true
}

fn lifecycle_target(current: Lifecycle, idle_ms: u64, policy: &RuntimePolicy) -> Lifecycle {
    let mut target = current;
    if policy
        .lifecycle
        .cold_after_ms
        .is_some_and(|threshold| idle_ms >= threshold)
    {
        target = Lifecycle::Cold;
    }
    if policy
        .lifecycle
        .archive_after_ms
        .is_some_and(|threshold| idle_ms >= threshold)
    {
        target = Lifecycle::Archive;
    }
    if lifecycle_rank(target) > lifecycle_rank(current) {
        target
    } else {
        current
    }
}

fn validate_append_secret_handling_policy(
    payload: &serde_json::Value,
    policy: &RuntimePolicy,
) -> Result<()> {
    if !policy.secret_handling.deny_raw_secret_fields {
        return Ok(());
    }
    if let Some(path) = first_raw_secret_field_path(payload) {
        return Err(Error::new(
            ErrorCode::PolicyDenied,
            format!(
                "policy_version {} denies append: payload field `{path}` looks like a raw secret",
                policy.version
            ),
        )
        .with_suggestion(
            "store credentials in the control-plane vault and reference them with credential_refs",
        ));
    }
    Ok(())
}

fn sanitize_append_payload(kind: &str, payload: &mut serde_json::Value) {
    if kind != "tool_result" {
        return;
    }
    let Some(obj) = payload.as_object_mut() else {
        return;
    };
    for field in ["summary", "output", "metadata"] {
        if let Some(value) = obj.get_mut(field) {
            sanitize_tool_output_value(value);
        }
    }
}

fn sanitize_tool_output_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => {
            *text = sanitize_secret_text(text);
        }
        serde_json::Value::Array(items) => {
            for item in items {
                sanitize_tool_output_value(item);
            }
        }
        serde_json::Value::Object(obj) => {
            for child in obj.values_mut() {
                sanitize_tool_output_value(child);
            }
        }
        _ => {}
    }
}

fn sanitize_secret_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut segment = String::new();
    let mut segment_is_space = None;
    let mut redact_after_bearer = false;
    for ch in text.chars() {
        let is_space = ch.is_whitespace();
        match segment_is_space {
            Some(current) if current == is_space => segment.push(ch),
            Some(current) => {
                flush_sanitized_segment(&mut out, &segment, current, &mut redact_after_bearer);
                segment.clear();
                segment.push(ch);
                segment_is_space = Some(is_space);
            }
            None => {
                segment.push(ch);
                segment_is_space = Some(is_space);
            }
        }
    }
    if let Some(is_space) = segment_is_space {
        flush_sanitized_segment(&mut out, &segment, is_space, &mut redact_after_bearer);
    }
    out
}

fn flush_sanitized_segment(
    out: &mut String,
    segment: &str,
    is_space: bool,
    redact_after_bearer: &mut bool,
) {
    if is_space {
        out.push_str(segment);
        return;
    }
    if *redact_after_bearer && looks_like_bearer_secret(segment) {
        out.push_str("[REDACTED]");
        *redact_after_bearer = false;
        return;
    }
    let sanitized = sanitize_secret_token(segment);
    if normalized_secret_key(segment) == "bearer" {
        *redact_after_bearer = true;
    }
    out.push_str(&sanitized);
}

fn sanitize_secret_token(token: &str) -> String {
    if let Some((idx, sep)) = token
        .char_indices()
        .find(|(_, ch)| *ch == '=' || *ch == ':')
    {
        let key = &token[..idx];
        let value_start = idx + sep.len_utf8();
        if value_start < token.len() && is_secret_assignment_key(key) {
            return format!("{}{}[REDACTED]", key, sep);
        }
    }
    if looks_like_secret_token(token) {
        "[REDACTED]".into()
    } else {
        token.to_string()
    }
}

fn is_secret_assignment_key(key: &str) -> bool {
    matches!(
        normalized_secret_key(key).as_str(),
        "accesstoken"
            | "apikey"
            | "authtoken"
            | "authorization"
            | "clientsecret"
            | "password"
            | "passwd"
            | "privatekey"
            | "refreshtoken"
            | "secretaccesskey"
    )
}

fn looks_like_secret_token(token: &str) -> bool {
    let trimmed = token.trim_matches(|ch: char| matches!(ch, '"' | '\'' | ',' | ';'));
    (trimmed.starts_with("sk-") && trimmed.len() >= 19)
        || (trimmed.starts_with("ghp_") && trimmed.len() >= 24)
        || (trimmed.starts_with("gho_") && trimmed.len() >= 24)
        || (trimmed.starts_with("ghu_") && trimmed.len() >= 24)
        || (trimmed.starts_with("ghs_") && trimmed.len() >= 24)
        || (trimmed.starts_with("ghr_") && trimmed.len() >= 24)
        || (trimmed.starts_with("github_pat_") && trimmed.len() >= 31)
        || ((trimmed.starts_with("AKIA") || trimmed.starts_with("ASIA"))
            && trimmed.len() == 20
            && trimmed.chars().all(|ch| ch.is_ascii_alphanumeric()))
}

fn looks_like_bearer_secret(token: &str) -> bool {
    let trimmed = token.trim_matches(|ch: char| matches!(ch, '"' | '\'' | ',' | ';'));
    trimmed.len() >= 20
        && trimmed.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '~' | '+' | '/' | '=' | '-')
        })
}

fn normalized_secret_key(key: &str) -> String {
    key.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| ch.to_lowercase())
        .collect()
}

fn first_raw_secret_field_path(payload: &serde_json::Value) -> Option<String> {
    fn visit(value: &serde_json::Value, path: &mut Vec<String>) -> Option<String> {
        match value {
            serde_json::Value::Object(obj) => {
                for (key, child) in obj {
                    path.push(key.clone());
                    if is_raw_secret_field_name(key) {
                        return Some(path.join("."));
                    }
                    if let Some(found) = visit(child, path) {
                        return Some(found);
                    }
                    path.pop();
                }
                None
            }
            serde_json::Value::Array(items) => {
                for (idx, child) in items.iter().enumerate() {
                    path.push(format!("[{idx}]"));
                    if let Some(found) = visit(child, path) {
                        return Some(found);
                    }
                    path.pop();
                }
                None
            }
            _ => None,
        }
    }

    visit(payload, &mut Vec::new())
}

fn is_raw_secret_field_name(key: &str) -> bool {
    let normalized = normalized_secret_key(key);
    matches!(
        normalized.as_str(),
        "accesstoken"
            | "apikey"
            | "authtoken"
            | "authorization"
            | "bearertoken"
            | "clientsecret"
            | "openaiapikey"
            | "password"
            | "passwd"
            | "personalaccesstoken"
            | "privatekey"
            | "pwd"
            | "refreshtoken"
            | "secret"
            | "secretaccesskey"
            | "token"
    )
}

fn priority_rank(p: Priority) -> u8 {
    match p {
        Priority::Low => 0,
        Priority::Normal => 1,
        Priority::High => 2,
        Priority::Critical => 3,
    }
}

/// Provisions a tenant's data-encryption keys when the tenant is created.
///
/// In a cluster the key catalog is shared and the seal/open path is read-only,
/// so a tenant's DEK must be minted by one coordinated caller — the runtime
/// calls this on the (leader) node that creates the tenant, before the tenant's
/// data can be written. Implemented by the cluster key provider; unset for
/// deployments whose provider mints on demand (single-node).
#[async_trait::async_trait]
pub trait TenantKeyProvisioner: Send + Sync {
    async fn provision_tenant(&self, tenant_id: &str) -> Result<()>;
}

/// Rotates a tenant's data-encryption keys (an admin operation).
///
/// Rotation mints a new active DEK version for each provisioned purpose and
/// demotes the previous version to read-only — old ciphertext stays readable
/// (each envelope records the version it was sealed under), while new writes
/// seal under the new key. Implemented by the key catalog / cluster key
/// provider; unset when encryption is disabled. The runtime invalidates its
/// keyring cache after a successful rotation so the seal path picks up the new
/// active key without waiting for the cache TTL.
#[async_trait::async_trait]
pub trait TenantKeyRotator: Send + Sync {
    async fn rotate_tenant(&self, tenant_id: &str) -> Result<()>;
}

/// Revokes a tenant's data-encryption keys (an admin operation).
///
/// `destroy = false` is a **soft** revoke: every DEK version is marked revoked,
/// so the serve path refuses to unwrap it and the tenant's encrypted data is
/// immediately unreadable — but the wrapped bytes remain, so it is reversible.
/// `destroy = true` is a **crypto-shred**: the wrapped DEK material is deleted,
/// so the tenant's encrypted data can never be decrypted again (irreversible).
/// Implemented by the key catalog / cluster key provider; unset when encryption
/// is disabled. The runtime invalidates its keyring cache after revocation.
#[async_trait::async_trait]
pub trait TenantKeyRevoker: Send + Sync {
    async fn revoke_tenant(&self, tenant_id: &str, destroy: bool) -> Result<()>;
}

/// Retires a tenant's drained DEK versions (an admin operation, run after a
/// clean re-encryption). Marks as `retired` every read-only version of
/// `(tenant, purpose)` whose version number is NOT in `versions_in_use` — the
/// complete set of versions the runtime observed still in use after scanning
/// every stored object of that purpose. Retiring a still-referenced version
/// would make its data unreadable, so the caller owns that safety guarantee;
/// this only ever touches already-rotated-away (read-only) versions.
/// Implemented by the key catalog / cluster key provider; unset when encryption
/// is disabled.
#[async_trait::async_trait]
pub trait TenantKeyRetirer: Send + Sync {
    async fn retire_drained_versions(
        &self,
        tenant_id: &str,
        purpose: KeyPurpose,
        versions_in_use: &[u32],
    ) -> Result<usize>;
}

/// Outcome of [`Runtime::retire_tenant_dek_versions`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetireReport {
    /// Metadata DEK versions marked retired.
    pub metadata_versions_retired: usize,
    /// Content DEK versions marked retired.
    pub content_versions_retired: usize,
    /// `false` when the content backend could not be enumerated, so content
    /// drainage could not be verified and no content version was retired.
    pub content_verified: bool,
}

pub struct Runtime {
    state: Mutex<State>,
    hydration_mode: HydrationMode,
    session_commit_locks: StdMutex<BTreeMap<String, Weak<Mutex<()>>>>,
    session_append_queues: StdMutex<BTreeMap<String, Weak<SessionAppendQueue>>>,
    idem: IdempotencyStore,
    persist: Option<Persist>,
    content: Arc<dyn ContentBackend>,
    content_crypto: Arc<dyn ContentCryptoAdapter>,
    /// Tenant envelope keyring for durable content (segments, archives, replay
    /// prefixes, artifacts). When enabled, content is sealed as an
    /// `AGDB-ENVELOPE-v2` frame before it reaches the content backend; when
    /// disabled (the default), content takes the legacy v1/identity path. Reads
    /// detect the frame version, so a disabled→enabled rollout stays readable.
    keyring: Arc<dyn EnvelopeCrypto>,
    /// Optional: mints a tenant's DEKs at creation time (cluster key provider).
    /// `None` when the keyring's provider mints on demand.
    key_provisioner: Option<Arc<dyn TenantKeyProvisioner>>,
    /// Optional: rotates a tenant's DEKs on the admin path. Installed once at
    /// startup via [`Runtime::set_key_rotator`] (the provider Arc is owned by
    /// the server wiring), so it is set out-of-band rather than through the many
    /// runtime constructors. Unset when encryption is disabled.
    key_rotator: OnceLock<Arc<dyn TenantKeyRotator>>,
    /// Optional: revokes / crypto-shreds a tenant's DEKs on the admin path.
    /// Installed once at startup via [`Runtime::set_key_revoker`]. Unset when
    /// encryption is disabled.
    key_revoker: OnceLock<Arc<dyn TenantKeyRevoker>>,
    /// Optional: retires a tenant's drained DEK versions on the admin path.
    /// Installed once at startup via [`Runtime::set_key_retirer`]. Unset when
    /// encryption is disabled.
    key_retirer: OnceLock<Arc<dyn TenantKeyRetirer>>,
    kms: Arc<dyn KmsKeyResolver>,
    /// Per-(tenant, namespace) BM25 text index for V2.2 searchable
    /// records. Wrapped in a `std::sync::Mutex` because
    /// `TextIndex::insert` takes `&mut self`. All operations are short
    /// (CPU-bound; no I/O) so a blocking mutex is fine.
    ///
    /// The `path` parameter we pass into the index is
    /// `"{tenant_id}::{namespace_path}"`, giving each tenant +
    /// namespace its own segment — the `path`-isolated semantics in
    /// [`InMemoryInverted`] are what enforce search isolation.
    text_index: StdMutex<InMemoryInverted>,
    /// Per-(tenant, namespace) HNSW vector index. Same locking story
    /// as `text_index`. The HNSW backend already keeps an internal
    /// `parking_lot::Mutex` per path; the outer `StdMutex` here only
    /// guards the `&mut self` requirement of the trait surface.
    vector_index: StdMutex<HnswVectorIndex>,
    global_record_index: Arc<dyn GlobalRecordIndex>,
    tablet_router: Option<Arc<RuntimeTabletRouterKvStore>>,
}

struct SessionAppendQueue {
    inner: Mutex<SessionAppendQueueState>,
}

#[derive(Default)]
struct SessionAppendQueueState {
    running: bool,
    jobs: VecDeque<SimpleAppendJob>,
}

struct SimpleAppendJob {
    req: AppendRequest,
    fetch_recent: Option<usize>,
    context_max_bytes: Option<usize>,
    done: oneshot::Sender<Result<AppendCommitResponse>>,
}

struct PreparedSimpleAppend {
    tenant_id: Id,
    envelope: EventEnvelope,
    appended: AppendResponse,
    policy: RuntimePolicy,
    session_after_append: Session,
    fetch_recent: Option<usize>,
}

#[derive(Clone)]
struct RuntimeTabletKvStore {
    store: Arc<TabletKvStore>,
    state_machine: Arc<dyn TabletStateMachine>,
}

impl RuntimeTabletKvStore {
    fn new(
        store: TabletKvStore,
        state_machine: Arc<dyn TabletStateMachine>,
    ) -> agentdb_storage::Result<Self> {
        state_machine.apply_committed()?;
        Ok(Self {
            store: Arc::new(store),
            state_machine,
        })
    }

    fn single_node(
        kv: Arc<dyn KvStore + Send + Sync>,
        tablet_id: i64,
    ) -> agentdb_storage::Result<Self> {
        let store = TabletKvStore::new(kv.clone(), tablet_id)?;
        let state_machine = Arc::new(LoggedTabletStateMachine::single_node(
            kv.clone(),
            tablet_id,
        )?);
        Self::new(store, state_machine)
    }

    fn logged(
        store: TabletKvStore,
        tablet_id: i64,
        log: Arc<dyn ReplicatedLog + Send + Sync>,
    ) -> agentdb_storage::Result<(Self, Arc<LoggedTabletStateMachine>)> {
        let state_machine = Arc::new(LoggedTabletStateMachine::new(
            tablet_id,
            log,
            store.clone(),
        )?);
        let kv_store = Self::new(store, state_machine.clone())?;
        Ok((kv_store, state_machine))
    }
}

impl KvStore for RuntimeTabletKvStore {
    fn get(&self, key: &[u8]) -> agentdb_storage::Result<Option<Vec<u8>>> {
        self.state_machine.apply_committed()?;
        self.store.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> agentdb_storage::Result<()> {
        self.write_batch(vec![WriteOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }])
    }

    fn delete(&self, key: &[u8]) -> agentdb_storage::Result<()> {
        self.write_batch(vec![WriteOp::Delete { key: key.to_vec() }])
    }

    fn scan_prefix(&self, prefix: &[u8]) -> agentdb_storage::Result<KvIter> {
        self.state_machine.apply_committed()?;
        self.store.scan_prefix(prefix)
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> agentdb_storage::Result<KvIter> {
        self.state_machine.apply_committed()?;
        self.store.scan_prefix_from(prefix, start)
    }

    fn write_batch(&self, ops: Vec<WriteOp>) -> agentdb_storage::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        self.state_machine.commit(TabletCommand::WriteBatch {
            ops: ops.into_iter().map(TabletWriteOp::from).collect(),
        })?;
        Ok(())
    }

    fn snapshot<'a>(&'a self) -> agentdb_storage::Result<Box<dyn KvSnapshot + 'a>> {
        self.state_machine.apply_committed()?;
        self.store.snapshot()
    }

    fn snapshot_owned(self: Arc<Self>) -> agentdb_storage::Result<Arc<dyn KvSnapshot>> {
        self.state_machine.apply_committed()?;
        self.store.clone().snapshot_owned()
    }
}

struct RuntimeTabletHandle {
    kv_store: Arc<RuntimeTabletKvStore>,
    state_machine: Arc<LoggedTabletStateMachine>,
}

struct RuntimeTabletRouterKvStore {
    store_lifecycle: Arc<dyn TabletStoreLifecycle>,
    default_tablet_id: i64,
    tablets: RwLock<BTreeMap<i64, RuntimeTabletHandle>>,
}

impl RuntimeTabletRouterKvStore {
    fn new(
        kv: Arc<dyn KvStore + Send + Sync>,
        default_tablet_id: i64,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> agentdb_storage::Result<Self> {
        Self::new_with_lifecycle(
            Arc::new(PrefixScopedTabletStore::new(kv)),
            default_tablet_id,
            logs,
        )
    }

    fn new_with_lifecycle(
        store_lifecycle: Arc<dyn TabletStoreLifecycle>,
        default_tablet_id: i64,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> agentdb_storage::Result<Self> {
        if logs.is_empty() {
            return Err(agentdb_storage::Error::Other(
                "runtime tablet router requires at least one tablet log".into(),
            ));
        }
        if !logs.contains_key(&default_tablet_id) {
            return Err(agentdb_storage::Error::Other(format!(
                "runtime tablet router default tablet {default_tablet_id} is not registered"
            )));
        }
        let mut tablets = BTreeMap::new();
        for (tablet_id, log) in logs {
            let store = store_lifecycle.open_tablet_store(tablet_id)?;
            let (kv_store, state_machine) = RuntimeTabletKvStore::logged(store, tablet_id, log)?;
            tablets.insert(
                tablet_id,
                RuntimeTabletHandle {
                    kv_store: Arc::new(kv_store),
                    state_machine,
                },
            );
        }
        Ok(Self {
            store_lifecycle,
            default_tablet_id,
            tablets: RwLock::new(tablets),
        })
    }

    fn register(
        &self,
        tablet_id: i64,
        log: Arc<dyn ReplicatedLog + Send + Sync>,
    ) -> agentdb_storage::Result<bool> {
        let mut tablets = self
            .tablets
            .write()
            .expect("runtime tablet router lock poisoned");
        if tablets.contains_key(&tablet_id) {
            return Ok(false);
        }
        let store = self.store_lifecycle.open_tablet_store(tablet_id)?;
        let (kv_store, state_machine) = RuntimeTabletKvStore::logged(store, tablet_id, log)?;
        tablets.insert(
            tablet_id,
            RuntimeTabletHandle {
                kv_store: Arc::new(kv_store),
                state_machine,
            },
        );
        Ok(true)
    }

    fn unregister(&self, tablet_id: i64) -> bool {
        self.tablets
            .write()
            .expect("runtime tablet router lock poisoned")
            .remove(&tablet_id)
            .is_some()
    }

    fn tablet_ids(&self) -> BTreeSet<i64> {
        self.tablets
            .read()
            .expect("runtime tablet router lock poisoned")
            .keys()
            .copied()
            .collect()
    }

    fn current_tablet_id() -> Option<i64> {
        CURRENT_RUNTIME_TABLET_ID
            .try_with(|tablet_id| *tablet_id)
            .ok()
    }

    fn resolve_tablet_id(&self) -> agentdb_storage::Result<Option<i64>> {
        let Some(tablet_id) = Self::current_tablet_id() else {
            return Ok(None);
        };
        if self
            .tablets
            .read()
            .expect("runtime tablet router lock poisoned")
            .contains_key(&tablet_id)
        {
            Ok(Some(tablet_id))
        } else {
            Err(agentdb_storage::Error::Other(format!(
                "runtime tablet {tablet_id} is not registered on this node"
            )))
        }
    }

    fn with_handle<T>(
        &self,
        tablet_id: i64,
        f: impl FnOnce(&RuntimeTabletHandle) -> agentdb_storage::Result<T>,
    ) -> agentdb_storage::Result<T> {
        let tablets = self
            .tablets
            .read()
            .expect("runtime tablet router lock poisoned");
        let handle = tablets.get(&tablet_id).ok_or_else(|| {
            agentdb_storage::Error::Other(format!(
                "runtime tablet {tablet_id} is not registered on this node"
            ))
        })?;
        f(handle)
    }

    fn export_snapshot_payload(&self, tablet_id: i64) -> agentdb_storage::Result<Vec<u8>> {
        self.with_handle(tablet_id, |handle| {
            // Fast path: eager whole-tablet SST movement (hardening §7), when
            // enabled and the tablet clears the size threshold. This ships an
            // `ADSS` payload the target bulk-loads via `IngestExternalFile`
            // instead of replaying every key. Falls back to the logical
            // compact-encoded snapshot when disabled, below threshold, empty,
            // or unsupported by the store backend. Shard-range (split/merge)
            // moves never reach here — they use the `_for_shard_range` export.
            if sst_movement_enabled() {
                if let Some(payload) = try_export_sst_snapshot_payload(&handle.state_machine)? {
                    return Ok(payload);
                }
            }
            Ok(handle.state_machine.export_snapshot()?.encode())
        })
    }

    fn export_snapshot_payload_for_shard_range(
        &self,
        source_tablet_id: i64,
        filter: TabletSnapshotShardFilter,
    ) -> agentdb_storage::Result<Vec<u8>> {
        self.with_handle(source_tablet_id, |handle| {
            Ok(handle
                .state_machine
                .export_snapshot_for_shard_range(filter)?
                .encode())
        })
    }

    fn install_snapshot_payload(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> agentdb_storage::Result<()> {
        // Whole-tablet SST movement payload (`ADSS`): range-delete the tablet
        // scope and bulk-load the SST. Any build recognizing the format can
        // install it regardless of whether *its own* movement flag is set, so
        // a rolling upgrade stays safe (senders are enabled last).
        if is_sst_snapshot_payload(payload) {
            return self.install_sst_snapshot_payload(tablet_id, payload);
        }
        let snapshot = TabletStateSnapshot::decode(payload).map_err(|err| {
            agentdb_storage::Error::Other(format!(
                "decode tablet {tablet_id} runtime snapshot: {err}"
            ))
        })?;
        self.with_handle(tablet_id, |handle| {
            handle.state_machine.install_snapshot(&snapshot)
        })
    }

    fn install_sst_snapshot_payload(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> agentdb_storage::Result<()> {
        let decoded = decode_sst_snapshot_payload(payload)?;
        // The SST is a byte-copy carrying the *source* tablet's key prefix, so
        // it is only valid when the move preserves the tablet id (the raft
        // whole-tablet replacement / add-replica path always does). A mismatch
        // means a caller wrongly routed a retarget through the SST path.
        if decoded.tablet_id != tablet_id {
            return Err(agentdb_storage::Error::Other(format!(
                "SST snapshot tablet_id {} does not match target tablet_id {tablet_id}; SST movement cannot retarget",
                decoded.tablet_id
            )));
        }
        self.with_handle(tablet_id, |handle| {
            // Place the incoming SST on the DB's own filesystem, and refuse the
            // install when it would not leave the disk reservation intact. On
            // the receiving side we fail fast (rather than fall back) so the
            // node surfaces "add disk" instead of driving the data disk to zero
            // and wedging every future write, compaction, and recovery — the
            // snapshot-apply doom loop TiKV's `reserve-space` guards against.
            let Some(data_dir) = handle.state_machine.store().data_dir() else {
                return Err(agentdb_storage::Error::Other(format!(
                    "tablet {tablet_id} store backend cannot ingest an SST movement payload"
                )));
            };
            let scratch_root = movement_scratch_root(&data_dir)?;
            let reserve = movement_disk_reserve_bytes();
            let needed = decoded.sst.len() as u64;
            if let Some(available) = agentdb_storage::available_disk_bytes(&scratch_root) {
                if available < needed.saturating_add(reserve) {
                    return Err(agentdb_storage::Error::Other(format!(
                        "tablet {tablet_id} SST movement install needs {needed} bytes + {reserve} reserve but only {available} free; add disk (AGENTDB_MOVEMENT_DISK_RESERVE_BYTES)"
                    )));
                }
            }
            let dir = tempfile::tempdir_in(&scratch_root).map_err(|err| {
                agentdb_storage::Error::Other(format!("sst install tempdir: {err}"))
            })?;
            let path = dir.path().join("tablet.sst");
            std::fs::write(&path, decoded.sst)
                .map_err(|err| agentdb_storage::Error::Other(format!("write sst: {err}")))?;
            if !handle
                .state_machine
                .install_snapshot_sst(&path, decoded.applied_index)?
            {
                return Err(agentdb_storage::Error::Other(format!(
                    "tablet {tablet_id} store backend cannot ingest an SST movement payload"
                )));
            }
            Ok(())
        })
    }

    fn install_snapshot_payload_merge(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> agentdb_storage::Result<()> {
        let snapshot = TabletStateSnapshot::decode(payload).map_err(|err| {
            agentdb_storage::Error::Other(format!(
                "decode tablet {tablet_id} runtime snapshot: {err}"
            ))
        })?;
        self.with_handle(tablet_id, |handle| {
            handle.state_machine.install_snapshot_merge(&snapshot)
        })
    }

    fn compact_log_through(
        &self,
        tablet_id: i64,
        up_to_index: u64,
    ) -> agentdb_storage::Result<usize> {
        self.with_handle(tablet_id, |handle| {
            handle.state_machine.compact_log_through(up_to_index)
        })
    }

    fn storage_stats(&self) -> agentdb_storage::Result<Vec<TabletStorageStats>> {
        let tablets = self
            .tablets
            .read()
            .expect("runtime tablet router lock poisoned");
        let mut stats = Vec::with_capacity(tablets.len());
        for handle in tablets.values() {
            stats.push(handle.state_machine.storage_stats()?);
        }
        stats.sort_by_key(|stats| stats.tablet_id);
        Ok(stats)
    }

    fn domain_metrics(
        &self,
    ) -> agentdb_storage::Result<Vec<agentdb_storage::TabletStoreDomainMetrics>> {
        self.store_lifecycle.domain_metrics()
    }

    fn compact_tablet_store(&self, tablet_id: i64) -> agentdb_storage::Result<()> {
        self.store_lifecycle.compact_tablet(tablet_id)
    }

    fn checkpoint_tablet_store(
        &self,
        tablet_id: i64,
        out_dir: &std::path::Path,
    ) -> agentdb_storage::Result<TabletStoreManifest> {
        let applied_index = self.with_handle(tablet_id, |handle| {
            handle.state_machine.apply_committed()?;
            Ok(handle.state_machine.applied_index())
        })?;
        let mut manifest = self.store_lifecycle.checkpoint_tablet(tablet_id, out_dir)?;
        manifest.latest_applied_index = applied_index;
        Ok(manifest)
    }

    fn applied_index(&self, tablet_id: i64) -> agentdb_storage::Result<u64> {
        self.with_handle(tablet_id, |handle| {
            handle.state_machine.apply_committed()?;
            Ok(handle.state_machine.applied_index())
        })
    }

    fn apply_committed(&self) -> agentdb_storage::Result<usize> {
        let tablets = self
            .tablets
            .read()
            .expect("runtime tablet router lock poisoned");
        let mut applied = 0usize;
        for handle in tablets.values() {
            applied = applied.saturating_add(handle.state_machine.apply_committed()?);
        }
        Ok(applied)
    }
}

impl KvStore for RuntimeTabletRouterKvStore {
    fn get(&self, key: &[u8]) -> agentdb_storage::Result<Option<Vec<u8>>> {
        if let Some(tablet_id) = self.resolve_tablet_id()? {
            return self.with_handle(tablet_id, |handle| handle.kv_store.get(key));
        }

        let tablets = self
            .tablets
            .read()
            .expect("runtime tablet router lock poisoned");
        for handle in tablets.values() {
            if let Some(value) = handle.kv_store.get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> agentdb_storage::Result<()> {
        self.write_batch(vec![WriteOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }])
    }

    fn delete(&self, key: &[u8]) -> agentdb_storage::Result<()> {
        self.write_batch(vec![WriteOp::Delete { key: key.to_vec() }])
    }

    fn scan_prefix(&self, prefix: &[u8]) -> agentdb_storage::Result<KvIter> {
        if let Some(tablet_id) = self.resolve_tablet_id()? {
            return self.with_handle(tablet_id, |handle| handle.kv_store.scan_prefix(prefix));
        }

        let tablets = self
            .tablets
            .read()
            .expect("runtime tablet router lock poisoned");
        let mut rows = BTreeMap::new();
        for handle in tablets.values() {
            for item in handle.kv_store.scan_prefix(prefix)? {
                let (key, value) = item?;
                rows.entry(key).or_insert(value);
            }
        }
        Ok(Box::new(rows.into_iter().map(Ok)))
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> agentdb_storage::Result<KvIter> {
        if let Some(tablet_id) = self.resolve_tablet_id()? {
            return self.with_handle(tablet_id, |handle| {
                handle.kv_store.scan_prefix_from(prefix, start)
            });
        }

        let tablets = self
            .tablets
            .read()
            .expect("runtime tablet router lock poisoned");
        let mut rows = BTreeMap::new();
        for handle in tablets.values() {
            for item in handle.kv_store.scan_prefix_from(prefix, start)? {
                let (key, value) = item?;
                rows.entry(key).or_insert(value);
            }
        }
        Ok(Box::new(rows.into_iter().map(Ok)))
    }

    fn write_batch(&self, ops: Vec<WriteOp>) -> agentdb_storage::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let tablet_id = self.resolve_tablet_id()?.unwrap_or(self.default_tablet_id);
        self.with_handle(tablet_id, |handle| handle.kv_store.write_batch(ops))
    }

    fn snapshot<'a>(&'a self) -> agentdb_storage::Result<Box<dyn KvSnapshot + 'a>> {
        Ok(Box::new(RuntimeTabletRouterSnapshot {
            rows: collect_kv_rows(self, b"")?,
        }))
    }

    fn snapshot_owned(self: Arc<Self>) -> agentdb_storage::Result<Arc<dyn KvSnapshot>> {
        Ok(Arc::new(RuntimeTabletRouterSnapshot {
            rows: collect_kv_rows(self.as_ref(), b"")?,
        }))
    }

    fn checkpoint(&self, out_dir: &std::path::Path) -> agentdb_storage::Result<()> {
        let _ = out_dir;
        Err(agentdb_storage::Error::Other(
            "checkpoint is not supported by the runtime tablet router".into(),
        ))
    }
}

struct RuntimeTabletRouterSnapshot {
    rows: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl KvSnapshot for RuntimeTabletRouterSnapshot {
    fn get(&self, key: &[u8]) -> agentdb_storage::Result<Option<Vec<u8>>> {
        Ok(self.rows.get(key).cloned())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> agentdb_storage::Result<KvIter> {
        let prefix = prefix.to_vec();
        let rows: Vec<_> = self
            .rows
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(key, value)| Ok((key.clone(), value.clone())))
            .collect();
        Ok(Box::new(rows.into_iter()))
    }

    fn scan_prefix_from(&self, prefix: &[u8], start: &[u8]) -> agentdb_storage::Result<KvIter> {
        let prefix = prefix.to_vec();
        let start = start.to_vec();
        let rows: Vec<_> = self
            .rows
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix) && key.as_slice() >= start.as_slice())
            .map(|(key, value)| Ok((key.clone(), value.clone())))
            .collect();
        Ok(Box::new(rows.into_iter()))
    }
}

fn collect_kv_rows(
    kv: &dyn KvStore,
    prefix: &[u8],
) -> agentdb_storage::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut rows = BTreeMap::new();
    for item in kv.scan_prefix(prefix)? {
        let (key, value) = item?;
        rows.insert(key, value);
    }
    Ok(rows)
}

struct RuntimeHydration {
    state: State,
    idempotency_records: Vec<(String, IdempotencyRecord)>,
    text_index: InMemoryInverted,
    vector_index: HnswVectorIndex,
    global_record_index: Arc<dyn GlobalRecordIndex>,
}

fn startup_hydration_limit(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn startup_hydration_max_bytes() -> u64 {
    std::env::var("AGENTDB_STARTUP_MAX_HYDRATED_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(512 * 1024 * 1024)
}

/// Whether eager SST-based whole-tablet movement is enabled. Default off: a
/// node must never *produce* an `ADSS` payload for a peer that might be running
/// an older build unable to decode it. Enable (`AGENTDB_SST_MOVEMENT=1`) only
/// after the whole fleet runs a build that recognizes the format; every such
/// build can always *decode* an `ADSS` payload regardless of this flag, so a
/// rolling upgrade stays safe as long as senders are enabled last.
fn sst_movement_enabled() -> bool {
    std::env::var("AGENTDB_SST_MOVEMENT")
        .ok()
        .map(|raw| {
            let raw = raw.trim();
            raw == "1" || raw.eq_ignore_ascii_case("true")
        })
        .unwrap_or(false)
}

/// Minimum whole-tablet SST size (bytes) before the fast movement path is
/// used. Ingest has a fixed per-file overhead, so small tablets stay on the
/// logical path where they are already cheap.
fn sst_movement_min_bytes() -> u64 {
    std::env::var("AGENTDB_SST_MOVEMENT_MIN_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(256 * 1024)
}

/// Disk-space reservation (bytes) that snapshot generate/apply must leave free
/// on the data filesystem. Movement never consumes into this floor: a snapshot
/// that fills the disk otherwise wedges the node into a doom loop, because even
/// compaction and recovery need scratch space (TiKV's store `reserve-space`
/// lesson). Default 2 GiB.
fn movement_disk_reserve_bytes() -> u64 {
    std::env::var("AGENTDB_MOVEMENT_DISK_RESERVE_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(2 * 1024 * 1024 * 1024)
}

/// Try to build a whole-tablet SST movement payload for `sm`. Returns `None`
/// (so the caller falls back to the logical snapshot) when the store backend
/// has no SST support, the tablet is empty, or the SST is below the movement
/// size threshold. Exports to a temp SST and inlines its bytes into an `ADSS`
/// payload.
fn try_export_sst_snapshot_payload(
    sm: &LoggedTabletStateMachine,
) -> agentdb_storage::Result<Option<Vec<u8>>> {
    // Place the scratch SST on the DB's own filesystem (so the reservation
    // check below measures the right disk, and the later ingest avoids a
    // cross-filesystem copy). No data dir → non-Rocks backend → no SST support.
    let Some(data_dir) = sm.store().data_dir() else {
        return Ok(None);
    };
    let scratch_root = movement_scratch_root(&data_dir)?;
    // Reservation floor: never even begin generating an SST when the disk is
    // already at/below the reserve — degrade to the logical path, whose payload
    // is built in memory and needs no scratch file. This keeps the export side
    // out of the doom loop.
    if let Some(available) = agentdb_storage::available_disk_bytes(&scratch_root) {
        if available <= movement_disk_reserve_bytes() {
            tracing::warn!(
                available,
                reserve = movement_disk_reserve_bytes(),
                "movement: disk at/below reserve, exporting logical snapshot instead of SST"
            );
            return Ok(None);
        }
    }
    let dir = tempfile::tempdir_in(&scratch_root)
        .map_err(|err| agentdb_storage::Error::Other(format!("sst export tempdir: {err}")))?;
    let path = dir.path().join("tablet.sst");
    let meta = match sm.export_snapshot_sst(&path) {
        Ok(Some(meta)) => meta,
        Ok(None) => return Ok(None), // backend has no SST support
        Err(err) => {
            // A failed generate (e.g. ENOSPC mid-write) must not abort the move:
            // fall back to the logical path rather than propagate.
            tracing::warn!(%err, "movement: SST export failed, falling back to logical snapshot");
            return Ok(None);
        }
    };
    if meta.sst_bytes == 0 || meta.sst_bytes < sst_movement_min_bytes() {
        return Ok(None); // empty or below threshold → cheaper on the logical path
    }
    let sst = std::fs::read(&path)
        .map_err(|err| agentdb_storage::Error::Other(format!("read exported sst: {err}")))?;
    Ok(Some(encode_sst_snapshot_payload(
        meta.tablet_id,
        meta.applied_index,
        &sst,
    )))
}

/// Ensure and return a movement scratch directory alongside the data dir. Kept
/// on the same filesystem as the DB so disk-reservation math and ingest both
/// see the right disk.
fn movement_scratch_root(
    data_dir: &std::path::Path,
) -> agentdb_storage::Result<std::path::PathBuf> {
    let root = data_dir.join("movement_scratch");
    std::fs::create_dir_all(&root)
        .map_err(|err| agentdb_storage::Error::Other(format!("movement scratch dir: {err}")))?;
    Ok(root)
}

fn startup_hydration_categories() -> [(&'static str, &'static [u8], usize); 12] {
    [
        (
            "tenants",
            b"record/tenant/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_TENANTS", 10_000),
        ),
        (
            "tenant aliases",
            b"record/tenant_alias/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_TENANTS", 10_000),
        ),
        (
            "agents",
            b"record/agent/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_AGENTS", 100_000),
        ),
        (
            "agent aliases",
            b"record/agent_alias/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_AGENTS", 100_000),
        ),
        (
            "sessions",
            b"record/session/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_SESSIONS", 250_000),
        ),
        (
            "session aliases",
            b"record/session_alias/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_SESSIONS", 250_000),
        ),
        (
            "artifacts",
            b"record/artifact/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_ARTIFACTS", 250_000),
        ),
        (
            "records",
            b"record/searchable/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_RECORDS", 100_000),
        ),
        (
            "record index outbox",
            b"record/searchable_index_outbox/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_RECORDS", 100_000),
        ),
        (
            "record index commits",
            b"record/searchable_commit/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_RECORDS", 100_000),
        ),
        (
            "tenant metadata",
            b"record/tenant_meta/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_TENANT_META", 500_000),
        ),
        (
            "idempotency records",
            b"record/idempotency/".as_slice(),
            startup_hydration_limit("AGENTDB_STARTUP_MAX_IDEMPOTENCY", 1_000_000),
        ),
    ]
}

fn enforce_startup_hydration_limits(
    persist: &Persist,
    mode: HydrationMode,
) -> agentdb_storage::Result<()> {
    let max_bytes = startup_hydration_max_bytes();
    let mut total_rows = 0_usize;
    let mut total_bytes = 0_u64;
    for (label, prefix, max_rows) in startup_hydration_categories() {
        if mode == HydrationMode::Lazy && LAZY_HYDRATION_SKIPPED_PREFIXES.contains(&prefix) {
            continue;
        }
        let remaining_bytes = max_bytes.saturating_sub(total_bytes);
        let (rows, bytes) = persist.ensure_hydration_prefix_within_limits(
            label,
            prefix,
            max_rows,
            remaining_bytes,
        )?;
        total_rows = total_rows.saturating_add(rows);
        total_bytes = total_bytes.saturating_add(bytes);
    }
    tracing::info!(
        total_rows,
        total_bytes,
        max_bytes,
        "startup hydration preflight passed"
    );
    Ok(())
}

/// How much metadata the runtime loads into the cache at startup.
///
/// `Full` (default) hydrates everything, preserving historical behavior.
/// `Lazy` skips the tenant/agent/session/artifact maps and the alias maps —
/// they load on first touch through the KV fallback path — so startup cost
/// and resident memory stop scaling with total retained metadata. Records,
/// search indexes, skills, export consumers, tenant metadata, and
/// idempotency rows still hydrate (their fallback lands in a later stage).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationMode {
    Full,
    Lazy,
}

fn startup_hydration_mode() -> HydrationMode {
    match std::env::var("AGENTDB_STARTUP_HYDRATION")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "lazy" => HydrationMode::Lazy,
        _ => HydrationMode::Full,
    }
}

/// Prefixes that lazy hydration leaves to the on-demand fallback path; the
/// startup preflight row caps do not apply to them in lazy mode (their whole
/// point is supporting populations larger than the caps).
const LAZY_HYDRATION_SKIPPED_PREFIXES: [&[u8]; 7] = [
    b"record/tenant/",
    b"record/tenant_alias/",
    b"record/agent/",
    b"record/agent_alias/",
    b"record/session/",
    b"record/session_alias/",
    b"record/artifact/",
];

fn hydrate_runtime_from_persist(
    persist: &Persist,
    mode: HydrationMode,
    use_snapshot: bool,
) -> RuntimeHydration {
    let mut state = State::default();
    // In lazy mode the tenant/agent/session/artifact maps and alias maps are
    // left empty: the KV fallback path loads rows on first touch and the
    // enumeration paths scan KV directly. Everything below the block still
    // hydrates in both modes.
    if mode == HydrationMode::Full {
        hydrate_metadata_maps(persist, &mut state);
    }
    for entry in persist.scan_all_tenant_meta().expect("hydrate tenant meta") {
        state
            .tenant_meta
            .insert(tenant_meta_key(&entry.tenant_id, &entry.key), entry);
    }

    let idempotency_records = persist
        .scan_all_idempotency()
        .expect("hydrate idempotency records");
    let commit_seq_rows = persist
        .scan_all_record_commit_seqs()
        .expect("hydrate record commit sequences");
    let mut hydrated_record_commit_seq: BTreeMap<String, u64> = commit_seq_rows
        .iter()
        .map(|(_, record_id, seq)| (record_id.clone(), *seq))
        .collect();
    if let Some(max_commit_seq) = hydrated_record_commit_seq.values().copied().max() {
        state.next_record_commit_seq = max_commit_seq;
        state.local_record_index_watermark = max_commit_seq;
    }

    let snapshot = if use_snapshot && record_index_snapshots_enabled() {
        persist.get_record_index_snapshot().unwrap_or_else(|error| {
            tracing::warn!(?error, "failed to read record index snapshot; rebuilding");
            None
        })
    } else {
        None
    };
    if let Some(snapshot) = snapshot {
        let (text_index, vector_index, global_record_index) =
            hydrate_record_indexes_from_snapshot(persist, &mut state, snapshot, &commit_seq_rows);
        return RuntimeHydration {
            state,
            idempotency_records,
            text_index,
            vector_index,
            global_record_index,
        };
    }

    // Build the V2.2 text + vector indexes from scratch and replay every live
    // record into them. DocIds are minted fresh per run; the reverse maps are
    // rebuilt alongside, trading brief warm-up cost for no persisted doc ids.
    let mut text_index = InMemoryInverted::new();
    let mut vector_index = HnswVectorIndex::new();
    let global_record_index: Arc<dyn GlobalRecordIndex> =
        Arc::new(InMemoryGlobalRecordIndex::default());
    for r in persist.scan_all_records().expect("hydrate records") {
        if r.deleted_at_ms.is_some() || r.is_expired() {
            continue;
        }
        if let Some(external_id) = r.external_id.as_deref() {
            state.record_external_ids.insert(
                record_external_key(&r.tenant_id, external_id),
                r.record_id.as_str().to_string(),
            );
        }
        let doc_id = mint_doc_id(&mut state.next_doc_seq);
        index_record_into(&mut text_index, &mut vector_index, &r, doc_id);
        state
            .doc_to_record
            .insert(doc_id, r.record_id.as_str().to_string());
        state
            .record_to_doc
            .insert(r.record_id.as_str().to_string(), doc_id);
        let commit_seq = hydrated_record_commit_seq
            .remove(r.record_id.as_str())
            .unwrap_or_else(|| next_record_commit_seq(&mut state));
        state
            .record_commit_seq
            .insert(r.record_id.as_str().to_string(), commit_seq);
        state.local_record_index_watermark = state.local_record_index_watermark.max(commit_seq);
        state.next_record_commit_seq = state.next_record_commit_seq.max(commit_seq);
        global_record_index
            .apply(global_index_mutation_for_record(
                &r,
                commit_seq,
                GlobalIndexMutationOp::Upsert,
            ))
            .expect("hydrate global record index");
        state.global_record_index_watermark = state.global_record_index_watermark.max(commit_seq);
        // In lazy mode the search indexes, doc-id maps, commit sequences,
        // and external-id map are built here as usual, but the record ROWS
        // (the large payloads) are not retained — they load on demand
        // through the KV fallback like the other metadata maps.
        if mode == HydrationMode::Full {
            state.records.insert(r.record_id.as_str().to_string(), r);
        }
    }

    RuntimeHydration {
        state,
        idempotency_records,
        text_index,
        vector_index,
        global_record_index,
    }
}

/// `1`/unset enables persisted record-index snapshots (write and load);
/// `AGENTDB_RECORD_INDEX_SNAPSHOT=0` is the kill switch — startup falls back
/// to the full-scan rebuild and the worker stops writing snapshots.
fn record_index_snapshots_enabled() -> bool {
    std::env::var("AGENTDB_RECORD_INDEX_SNAPSHOT")
        .map(|raw| raw.trim() != "0")
        .unwrap_or(true)
}

/// Incremental replicated apply (Stage 3) is the default;
/// `AGENTDB_REPLICATED_APPLY=full` restores the legacy full rehydrate after
/// every committed apply as a kill switch.
fn replicated_incremental_apply_enabled() -> bool {
    std::env::var("AGENTDB_REPLICATED_APPLY")
        .map(|raw| raw.trim() != "full")
        .unwrap_or(true)
}

/// Restore the record indexes from a persisted snapshot, then delta-replay
/// every record whose commit sequence moved past the snapshot's watermarks.
/// Replaces the full scan-and-retokenize rebuild; record ROWS are not made
/// resident (they load on demand through the KV fallback, in every
/// hydration mode — the same machinery lazy hydration and cache eviction
/// already rely on).
fn hydrate_record_indexes_from_snapshot(
    persist: &Persist,
    state: &mut State,
    snapshot: crate::persist::RecordIndexSnapshot,
    commit_seq_rows: &[(String, String, u64)],
) -> (
    InMemoryInverted,
    HnswVectorIndex,
    Arc<dyn GlobalRecordIndex>,
) {
    let mut text_index = InMemoryInverted::from_snapshot(&snapshot.text);
    let mut vector_index =
        HnswVectorIndex::from_snapshot(&snapshot.vector, HnswVectorIndex::default_ef());
    let global_record_index: Arc<dyn GlobalRecordIndex> =
        Arc::new(InMemoryGlobalRecordIndex::from_snapshot(&snapshot.global));

    state.next_doc_seq = snapshot.next_doc_seq;
    for (session_internal_id, doc_seq, record_id) in &snapshot.doc_to_record {
        let doc_id = DocId::new(*session_internal_id, *doc_seq);
        state.doc_to_record.insert(doc_id, record_id.clone());
        state.record_to_doc.insert(record_id.clone(), doc_id);
    }
    for (key, record_id) in &snapshot.record_external_ids {
        state
            .record_external_ids
            .insert(key.clone(), record_id.clone());
    }
    // Commit sequences for records live at capture come from the
    // authoritative rows (which may already be newer; the delta pass below
    // reconciles the index state to match).
    let seq_by_record: BTreeMap<&str, u64> = commit_seq_rows
        .iter()
        .map(|(_, record_id, seq)| (record_id.as_str(), *seq))
        .collect();
    for (_, _, record_id) in &snapshot.doc_to_record {
        if let Some(seq) = seq_by_record.get(record_id.as_str()) {
            state.record_commit_seq.insert(record_id.clone(), *seq);
        }
    }
    state.local_record_index_watermark = snapshot.watermark;
    state.global_record_index_watermark = snapshot.global_watermark;
    state.next_record_commit_seq = state
        .next_record_commit_seq
        .max(snapshot.next_record_commit_seq);
    state.last_record_index_snapshot_watermark = snapshot.watermark;
    state.record_index_snapshot_loaded = true;

    let floor = snapshot.watermark.min(snapshot.global_watermark);
    for (tenant_id, record_id, seq) in commit_seq_rows {
        state.next_record_commit_seq = state.next_record_commit_seq.max(*seq);
        if *seq <= floor {
            continue;
        }
        let row = persist
            .get_record(tenant_id, record_id)
            .expect("hydrate delta record");
        let live = row
            .as_ref()
            .is_some_and(|r| r.deleted_at_ms.is_none() && !r.is_expired());
        if *seq > snapshot.watermark {
            // Same steps as a live update: drop the old doc mapping, then
            // re-index if the record is still live. Ghost postings for the
            // old doc id stay in the text/vector structures, exactly as
            // they would in a running process.
            if let Some(doc_id) = state.record_to_doc.remove(record_id.as_str()) {
                state.doc_to_record.remove(&doc_id);
            }
            if live {
                let r = row.as_ref().expect("live implies present");
                let doc_id = mint_doc_id(&mut state.next_doc_seq);
                index_record_into(&mut text_index, &mut vector_index, r, doc_id);
                state.doc_to_record.insert(doc_id, record_id.clone());
                state.record_to_doc.insert(record_id.clone(), doc_id);
                if let Some(external_id) = r.external_id.as_deref() {
                    state.record_external_ids.insert(
                        record_external_key(&r.tenant_id, external_id),
                        record_id.clone(),
                    );
                }
                state.record_commit_seq.insert(record_id.clone(), *seq);
            } else {
                match row.as_ref() {
                    Some(r) => {
                        // Soft-deleted or expired after capture.
                        if let Some(external_id) = r.external_id.as_deref() {
                            state
                                .record_external_ids
                                .remove(&record_external_key(&r.tenant_id, external_id));
                        }
                        state.record_commit_seq.insert(record_id.clone(), *seq);
                    }
                    None => {
                        // Purged after a post-snapshot soft-delete; drop any
                        // snapshot-era external-id entries pointing at it.
                        state
                            .record_external_ids
                            .retain(|_, mapped| mapped != record_id);
                        state.record_commit_seq.remove(record_id.as_str());
                    }
                }
            }
            state.local_record_index_watermark = state.local_record_index_watermark.max(*seq);
        }
        if *seq > snapshot.global_watermark {
            let mutation = match row.as_ref().filter(|_| live) {
                Some(r) => global_index_mutation_for_record(r, *seq, GlobalIndexMutationOp::Upsert),
                None => GlobalIndexMutation {
                    tenant_id: tenant_id.clone(),
                    record_id: record_id.clone(),
                    namespace: vec![],
                    placement_id: 0,
                    commit_seq: *seq,
                    op: GlobalIndexMutationOp::Delete,
                    terms: vec![],
                },
            };
            global_record_index
                .apply(mutation)
                .expect("hydrate global record index delta");
            state.global_record_index_watermark = state.global_record_index_watermark.max(*seq);
        }
    }

    (text_index, vector_index, global_record_index)
}

fn hydrate_metadata_maps(persist: &Persist, state: &mut State) {
    for t in persist.scan_all_tenants().expect("hydrate tenants") {
        state.tenants.insert(t.tenant_id.as_str().to_string(), t);
    }
    for a in persist.scan_all_agents().expect("hydrate agents") {
        state.agents.insert(a.agent_id.as_str().to_string(), a);
    }
    for mut s in persist.scan_all_sessions().expect("hydrate sessions") {
        if let Some(log_session) = persist
            .latest_append_log_session(s.tenant_id.as_str(), s.session_id.as_str())
            .expect("hydrate append log session")
        {
            s = log_session;
        }
        // Reconcile next_seq against persisted events.
        let max_seq = persist
            .max_event_seq(s.tenant_id.as_str(), s.session_id.as_str())
            .expect("hydrate session events");
        if let Some(m) = max_seq {
            if s.next_seq <= m {
                s.next_seq = m + 1;
            }
        }
        state.sessions.insert(s.session_id.as_str().to_string(), s);
    }
    for art in persist.scan_all_artifacts().expect("hydrate artifacts") {
        state
            .artifacts
            .insert(art.artifact_id.as_str().to_string(), art);
    }
    // Rebuild alias-uniqueness maps from on-disk alias rows so a restarted
    // runtime keeps enforcing the same constraints. We scan the alias prefixes
    // directly rather than walking records; alias rows are the source of truth.
    for (external_key, tenant_id) in persist
        .scan_all_tenant_aliases()
        .expect("hydrate tenant aliases")
    {
        state.tenant_aliases.insert(external_key, tenant_id);
    }
    for ((tenant, alias), agent_id) in persist
        .scan_all_agent_aliases()
        .expect("hydrate agent aliases")
    {
        state
            .agent_aliases
            .insert(scoped_alias_key(&tenant, &alias), agent_id);
    }
    for ((tenant, alias), session_id) in persist
        .scan_all_session_aliases()
        .expect("hydrate session aliases")
    {
        state
            .session_aliases
            .insert(scoped_alias_key(&tenant, &alias), session_id);
    }
}

fn reconcile_session_next_seq_from_persisted_events(
    state: &mut State,
    tenant_id: &Id,
    session_id: &Id,
    max_seq: u64,
) -> Result<()> {
    let Some(session) = state.sessions.get_mut(session_id.as_str()) else {
        return Ok(());
    };
    if session.tenant_id.as_str() != tenant_id.as_str() {
        return Ok(());
    }
    if session.next_seq > max_seq {
        return Ok(());
    }
    session.next_seq = max_seq.checked_add(1).ok_or_else(|| {
        Error::new(
            ErrorCode::Internal,
            "persisted session event sequence overflow",
        )
    })?;
    if session.warm_head_from_seq > session.next_seq {
        session.warm_head_from_seq = session.next_seq;
    }
    Ok(())
}

fn advance_session_next_seq_past_persisted_collision(
    state: &mut State,
    persist: &Persist,
    tenant_id: &Id,
    session_id: &Id,
) -> Result<()> {
    loop {
        let Some(next_seq) = state
            .sessions
            .get(session_id.as_str())
            .filter(|session| session.tenant_id.as_str() == tenant_id.as_str())
            .map(|session| session.next_seq)
        else {
            return Ok(());
        };
        if persist
            .get_event(tenant_id.as_str(), session_id.as_str(), next_seq)
            .map_err(internal)?
            .is_none()
        {
            return Ok(());
        }
        reconcile_session_next_seq_from_persisted_events(state, tenant_id, session_id, next_seq)?;
    }
}

impl Runtime {
    fn bump_lifecycle_counts(counts: &mut LifecycleCounts, lifecycle: Lifecycle) {
        match lifecycle {
            Lifecycle::Warm => counts.warm += 1,
            Lifecycle::Cold => counts.cold += 1,
            Lifecycle::Archive => counts.archive += 1,
            Lifecycle::DerivativeOnly => counts.derivative_only += 1,
        }
    }

    fn summarize_tenant_storage(tenant_id: &Id, sessions: &[Session]) -> TenantStorageSummary {
        let mut lifecycle_counts = LifecycleCounts::default();
        let mut active_session_count = 0usize;
        let mut sealed_segment_count = 0usize;
        let mut archive_manifest_count = 0usize;
        let mut sealed_bytes = 0u64;
        let mut archived_bytes = 0u64;

        for session in sessions {
            Self::bump_lifecycle_counts(&mut lifecycle_counts, session.lifecycle);
            if session.status == crate::session::Status::Active {
                active_session_count += 1;
            }
            sealed_segment_count += session.sealed_segments.len();
            sealed_bytes += session
                .sealed_segments
                .iter()
                .map(|segment| segment.bytes)
                .sum::<u64>();
            if let Some(manifest) = &session.archive_manifest {
                archive_manifest_count += 1;
                archived_bytes += manifest.bytes;
            }
        }

        TenantStorageSummary {
            tenant_id: tenant_id.clone(),
            session_count: sessions.len(),
            active_session_count,
            lifecycle_counts,
            sealed_segment_count,
            archive_manifest_count,
            sealed_bytes,
            archived_bytes,
        }
    }

    fn session_storage_summary(session: &Session) -> SessionStorageSummary {
        SessionStorageSummary {
            session_id: session.session_id.clone(),
            lifecycle: session.lifecycle,
            next_seq: session.next_seq,
            warm_head_from_seq: session.warm_head_from_seq,
            sealed_segment_count: session.sealed_segments.len(),
            sealed_segments: session.sealed_segments.clone(),
            archive_manifest: session.archive_manifest.clone(),
            replay_prefix: session.replay_prefix.clone(),
        }
    }

    /// Seal durable content before it reaches the content backend.
    ///
    /// With the envelope keyring enabled, the plaintext is sealed as an
    /// `AGDB-ENVELOPE-v2` frame under the tenant's content DEK, bound to
    /// (tenant, object_kind, object_id). With the keyring disabled (default),
    /// it takes the legacy path: the v1 content adapter (identity by default)
    /// wrapped in the `AGDB-ENVELOPE-v1` keyed frame. `content_key` is the KMS
    /// key resolution used only by the legacy path.
    fn seal_content(
        &self,
        tenant_id: &str,
        object_kind: &str,
        object_id: &str,
        content_key: &KeyResolution,
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        if self.keyring.is_enabled() {
            let context = CryptoContext {
                tenant_id,
                purpose: KeyPurpose::Content,
                object_kind,
                object_id,
            };
            return self.keyring.seal(&context, plaintext).map_err(|e| {
                Error::new(ErrorCode::Internal, format!("{object_kind} seal failed: {e}"))
            });
        }
        let crypto_context = ContentCryptoContext {
            tenant_id,
            artifact_id: object_id,
            key: content_key,
        };
        let encrypted = self
            .content_crypto
            .encrypt(&crypto_context, plaintext)
            .map_err(|e| {
                Error::new(ErrorCode::Internal, format!("{object_kind} encrypt failed: {e}"))
            })?;
        Ok(encode_keyed_envelope(content_key, &encrypted))
    }

    /// Open durable content read back from the content backend. Detects the
    /// frame version so a disabled→enabled rollout (or the reverse) stays
    /// readable: `AGDB-ENVELOPE-v2` opens through the keyring; `AGDB-ENVELOPE-v1`
    /// (or bare plaintext, the `None` case) opens through the legacy adapter
    /// with `fallback_key`.
    fn open_content(
        &self,
        tenant_id: &str,
        object_kind: &str,
        object_id: &str,
        fallback_key: KeyResolution,
        stored_bytes: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if is_envelope_v2(&stored_bytes) {
            let context = CryptoContext {
                tenant_id,
                purpose: KeyPurpose::Content,
                object_kind,
                object_id,
            };
            return self.keyring.open(&context, &stored_bytes).map_err(|e| {
                Error::new(ErrorCode::Internal, format!("{object_kind} open failed: {e}"))
            });
        }
        let (content_key, ciphertext) = match decode_keyed_envelope(&stored_bytes).map_err(|e| {
            Error::new(ErrorCode::Internal, format!("{object_kind} envelope failed: {e}"))
        })? {
            Some((recorded_key, ciphertext)) => (recorded_key, ciphertext),
            None => (fallback_key, stored_bytes),
        };
        let crypto_context = ContentCryptoContext {
            tenant_id,
            artifact_id: object_id,
            key: &content_key,
        };
        self.content_crypto
            .decrypt(&crypto_context, &ciphertext)
            .map_err(|e| {
                Error::new(ErrorCode::Internal, format!("{object_kind} decrypt failed: {e}"))
            })
    }

    async fn read_session_segment_events(
        &self,
        segment: &SessionSegment,
    ) -> Result<Vec<EventEnvelope>> {
        let current_key = self.resolve_key(&segment.tenant_id, KeyOperation::ContentRead)?;
        let stored_bytes = self
            .content
            .get(segment.tenant_id.as_str(), segment.segment_id.as_str())
            .await
            .map_err(|e| Error::new(ErrorCode::Internal, format!("segment get failed: {e}")))?;
        let fallback_key = key_from_parts(&segment.content_key_id, &segment.content_key_version)
            .unwrap_or(current_key);
        let plaintext = self.open_content(
            segment.tenant_id.as_str(),
            "segment",
            segment.segment_id.as_str(),
            fallback_key,
            stored_bytes,
        )?;
        let file: SessionSegmentFile = serde_json::from_slice(&plaintext)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("segment decode failed: {e}")))?;
        Ok(file.events)
    }

    async fn write_session_segment(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        events: Vec<EventEnvelope>,
    ) -> Result<SessionSegment> {
        let Some((seq_start, seq_end)) = events
            .first()
            .zip(events.last())
            .map(|(first, last)| (first.seq, last.seq))
        else {
            return Err(Error::new(
                ErrorCode::Internal,
                "cannot seal empty session segment",
            ));
        };
        let segment_id = Id::new(IdKind::Segment);
        let content_key = self.resolve_key(tenant_id, KeyOperation::ContentWrite)?;
        let file = SessionSegmentFile {
            runtime_version: crate::RUNTIME_VERSION.to_string(),
            segment_id: segment_id.clone(),
            tenant_id: tenant_id.clone(),
            session_id: session_id.clone(),
            seq_start,
            seq_end,
            events,
        };
        let plaintext = serde_json::to_vec(&file)
            .map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?;
        let plaintext_sha256 = sha256_hex(&plaintext);
        let stored_bytes = self.seal_content(
            tenant_id.as_str(),
            "segment",
            segment_id.as_str(),
            &content_key,
            &plaintext,
        )?;
        self.content
            .put(tenant_id.as_str(), segment_id.as_str(), &stored_bytes)
            .await
            .map_err(|e| Error::new(ErrorCode::Internal, format!("segment put failed: {e}")))?;
        Ok(SessionSegment {
            segment_id,
            tenant_id: tenant_id.clone(),
            session_id: session_id.clone(),
            seq_start,
            seq_end,
            event_count: (seq_end - seq_start) + 1,
            bytes: plaintext.len() as u64,
            sha256: plaintext_sha256,
            content_key_id: Some(content_key.key_id.clone()),
            content_key_version: content_key.key_version.clone(),
            created_at_ms: crate::now_ms(),
        })
    }

    async fn read_session_archive_events(
        &self,
        manifest: &SessionArchiveManifest,
    ) -> Result<Vec<EventEnvelope>> {
        let current_key = self.resolve_key(&manifest.tenant_id, KeyOperation::ContentRead)?;
        let stored_bytes = self
            .content
            .get(manifest.tenant_id.as_str(), manifest.manifest_id.as_str())
            .await
            .map_err(|e| Error::new(ErrorCode::Internal, format!("archive get failed: {e}")))?;
        let fallback_key = key_from_parts(&manifest.content_key_id, &manifest.content_key_version)
            .unwrap_or(current_key);
        let plaintext = self.open_content(
            manifest.tenant_id.as_str(),
            "archive",
            manifest.manifest_id.as_str(),
            fallback_key,
            stored_bytes,
        )?;
        let file: SessionArchiveFile = serde_json::from_slice(&plaintext)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("archive decode failed: {e}")))?;
        Ok(file.events)
    }

    async fn write_session_archive(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        source_segment_ids: Vec<Id>,
        events: Vec<EventEnvelope>,
    ) -> Result<SessionArchiveManifest> {
        let manifest_id = Id::new(IdKind::Segment);
        let content_key = self.resolve_key(tenant_id, KeyOperation::ContentWrite)?;
        let file = SessionArchiveFile {
            runtime_version: crate::RUNTIME_VERSION.to_string(),
            manifest_id: manifest_id.clone(),
            tenant_id: tenant_id.clone(),
            session_id: session_id.clone(),
            source_segment_ids: source_segment_ids.clone(),
            events,
        };
        let plaintext = serde_json::to_vec(&file)
            .map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?;
        let plaintext_sha256 = sha256_hex(&plaintext);
        let stored_bytes = self.seal_content(
            tenant_id.as_str(),
            "archive",
            manifest_id.as_str(),
            &content_key,
            &plaintext,
        )?;
        self.content
            .put(tenant_id.as_str(), manifest_id.as_str(), &stored_bytes)
            .await
            .map_err(|e| Error::new(ErrorCode::Internal, format!("archive put failed: {e}")))?;
        Ok(SessionArchiveManifest {
            manifest_id,
            tenant_id: tenant_id.clone(),
            session_id: session_id.clone(),
            source_segment_ids,
            event_count: file.events.len() as u64,
            bytes: plaintext.len() as u64,
            sha256: plaintext_sha256,
            content_key_id: Some(content_key.key_id.clone()),
            content_key_version: content_key.key_version.clone(),
            created_at_ms: crate::now_ms(),
        })
    }

    async fn read_replay_prefix_events(
        &self,
        manifest: &ReplayPrefixManifest,
    ) -> Result<Vec<EventEnvelope>> {
        let current_key = self.resolve_key(&manifest.tenant_id, KeyOperation::ContentRead)?;
        let stored_bytes = self
            .content
            .get(manifest.tenant_id.as_str(), manifest.prefix_id.as_str())
            .await
            .map_err(|e| {
                Error::new(
                    ErrorCode::Internal,
                    format!("replay prefix get failed: {e}"),
                )
            })?;
        let fallback_key = key_from_parts(&manifest.content_key_id, &manifest.content_key_version)
            .unwrap_or(current_key);
        let plaintext = self.open_content(
            manifest.tenant_id.as_str(),
            "replay_prefix",
            manifest.prefix_id.as_str(),
            fallback_key,
            stored_bytes,
        )?;
        let file: ReplayPrefixFile = serde_json::from_slice(&plaintext).map_err(|e| {
            Error::new(
                ErrorCode::Internal,
                format!("replay prefix decode failed: {e}"),
            )
        })?;
        if file.prefix_id.as_str() != manifest.prefix_id.as_str()
            || file.tenant_id.as_str() != manifest.tenant_id.as_str()
            || file.session_id.as_str() != manifest.session_id.as_str()
        {
            return Err(Error::new(
                ErrorCode::Internal,
                "replay prefix content does not match manifest",
            ));
        }
        Ok(file.events)
    }

    async fn write_replay_prefix(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        events: Vec<EventEnvelope>,
        fork_depth: usize,
    ) -> Result<ReplayPrefixManifest> {
        let prefix_id = Id::new(IdKind::Segment);
        let content_key = self.resolve_key(tenant_id, KeyOperation::ContentWrite)?;
        let file = ReplayPrefixFile {
            runtime_version: crate::RUNTIME_VERSION.to_string(),
            prefix_id: prefix_id.clone(),
            tenant_id: tenant_id.clone(),
            session_id: session_id.clone(),
            events,
        };
        let plaintext = serde_json::to_vec(&file)
            .map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?;
        let plaintext_sha256 = sha256_hex(&plaintext);
        let stored_bytes = self.seal_content(
            tenant_id.as_str(),
            "replay_prefix",
            prefix_id.as_str(),
            &content_key,
            &plaintext,
        )?;
        self.content
            .put(tenant_id.as_str(), prefix_id.as_str(), &stored_bytes)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorCode::Internal,
                    format!("replay prefix put failed: {e}"),
                )
            })?;
        Ok(ReplayPrefixManifest {
            prefix_id,
            tenant_id: tenant_id.clone(),
            session_id: session_id.clone(),
            event_count: file.events.len() as u64,
            bytes: plaintext.len() as u64,
            sha256: plaintext_sha256,
            fork_depth,
            content_key_id: Some(content_key.key_id.clone()),
            content_key_version: content_key.key_version.clone(),
            created_at_ms: crate::now_ms(),
        })
    }

    async fn read_hot_events_from_persist(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        if let Some(p) = &self.persist {
            return p
                .scan_events(tenant_id.as_str(), session_id.as_str(), from_seq, limit)
                .map_err(internal);
        }
        Ok(vec![])
    }

    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            hydration_mode: HydrationMode::Full,
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            session_append_queues: StdMutex::new(BTreeMap::new()),
            idem: IdempotencyStore::default(),
            persist: None,
            content: Arc::new(InMemoryContentBackend::default()),
            content_crypto: Arc::new(IdentityContentCryptoAdapter),
            keyring: Arc::new(DisabledEnvelopeCrypto),
            key_provisioner: None,
            key_rotator: OnceLock::new(),
            key_revoker: OnceLock::new(),
            key_retirer: OnceLock::new(),
            kms: Arc::new(AllowAllKmsKeyResolver),
            text_index: StdMutex::new(InMemoryInverted::new()),
            vector_index: StdMutex::new(HnswVectorIndex::new()),
            global_record_index: Arc::new(InMemoryGlobalRecordIndex::default()),
            tablet_router: None,
        })
    }

    pub fn in_memory_with_kms(kms: Arc<dyn KmsKeyResolver>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            hydration_mode: HydrationMode::Full,
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            session_append_queues: StdMutex::new(BTreeMap::new()),
            idem: IdempotencyStore::default(),
            persist: None,
            content: Arc::new(InMemoryContentBackend::default()),
            content_crypto: Arc::new(IdentityContentCryptoAdapter),
            keyring: Arc::new(DisabledEnvelopeCrypto),
            key_provisioner: None,
            key_rotator: OnceLock::new(),
            key_revoker: OnceLock::new(),
            key_retirer: OnceLock::new(),
            kms,
            text_index: StdMutex::new(InMemoryInverted::new()),
            vector_index: StdMutex::new(HnswVectorIndex::new()),
            global_record_index: Arc::new(InMemoryGlobalRecordIndex::default()),
            tablet_router: None,
        })
    }

    pub fn in_memory_with_kms_and_content_crypto(
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            hydration_mode: HydrationMode::Full,
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            session_append_queues: StdMutex::new(BTreeMap::new()),
            idem: IdempotencyStore::default(),
            persist: None,
            content: Arc::new(InMemoryContentBackend::default()),
            content_crypto,
            keyring: Arc::new(DisabledEnvelopeCrypto),
            key_provisioner: None,
            key_rotator: OnceLock::new(),
            key_revoker: OnceLock::new(),
            key_retirer: OnceLock::new(),
            kms,
            text_index: StdMutex::new(InMemoryInverted::new()),
            vector_index: StdMutex::new(HnswVectorIndex::new()),
            global_record_index: Arc::new(InMemoryGlobalRecordIndex::default()),
            tablet_router: None,
        })
    }

    /// Build a Runtime backed by `kv` for metadata and `content` for artifact
    /// bytes. Hydrates tenants, agents, sessions, artifacts, and idempotency
    /// records from storage at startup; events are NOT preloaded and are read
    /// on demand via `fetch_from_seq`.
    ///
    /// Reconciliation: for every hydrated session, the highest persisted event
    /// seq is checked and `next_seq` is bumped past it if needed. This is a
    /// belt-and-suspenders defense against partial-batch states that
    /// theoretically shouldn't exist (the append path uses `write_batch` which
    /// the RocksDB backend honors atomically) but is cheap to do and makes
    /// post-restart correctness independent of backend-specific batch
    /// guarantees.
    ///
    /// Artifact metadata is hydrated; the bytes themselves stay in `content`
    /// and are read on demand via `get_artifact_blob`.
    pub fn with_kv(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
    ) -> Arc<Self> {
        Self::with_kv_and_kms(kv, content, Arc::new(AllowAllKmsKeyResolver))
    }

    pub fn with_kv_and_kms(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
    ) -> Arc<Self> {
        Self::with_kv_kms_content_and_metadata_crypto(
            kv,
            content,
            kms,
            Arc::new(IdentityContentCryptoAdapter),
            Arc::new(IdentityMetadataCryptoAdapter),
        )
    }

    pub fn durable_export_ledger_enabled(&self) -> bool {
        self.persist.is_some()
    }

    pub async fn export_high_watermark(&self, tenant_id: &Id) -> Result<Option<String>> {
        if let Some(persist) = &self.persist {
            {
                let s = self.state.lock().await;
                validate_tenant_live_in_state(&s, tenant_id)?;
            }
            return persist
                .export_ledger_high_watermark(tenant_id.as_str())
                .map_err(internal);
        }

        let s = self.state.lock().await;
        validate_tenant_live_in_state(&s, tenant_id)?;
        Ok(s.export_ledger.get(tenant_id.as_str()).and_then(|ledger| {
            ledger
                .iter()
                .next_back()
                .map(|(&seq, item)| in_memory_export_ledger_cursor(tenant_id.as_str(), seq, item))
        }))
    }

    pub async fn export_oldest_available_cursor(&self, tenant_id: &Id) -> Result<Option<String>> {
        if let Some(persist) = &self.persist {
            {
                let s = self.state.lock().await;
                validate_tenant_live_in_state(&s, tenant_id)?;
            }
            return persist
                .export_ledger_oldest_cursor(tenant_id.as_str())
                .map_err(internal);
        }

        let s = self.state.lock().await;
        validate_tenant_live_in_state(&s, tenant_id)?;
        Ok(s.export_ledger.get(tenant_id.as_str()).and_then(|ledger| {
            ledger
                .iter()
                .next()
                .map(|(&seq, item)| in_memory_export_ledger_cursor(tenant_id.as_str(), seq, item))
        }))
    }

    pub fn with_kv_kms_and_content_crypto(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
    ) -> Arc<Self> {
        Self::with_kv_kms_content_and_metadata_crypto(
            kv,
            content,
            kms,
            content_crypto,
            Arc::new(IdentityMetadataCryptoAdapter),
        )
    }

    pub fn with_kv_and_replicated_log(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        log: Arc<dyn ReplicatedLog + Send + Sync>,
    ) -> Arc<Self> {
        let logs = BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]);
        Self::with_kv_kms_content_metadata_and_replicated_logs_for_default_tablet(
            kv,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            Arc::new(IdentityMetadataCryptoAdapter),
            logs,
            RUNTIME_PRIMARY_TABLET_ID,
        )
    }

    pub fn with_kv_and_replicated_logs(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> Arc<Self> {
        Self::with_kv_and_replicated_logs_for_default_tablet(
            kv,
            content,
            logs,
            RUNTIME_PRIMARY_TABLET_ID,
        )
    }

    pub fn with_kv_and_replicated_logs_for_default_tablet(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
        default_tablet_id: i64,
    ) -> Arc<Self> {
        Self::with_kv_kms_content_metadata_and_replicated_logs_for_default_tablet(
            kv,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            Arc::new(IdentityMetadataCryptoAdapter),
            logs,
            default_tablet_id,
        )
    }

    /// Build a replicated runtime with an explicit envelope keyring, so a
    /// clustered deployment can encrypt tenant payloads. The keyring seals both
    /// durable content (via the runtime) and event/metadata values (via
    /// Persist) *before* the write reaches the replicated log, so the Raft log
    /// and any tablet snapshot hold ciphertext, never plaintext. Pass
    /// `DisabledEnvelopeCrypto` for the plaintext/legacy path.
    pub fn with_kv_content_keyring_and_replicated_logs(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        keyring: Arc<dyn EnvelopeCrypto>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> Arc<Self> {
        let tablet_router = Arc::new(
            RuntimeTabletRouterKvStore::new(kv, RUNTIME_PRIMARY_TABLET_ID, logs)
                .expect("open runtime tablet router"),
        );
        let persist = Persist::with_metadata_crypto(
            tablet_router.clone(),
            Arc::new(IdentityMetadataCryptoAdapter),
        )
        .with_keyring(keyring.clone());
        Self::from_persist_with_tablet_router_and_mode(
            persist,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            keyring,
            None,
            Some(tablet_router),
            None,
        )
    }

    /// Replicated runtime with an envelope keyring *and* a tenant key
    /// provisioner — the cluster shape. The provisioner mints a tenant's DEKs at
    /// creation time (the serve path is read-only and fails closed otherwise).
    pub fn with_kv_content_keyring_provisioner_and_replicated_logs(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        keyring: Arc<dyn EnvelopeCrypto>,
        key_provisioner: Arc<dyn TenantKeyProvisioner>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> Arc<Self> {
        Self::with_kv_content_keyring_provisioner_and_replicated_logs_for_default_tablet(
            kv,
            content,
            keyring,
            key_provisioner,
            logs,
            RUNTIME_PRIMARY_TABLET_ID,
        )
    }

    /// As above, with an explicit default tablet id (the server's configured
    /// `route_default_tablet_id`).
    pub fn with_kv_content_keyring_provisioner_and_replicated_logs_for_default_tablet(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        keyring: Arc<dyn EnvelopeCrypto>,
        key_provisioner: Arc<dyn TenantKeyProvisioner>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
        default_tablet_id: i64,
    ) -> Arc<Self> {
        let tablet_router = Arc::new(
            RuntimeTabletRouterKvStore::new(kv, default_tablet_id, logs)
                .expect("open runtime tablet router"),
        );
        let persist = Persist::with_metadata_crypto(
            tablet_router.clone(),
            Arc::new(IdentityMetadataCryptoAdapter),
        )
        .with_keyring(keyring.clone());
        Self::from_persist_with_tablet_router_and_mode(
            persist,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            keyring,
            Some(key_provisioner),
            Some(tablet_router),
            None,
        )
    }

    pub fn with_tablet_store_lifecycle_and_replicated_logs(
        store_lifecycle: Arc<dyn TabletStoreLifecycle>,
        content: Arc<dyn ContentBackend>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> Arc<Self> {
        Self::with_tablet_store_lifecycle_and_replicated_logs_for_default_tablet(
            store_lifecycle,
            content,
            logs,
            RUNTIME_PRIMARY_TABLET_ID,
        )
    }

    pub fn with_tablet_store_lifecycle_and_replicated_logs_for_default_tablet(
        store_lifecycle: Arc<dyn TabletStoreLifecycle>,
        content: Arc<dyn ContentBackend>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
        default_tablet_id: i64,
    ) -> Arc<Self> {
        let tablet_router = Arc::new(
            RuntimeTabletRouterKvStore::new_with_lifecycle(
                store_lifecycle,
                default_tablet_id,
                logs,
            )
            .expect("open runtime tablet router"),
        );
        let persist = Persist::with_metadata_crypto(
            tablet_router.clone(),
            Arc::new(IdentityMetadataCryptoAdapter),
        );
        Self::from_persist_with_tablet_router(
            persist,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            Some(tablet_router),
        )
    }

    /// Grouped-tablet-store replicated runtime with an envelope keyring + tenant
    /// key provisioner — the cluster shape for the grouped-rocksdb layout.
    pub fn with_tablet_store_lifecycle_content_keyring_provisioner_and_replicated_logs_for_default_tablet(
        store_lifecycle: Arc<dyn TabletStoreLifecycle>,
        content: Arc<dyn ContentBackend>,
        keyring: Arc<dyn EnvelopeCrypto>,
        key_provisioner: Arc<dyn TenantKeyProvisioner>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
        default_tablet_id: i64,
    ) -> Arc<Self> {
        let tablet_router = Arc::new(
            RuntimeTabletRouterKvStore::new_with_lifecycle(store_lifecycle, default_tablet_id, logs)
                .expect("open runtime tablet router"),
        );
        let persist = Persist::with_metadata_crypto(
            tablet_router.clone(),
            Arc::new(IdentityMetadataCryptoAdapter),
        )
        .with_keyring(keyring.clone());
        Self::from_persist_with_tablet_router_and_mode(
            persist,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            keyring,
            Some(key_provisioner),
            Some(tablet_router),
            None,
        )
    }

    pub fn with_kv_kms_content_metadata_and_replicated_log(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
        metadata_crypto: Arc<dyn MetadataCryptoAdapter>,
        log: Arc<dyn ReplicatedLog + Send + Sync>,
    ) -> Arc<Self> {
        let logs = BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]);
        Self::with_kv_kms_content_metadata_and_replicated_logs(
            kv,
            content,
            kms,
            content_crypto,
            metadata_crypto,
            logs,
        )
    }

    pub fn with_kv_kms_content_metadata_and_replicated_logs(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
        metadata_crypto: Arc<dyn MetadataCryptoAdapter>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> Arc<Self> {
        Self::with_kv_kms_content_metadata_and_replicated_logs_for_default_tablet(
            kv,
            content,
            kms,
            content_crypto,
            metadata_crypto,
            logs,
            RUNTIME_PRIMARY_TABLET_ID,
        )
    }

    pub fn with_kv_kms_content_metadata_and_replicated_logs_for_default_tablet(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
        metadata_crypto: Arc<dyn MetadataCryptoAdapter>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
        default_tablet_id: i64,
    ) -> Arc<Self> {
        let tablet_router = Arc::new(
            RuntimeTabletRouterKvStore::new(kv, default_tablet_id, logs)
                .expect("open runtime tablet router"),
        );
        let persist = Persist::with_metadata_crypto(tablet_router.clone(), metadata_crypto);
        Self::from_persist_with_tablet_router(
            persist,
            content,
            kms,
            content_crypto,
            Some(tablet_router),
        )
    }

    pub fn with_kv_kms_content_and_metadata_crypto(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
        metadata_crypto: Arc<dyn MetadataCryptoAdapter>,
    ) -> Arc<Self> {
        let runtime_kv = Arc::new(
            RuntimeTabletKvStore::single_node(kv, RUNTIME_PRIMARY_TABLET_ID)
                .expect("open runtime tablet state"),
        );
        let persist = Persist::with_metadata_crypto(runtime_kv, metadata_crypto);
        Self::from_persist_with_tablet_router(persist, content, kms, content_crypto, None)
    }

    fn from_persist_with_tablet_router(
        persist: Persist,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
        tablet_router: Option<Arc<RuntimeTabletRouterKvStore>>,
    ) -> Arc<Self> {
        Self::from_persist_with_tablet_router_and_mode(
            persist,
            content,
            kms,
            content_crypto,
            Arc::new(DisabledEnvelopeCrypto),
            None,
            tablet_router,
            None,
        )
    }

    /// Test-only constructor variant with an explicit hydration mode, so
    /// tests can exercise lazy hydration without racing on process-global
    /// environment variables.
    #[doc(hidden)]
    pub fn with_kv_and_hydration_mode(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        mode: HydrationMode,
    ) -> Arc<Self> {
        let runtime_kv = Arc::new(
            RuntimeTabletKvStore::single_node(kv, RUNTIME_PRIMARY_TABLET_ID)
                .expect("open runtime tablet state"),
        );
        let persist =
            Persist::with_metadata_crypto(runtime_kv, Arc::new(IdentityMetadataCryptoAdapter));
        Self::from_persist_with_tablet_router_and_mode(
            persist,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            Arc::new(DisabledEnvelopeCrypto),
            None,
            None,
            Some(mode),
        )
    }

    /// Build a single-node persist-backed runtime with an explicit envelope
    /// keyring, so a deployment (or test) can enable content encryption. The
    /// keyring seals durable content (segments, archives, replay prefixes,
    /// artifacts) as `AGDB-ENVELOPE-v2` before it reaches the content backend;
    /// pass `DisabledEnvelopeCrypto` for the plaintext/legacy path. Other crypto
    /// seams default to identity/allow-all.
    pub fn with_kv_content_and_keyring(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        keyring: Arc<dyn EnvelopeCrypto>,
    ) -> Arc<Self> {
        let runtime_kv = Arc::new(
            RuntimeTabletKvStore::single_node(kv, RUNTIME_PRIMARY_TABLET_ID)
                .expect("open runtime tablet state"),
        );
        let persist = Persist::with_metadata_crypto(runtime_kv, Arc::new(IdentityMetadataCryptoAdapter))
            .with_keyring(keyring.clone());
        Self::from_persist_with_tablet_router_and_mode(
            persist,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            keyring,
            None,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_persist_with_tablet_router_and_mode(
        mut persist: Persist,
        content: Arc<dyn ContentBackend>,
        kms: Arc<dyn KmsKeyResolver>,
        content_crypto: Arc<dyn ContentCryptoAdapter>,
        keyring: Arc<dyn EnvelopeCrypto>,
        key_provisioner: Option<Arc<dyn TenantKeyProvisioner>>,
        tablet_router: Option<Arc<RuntimeTabletRouterKvStore>>,
        hydration_override: Option<HydrationMode>,
    ) -> Arc<Self> {
        // Replicated runtimes rebuild full state on Raft apply, so they
        // always hydrate fully; lazy mode is a single-node startup
        // optimization until Stage 3 makes apply incremental.
        let hydration_mode = if tablet_router.is_some() {
            HydrationMode::Full
        } else {
            hydration_override.unwrap_or_else(startup_hydration_mode)
        };
        if tablet_router.is_some() {
            // Replicated tablets are durable through the raft log; the
            // group-commit batcher's single writer thread would let one
            // stalled raft commit head-of-line-block every tablet's writes.
            persist.disable_adaptive_write_batcher();
        }
        enforce_startup_hydration_limits(&persist, hydration_mode)
            .expect("startup hydration limits");
        let hydration =
            hydrate_runtime_from_persist(&persist, hydration_mode, tablet_router.is_none());
        let idem = IdempotencyStore::default();
        idem.extend(hydration.idempotency_records);

        Arc::new(Self {
            state: Mutex::new(hydration.state),
            hydration_mode,
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            session_append_queues: StdMutex::new(BTreeMap::new()),
            idem,
            persist: Some(persist),
            content,
            content_crypto,
            keyring,
            key_provisioner,
            key_rotator: OnceLock::new(),
            key_revoker: OnceLock::new(),
            key_retirer: OnceLock::new(),
            kms,
            text_index: StdMutex::new(hydration.text_index),
            vector_index: StdMutex::new(hydration.vector_index),
            global_record_index: hydration.global_record_index,
            tablet_router,
        })
    }

    /// Install the tenant key rotator once at startup. The server owns the key
    /// provider `Arc` (it is also the keyring's provider and, in a cluster, the
    /// provisioner), so the rotator is wired in out-of-band rather than through
    /// every constructor. A second call is ignored (the first install wins).
    pub fn set_key_rotator(&self, rotator: Arc<dyn TenantKeyRotator>) {
        let _ = self.key_rotator.set(rotator);
    }

    /// Rotate a tenant's data-encryption keys (admin operation). Advances each
    /// provisioned purpose to a new active DEK version and demotes the previous
    /// one to read-only — existing ciphertext stays readable, new writes seal
    /// under the new key. Cached DEK leases for the tenant are then dropped so
    /// the seal path adopts the new active key immediately. Fails closed when no
    /// rotator is installed (encryption disabled or no key catalog).
    pub async fn rotate_tenant_dek(&self, tenant_id: &str) -> Result<()> {
        let rotator = self.key_rotator.get().ok_or_else(|| {
            Error::new(
                ErrorCode::KeyUnavailable,
                "key rotation is unavailable (encryption disabled or no key catalog configured)",
            )
        })?;
        rotator.rotate_tenant(tenant_id).await?;
        self.keyring.invalidate(tenant_id);
        Ok(())
    }

    /// Install the tenant key revoker once at startup (see [`set_key_rotator`]).
    ///
    /// [`set_key_rotator`]: Runtime::set_key_rotator
    pub fn set_key_revoker(&self, revoker: Arc<dyn TenantKeyRevoker>) {
        let _ = self.key_revoker.set(revoker);
    }

    /// Revoke a tenant's data-encryption keys (admin operation). With
    /// `destroy = false` every DEK version is marked revoked (reversible); with
    /// `destroy = true` the wrapped key material is deleted (crypto-shred,
    /// irreversible). Cached DEK leases for the tenant are then dropped. Fails
    /// closed when no revoker is installed (encryption disabled or no catalog).
    pub async fn revoke_tenant_dek(&self, tenant_id: &str, destroy: bool) -> Result<()> {
        let revoker = self.key_revoker.get().ok_or_else(|| {
            Error::new(
                ErrorCode::KeyUnavailable,
                "key revocation is unavailable (encryption disabled or no key catalog configured)",
            )
        })?;
        revoker.revoke_tenant(tenant_id, destroy).await?;
        self.keyring.invalidate(tenant_id);
        Ok(())
    }

    /// Install the tenant key retirer once at startup (see [`set_key_rotator`]).
    ///
    /// [`set_key_rotator`]: Runtime::set_key_rotator
    pub fn set_key_retirer(&self, retirer: Arc<dyn TenantKeyRetirer>) {
        let _ = self.key_retirer.set(retirer);
    }

    /// Retire a tenant's drained DEK versions (admin operation). This is the safe
    /// completion of a rotation: after [`reencrypt_tenant_dek`](Self::reencrypt_tenant_dek)
    /// has moved every object to the active version, the old versions have no
    /// remaining references and can be marked `retired` (never able to decrypt
    /// again), so a later crypto-shred or key-material deletion is possible.
    ///
    /// It is **safe by construction**: it first re-encrypts (idempotent) and
    /// refuses to proceed if that pass reported any error; it then *scans every
    /// stored object* and retires only versions it did **not** observe in use.
    /// The content backend must be enumerable to verify content drainage — when
    /// it is not, content versions are left untouched (`content_verified = false`)
    /// and only metadata versions are retired. Fails closed when no retirer is
    /// installed (encryption disabled or no key catalog).
    pub async fn retire_tenant_dek_versions(&self, tenant_id: &str) -> Result<RetireReport> {
        let retirer = self.key_retirer.get().ok_or_else(|| {
            Error::new(
                ErrorCode::KeyUnavailable,
                "key retirement is unavailable (encryption disabled or no key catalog configured)",
            )
        })?;
        // Move everything to the active version first; refuse to retire if any
        // object could not be re-encrypted (it may still reference an old key).
        let reencrypt = self.reencrypt_tenant_dek(tenant_id).await?;
        if reencrypt.errors > 0 {
            return Err(Error::new(
                ErrorCode::Internal,
                format!(
                    "re-encryption reported {} error(s); refusing to retire DEK versions that \
                     may still be referenced",
                    reencrypt.errors
                ),
            ));
        }

        // Observe which versions are still in use, then retire only the rest.
        let metadata_in_use = match &self.persist {
            Some(p) => p.metadata_versions_in_use(tenant_id).map_err(internal)?,
            None => Vec::new(),
        };
        let metadata_versions_retired = retirer
            .retire_drained_versions(tenant_id, KeyPurpose::Metadata, &metadata_in_use)
            .await?;

        let (content_versions_retired, content_verified) =
            match self.content_versions_in_use(tenant_id).await? {
                Some(content_in_use) => (
                    retirer
                        .retire_drained_versions(tenant_id, KeyPurpose::Content, &content_in_use)
                        .await?,
                    true,
                ),
                None => (0, false),
            };

        Ok(RetireReport {
            metadata_versions_retired,
            content_versions_retired,
            content_verified,
        })
    }

    /// The set of Content DEK versions still referenced by a tenant's stored
    /// blobs, or `None` when the content backend cannot enumerate its namespace
    /// (so content drainage cannot be verified). Used by retirement.
    async fn content_versions_in_use(&self, tenant_id: &str) -> Result<Option<Vec<u32>>> {
        let max = startup_hydration_limit("AGENTDB_STARTUP_MAX_CONTENT_OBJECTS", 500_000);
        let listing = match self.content.list_objects(max).await {
            Ok(listing) => listing,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => return Ok(None),
            Err(error) => {
                return Err(Error::new(
                    ErrorCode::Internal,
                    format!("content object listing failed: {error}"),
                ))
            }
        };
        let mut versions = Vec::new();
        for obj in listing.objects {
            if obj.tenant_id != tenant_id {
                continue;
            }
            let Ok(stored) = self.content.get(&obj.tenant_id, &obj.artifact_id).await else {
                // A blob we cannot read is treated as still-in-use of every
                // version (conservative): return None so no content version is
                // retired this pass.
                return Ok(None);
            };
            if let Some(version) = envelope_dek_version(&stored) {
                if !versions.contains(&version) {
                    versions.push(version);
                }
            }
        }
        Ok(Some(versions))
    }

    /// Re-encrypt a tenant's data sealed under an older DEK version so it moves
    /// to the active version (the data-plane half of DEK rotation) — both the
    /// metadata rows (under the Metadata DEK) and the content blobs (segments,
    /// archives, replay prefixes, artifacts, under the Content DEK), matching the
    /// two purposes `rotate_tenant_dek` advances. Idempotent; a no-op when
    /// encryption is disabled. Returns the combined count of rows/blobs
    /// inspected, rewritten, and skipped.
    pub async fn reencrypt_tenant_dek(
        &self,
        tenant_id: &str,
    ) -> Result<crate::persist::ReencryptReport> {
        let mut report = match &self.persist {
            Some(p) => p.reencrypt_tenant_metadata(tenant_id).map_err(internal)?,
            None => crate::persist::ReencryptReport::default(),
        };
        self.reencrypt_tenant_content(tenant_id, &mut report).await?;
        Ok(report)
    }

    /// Re-encrypt a tenant's content blobs under the active Content DEK, folding
    /// the counts into `report`. A no-op when encryption is disabled or the
    /// content backend cannot enumerate its objects. Each stale blob's object
    /// kind is recovered by trying each content kind until one opens (safe: a
    /// wrong kind fails the AAD check rather than corrupting the blob).
    async fn reencrypt_tenant_content(
        &self,
        tenant_id: &str,
        report: &mut crate::persist::ReencryptReport,
    ) -> Result<()> {
        if !self.keyring.is_enabled() {
            return Ok(());
        }
        let active = self
            .keyring
            .active_key(tenant_id, KeyPurpose::Content)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("active content DEK: {e}")))?
            .version;
        let max = startup_hydration_limit("AGENTDB_STARTUP_MAX_CONTENT_OBJECTS", 500_000);
        let listing = match self.content.list_objects(max).await {
            Ok(listing) => listing,
            // A backend that cannot enumerate its namespace cannot be walked;
            // there is nothing to do rather than an error.
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => return Ok(()),
            Err(error) => {
                return Err(Error::new(
                    ErrorCode::Internal,
                    format!("content object listing failed: {error}"),
                ))
            }
        };
        // The keyring path of seal/open_content ignores this key (it is only used
        // by the legacy adapter), and this walk runs only with the keyring
        // enabled, so a placeholder suffices.
        let content_key = KeyResolution {
            key_id: format!("tenant:{tenant_id}:content"),
            key_version: None,
        };
        const KINDS: [&str; 4] = ["artifact", "segment", "archive", "replay_prefix"];
        for obj in listing.objects {
            if obj.tenant_id != tenant_id {
                continue;
            }
            let stored = match self.content.get(&obj.tenant_id, &obj.artifact_id).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    report.errors += 1;
                    continue;
                }
            };
            if !is_envelope_v2(&stored) {
                continue; // plaintext / legacy — not sealed under a Content DEK version
            }
            let Some(version) = envelope_dek_version(&stored) else {
                continue;
            };
            report.scanned += 1;
            if version >= active {
                continue; // already current
            }
            let mut resealed_ok = false;
            for kind in KINDS {
                let Ok(plaintext) = self.open_content(
                    tenant_id,
                    kind,
                    &obj.artifact_id,
                    content_key.clone(),
                    stored.clone(),
                ) else {
                    continue;
                };
                if let Ok(resealed) =
                    self.seal_content(tenant_id, kind, &obj.artifact_id, &content_key, &plaintext)
                {
                    if self
                        .content
                        .put(tenant_id, &obj.artifact_id, &resealed)
                        .await
                        .is_ok()
                    {
                        report.reencrypted += 1;
                        resealed_ok = true;
                        break;
                    }
                }
            }
            if !resealed_ok {
                tracing::warn!(
                    object_id = %obj.artifact_id,
                    "content re-encryption could not re-seal a blob"
                );
                report.errors += 1;
            }
        }
        Ok(())
    }

    pub async fn with_tablet_context<F, T>(&self, tablet_id: i64, future: F) -> T
    where
        F: Future<Output = T>,
    {
        self.with_route_context(
            RuntimeRouteContext {
                tablet_id: Some(tablet_id),
                ..RuntimeRouteContext::default()
            },
            future,
        )
        .await
    }

    pub async fn with_route_context<F, T>(&self, context: RuntimeRouteContext, future: F) -> T
    where
        F: Future<Output = T>,
    {
        if let Some(tablet_id) = context.tablet_id {
            CURRENT_RUNTIME_ROUTE_CONTEXT
                .scope(context, CURRENT_RUNTIME_TABLET_ID.scope(tablet_id, future))
                .await
        } else {
            CURRENT_RUNTIME_ROUTE_CONTEXT.scope(context, future).await
        }
    }

    fn session_commit_lock(&self, session_id: &Id) -> Arc<Mutex<()>> {
        let mut locks = self
            .session_commit_locks
            .lock()
            .expect("session commit lock map poisoned");
        let key = session_id.as_str().to_string();
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        if locks.len() > 4096 {
            locks.retain(|_, lock| lock.strong_count() > 0);
        }
        lock
    }

    fn session_append_queue(&self, session_id: &Id) -> Arc<SessionAppendQueue> {
        let mut queues = self
            .session_append_queues
            .lock()
            .expect("session append queue map poisoned");
        let key = session_id.as_str().to_string();
        if let Some(queue) = queues.get(&key).and_then(Weak::upgrade) {
            return queue;
        }
        let queue = Arc::new(SessionAppendQueue {
            inner: Mutex::new(SessionAppendQueueState::default()),
        });
        queues.insert(key, Arc::downgrade(&queue));
        if queues.len() > 4096 {
            queues.retain(|_, queue| queue.strong_count() > 0);
        }
        queue
    }

    pub fn export_tablet_snapshot_payload(&self, tablet_id: i64) -> Result<Vec<u8>> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Err(Error::new(
                ErrorCode::Internal,
                "runtime tablet snapshots require a replicated tablet router",
            ));
        };
        router.export_snapshot_payload(tablet_id).map_err(internal)
    }

    pub fn export_tablet_snapshot_payload_for_shard_range(
        &self,
        source_tablet_id: i64,
        target_tablet_id: i64,
        start_shard: i64,
        end_shard: i64,
        shard_count: i64,
    ) -> Result<Vec<u8>> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Err(Error::new(
                ErrorCode::Internal,
                "runtime tablet snapshots require a replicated tablet router",
            ));
        };
        router
            .export_snapshot_payload_for_shard_range(
                source_tablet_id,
                TabletSnapshotShardFilter {
                    target_tablet_id,
                    start_shard,
                    end_shard,
                    shard_count,
                },
            )
            .map_err(internal)
    }

    pub fn install_tablet_snapshot_payload(&self, tablet_id: i64, payload: &[u8]) -> Result<bool> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Ok(false);
        };
        router
            .install_snapshot_payload(tablet_id, payload)
            .map_err(internal)?;
        Ok(true)
    }

    pub fn install_tablet_snapshot_payload_merge(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> Result<bool> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Ok(false);
        };
        router
            .install_snapshot_payload_merge(tablet_id, payload)
            .map_err(internal)?;
        Ok(true)
    }

    fn validate_tablet_snapshot_hydration_limits(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> Result<()> {
        let Some(persist) = self.persist.as_ref() else {
            return Ok(());
        };
        // A whole-tablet SST payload cannot be enumerated key-by-key without
        // reading the SST, so the per-category row caps (a logical-hydration
        // concern) do not apply. Guard the total instead: the compressed SST
        // must not itself exceed the hydrated-bytes budget — a conservative
        // bound, since the decompressed range is what lands in memory.
        if is_sst_snapshot_payload(payload) {
            let max_bytes = startup_hydration_max_bytes();
            let payload_bytes = payload.len() as u64;
            if payload_bytes > max_bytes {
                return Err(Error::new(
                    ErrorCode::Internal,
                    format!(
                        "tablet {tablet_id} SST movement payload {payload_bytes} bytes > AGENTDB_STARTUP_MAX_HYDRATED_BYTES {max_bytes}"
                    ),
                ));
            }
            return Ok(());
        }
        let snapshot = TabletStateSnapshot::decode(payload).map_err(|error| {
            Error::new(
                ErrorCode::Internal,
                format!("decode tablet {tablet_id} runtime snapshot: {error}"),
            )
        })?;
        if snapshot.tablet_id != tablet_id {
            return Err(Error::new(
                ErrorCode::Internal,
                format!(
                    "snapshot tablet_id {} does not match target tablet_id {tablet_id}",
                    snapshot.tablet_id
                ),
            ));
        }

        let max_bytes = startup_hydration_max_bytes();
        let mut prospective_total_bytes = 0_u64;
        for (label, prefix, max_rows) in startup_hydration_categories() {
            let (current_rows, current_bytes) = persist
                .ensure_hydration_prefix_within_limits(label, prefix, usize::MAX, u64::MAX)
                .map_err(internal)?;
            let mut added_rows = 0_usize;
            let mut added_bytes = 0_u64;
            for entry in snapshot
                .entries
                .iter()
                .filter(|entry| entry.key.starts_with(prefix))
            {
                let new_bytes = u64::try_from(entry.key.len().saturating_add(entry.value.len()))
                    .unwrap_or(u64::MAX);
                match persist.kv.get(&entry.key).map_err(internal)? {
                    Some(current_value) => {
                        let current_entry_bytes =
                            u64::try_from(entry.key.len().saturating_add(current_value.len()))
                                .unwrap_or(u64::MAX);
                        added_bytes = added_bytes
                            .saturating_add(new_bytes.saturating_sub(current_entry_bytes));
                    }
                    None => {
                        added_rows = added_rows.saturating_add(1);
                        added_bytes = added_bytes.saturating_add(new_bytes);
                    }
                }
            }
            let prospective_rows = current_rows.saturating_add(added_rows);
            if prospective_rows > max_rows {
                return Err(Error::new(
                    ErrorCode::Internal,
                    format!(
                        "tablet snapshot would exceed startup hydration limit for {label}: {prospective_rows} rows > {max_rows}; raise the matching AGENTDB_STARTUP_MAX_* setting only after sizing memory"
                    ),
                ));
            }
            prospective_total_bytes = prospective_total_bytes
                .saturating_add(current_bytes)
                .saturating_add(added_bytes);
            if prospective_total_bytes > max_bytes {
                return Err(Error::new(
                    ErrorCode::Internal,
                    format!(
                        "tablet snapshot would exceed AGENTDB_STARTUP_MAX_HYDRATED_BYTES: {prospective_total_bytes} key/value bytes > {max_bytes}"
                    ),
                ));
            }
        }
        Ok(())
    }

    pub async fn install_tablet_snapshot_payload_and_rehydrate(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> Result<bool> {
        if self.tablet_router.is_none() {
            return Ok(false);
        }
        self.validate_tablet_snapshot_hydration_limits(tablet_id, payload)?;
        if !self.install_tablet_snapshot_payload(tablet_id, payload)? {
            return Ok(false);
        }
        self.rehydrate_from_persist(false).await?;
        Ok(true)
    }

    pub async fn install_tablet_snapshot_payload_merge_and_rehydrate(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> Result<bool> {
        if self.tablet_router.is_none() {
            return Ok(false);
        }
        self.validate_tablet_snapshot_hydration_limits(tablet_id, payload)?;
        if !self.install_tablet_snapshot_payload_merge(tablet_id, payload)? {
            return Ok(false);
        }
        self.rehydrate_from_persist(false).await?;
        Ok(true)
    }

    pub fn register_replicated_tablet(
        &self,
        tablet_id: i64,
        log: Arc<dyn ReplicatedLog + Send + Sync>,
    ) -> Result<bool> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Err(Error::new(
                ErrorCode::Internal,
                "runtime tablet registration requires a replicated tablet router",
            ));
        };
        router.register(tablet_id, log).map_err(internal)
    }

    pub fn unregister_replicated_tablet(&self, tablet_id: i64) -> Result<bool> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Ok(false);
        };
        Ok(router.unregister(tablet_id))
    }

    pub fn registered_tablet_ids(&self) -> BTreeSet<i64> {
        self.tablet_router
            .as_ref()
            .map(|router| router.tablet_ids())
            .unwrap_or_else(|| BTreeSet::from([RUNTIME_PRIMARY_TABLET_ID]))
    }

    pub fn local_tablet_storage_stats(&self) -> Result<Vec<TabletStorageStats>> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Ok(Vec::new());
        };
        router.storage_stats().map_err(internal)
    }

    pub fn local_tablet_store_domain_metrics(
        &self,
    ) -> Result<Vec<agentdb_storage::TabletStoreDomainMetrics>> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Ok(Vec::new());
        };
        router.domain_metrics().map_err(internal)
    }

    pub fn compact_tablet_store(&self, tablet_id: i64) -> Result<()> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Err(Error::new(
                ErrorCode::Internal,
                "tablet-store compaction requires a replicated tablet router",
            ));
        };
        router.compact_tablet_store(tablet_id).map_err(internal)
    }

    pub fn checkpoint_tablet_store(
        &self,
        tablet_id: i64,
        out_dir: &std::path::Path,
    ) -> Result<TabletStoreManifest> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Err(Error::new(
                ErrorCode::Internal,
                "tablet-store checkpoint requires a replicated tablet router",
            ));
        };
        router
            .checkpoint_tablet_store(tablet_id, out_dir)
            .map_err(internal)
    }

    fn changefeed_cursor_context(&self) -> Result<ChangefeedCursorContext> {
        let route_context = CURRENT_RUNTIME_ROUTE_CONTEXT
            .try_with(Clone::clone)
            .unwrap_or_default();
        let tablet_id = route_context
            .tablet_id
            .or_else(RuntimeTabletRouterKvStore::current_tablet_id);
        let applied_index = match (self.tablet_router.as_ref(), tablet_id) {
            (Some(router), Some(tablet_id)) => {
                Some(router.applied_index(tablet_id).map_err(internal)?)
            }
            _ => None,
        };
        Ok(ChangefeedCursorContext {
            cell_id: route_context.cell_id,
            placement_scope_id: route_context.placement_scope_id,
            tablet_id,
            tablet_epoch: route_context.tablet_epoch,
            applied_index,
        })
    }

    pub fn compact_tablet_log_through(&self, tablet_id: i64, up_to_index: u64) -> Result<usize> {
        let Some(router) = self.tablet_router.as_ref() else {
            return Err(Error::new(
                ErrorCode::Internal,
                "runtime tablet log compaction requires a replicated tablet router",
            ));
        };
        router
            .compact_log_through(tablet_id, up_to_index)
            .map_err(internal)
    }

    async fn rehydrate_from_persist(&self, enforce_startup_limits: bool) -> Result<()> {
        let Some(persist) = self.persist.as_ref() else {
            return Ok(());
        };
        if enforce_startup_limits {
            enforce_startup_hydration_limits(persist, HydrationMode::Full).map_err(internal)?;
        }
        // The replicated-apply rehydrate replaces the whole state, so it
        // always runs in full mode; lazy hydration is a single-node startup
        // optimization until Stage 3 makes apply incremental.
        // Snapshots are skipped here: the replicated apply path must see the
        // exact persisted rows, not an accelerator view.
        let hydration = hydrate_runtime_from_persist(persist, HydrationMode::Full, false);
        self.idem.replace(hydration.idempotency_records);
        {
            let mut state = self.state.lock().await;
            *state = hydration.state;
        }
        *self.text_index.lock().expect("text index lock poisoned") = hydration.text_index;
        *self
            .vector_index
            .lock()
            .expect("vector index lock poisoned") = hydration.vector_index;
        self.global_record_index.reset()?;
        let state = self.state.lock().await;
        for record in state.records.values() {
            let commit_seq = state
                .record_commit_seq
                .get(record.record_id.as_str())
                .copied()
                .unwrap_or(0);
            self.global_record_index
                .apply(global_index_mutation_for_record(
                    record,
                    commit_seq,
                    GlobalIndexMutationOp::Upsert,
                ))?;
        }
        Ok(())
    }

    async fn refresh_replicated_state(&self) -> Result<()> {
        if self
            .tablet_router
            .as_ref()
            .map(|router| router.apply_committed())
            .transpose()
            .map_err(internal)?
            .unwrap_or(0)
            > 0
        {
            if replicated_incremental_apply_enabled() {
                self.invalidate_after_replicated_apply().await?;
            } else {
                // Legacy full rehydrate (AGENTDB_REPLICATED_APPLY=full).
                // No second full-prefix startup preflight: startup/snapshot
                // paths enforce the explicit caps.
                self.rehydrate_from_persist(false).await?;
            }
        }
        Ok(())
    }

    /// Stage 3 of KV-authoritative state: after committed Raft entries are
    /// applied to the tablet KV, invalidate instead of rebuilding. The large
    /// cached sets (tenants/agents/sessions/artifacts/aliases/records/rows/
    /// event hot tails) are evicted and reload on demand through the Stage 1
    /// fallbacks (skills and their bindings included); the small config sets
    /// with no per-row fallback (record edges, tenant
    /// meta) are re-scanned;
    /// the search indexes catch up incrementally from the replicated outbox
    /// rows. Cost per apply: O(changed + small sets), not O(everything).
    async fn invalidate_after_replicated_apply(&self) -> Result<()> {
        let Some(persist) = self.persist.as_ref() else {
            return Ok(());
        };
        let meta = persist.scan_all_tenant_meta().map_err(internal)?;
        {
            let mut s = self.state.lock().await;
            s.tenants.clear();
            s.agents.clear();
            s.sessions.clear();
            s.artifacts.clear();
            s.tenant_aliases.clear();
            s.agent_aliases.clear();
            s.session_aliases.clear();
            s.records.clear();
            s.events.clear();
            s.tenant_aggregate_cache.clear();
            s.tenant_meta.clear();
            for entry in meta {
                s.tenant_meta
                    .insert(tenant_meta_key(&entry.tenant_id, &entry.key), entry);
            }
        }
        // Text/vector/global indexes, doc maps, commit seqs, and the
        // external-id map converge through the replicated outbox rows.
        self.apply_record_index_outbox_once().await?;
        Ok(())
    }

    fn lookup_idempotency_record(&self, full_key: &str) -> Result<Option<IdempotencyRecord>> {
        if let Some(rec) = self.idem.lookup(full_key) {
            return Ok(Some(rec));
        }
        let Some(persist) = self.persist.as_ref() else {
            return Ok(None);
        };
        let Some(rec) = persist.get_idempotency(full_key).map_err(internal)? else {
            return Ok(None);
        };
        self.idem.put(full_key.to_string(), rec.clone());
        Ok(Some(rec))
    }

    async fn replay_append_idempotency_record(
        &self,
        rec: &IdempotencyRecord,
        req: &AppendRequest,
    ) -> Result<AppendCommitResponse> {
        let appended = verify_idem_replay(rec, req)?;
        self.verify_append_replay_event_visible(req, &appended)
            .await?;
        Ok(AppendCommitResponse {
            appended,
            recent: Vec::new(),
            recent_complete: false,
            backpressure: BackpressureReport::default(),
            backpressure_complete: false,
        })
    }

    async fn verify_append_replay_event_visible(
        &self,
        req: &AppendRequest,
        appended: &AppendResponse,
    ) -> Result<()> {
        let Some(persist) = self.persist.as_ref() else {
            return Ok(());
        };
        let event = persist
            .scan_events(
                req.tenant_id.as_str(),
                req.session_id.as_str(),
                appended.seq,
                1,
            )
            .map_err(internal)?
            .into_iter()
            .find(|event| event.seq == appended.seq);
        let Some(event) = event else {
            return Err(Error::new(
                ErrorCode::Internal,
                format!(
                    "idempotency replay for session {} seq {} is not yet visible",
                    req.session_id, appended.seq
                ),
            )
            .with_suggestion("retry the append with the same Idempotency-Key"));
        };
        if event.event_id != appended.event_id {
            return Err(Error::new(
                ErrorCode::Internal,
                format!(
                    "idempotency replay for session {} seq {} resolved event {}, expected {}",
                    req.session_id, appended.seq, event.event_id, appended.event_id
                ),
            ));
        }
        let mut state = self.state.lock().await;
        reconcile_session_next_seq_from_persisted_events(
            &mut state,
            &req.tenant_id,
            &req.session_id,
            appended.seq,
        )?;
        insert_runtime_event_cache(&mut state, true, req.session_id.as_str(), event);
        Ok(())
    }

    fn resolve_key(&self, tenant_id: &Id, operation: KeyOperation) -> Result<KeyResolution> {
        self.kms
            .resolve_tenant_key(tenant_id.as_str(), operation)
            .map_err(kms_error)
    }

    // ----- Cache-miss fallback (Stage 1 of KV-authoritative state) --------
    //
    // The in-memory `State` maps are a cache over the durable KV rows, not
    // the authority. Public funnels call these `ensure_*_cached` helpers
    // before taking the state lock, so a row evicted from (or never loaded
    // into) the cache is transparently re-read from KV. The insert uses
    // entry-or-insert semantics: a concurrent writer's newer in-memory row
    // is never clobbered by the older row we read before racing it.
    // Soft-deleted rows are cached too (tombstones) so liveness filters in
    // the lock-holding code behave identically on hit and miss.

    fn metadata_cache_miss_retryable(code: ErrorCode) -> bool {
        matches!(
            code,
            ErrorCode::TenantNotFound
                | ErrorCode::AgentNotFound
                | ErrorCode::SessionNotFound
                | ErrorCode::ArtifactNotFound
        )
    }

    async fn ensure_tenant_cached(&self, tenant_id: &Id) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        {
            let s = self.state.lock().await;
            if s.tenants.contains_key(tenant_id.as_str()) {
                return Ok(());
            }
        }
        if let Some(row) = p.get_tenant_row(tenant_id.as_str()).map_err(internal)? {
            let mut s = self.state.lock().await;
            s.tenants
                .entry(tenant_id.as_str().to_string())
                .or_insert(row);
        }
        Ok(())
    }

    async fn ensure_agent_cached(&self, tenant_id: &Id, agent_id: &Id) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        {
            let s = self.state.lock().await;
            if s.agents.contains_key(agent_id.as_str()) {
                return Ok(());
            }
        }
        if let Some(row) = p
            .get_agent_row(tenant_id.as_str(), agent_id.as_str())
            .map_err(internal)?
        {
            let mut s = self.state.lock().await;
            s.agents.entry(agent_id.as_str().to_string()).or_insert(row);
        }
        Ok(())
    }

    async fn ensure_session_cached(&self, tenant_id: &Id, session_id: &Id) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        {
            let s = self.state.lock().await;
            if s.sessions.contains_key(session_id.as_str()) {
                return Ok(());
            }
        }
        let mut loaded = p
            .get_session_row(tenant_id.as_str(), session_id.as_str())
            .map_err(internal)?;
        // The append-log fast path persists newer session snapshots in the
        // log instead of rewriting the row; mirror hydration and prefer the
        // latest log snapshot when present.
        if let Some(log_session) = p
            .latest_append_log_session(tenant_id.as_str(), session_id.as_str())
            .map_err(internal)?
        {
            loaded = Some(log_session);
        }
        if let Some(mut row) = loaded {
            // The durable session row's next_seq can lag the event rows (the
            // append batch includes the session row only periodically); the
            // events themselves are the sequence authority, so reconcile
            // before publishing the row into the cache.
            if let Some(max_seq) = p
                .max_event_seq(tenant_id.as_str(), session_id.as_str())
                .map_err(internal)?
            {
                let next = max_seq.saturating_add(1);
                if next > row.next_seq {
                    row.next_seq = next;
                }
            }
            let mut s = self.state.lock().await;
            s.sessions
                .entry(session_id.as_str().to_string())
                .or_insert(row);
        }
        Ok(())
    }

    async fn ensure_artifact_cached(&self, tenant_id: &Id, artifact_id: &Id) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        {
            let s = self.state.lock().await;
            if s.artifacts.contains_key(artifact_id.as_str()) {
                return Ok(());
            }
        }
        if let Some(row) = p
            .get_artifact_row(tenant_id.as_str(), artifact_id.as_str())
            .map_err(internal)?
        {
            let mut s = self.state.lock().await;
            s.artifacts
                .entry(artifact_id.as_str().to_string())
                .or_insert(row);
        }
        Ok(())
    }

    async fn ensure_record_cached(&self, tenant_id: &Id, record_id: &Id) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        {
            let s = self.state.lock().await;
            if s.records.contains_key(record_id.as_str()) {
                return Ok(());
            }
        }
        if let Some(row) = p
            .get_record_row(tenant_id.as_str(), record_id.as_str())
            .map_err(internal)?
        {
            let mut s = self.state.lock().await;
            s.records
                .entry(record_id.as_str().to_string())
                .or_insert(row);
        }
        Ok(())
    }

    async fn authoritative_tenant_records(&self, tenant_id: &Id) -> Result<Vec<SearchableRecord>> {
        match &self.persist {
            Some(p) => {
                let scanned = p
                    .scan_tenant_records(tenant_id.as_str())
                    .map_err(internal)?;
                let s = self.state.lock().await;
                Ok(scanned
                    .into_iter()
                    .map(|row| {
                        s.records
                            .get(row.record_id.as_str())
                            .cloned()
                            .unwrap_or(row)
                    })
                    .collect())
            }
            None => {
                let s = self.state.lock().await;
                Ok(s.records
                    .values()
                    .filter(|r| r.tenant_id.as_str() == tenant_id.as_str())
                    .cloned()
                    .collect())
            }
        }
    }

    async fn ensure_tenant_alias_cached(&self, external_key: &str) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        if external_key.is_empty() {
            return Ok(());
        }
        {
            let s = self.state.lock().await;
            if s.tenant_aliases.contains_key(external_key) {
                return Ok(());
            }
        }
        if let Some(tenant_id) = p.get_tenant_alias_row(external_key).map_err(internal)? {
            if let Ok(tenant_id) = Id::parse_kind(&tenant_id, crate::id::IdKind::Tenant) {
                self.ensure_tenant_cached(&tenant_id).await?;
            }
            let mut s = self.state.lock().await;
            s.tenant_aliases
                .entry(external_key.to_string())
                .or_insert(tenant_id);
        }
        Ok(())
    }

    async fn ensure_agent_alias_cached(&self, tenant_id: &Id, alias: &str) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        if alias.is_empty() {
            return Ok(());
        }
        let key = scoped_alias_key(tenant_id.as_str(), alias);
        {
            let s = self.state.lock().await;
            if s.agent_aliases.contains_key(&key) {
                return Ok(());
            }
        }
        if let Some(agent_id) = p
            .get_agent_alias_row(tenant_id.as_str(), alias)
            .map_err(internal)?
        {
            if let Ok(agent_id) = Id::parse_kind(&agent_id, crate::id::IdKind::Agent) {
                self.ensure_agent_cached(tenant_id, &agent_id).await?;
            }
            let mut s = self.state.lock().await;
            s.agent_aliases.entry(key).or_insert(agent_id);
        }
        Ok(())
    }

    async fn ensure_session_alias_cached(&self, tenant_id: &Id, alias: &str) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        if alias.is_empty() {
            return Ok(());
        }
        let key = scoped_alias_key(tenant_id.as_str(), alias);
        {
            let s = self.state.lock().await;
            if s.session_aliases.contains_key(&key) {
                return Ok(());
            }
        }
        if let Some(session_id) = p
            .get_session_alias_row(tenant_id.as_str(), alias)
            .map_err(internal)?
        {
            if let Ok(session_id) = Id::parse_kind(&session_id, crate::id::IdKind::Session) {
                self.ensure_session_cached(tenant_id, &session_id).await?;
            }
            let mut s = self.state.lock().await;
            s.session_aliases.entry(key).or_insert(session_id);
        }
        Ok(())
    }

    // ----- Alias resolvers (O(1) resolve-or-create support) ----------------
    //
    // These back `POST /api/sessions/open`: look up an existing row by its
    // alias/external key through the alias map (a single point read on a
    // miss), never an unbounded list scan. `None` means "no live row for this
    // alias" — the caller then creates one.

    /// Resolve a live tenant by its external key (workspace name), or `None`.
    pub async fn resolve_tenant_by_external_key(&self, external_key: &str) -> Result<Option<Tenant>> {
        if external_key.is_empty() {
            return Ok(None);
        }
        self.ensure_tenant_alias_cached(external_key).await?;
        let tenant_id = {
            let s = self.state.lock().await;
            s.tenant_aliases.get(external_key).cloned()
        };
        let Some(tenant_id) = tenant_id else {
            return Ok(None);
        };
        let Ok(id) = Id::parse_kind(&tenant_id, crate::id::IdKind::Tenant) else {
            return Ok(None);
        };
        match self.get_tenant(&id).await {
            Ok(t) => Ok(Some(t)),
            Err(e) if e.code == ErrorCode::TenantNotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Resolve a live agent by `(tenant, alias)`, or `None`.
    pub async fn resolve_agent_by_alias(
        &self,
        tenant_id: &Id,
        alias: &str,
    ) -> Result<Option<Agent>> {
        if alias.is_empty() {
            return Ok(None);
        }
        self.ensure_agent_alias_cached(tenant_id, alias).await?;
        let key = scoped_alias_key(tenant_id.as_str(), alias);
        let agent_id = {
            let s = self.state.lock().await;
            s.agent_aliases.get(&key).cloned()
        };
        let Some(agent_id) = agent_id else {
            return Ok(None);
        };
        let Ok(id) = Id::parse_kind(&agent_id, crate::id::IdKind::Agent) else {
            return Ok(None);
        };
        match self.get_agent(tenant_id, &id).await {
            Ok(a) => Ok(Some(a)),
            Err(e) if e.code == ErrorCode::AgentNotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Resolve a live session by `(tenant, alias)`, or `None`. Session aliases
    /// are unique per tenant (not per agent), so this does not take an agent —
    /// the caller checks `session.agent_id` against the intended agent.
    pub async fn resolve_session_by_alias(
        &self,
        tenant_id: &Id,
        alias: &str,
    ) -> Result<Option<Session>> {
        if alias.is_empty() {
            return Ok(None);
        }
        self.ensure_session_alias_cached(tenant_id, alias).await?;
        let key = scoped_alias_key(tenant_id.as_str(), alias);
        let session_id = {
            let s = self.state.lock().await;
            s.session_aliases.get(&key).cloned()
        };
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let Ok(id) = Id::parse_kind(&session_id, crate::id::IdKind::Session) else {
            return Ok(None);
        };
        match self.get_session(tenant_id, &id).await {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.code == ErrorCode::SessionNotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    // ----- Authoritative enumerations (Stage 2 of KV-authoritative state) --
    //
    // Enumerations must not depend on the cache being complete: in lazy
    // hydration mode the maps hold only touched rows. The durable KV scan is
    // the complete base set; cached rows overlay it because they are fresher
    // (correct next_seq, uncheckpointed mutations). When persist is None the
    // maps ARE the store and are returned directly.

    async fn authoritative_all_tenants(&self) -> Result<Vec<Tenant>> {
        match &self.persist {
            Some(p) => {
                let scanned = p.scan_all_tenants().map_err(internal)?;
                let s = self.state.lock().await;
                Ok(scanned
                    .into_iter()
                    .map(|row| {
                        s.tenants
                            .get(row.tenant_id.as_str())
                            .cloned()
                            .unwrap_or(row)
                    })
                    .collect())
            }
            None => {
                let s = self.state.lock().await;
                Ok(s.tenants.values().cloned().collect())
            }
        }
    }

    async fn authoritative_tenant_agents(&self, tenant_id: &Id) -> Result<Vec<Agent>> {
        match &self.persist {
            Some(p) => {
                let scanned = p.scan_tenant_agents(tenant_id.as_str()).map_err(internal)?;
                let s = self.state.lock().await;
                Ok(scanned
                    .into_iter()
                    .map(|row| s.agents.get(row.agent_id.as_str()).cloned().unwrap_or(row))
                    .collect())
            }
            None => {
                let s = self.state.lock().await;
                Ok(s.agents
                    .values()
                    .filter(|a| a.tenant_id.as_str() == tenant_id.as_str())
                    .cloned()
                    .collect())
            }
        }
    }

    async fn authoritative_tenant_sessions(&self, tenant_id: &Id) -> Result<Vec<Session>> {
        match &self.persist {
            Some(p) => {
                let scanned = p
                    .scan_tenant_sessions(tenant_id.as_str())
                    .map_err(internal)?;
                let s = self.state.lock().await;
                Ok(scanned
                    .into_iter()
                    .map(|row| {
                        s.sessions
                            .get(row.session_id.as_str())
                            .cloned()
                            .unwrap_or(row)
                    })
                    .collect())
            }
            None => {
                let s = self.state.lock().await;
                Ok(s.sessions
                    .values()
                    .filter(|sess| sess.tenant_id.as_str() == tenant_id.as_str())
                    .cloned()
                    .collect())
            }
        }
    }

    async fn authoritative_tenant_artifacts(&self, tenant_id: &Id) -> Result<Vec<Artifact>> {
        match &self.persist {
            Some(p) => {
                let scanned = p
                    .scan_tenant_artifacts(tenant_id.as_str())
                    .map_err(internal)?;
                let s = self.state.lock().await;
                Ok(scanned
                    .into_iter()
                    .map(|row| {
                        s.artifacts
                            .get(row.artifact_id.as_str())
                            .cloned()
                            .unwrap_or(row)
                    })
                    .collect())
            }
            None => {
                let s = self.state.lock().await;
                Ok(s.artifacts
                    .values()
                    .filter(|a| a.tenant_id.as_str() == tenant_id.as_str())
                    .cloned()
                    .collect())
            }
        }
    }

    async fn authoritative_all_sessions(&self) -> Result<Vec<Session>> {
        match &self.persist {
            Some(p) => {
                let scanned = p.scan_all_sessions().map_err(internal)?;
                let s = self.state.lock().await;
                Ok(scanned
                    .into_iter()
                    .map(|row| {
                        s.sessions
                            .get(row.session_id.as_str())
                            .cloned()
                            .unwrap_or(row)
                    })
                    .collect())
            }
            None => {
                let s = self.state.lock().await;
                Ok(s.sessions.values().cloned().collect())
            }
        }
    }

    async fn authoritative_all_artifacts(&self) -> Result<Vec<Artifact>> {
        match &self.persist {
            Some(p) => {
                let scanned = p.scan_all_artifacts().map_err(internal)?;
                let s = self.state.lock().await;
                Ok(scanned
                    .into_iter()
                    .map(|row| {
                        s.artifacts
                            .get(row.artifact_id.as_str())
                            .cloned()
                            .unwrap_or(row)
                    })
                    .collect())
            }
            None => {
                let s = self.state.lock().await;
                Ok(s.artifacts.values().cloned().collect())
            }
        }
    }

    /// Drop a tenant's cached aggregates after any mutation that changes
    /// them, so the next advisory read recomputes instead of serving a
    /// pre-mutation snapshot for up to the cache TTL.
    async fn invalidate_tenant_aggregates(&self, tenant_id: &Id) {
        let mut s = self.state.lock().await;
        s.tenant_aggregate_cache.remove(tenant_id.as_str());
    }

    /// Compute per-tenant aggregates from the authoritative enumerations and
    /// refresh the cache entry. `max_age` permits serving the cached value
    /// (advisory paths pass ~2s; quota-enforcing creates pass ZERO).
    async fn tenant_aggregates_cached(
        &self,
        tenant_id: &Id,
        max_age: std::time::Duration,
    ) -> Result<TenantAggregates> {
        if !max_age.is_zero() {
            let s = self.state.lock().await;
            if let Some((agg, at)) = s.tenant_aggregate_cache.get(tenant_id.as_str()) {
                if at.elapsed() <= max_age {
                    return Ok(*agg);
                }
            }
        }
        let sessions = self.authoritative_tenant_sessions(tenant_id).await?;
        let agents = self.authoritative_tenant_agents(tenant_id).await?;
        let artifacts = self.authoritative_tenant_artifacts(tenant_id).await?;
        let agg = TenantAggregates {
            active_sessions: sessions
                .iter()
                .filter(|sess| {
                    sess.deleted_at_ms.is_none() && sess.status == crate::session::Status::Active
                })
                .count(),
            live_agents: agents.iter().filter(|a| a.deleted_at_ms.is_none()).count(),
            live_artifacts: artifacts
                .iter()
                .filter(|a| a.deleted_at_ms.is_none())
                .count(),
            live_artifact_bytes: artifacts
                .iter()
                .filter(|a| a.deleted_at_ms.is_none())
                .map(|a| a.bytes)
                .sum(),
            live_records: self
                .authoritative_tenant_records(tenant_id)
                .await?
                .iter()
                .filter(|r| r.deleted_at_ms.is_none() && !r.is_expired())
                .count(),
        };
        let mut s = self.state.lock().await;
        s.tenant_aggregate_cache.insert(
            tenant_id.as_str().to_string(),
            (agg, std::time::Instant::now()),
        );
        Ok(agg)
    }

    /// Pull the authoritative metadata population for a scope into the
    /// cache (entry-or-insert, so fresher cached rows win). Batch jobs whose
    /// mutation logic walks the cached maps (GC purge, policy workers) call
    /// this first so lazy hydration mode sees the full population; the
    /// eviction policy trims the cache again afterwards.
    async fn warm_metadata_caches_for_scope(&self, tenant_scope: Option<&Id>) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        let tenants = match tenant_scope {
            Some(tenant_id) => p
                .get_tenant_row(tenant_id.as_str())
                .map_err(internal)?
                .into_iter()
                .collect(),
            None => p.scan_all_tenants().map_err(internal)?,
        };
        let (agents, sessions, artifacts) = match tenant_scope {
            Some(tenant_id) => (
                p.scan_tenant_agents(tenant_id.as_str()).map_err(internal)?,
                p.scan_tenant_sessions(tenant_id.as_str())
                    .map_err(internal)?,
                p.scan_tenant_artifacts(tenant_id.as_str())
                    .map_err(internal)?,
            ),
            None => (
                p.scan_all_agents().map_err(internal)?,
                p.scan_all_sessions().map_err(internal)?,
                p.scan_all_artifacts().map_err(internal)?,
            ),
        };
        let mut s = self.state.lock().await;
        for row in tenants {
            s.tenants
                .entry(row.tenant_id.as_str().to_string())
                .or_insert(row);
        }
        for row in agents {
            s.agents
                .entry(row.agent_id.as_str().to_string())
                .or_insert(row);
        }
        for row in sessions {
            s.sessions
                .entry(row.session_id.as_str().to_string())
                .or_insert(row);
        }
        for row in artifacts {
            s.artifacts
                .entry(row.artifact_id.as_str().to_string())
                .or_insert(row);
        }
        drop(s);
        let records = match tenant_scope {
            Some(tenant_id) => p
                .scan_tenant_records(tenant_id.as_str())
                .map_err(internal)?,
            None => p.scan_all_records().map_err(internal)?,
        };
        let mut s = self.state.lock().await;
        for row in records {
            s.records
                .entry(row.record_id.as_str().to_string())
                .or_insert(row);
        }
        Ok(())
    }

    /// Warm the tenant cache for every id in the iterator. Batch jobs that
    /// resolve policy through the cached tenant rows call this after an
    /// authoritative enumeration so lazy hydration mode sees every tenant.
    async fn ensure_tenants_cached<I>(&self, tenant_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = Id>,
    {
        let mut seen = std::collections::BTreeSet::new();
        for tenant_id in tenant_ids {
            if seen.insert(tenant_id.as_str().to_string()) {
                self.ensure_tenant_cached(&tenant_id).await?;
            }
        }
        Ok(())
    }

    /// Metadata-cache eviction caps: when a cached map grows past its cap,
    /// the sweep clears it (rows reload on demand through the KV fallback).
    /// Defaults match the historical hydration caps, so a lazy runtime's
    /// resident metadata never exceeds what full hydration would have held.
    fn metadata_cache_cap(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default)
    }

    /// Evict oversized metadata caches (lazy mode only — in full hydration
    /// the maps ARE the hydrated working set and eviction would fight the
    /// hydrator). Whole-map clear is deliberate: entries carry no recency
    /// information, every read path tolerates a cold cache (Stage 1's
    /// eviction-chaos test is the proof), and point reloads are cheap.
    /// Alias maps are exempt — fork/create hold in-memory alias
    /// *reservations* that are not yet durable, and they are small.
    pub async fn sweep_metadata_cache(&self) -> usize {
        let max_tenants = Self::metadata_cache_cap("AGENTDB_METADATA_CACHE_MAX_TENANTS", 10_000);
        let max_agents = Self::metadata_cache_cap("AGENTDB_METADATA_CACHE_MAX_AGENTS", 100_000);
        let max_sessions = Self::metadata_cache_cap("AGENTDB_METADATA_CACHE_MAX_SESSIONS", 250_000);
        let max_artifacts =
            Self::metadata_cache_cap("AGENTDB_METADATA_CACHE_MAX_ARTIFACTS", 250_000);
        self.sweep_metadata_cache_with_caps(max_tenants, max_agents, max_sessions, max_artifacts)
            .await
    }

    /// Caps-explicit sweep core; public for deterministic tests.
    #[doc(hidden)]
    pub async fn sweep_metadata_cache_with_caps(
        &self,
        max_tenants: usize,
        max_agents: usize,
        max_sessions: usize,
        max_artifacts: usize,
    ) -> usize {
        if self.hydration_mode != HydrationMode::Lazy {
            return 0;
        }
        let mut evicted = 0usize;
        let mut s = self.state.lock().await;
        let max_records = Self::metadata_cache_cap("AGENTDB_METADATA_CACHE_MAX_RECORDS", 100_000);
        if s.records.len() > max_records {
            evicted += s.records.len();
            s.records.clear();
        }
        if s.sessions.len() > max_sessions {
            evicted += s.sessions.len();
            s.sessions.clear();
        }
        if s.artifacts.len() > max_artifacts {
            evicted += s.artifacts.len();
            s.artifacts.clear();
        }
        if s.agents.len() > max_agents {
            evicted += s.agents.len();
            s.agents.clear();
        }
        if s.tenants.len() > max_tenants {
            evicted += s.tenants.len();
            s.tenants.clear();
            s.tenant_aggregate_cache.clear();
        }
        if evicted > 0 {
            tracing::info!(evicted, "metadata cache sweep cleared oversized maps");
        }
        evicted
    }

    /// Test-only: drop the metadata caches so the KV fallback path is
    /// exercised. Meaningful only for persistent runtimes — an in-memory
    /// runtime's maps ARE the store. Records, indexes, skills, and
    /// tenant-meta stay cached (their fallback lands in Stage 2).
    #[doc(hidden)]
    pub async fn debug_evict_metadata_caches(&self) {
        if self.persist.is_none() {
            return;
        }
        let mut s = self.state.lock().await;
        s.tenants.clear();
        s.agents.clear();
        s.sessions.clear();
        s.artifacts.clear();
        s.tenant_aliases.clear();
        s.agent_aliases.clear();
        s.session_aliases.clear();
    }

    pub async fn get_tenant(&self, tenant_id: &Id) -> Result<Tenant> {
        self.refresh_replicated_state().await?;
        // Bounded retry: an eviction can race between the ensure step and the
        // cache read; re-running the ensure re-loads the row from KV. A
        // genuine not-found simply fails all attempts with cheap point gets.
        for attempt in 0..16 {
            self.ensure_tenant_cached(tenant_id).await?;
            let result = {
                let s = self.state.lock().await;
                s.tenants
                    .get(tenant_id.as_str())
                    .filter(|t| t.deleted_at_ms.is_none())
                    .cloned()
                    .ok_or_else(|| Error::new(ErrorCode::TenantNotFound, format!("{}", tenant_id)))
            };
            match result {
                Err(e) if Self::metadata_cache_miss_retryable(e.code) && attempt < 15 => continue,
                result => return result,
            }
        }
        unreachable!("bounded retry loop returns")
    }

    pub async fn list_tenants(&self) -> Vec<Tenant> {
        let mut tenants = self.authoritative_all_tenants().await.unwrap_or_default();
        tenants.retain(|t| t.deleted_at_ms.is_none() && !is_system_tenant(t.tenant_id.as_str()));
        tenants
    }

    /// Look up an agent by id, scoped to the given tenant. Returns
    /// `AgentNotFound` if the id is unknown or belongs to a different tenant.
    pub async fn get_agent(&self, tenant_id: &Id, agent_id: &Id) -> Result<Agent> {
        for attempt in 0..16 {
            self.ensure_tenant_cached(tenant_id).await?;
            self.ensure_agent_cached(tenant_id, agent_id).await?;
            let result = {
                let s = self.state.lock().await;
                let tenant_live = s
                    .tenants
                    .get(tenant_id.as_str())
                    .is_some_and(|t| t.deleted_at_ms.is_none());
                s.agents
                    .get(agent_id.as_str())
                    .filter(|a| {
                        tenant_live
                            && a.deleted_at_ms.is_none()
                            && a.tenant_id.as_str() == tenant_id.as_str()
                    })
                    .cloned()
                    .ok_or_else(|| Error::new(ErrorCode::AgentNotFound, format!("{}", agent_id)))
            };
            match result {
                Err(e) if Self::metadata_cache_miss_retryable(e.code) && attempt < 15 => continue,
                result => return result,
            }
        }
        unreachable!("bounded retry loop returns")
    }

    pub async fn list_agents(&self, tenant_id: &Id) -> Result<Vec<Agent>> {
        self.get_tenant(tenant_id).await?;
        let mut agents: Vec<Agent> = self
            .authoritative_tenant_agents(tenant_id)
            .await?
            .into_iter()
            .filter(|agent| {
                agent.deleted_at_ms.is_none() && agent.tenant_id.as_str() == tenant_id.as_str()
            })
            .collect();
        agents.sort_by(|a, b| {
            a.alias
                .as_deref()
                .unwrap_or(a.agent_id.as_str())
                .cmp(b.alias.as_deref().unwrap_or(b.agent_id.as_str()))
        });
        Ok(agents)
    }

    pub async fn delete_tenant(&self, tenant_id: &Id) -> Result<()> {
        let mut s = self.state.lock().await;
        let live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        if !live {
            return Err(Error::new(
                ErrorCode::TenantNotFound,
                format!("{}", tenant_id),
            ));
        }

        let updated = {
            let tenant = s
                .tenants
                .get_mut(tenant_id.as_str())
                .expect("presence checked above");
            tenant.deleted_at_ms = Some(crate::now_ms());
            tenant.clone()
        };

        if let Some(p) = &self.persist {
            if let Err(e) = p.put_tenant(&updated) {
                if let Some(tenant) = s.tenants.get_mut(tenant_id.as_str()) {
                    tenant.deleted_at_ms = None;
                }
                drop(s);
                return Err(internal(e));
            }
        }
        drop(s);
        self.invalidate_tenant_aggregates(tenant_id).await;
        Ok(())
    }

    pub async fn delete_agent(&self, tenant_id: &Id, agent_id: &Id) -> Result<()> {
        let mut s = self.state.lock().await;
        let live = s.agents.get(agent_id.as_str()).is_some_and(|agent| {
            agent.deleted_at_ms.is_none() && agent.tenant_id.as_str() == tenant_id.as_str()
        });
        if !live {
            return Err(Error::new(
                ErrorCode::AgentNotFound,
                format!("{}", agent_id),
            ));
        }

        let updated = {
            let agent = s
                .agents
                .get_mut(agent_id.as_str())
                .expect("presence checked above");
            agent.deleted_at_ms = Some(crate::now_ms());
            agent.clone()
        };

        if let Some(p) = &self.persist {
            if let Err(e) = p.put_agent(&updated) {
                if let Some(agent) = s.agents.get_mut(agent_id.as_str()) {
                    agent.deleted_at_ms = None;
                }
                drop(s);
                return Err(internal(e));
            }
        }
        drop(s);
        self.invalidate_tenant_aggregates(tenant_id).await;
        Ok(())
    }

    /// Look up a session by id, scoped to the given tenant. Returns
    /// `SessionNotFound` if the id is unknown or belongs to a different tenant.
    pub async fn get_session(&self, tenant_id: &Id, session_id: &Id) -> Result<Session> {
        for attempt in 0..16 {
            self.ensure_tenant_cached(tenant_id).await?;
            self.ensure_session_cached(tenant_id, session_id).await?;
            let result = {
                let s = self.state.lock().await;
                let tenant_live = s
                    .tenants
                    .get(tenant_id.as_str())
                    .is_some_and(|t| t.deleted_at_ms.is_none());
                s.sessions
                    .get(session_id.as_str())
                    .filter(|sess| {
                        tenant_live
                            && sess.deleted_at_ms.is_none()
                            && sess.tenant_id.as_str() == tenant_id.as_str()
                    })
                    .cloned()
                    .ok_or_else(|| {
                        Error::new(ErrorCode::SessionNotFound, format!("{}", session_id))
                    })
            };
            match result {
                Err(e) if Self::metadata_cache_miss_retryable(e.code) && attempt < 15 => continue,
                result => return result,
            }
        }
        unreachable!("bounded retry loop returns")
    }

    pub async fn get_session_storage(
        &self,
        tenant_id: &Id,
        session_id: &Id,
    ) -> Result<SessionStorageSummary> {
        let session = self.get_session(tenant_id, session_id).await?;
        Ok(Self::session_storage_summary(&session))
    }

    pub async fn tenant_storage_summary(&self, tenant_id: &Id) -> Result<TenantStorageSummary> {
        let sessions = self.list_sessions(tenant_id).await?;
        Ok(Self::summarize_tenant_storage(tenant_id, &sessions))
    }

    pub async fn fleet_storage_summary(&self) -> FleetStorageSummary {
        let live_tenants: Vec<Tenant> = self
            .authoritative_all_tenants()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|tenant| tenant.deleted_at_ms.is_none())
            .collect();
        let all_sessions = self.authoritative_all_sessions().await.unwrap_or_default();

        let mut tenant_summaries = Vec::with_capacity(live_tenants.len());
        let mut lifecycle_counts = LifecycleCounts::default();
        let mut session_count = 0usize;
        let mut active_session_count = 0usize;
        let mut sealed_segment_count = 0usize;
        let mut archive_manifest_count = 0usize;
        let mut sealed_bytes = 0u64;
        let mut archived_bytes = 0u64;

        for tenant in &live_tenants {
            let sessions: Vec<Session> = all_sessions
                .iter()
                .filter(|session| {
                    session.deleted_at_ms.is_none()
                        && session.tenant_id.as_str() == tenant.tenant_id.as_str()
                })
                .cloned()
                .collect();
            let summary = Self::summarize_tenant_storage(&tenant.tenant_id, &sessions);
            session_count += summary.session_count;
            active_session_count += summary.active_session_count;
            sealed_segment_count += summary.sealed_segment_count;
            archive_manifest_count += summary.archive_manifest_count;
            sealed_bytes += summary.sealed_bytes;
            archived_bytes += summary.archived_bytes;
            lifecycle_counts.warm += summary.lifecycle_counts.warm;
            lifecycle_counts.cold += summary.lifecycle_counts.cold;
            lifecycle_counts.archive += summary.lifecycle_counts.archive;
            lifecycle_counts.derivative_only += summary.lifecycle_counts.derivative_only;
            tenant_summaries.push(summary);
        }

        FleetStorageSummary {
            tenant_count: live_tenants.len(),
            session_count,
            active_session_count,
            lifecycle_counts,
            sealed_segment_count,
            archive_manifest_count,
            sealed_bytes,
            archived_bytes,
            tenants: tenant_summaries,
        }
    }

    pub async fn transition_session_lifecycle(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        target: Lifecycle,
    ) -> Result<SessionStorageSummary> {
        self.ensure_tenant_cached(tenant_id).await?;
        self.ensure_session_cached(tenant_id, session_id).await?;
        let session_commit_lock = self.session_commit_lock(session_id);
        let _session_commit_guard = session_commit_lock.lock().await;
        match target {
            Lifecycle::Warm => {
                let updated = {
                    let mut s = self.state.lock().await;
                    let session = s
                        .sessions
                        .get_mut(session_id.as_str())
                        .filter(|sess| {
                            sess.deleted_at_ms.is_none()
                                && sess.tenant_id.as_str() == tenant_id.as_str()
                        })
                        .ok_or_else(|| {
                            Error::new(ErrorCode::SessionNotFound, format!("{}", session_id))
                        })?;
                    session.lifecycle = Lifecycle::Warm;
                    if session.warm_head_from_seq > session.next_seq {
                        session.warm_head_from_seq = session.next_seq;
                    }
                    session.clone()
                };
                if let Some(p) = &self.persist {
                    p.put_session(&updated).map_err(internal)?;
                }
                Ok(Self::session_storage_summary(&updated))
            }
            Lifecycle::Cold => self.seal_session_to_cold(tenant_id, session_id).await,
            Lifecycle::Archive => {
                self.archive_session(tenant_id, session_id).await
            }
            Lifecycle::DerivativeOnly => Err(Error::new(
                ErrorCode::PolicyDenied,
                "derivative_only transition is driven by lifecycle/gc policy, not direct session transition",
            )),
        }
    }

    pub async fn restore_session_from_archive(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        target: Lifecycle,
    ) -> Result<SessionStorageSummary> {
        self.ensure_tenant_cached(tenant_id).await?;
        self.ensure_session_cached(tenant_id, session_id).await?;
        let session_commit_lock = self.session_commit_lock(session_id);
        let _session_commit_guard = session_commit_lock.lock().await;
        if !matches!(target, Lifecycle::Warm | Lifecycle::Cold) {
            return Err(Error::new(
                ErrorCode::PolicyDenied,
                "archive restore target must be warm or cold",
            ));
        }

        let snapshot = self.get_session(tenant_id, session_id).await?;
        let manifest = snapshot.archive_manifest.clone().ok_or_else(|| {
            Error::new(
                ErrorCode::PolicyDenied,
                format!("session {} has no archive manifest to restore", session_id),
            )
        })?;

        let history = self
            .fetch_from_seq(FetchRequest {
                tenant_id: tenant_id.clone(),
                session_id: session_id.clone(),
                from_seq: 0,
                limit: usize::MAX,
            })
            .await?;
        let restored_segment = if history.is_empty() {
            None
        } else {
            Some(
                self.write_session_segment(tenant_id, session_id, history)
                    .await?,
            )
        };

        let updated = {
            let mut s = self.state.lock().await;
            {
                let session = s
                    .sessions
                    .get_mut(session_id.as_str())
                    .filter(|sess| {
                        sess.deleted_at_ms.is_none()
                            && sess.tenant_id.as_str() == tenant_id.as_str()
                    })
                    .ok_or_else(|| {
                        Error::new(ErrorCode::SessionNotFound, format!("{}", session_id))
                    })?;
                if session.next_seq != snapshot.next_seq
                    || session.warm_head_from_seq != snapshot.warm_head_from_seq
                {
                    return Err(Error::new(
                        ErrorCode::Internal,
                        "session changed during archive restore; retry the restore",
                    ));
                }
                session.archive_manifest = None;
                session.sealed_segments = restored_segment.clone().into_iter().collect();
                session.warm_head_from_seq = session.next_seq;
                session.lifecycle = target;
            }
            if self.persist.is_none() {
                let lo = (session_id.as_str().to_string(), snapshot.warm_head_from_seq);
                let hi = (session_id.as_str().to_string(), u64::MAX);
                let keys: Vec<(String, u64)> =
                    s.events.range(lo..=hi).map(|(k, _)| k.clone()).collect();
                for key in keys {
                    if let Some(event) = s.events.remove(&key) {
                        s.event_by_id.remove(event.event_id.as_str());
                    }
                }
            }
            s.sessions
                .get(session_id.as_str())
                .expect("session exists after archive restore")
                .clone()
        };

        if let Some(p) = &self.persist {
            if let Some(segment) = &restored_segment {
                p.put_session_segment(segment).map_err(internal)?;
            }
            p.put_session(&updated).map_err(internal)?;
            let _ = p.delete_events_from_seq(
                tenant_id.as_str(),
                session_id.as_str(),
                snapshot.warm_head_from_seq,
            );
        }

        let _ = self
            .content
            .delete(tenant_id.as_str(), manifest.manifest_id.as_str())
            .await;

        Ok(Self::session_storage_summary(&updated))
    }

    async fn seal_session_to_cold(
        &self,
        tenant_id: &Id,
        session_id: &Id,
    ) -> Result<SessionStorageSummary> {
        let (snapshot, hot_events) = {
            let s = self.state.lock().await;
            let session = s
                .sessions
                .get(session_id.as_str())
                .filter(|sess| {
                    sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
                })
                .cloned()
                .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))?;
            let hot_events = if self.persist.is_none() {
                let lo = (session_id.as_str().to_string(), session.warm_head_from_seq);
                let hi = (session_id.as_str().to_string(), u64::MAX);
                s.events
                    .range(lo..=hi)
                    .map(|(_, event)| event.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            (session, hot_events)
        };

        if snapshot.lifecycle == Lifecycle::Cold {
            return Ok(Self::session_storage_summary(&snapshot));
        }

        if snapshot.warm_head_from_seq >= snapshot.next_seq {
            let updated = {
                let mut s = self.state.lock().await;
                let session = s
                    .sessions
                    .get_mut(session_id.as_str())
                    .expect("session exists");
                session.lifecycle = Lifecycle::Cold;
                session.clone()
            };
            if let Some(p) = &self.persist {
                p.put_session(&updated).map_err(internal)?;
            }
            return Ok(Self::session_storage_summary(&updated));
        }

        let events = if self.persist.is_some() {
            self.read_hot_events_from_persist(
                tenant_id,
                session_id,
                snapshot.warm_head_from_seq,
                usize::MAX,
            )
            .await?
        } else {
            hot_events
        };

        if events.is_empty() {
            let updated = {
                let mut s = self.state.lock().await;
                let session = s
                    .sessions
                    .get_mut(session_id.as_str())
                    .expect("session exists");
                session.lifecycle = Lifecycle::Cold;
                session.clone()
            };
            if let Some(p) = &self.persist {
                p.put_session(&updated).map_err(internal)?;
            }
            return Ok(Self::session_storage_summary(&updated));
        }

        let segment = self
            .write_session_segment(tenant_id, session_id, events.clone())
            .await?;

        let updated = {
            let mut s = self.state.lock().await;
            {
                let session = s
                    .sessions
                    .get_mut(session_id.as_str())
                    .filter(|sess| {
                        sess.deleted_at_ms.is_none()
                            && sess.tenant_id.as_str() == tenant_id.as_str()
                    })
                    .ok_or_else(|| {
                        Error::new(ErrorCode::SessionNotFound, format!("{}", session_id))
                    })?;
                if session.next_seq != snapshot.next_seq
                    || session.warm_head_from_seq != snapshot.warm_head_from_seq
                {
                    return Err(Error::new(
                        ErrorCode::Internal,
                        "session changed during cold transition; retry the transition",
                    ));
                }
                session.sealed_segments.push(segment.clone());
                session.warm_head_from_seq = session.next_seq;
                session.lifecycle = Lifecycle::Cold;
            }
            if self.persist.is_none() {
                let lo = (session_id.as_str().to_string(), snapshot.warm_head_from_seq);
                let hi = (session_id.as_str().to_string(), u64::MAX);
                let keys: Vec<(String, u64)> =
                    s.events.range(lo..=hi).map(|(k, _)| k.clone()).collect();
                for key in keys {
                    if let Some(event) = s.events.remove(&key) {
                        s.event_by_id.remove(event.event_id.as_str());
                    }
                }
            }
            s.sessions
                .get(session_id.as_str())
                .expect("session exists after cold seal")
                .clone()
        };

        if let Some(p) = &self.persist {
            p.put_session_segment(&segment).map_err(internal)?;
            p.put_session(&updated).map_err(internal)?;
            let _ = p.delete_events_from_seq(
                tenant_id.as_str(),
                session_id.as_str(),
                snapshot.warm_head_from_seq,
            );
        }

        Ok(Self::session_storage_summary(&updated))
    }

    async fn archive_session(
        &self,
        tenant_id: &Id,
        session_id: &Id,
    ) -> Result<SessionStorageSummary> {
        let session = self.get_session(tenant_id, session_id).await?;
        if session.lifecycle == Lifecycle::Warm {
            self.seal_session_to_cold(tenant_id, session_id).await?;
        }
        let snapshot = self.get_session(tenant_id, session_id).await?;
        let history = self
            .fetch_from_seq(FetchRequest {
                tenant_id: tenant_id.clone(),
                session_id: session_id.clone(),
                from_seq: 0,
                limit: usize::MAX,
            })
            .await?;
        let source_segment_ids = snapshot
            .sealed_segments
            .iter()
            .map(|segment| segment.segment_id.clone())
            .collect::<Vec<_>>();
        let old_manifest = snapshot.archive_manifest.clone();
        let manifest = self
            .write_session_archive(tenant_id, session_id, source_segment_ids, history)
            .await?;
        let updated = {
            let mut s = self.state.lock().await;
            let session = s
                .sessions
                .get_mut(session_id.as_str())
                .filter(|sess| {
                    sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
                })
                .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))?;
            session.archive_manifest = Some(manifest.clone());
            session.sealed_segments.clear();
            session.warm_head_from_seq = session.next_seq;
            session.lifecycle = Lifecycle::Archive;
            session.clone()
        };
        if let Some(p) = &self.persist {
            p.put_session(&updated).map_err(internal)?;
        }
        for segment in snapshot.sealed_segments {
            let _ = self
                .content
                .delete(tenant_id.as_str(), segment.segment_id.as_str())
                .await;
        }
        if let Some(manifest) = old_manifest {
            let _ = self
                .content
                .delete(tenant_id.as_str(), manifest.manifest_id.as_str())
                .await;
        }
        Ok(Self::session_storage_summary(&updated))
    }

    pub async fn list_sessions(&self, tenant_id: &Id) -> Result<Vec<Session>> {
        self.get_tenant(tenant_id).await?;
        let mut sessions: Vec<Session> = self
            .authoritative_tenant_sessions(tenant_id)
            .await?
            .into_iter()
            .filter(|sess| {
                sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
            })
            .collect();
        sessions.sort_by(|a, b| {
            b.last_active_at_ms
                .cmp(&a.last_active_at_ms)
                .then_with(|| a.session_id.as_str().cmp(b.session_id.as_str()))
        });
        Ok(sessions)
    }

    pub async fn delete_session(&self, tenant_id: &Id, session_id: &Id) -> Result<()> {
        let session_commit_lock = self.session_commit_lock(session_id);
        let _session_commit_guard = session_commit_lock.lock().await;
        let mut s = self.state.lock().await;
        let live = s.sessions.get(session_id.as_str()).is_some_and(|sess| {
            sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
        });
        if !live {
            return Err(Error::new(
                ErrorCode::SessionNotFound,
                format!("{}", session_id),
            ));
        }

        let updated = {
            let sess = s
                .sessions
                .get_mut(session_id.as_str())
                .expect("presence checked above");
            sess.deleted_at_ms = Some(crate::now_ms());
            sess.clone()
        };

        if let Some(p) = &self.persist {
            if let Err(e) = p.put_session(&updated) {
                if let Some(sess) = s.sessions.get_mut(session_id.as_str()) {
                    sess.deleted_at_ms = None;
                }
                drop(s);
                return Err(internal(e));
            }
        }
        drop(s);
        self.invalidate_tenant_aggregates(tenant_id).await;
        Ok(())
    }

    pub async fn create_tenant(&self, req: NewTenant) -> Result<Tenant> {
        self.create_tenant_with_id(Id::new(IdKind::Tenant), req)
            .await
    }

    pub async fn create_tenant_with_id(&self, tenant_id: Id, req: NewTenant) -> Result<Tenant> {
        if is_system_tenant(tenant_id.as_str()) || req.external_key == SYSTEM_TENANT_EXTERNAL_KEY {
            return Err(Error::new(
                ErrorCode::PolicyDenied,
                "the system tenant is reserved and cannot be created through the public API",
            ));
        }
        self.create_tenant_with_id_unchecked(tenant_id, req).await
    }

    /// Idempotently create the reserved system tenant. Internal only: the
    /// public create path rejects the reserved id/key.
    pub async fn ensure_system_tenant(&self) -> Result<Tenant> {
        let system_id = Id::parse_kind(SYSTEM_TENANT_ID, IdKind::Tenant)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("system tenant id: {e:?}")))?;
        if let Ok(existing) = self.get_tenant(&system_id).await {
            return Ok(existing);
        }
        match self
            .create_tenant_with_id_unchecked(
                system_id.clone(),
                NewTenant {
                    external_key: SYSTEM_TENANT_EXTERNAL_KEY.to_string(),
                    policy: serde_json::json!({}),
                },
            )
            .await
        {
            Ok(tenant) => Ok(tenant),
            // Concurrent bootstrap on another node won the race.
            Err(e)
                if matches!(
                    e.code,
                    ErrorCode::IdempotencyConflict | ErrorCode::AliasConflict
                ) =>
            {
                self.get_tenant(&system_id).await
            }
            Err(e) => Err(e),
        }
    }

    async fn create_tenant_with_id_unchecked(
        &self,
        tenant_id: Id,
        req: NewTenant,
    ) -> Result<Tenant> {
        self.refresh_replicated_state().await?;
        self.ensure_tenant_cached(&tenant_id).await?;
        self.ensure_tenant_alias_cached(&req.external_key).await?;
        let t = {
            let mut s = self.state.lock().await;
            if s.tenants.contains_key(tenant_id.as_str()) {
                return Err(Error::new(
                    ErrorCode::IdempotencyConflict,
                    format!("tenant_id already in use: {tenant_id}"),
                ));
            }
            // External keys are tenant-name slots; a duplicate is an
            // unrecoverable client error (not a retry), so we surface it as
            // AliasConflict rather than IdempotencyConflict.
            if !req.external_key.is_empty() && s.tenant_aliases.contains_key(&req.external_key) {
                return Err(Error::new(
                    ErrorCode::AliasConflict,
                    format!("tenant external_key already in use: {}", req.external_key),
                ));
            }
            let t = Tenant::create_with_id(tenant_id, req);
            s.tenants
                .insert(t.tenant_id.as_str().to_string(), t.clone());
            if !t.external_key.is_empty() {
                s.tenant_aliases
                    .insert(t.external_key.clone(), t.tenant_id.as_str().to_string());
            }
            t
        };
        // Mint the tenant's data-encryption keys BEFORE persisting it: the very
        // first write (`put_tenant`, which also seals the tenant's export-ledger
        // copy) needs the DEK to already exist (cluster deployments; a no-op when
        // unset).
        if let Some(provisioner) = &self.key_provisioner {
            provisioner.provision_tenant(t.tenant_id.as_str()).await?;
        }
        if let Some(p) = &self.persist {
            p.put_tenant(&t).map_err(internal)?;
        }
        Ok(t)
    }

    pub async fn create_agent(&self, req: NewAgent) -> Result<Agent> {
        self.ensure_tenant_cached(&req.tenant_id).await?;
        if let Some(alias) = req.alias.as_deref() {
            self.ensure_agent_alias_cached(&req.tenant_id, alias)
                .await?;
        }
        // Quota admission uses the authoritative count (max_age ZERO), not
        // the cache, so lazy hydration cannot under-count.
        let agg = self
            .tenant_aggregates_cached(&req.tenant_id, std::time::Duration::ZERO)
            .await?;
        self.refresh_replicated_state().await?;
        let a = {
            let mut s = self.state.lock().await;
            let policy = active_tenant_policy_for_id(&s, &req.tenant_id).ok_or_else(|| {
                Error::new(ErrorCode::TenantNotFound, format!("{}", req.tenant_id))
            })?;
            if let Some(max) = policy.quota.max_agents {
                let live_agents = agg.live_agents;
                if live_agents >= max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies agent create: agent quota {max} exhausted",
                            policy.version
                        ),
                    ));
                }
            }
            if s.tenants
                .get(req.tenant_id.as_str())
                .is_none_or(|tenant| tenant.deleted_at_ms.is_some())
            {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
            // Agent alias is unique within (tenant, alias). Two tenants can
            // both have an agent named "rep-1".
            if let Some(alias) = req.alias.as_deref() {
                if !alias.is_empty() {
                    let key = scoped_alias_key(req.tenant_id.as_str(), alias);
                    if s.agent_aliases.contains_key(&key) {
                        return Err(Error::new(
                            ErrorCode::AliasConflict,
                            format!("agent alias already in use in tenant: {alias}"),
                        ));
                    }
                }
            }
            let a = Agent::create(req);
            s.agents.insert(a.agent_id.as_str().to_string(), a.clone());
            if let Some(alias) = a.alias.as_deref() {
                if !alias.is_empty() {
                    s.agent_aliases.insert(
                        scoped_alias_key(a.tenant_id.as_str(), alias),
                        a.agent_id.as_str().to_string(),
                    );
                }
            }
            a
        };
        if let Some(p) = &self.persist {
            p.put_agent(&a).map_err(internal)?;
        }
        self.invalidate_tenant_aggregates(&a.tenant_id).await;
        Ok(a)
    }

    pub async fn create_session(&self, req: NewSession) -> Result<Session> {
        self.create_session_inner(req, None).await
    }

    pub async fn create_session_with_id(&self, req: NewSession, session_id: Id) -> Result<Session> {
        self.create_session_inner(req, Some(session_id)).await
    }

    async fn create_session_inner(
        &self,
        req: NewSession,
        session_id: Option<Id>,
    ) -> Result<Session> {
        self.ensure_tenant_cached(&req.tenant_id).await?;
        self.ensure_agent_cached(&req.tenant_id, &req.agent_id)
            .await?;
        if let Some(alias) = req.alias.as_deref() {
            self.ensure_session_alias_cached(&req.tenant_id, alias)
                .await?;
        }
        let agg = self
            .tenant_aggregates_cached(&req.tenant_id, std::time::Duration::ZERO)
            .await?;
        self.refresh_replicated_state().await?;
        let sess = {
            let mut s = self.state.lock().await;
            let policy = active_tenant_policy_for_id(&s, &req.tenant_id).ok_or_else(|| {
                Error::new(ErrorCode::TenantNotFound, format!("{}", req.tenant_id))
            })?;
            if let Some(max) = policy.quota.max_active_sessions {
                let active_sessions = agg.active_sessions;
                if active_sessions >= max {
                    // Transient by design: lifecycle sweeps and session
                    // closure drain this quota, so a retrying agent should
                    // back off (429 + Retry-After), not treat it as a hard
                    // policy denial.
                    return Err(Error::new(
                        ErrorCode::RateLimited,
                        format!(
                            "policy_version {} defers session create: active session quota {max} exhausted",
                            policy.version
                        ),
                    )
                    .with_suggestion(
                        "retry after backoff; close or archive idle sessions to free quota",
                    ));
                }
            }
            // Verify the agent exists AND belongs to the requesting tenant. We
            // collapse "agent unknown" and "agent belongs to another tenant"
            // into the same `AgentNotFound` response so a caller in tenant A
            // can't probe tenant B's agent ids by error-code differential.
            let agent = s
                .agents
                .get(req.agent_id.as_str())
                .filter(|a| a.deleted_at_ms.is_none())
                .ok_or_else(|| Error::new(ErrorCode::AgentNotFound, format!("{}", req.agent_id)))?;
            if agent.tenant_id.as_str() != req.tenant_id.as_str() {
                return Err(Error::new(
                    ErrorCode::AgentNotFound,
                    format!("{}", req.agent_id),
                ));
            }
            // Session alias is unique within (tenant, alias).
            if let Some(alias) = req.alias.as_deref() {
                if !alias.is_empty() {
                    let key = scoped_alias_key(req.tenant_id.as_str(), alias);
                    if s.session_aliases.contains_key(&key) {
                        return Err(Error::new(
                            ErrorCode::AliasConflict,
                            format!("session alias already in use in tenant: {alias}"),
                        ));
                    }
                }
            }
            let sess = match session_id {
                Some(session_id) => Session::create_with_id(req, session_id),
                None => Session::create(req),
            };
            if s.sessions.contains_key(sess.session_id.as_str()) {
                return Err(Error::new(
                    ErrorCode::Internal,
                    format!("session id collision: {}", sess.session_id),
                ));
            }
            s.sessions
                .insert(sess.session_id.as_str().to_string(), sess.clone());
            if let Some(alias) = sess.alias.as_deref() {
                if !alias.is_empty() {
                    s.session_aliases.insert(
                        scoped_alias_key(sess.tenant_id.as_str(), alias),
                        sess.session_id.as_str().to_string(),
                    );
                }
            }
            sess
        };
        if let Some(p) = &self.persist {
            p.put_session(&sess).map_err(internal)?;
        }
        self.invalidate_tenant_aggregates(&sess.tenant_id).await;
        Ok(sess)
    }

    pub async fn fork_session(&self, req: ForkSessionRequest) -> Result<Session> {
        self.ensure_tenant_cached(&req.tenant_id).await?;
        self.ensure_session_cached(&req.tenant_id, &req.session_id)
            .await?;
        if let Some(alias) = req.alias.as_deref() {
            self.ensure_session_alias_cached(&req.tenant_id, alias)
                .await?;
        }
        let child = {
            let mut s = self.state.lock().await;
            let parent = s
                .sessions
                .get(req.session_id.as_str())
                .filter(|session| {
                    session.deleted_at_ms.is_none()
                        && session.tenant_id.as_str() == req.tenant_id.as_str()
                })
                .cloned()
                .ok_or_else(|| {
                    Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id))
                })?;
            let tenant_live = s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
            // `u64::MAX` is the "fork here" sentinel the API sends when the
            // caller omits at_seq: fork at the parent's current tip so the child
            // inherits the full committed history. An explicit at_seq forks at
            // an earlier point (rewind-and-branch).
            let at_seq = if req.at_seq == u64::MAX {
                parent.next_seq.saturating_sub(1)
            } else {
                req.at_seq
            };
            if at_seq >= parent.next_seq {
                return Err(Error::new(
                    ErrorCode::InvalidCursor,
                    format!(
                        "fork at_seq {} is outside parent session next_seq {}",
                        at_seq, parent.next_seq
                    ),
                ));
            }
            if let Some(alias) = req.alias.as_deref() {
                if !alias.is_empty() {
                    let key = scoped_alias_key(req.tenant_id.as_str(), alias);
                    if s.session_aliases.contains_key(&key) {
                        return Err(Error::new(
                            ErrorCode::AliasConflict,
                            format!("session alias already in use in tenant: {alias}"),
                        ));
                    }
                }
            }
            let mut child = Session::create(NewSession {
                tenant_id: req.tenant_id.clone(),
                agent_id: parent.agent_id.clone(),
                alias: req.alias,
            });
            child.forked_from = Some(ForkRef {
                session_id: parent.session_id,
                at_seq,
            });
            child.policy_version = parent.policy_version;
            // Reserve the alias under the lock so a concurrent fork/create
            // cannot claim it while we persist, but do NOT publish the
            // session yet: memory follows durable commit (same rule as the
            // append path), so a persist failure must leave no visible child.
            if let Some(alias) = child.alias.as_deref() {
                if !alias.is_empty() {
                    s.session_aliases.insert(
                        scoped_alias_key(child.tenant_id.as_str(), alias),
                        child.session_id.as_str().to_string(),
                    );
                }
            }
            child
        };
        if let Some(p) = &self.persist {
            if let Err(error) = p.put_forked_session(&child) {
                let mut s = self.state.lock().await;
                if let Some(alias) = child.alias.as_deref() {
                    if !alias.is_empty() {
                        let key = scoped_alias_key(child.tenant_id.as_str(), alias);
                        if s.session_aliases.get(&key).map(String::as_str)
                            == Some(child.session_id.as_str())
                        {
                            s.session_aliases.remove(&key);
                        }
                    }
                }
                return Err(internal(error));
            }
        }
        {
            let mut s = self.state.lock().await;
            s.sessions
                .insert(child.session_id.as_str().to_string(), child.clone());
            if self.persist.is_none() {
                // Persisted runtimes emit the fork-edge ledger item inside
                // `put_forked_session`; the in-memory ledger records it here so
                // parent-scoped export consumers discover the child branch.
                if let Some(fork) = child.forked_from.as_ref() {
                    let edge = ForkEdge {
                        parent_session_id: fork.session_id.clone(),
                        child_session_id: child.session_id.clone(),
                        fork_point_seq: fork.at_seq,
                        created_at_ms: child.created_at_ms,
                    };
                    push_in_memory_export_ledger_item(
                        &mut s,
                        export_ledger_item_from_fork_edge(
                            &child.tenant_id,
                            Some(child.agent_id.clone()),
                            edge,
                        ),
                    );
                }
            }
        }
        self.invalidate_tenant_aggregates(&req.tenant_id).await;
        Ok(child)
    }

    /// Record a sandbox snapshot reference at a session `seq`. The reference is
    /// stored on the session and travels with it; AgentDB never holds the bytes.
    /// `at_seq` must be within the session's committed range (`<= next_seq`).
    pub async fn record_session_snapshot(&self, req: NewSnapshotRef) -> Result<SnapshotRef> {
        self.ensure_tenant_cached(&req.tenant_id).await?;
        self.ensure_session_cached(&req.tenant_id, &req.session_id)
            .await?;
        if req.layers.is_empty() {
            return Err(Error::new(
                ErrorCode::PolicyDenied,
                "snapshot must reference at least one layer",
            ));
        }
        let session_commit_lock = self.session_commit_lock(&req.session_id);
        let _session_commit_guard = session_commit_lock.lock().await;
        let snapshot = SnapshotRef {
            snapshot_id: Id::new(IdKind::Snapshot),
            at_seq: req.at_seq,
            layers: req.layers,
            created_at_ms: crate::now_ms(),
            expires_at_ms: req.expires_at_ms,
        };
        let updated = {
            let mut s = self.state.lock().await;
            let session = s
                .sessions
                .get_mut(req.session_id.as_str())
                .filter(|sess| {
                    sess.deleted_at_ms.is_none()
                        && sess.tenant_id.as_str() == req.tenant_id.as_str()
                })
                .ok_or_else(|| {
                    Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id))
                })?;
            if req.at_seq > session.next_seq {
                return Err(Error::new(
                    ErrorCode::InvalidCursor,
                    format!(
                        "snapshot at_seq {} is beyond session next_seq {}",
                        req.at_seq, session.next_seq
                    ),
                ));
            }
            session.snapshots.push(snapshot.clone());
            session.snapshots.sort_by(|a, b| {
                a.at_seq
                    .cmp(&b.at_seq)
                    .then(a.created_at_ms.cmp(&b.created_at_ms))
            });
            session.clone()
        };
        if let Some(p) = &self.persist {
            p.put_session(&updated).map_err(internal)?;
        }
        Ok(snapshot)
    }

    /// Resolve which snapshot to restore to resume `session_id` at `now_ms`.
    /// Walks the fork chain so a freshly forked child inherits the parent's
    /// snapshot taken at or before the fork point. Expired (dangling) snapshots
    /// are skipped and reported via `dangling_skipped` so the caller can fall
    /// back to an earlier snapshot plus replay, or a clean start.
    pub async fn resolve_resume_snapshot(
        &self,
        tenant_id: &Id,
        session_id: &Id,
        now_ms: u64,
    ) -> Result<SnapshotResolution> {
        let mut current_id = session_id.clone();
        // For the leaf session every snapshot is eligible; for an ancestor only
        // snapshots at or before the fork point matter (later ancestor state is
        // not on this branch).
        let mut cutoff: Option<u64> = None;
        let mut is_leaf = true;
        let mut dangling_skipped = false;
        loop {
            let session = self.get_session(tenant_id, &current_id).await?;
            let mut eligible: Vec<&SnapshotRef> = session
                .snapshots
                .iter()
                .filter(|snap| cutoff.is_none_or(|max| snap.at_seq <= max))
                .collect();
            // Newest first.
            eligible.sort_by(|a, b| {
                b.at_seq
                    .cmp(&a.at_seq)
                    .then(b.created_at_ms.cmp(&a.created_at_ms))
            });
            for snap in eligible {
                if snap.is_expired_at(now_ms) {
                    dangling_skipped = true;
                    continue;
                }
                let replay_from_seq = if is_leaf { snap.at_seq } else { 0 };
                return Ok(SnapshotResolution {
                    snapshot: Some(snap.clone()),
                    owning_session_id: Some(session.session_id.clone()),
                    replay_from_seq,
                    dangling_skipped,
                });
            }
            let Some(parent) = session.forked_from else {
                break;
            };
            current_id = parent.session_id.clone();
            cutoff = Some(parent.at_seq);
            is_leaf = false;
        }
        Ok(SnapshotResolution {
            snapshot: None,
            owning_session_id: None,
            replay_from_seq: 0,
            dangling_skipped,
        })
    }

    pub async fn replay_session(&self, tenant_id: &Id, session_id: &Id) -> Result<ReplayResponse> {
        let leaf = self.get_session(tenant_id, session_id).await?;
        if let Some(prefix) = leaf.replay_prefix.clone() {
            let mut events = self.read_replay_prefix_events(&prefix).await?;
            let mut leaf_events = self
                .fetch_from_seq(FetchRequest {
                    tenant_id: tenant_id.clone(),
                    session_id: session_id.clone(),
                    from_seq: 0,
                    limit: usize::MAX,
                })
                .await?;
            let events_scanned = events.len() + leaf_events.len();
            events.append(&mut leaf_events);
            return Ok(ReplayResponse {
                session_id: session_id.clone(),
                events,
                fork_depth: prefix.fork_depth,
                segments_scanned: 2,
                events_scanned,
                materialized_prefix: true,
            });
        }

        let mut segments: Vec<(Id, Option<u64>)> = Vec::new();
        let mut current_id = leaf.session_id.clone();
        let mut current_cutoff = None;
        let mut next_session = Some(leaf);
        loop {
            let session = if let Some(session) = next_session.take() {
                session
            } else {
                self.get_session(tenant_id, &current_id).await?
            };
            segments.push((session.session_id.clone(), current_cutoff));
            let Some(parent) = session.forked_from else {
                break;
            };
            current_id = parent.session_id.clone();
            current_cutoff = Some(parent.at_seq);
        }

        let fork_depth = segments.len().saturating_sub(1);
        let segments_scanned = segments.len();
        segments.reverse();
        let mut events = Vec::new();
        let mut events_scanned = 0;
        for (sid, cutoff) in segments {
            let limit = cutoff
                .map(|max_seq| usize::try_from(max_seq.saturating_add(1)).unwrap_or(usize::MAX))
                .unwrap_or(usize::MAX);
            let mut segment = self
                .fetch_from_seq(FetchRequest {
                    tenant_id: tenant_id.clone(),
                    session_id: sid,
                    from_seq: 0,
                    limit,
                })
                .await?;
            events_scanned += segment.len();
            if let Some(max_seq) = cutoff {
                segment.retain(|event| event.seq <= max_seq);
            }
            events.extend(segment);
        }
        Ok(ReplayResponse {
            session_id: session_id.clone(),
            events,
            fork_depth,
            segments_scanned,
            events_scanned,
            materialized_prefix: false,
        })
    }

    /// Materialize the non-leaf replay prefix for a forked session. Future
    /// `replay_session` calls read one compact prefix object plus the leaf tail
    /// instead of rewalking every ancestor.
    pub async fn materialize_replay_prefix(
        &self,
        tenant_id: &Id,
        session_id: &Id,
    ) -> Result<ReplayResponse> {
        let session_commit_lock = self.session_commit_lock(session_id);
        let _session_commit_guard = session_commit_lock.lock().await;
        let leaf = self.get_session(tenant_id, session_id).await?;
        if leaf.forked_from.is_none() {
            return self.replay_session(tenant_id, session_id).await;
        }
        if leaf.replay_prefix.is_some() {
            return self.replay_session(tenant_id, session_id).await;
        }

        let replay = self.replay_session(tenant_id, session_id).await?;
        let prefix_events = replay
            .events
            .iter()
            .filter(|event| event.session_id.as_str() != session_id.as_str())
            .cloned()
            .collect::<Vec<_>>();
        let manifest = self
            .write_replay_prefix(tenant_id, session_id, prefix_events, replay.fork_depth)
            .await?;
        let updated = {
            let mut s = self.state.lock().await;
            let session = s
                .sessions
                .get_mut(session_id.as_str())
                .filter(|sess| {
                    sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
                })
                .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))?;
            if session.forked_from != leaf.forked_from {
                return Err(Error::new(
                    ErrorCode::InvalidCursor,
                    "session fork lineage changed during replay prefix materialization",
                ));
            }
            session.replay_prefix = Some(manifest.clone());
            session.clone()
        };
        if let Some(p) = &self.persist {
            if let Err(e) = p.put_session(&updated) {
                {
                    let mut s = self.state.lock().await;
                    if let Some(session) = s.sessions.get_mut(session_id.as_str()) {
                        session.replay_prefix = leaf.replay_prefix.clone();
                    }
                }
                let _ = self
                    .content
                    .delete(tenant_id.as_str(), manifest.prefix_id.as_str())
                    .await;
                return Err(internal(e));
            }
        }

        self.replay_session(tenant_id, session_id).await
    }

    pub async fn resolve_policy_for_tenant(&self, tenant_id: &Id) -> Result<RuntimePolicy> {
        let s = self.state.lock().await;
        let tenant = s
            .tenants
            .get(tenant_id.as_str())
            .filter(|t| t.deleted_at_ms.is_none())
            .ok_or_else(|| Error::new(ErrorCode::TenantNotFound, format!("{}", tenant_id)))?;
        Ok(RuntimePolicy::from_json(&tenant.policy))
    }

    pub async fn resolve_policy_for_agent(
        &self,
        tenant_id: &Id,
        agent_id: &Id,
    ) -> Result<RuntimePolicy> {
        let s = self.state.lock().await;
        resolve_policy_for_agent_in_state(&s, tenant_id, agent_id)
    }

    pub async fn backpressure_for_tenant(&self, tenant_id: &Id) -> Result<BackpressureReport> {
        self.ensure_tenant_cached(tenant_id).await?;
        let agg = self
            .tenant_aggregates_cached(tenant_id, std::time::Duration::from_secs(2))
            .await?;
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        if !tenant_live {
            return Err(Error::new(
                ErrorCode::TenantNotFound,
                format!("{}", tenant_id),
            ));
        }
        let policy = policy_for_tenant_in_state(&s, tenant_id);
        let mut report = BackpressureReport::default();

        if let Some(soft) = policy.quota.soft_max_active_sessions {
            report.push_if_over("active_sessions", agg.active_sessions as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_agents {
            report.push_if_over("agents", agg.live_agents as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_records {
            report.push_if_over("records", agg.live_records as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_artifacts {
            report.push_if_over("artifacts", agg.live_artifacts as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_artifact_storage_bytes {
            report.push_if_over("artifact_storage", agg.live_artifact_bytes, soft);
        }

        Ok(report)
    }

    pub async fn backpressure_for_session(
        &self,
        tenant_id: &Id,
        session_id: &Id,
    ) -> Result<BackpressureReport> {
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        if !tenant_live {
            return Err(Error::new(
                ErrorCode::TenantNotFound,
                format!("{}", tenant_id),
            ));
        }
        let session = s
            .sessions
            .get(session_id.as_str())
            .filter(|sess| {
                sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
            })
            .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))?;
        let policy = policy_for_tenant_in_state(&s, tenant_id);
        let mut report = BackpressureReport::default();
        if let Some(soft) = policy.quota.soft_max_events_per_session {
            report.push_if_over("session_events", session.next_seq, soft);
        }
        Ok(report)
    }

    pub async fn backpressure_for_tenant_and_session(
        &self,
        tenant_id: &Id,
        session_id: &Id,
    ) -> Result<BackpressureReport> {
        self.ensure_tenant_cached(tenant_id).await?;
        self.ensure_session_cached(tenant_id, session_id).await?;
        let agg = self
            .tenant_aggregates_cached(tenant_id, std::time::Duration::from_secs(2))
            .await?;
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        if !tenant_live {
            return Err(Error::new(
                ErrorCode::TenantNotFound,
                format!("{}", tenant_id),
            ));
        }
        let session = s
            .sessions
            .get(session_id.as_str())
            .filter(|sess| {
                sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
            })
            .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))?;
        let policy = policy_for_tenant_in_state(&s, tenant_id);
        let mut report = BackpressureReport::default();

        if let Some(soft) = policy.quota.soft_max_active_sessions {
            report.push_if_over("active_sessions", agg.active_sessions as u64, soft as u64);
        }
        if let Some(soft) = policy.quota.soft_max_agents {
            report.push_if_over("agents", agg.live_agents as u64, soft as u64);
        }
        if let Some(soft) = policy.quota.soft_max_records {
            report.push_if_over("records", agg.live_records as u64, soft as u64);
        }
        if let Some(soft) = policy.quota.soft_max_artifacts {
            report.push_if_over("artifacts", agg.live_artifacts as u64, soft as u64);
        }
        if let Some(soft) = policy.quota.soft_max_artifact_storage_bytes {
            report.push_if_over("artifact_storage", agg.live_artifact_bytes, soft);
        }
        if let Some(soft) = policy.quota.soft_max_events_per_session {
            report.push_if_over("session_events", session.next_seq, soft);
        }

        Ok(report)
    }

    /// Create an artifact: write the bytes to the content backend, persist
    /// the metadata, return the metadata record. Tenant must exist.
    ///
    /// On success the bytes are durable (or in-memory equivalent) and the
    /// metadata is committed atomically with the secondary `by_sha256` index.
    ///
    /// The content write and metadata write are separate persistence steps.
    /// A metadata error triggers immediate content cleanup; a process crash in
    /// the narrow interval is repaired by startup orphan reconciliation.
    pub async fn create_artifact(&self, req: NewArtifact, bytes: Vec<u8>) -> Result<Artifact> {
        self.create_artifact_with_idempotency(req, bytes, None)
            .await
    }

    /// `create_artifact` with retry semantics: a repeated call with the same
    /// `idempotency_key` and identical content returns the original artifact
    /// instead of storing a second blob; the same key with different content
    /// is an `IdempotencyConflict`.
    pub async fn create_artifact_with_idempotency(
        &self,
        req: NewArtifact,
        bytes: Vec<u8>,
        idempotency_key: Option<String>,
    ) -> Result<Artifact> {
        let idem_entry = match idempotency_key.as_deref().filter(|key| !key.is_empty()) {
            Some(key) => {
                let full_key = format!("{}::artifact::{key}", req.tenant_id);
                let payload_hash = hash_json(&serde_json::json!({
                    "op": "create_artifact",
                    "tenant_id": req.tenant_id.as_str(),
                    "content_type": req.content_type,
                    "sha256": sha256_hex(&bytes),
                }));
                if let Some(record) = self.idem.lookup(&full_key) {
                    if record.payload_hash != payload_hash {
                        return Err(Error::new(
                            ErrorCode::IdempotencyConflict,
                            format!(
                                "idempotency key {key} was used with different artifact content"
                            ),
                        ));
                    }
                    let artifact: Artifact = serde_json::from_str(&record.result_json)
                        .map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?;
                    return Ok(artifact);
                }
                Some((full_key, payload_hash))
            }
            None => None,
        };
        self.ensure_tenant_cached(&req.tenant_id).await?;
        let agg = self
            .tenant_aggregates_cached(&req.tenant_id, std::time::Duration::ZERO)
            .await?;
        let policy = self.resolve_policy_for_tenant(&req.tenant_id).await?;
        // Reject oversized blobs before touching the content backend so a
        // misbehaving caller can't fill disk by streaming a huge body.
        let len = bytes.len() as u64;
        if len > policy.artifact_size.max_artifact_bytes {
            return Err(Error::new(
                ErrorCode::ArtifactTooLarge,
                format!(
                    "artifact body {len} bytes exceeds cap {}",
                    policy.artifact_size.max_artifact_bytes
                ),
            ));
        }

        // Validate tenant exists. We hold the lock only long enough to look
        // up the tenant; the content I/O happens after we drop it.
        {
            let s = self.state.lock().await;
            if s.tenants
                .get(req.tenant_id.as_str())
                .is_none_or(|tenant| tenant.deleted_at_ms.is_some())
            {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
            if let Some(max) = policy.quota.max_artifacts {
                let live_artifacts = agg.live_artifacts;
                if live_artifacts >= max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies artifact write: artifact count quota {max} exhausted",
                            policy.version
                        ),
                    ));
                }
            }
            if let Some(max) = policy.quota.max_artifact_storage_bytes {
                let live_bytes: u64 = agg.live_artifact_bytes;
                if live_bytes.saturating_add(len) > max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies artifact write: artifact storage quota {max} bytes exhausted",
                            policy.version
                        ),
                    ));
                }
            }
        }

        // Mint the id BEFORE the put so the content backend path is stable
        // regardless of how long the put takes.
        let artifact_id = Id::new(crate::id::IdKind::Artifact);
        let plaintext_sha256 = sha256_hex(&bytes);

        // Verify caller-provided sha256 against the plaintext before any
        // storage write. Envelope encryption is an implementation boundary;
        // artifact identity remains the caller-visible plaintext hash.
        if let Some(expected) = req.expected_sha256.as_deref() {
            if !expected.eq_ignore_ascii_case(&plaintext_sha256) {
                return Err(Error::new(
                    ErrorCode::HashMismatch,
                    format!(
                        "expected sha256 {expected} but content hashed to {}",
                        plaintext_sha256
                    ),
                ));
            }
        }

        let content_key = self.resolve_key(&req.tenant_id, KeyOperation::ContentWrite)?;
        let metadata_key = self.resolve_key(&req.tenant_id, KeyOperation::MetadataWrite)?;

        let stored_bytes = self.seal_content(
            req.tenant_id.as_str(),
            "artifact",
            artifact_id.as_str(),
            &content_key,
            &bytes,
        )?;
        if let Err(error) = self
            .content
            .put(req.tenant_id.as_str(), artifact_id.as_str(), &stored_bytes)
            .await
        {
            // The mirror throttles (rejecting before the primary write) when
            // its object-store upload backlog is over the ceiling. Nothing
            // was written, so there is nothing to clean up — surface it as a
            // retryable rate limit (429 + Retry-After) rather than an
            // internal error, so the writer backs off and the responsible
            // tenant is slowed before the backlog grows unbounded (§5).
            if error
                .to_string()
                .contains(agentdb_storage::MIRROR_BACKPRESSURE_MARKER)
            {
                return Err(Error::new(
                    ErrorCode::RateLimited,
                    "object-store upload backlog is saturated; retry after it drains",
                )
                .with_suggestion(
                    "the node is throttling artifact writes to keep the upload backlog bounded; \
                     retry with backoff, or inspect GET /api/admin/content-mirror",
                ));
            }
            if let Err(cleanup_error) = self
                .content
                .delete(req.tenant_id.as_str(), artifact_id.as_str())
                .await
            {
                tracing::error!(
                    ?cleanup_error,
                    tenant_id = %req.tenant_id,
                    artifact_id = %artifact_id,
                    "failed to clean up partial artifact content write"
                );
            }
            return Err(Error::new(
                ErrorCode::Internal,
                format!("content put failed: {error}"),
            ));
        }

        // Build the record with the freshly-minted id rather than relying on
        // Artifact::from_put to mint one (it would mint a different id from
        // the one we already used to write the blob).
        let artifact = Artifact {
            artifact_id: artifact_id.clone(),
            tenant_id: req.tenant_id,
            content_type: req.content_type,
            bytes: bytes.len() as u64,
            sha256: plaintext_sha256,
            content_key_id: Some(content_key.key_id.clone()),
            content_key_version: content_key.key_version.clone(),
            metadata_key_id: Some(metadata_key.key_id.clone()),
            metadata_key_version: metadata_key.key_version.clone(),
            created_at_ms: crate::now_ms(),
            deleted_at_ms: None,
            raw_purged_at_ms: None,
        };

        // Serialize the metadata commit and cache publication under the state
        // lock. Export snapshots capture their durable high-watermark before
        // reading this cache; without this lock boundary they could observe
        // the new ledger cursor but miss the corresponding artifact snapshot.
        let mut state = self.state.lock().await;
        if let Some(p) = &self.persist {
            if let Err(error) = p.put_artifact_async(&artifact).await {
                drop(state);
                if let Err(cleanup_error) = self
                    .content
                    .delete(artifact.tenant_id.as_str(), artifact.artifact_id.as_str())
                    .await
                {
                    tracing::error!(
                        ?cleanup_error,
                        tenant_id = %artifact.tenant_id,
                        artifact_id = %artifact.artifact_id,
                        "failed to clean up artifact content after metadata write error"
                    );
                }
                return Err(internal(error));
            }
        }
        state
            .artifacts
            .insert(artifact.artifact_id.as_str().to_string(), artifact.clone());
        if let Some((full_key, payload_hash)) = idem_entry {
            let record = IdempotencyRecord {
                payload_hash,
                result_json: serde_json::to_string(&artifact)
                    .map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?,
                created_at_ms: crate::now_ms(),
            };
            if let Some(p) = &self.persist {
                if let Err(error) = p.put_idempotency(&full_key, &record) {
                    tracing::warn!(?error, "artifact idempotency record persist failed");
                }
            }
            self.idem.put(full_key, record);
        }
        drop(state);
        self.invalidate_tenant_aggregates(&artifact.tenant_id).await;
        Ok(artifact)
    }

    /// Remove content blobs that have no metadata record still retaining raw
    /// content (soft-deleted artifacts remain referenced until GC/purge).
    /// Call this during single-node startup before accepting writes; running it
    /// concurrently with artifact creation can observe the intentional
    /// content-before-metadata commit interval.
    pub async fn reconcile_orphan_artifact_content(
        &self,
    ) -> Result<ArtifactContentReconciliationReport> {
        let max_objects = startup_hydration_limit("AGENTDB_STARTUP_MAX_CONTENT_OBJECTS", 500_000);
        // Crash-left temporaries first (destructive, startup-only), then the
        // read-only listing used for orphan reconciliation.
        let temporary_objects_deleted =
            match self.content.sweep_temporary_objects(max_objects).await {
                Ok(removed) => removed,
                Err(error) if error.kind() == std::io::ErrorKind::Unsupported => 0,
                Err(error) => {
                    return Err(Error::new(
                        ErrorCode::Internal,
                        format!("temporary content sweep failed: {error}"),
                    ))
                }
            };
        let listing = match self.content.list_objects(max_objects).await {
            Ok(listing) => listing,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                return Ok(ArtifactContentReconciliationReport::default())
            }
            Err(error) => {
                return Err(Error::new(
                    ErrorCode::Internal,
                    format!("content object listing failed: {error}"),
                ))
            }
        };
        let referenced = {
            let all_artifacts = self.authoritative_all_artifacts().await?;
            let all_sessions = self.authoritative_all_sessions().await?;
            let mut referenced: BTreeSet<(String, String)> = all_artifacts
                .iter()
                .filter(|artifact| artifact.raw_purged_at_ms.is_none())
                .map(|artifact| {
                    (
                        artifact.tenant_id.as_str().to_string(),
                        artifact.artifact_id.as_str().to_string(),
                    )
                })
                .collect();
            // Tiered session history shares the content namespace with
            // artifacts: sealed segments, archive bundles, and replay-prefix
            // blobs are all referenced from session metadata, not from the
            // artifacts map. Missing any of them here turns the orphan sweep
            // into a data destroyer — a restart would delete every cold and
            // archived session's history. Source segments of an archive are
            // kept too: a crash between archive-manifest commit and source
            // cleanup leaves them present, and retaining a stale blob until
            // GC is always safer than deleting a referenced one.
            for session in &all_sessions {
                let tenant = session.tenant_id.as_str().to_string();
                for segment in &session.sealed_segments {
                    referenced.insert((tenant.clone(), segment.segment_id.as_str().to_string()));
                }
                if let Some(archive) = &session.archive_manifest {
                    referenced.insert((tenant.clone(), archive.manifest_id.as_str().to_string()));
                    for source in &archive.source_segment_ids {
                        referenced.insert((tenant.clone(), source.as_str().to_string()));
                    }
                }
                if let Some(prefix) = &session.replay_prefix {
                    referenced.insert((tenant.clone(), prefix.prefix_id.as_str().to_string()));
                }
            }
            referenced
        };
        let mut report = ArtifactContentReconciliationReport {
            listing_supported: true,
            temporary_objects_deleted,
            ..Default::default()
        };
        for object in listing.objects {
            report.objects_scanned = report.objects_scanned.saturating_add(1);
            if referenced.contains(&(object.tenant_id.clone(), object.artifact_id.clone())) {
                report.referenced_objects = report.referenced_objects.saturating_add(1);
                continue;
            }
            match self
                .content
                .delete(&object.tenant_id, &object.artifact_id)
                .await
            {
                Ok(()) => {
                    report.orphan_objects_deleted = report.orphan_objects_deleted.saturating_add(1);
                }
                Err(error) => {
                    report.delete_errors = report.delete_errors.saturating_add(1);
                    tracing::error!(
                        ?error,
                        tenant_id = %object.tenant_id,
                        artifact_id = %object.artifact_id,
                        "failed to delete orphan artifact content"
                    );
                }
            }
        }
        Ok(report)
    }

    /// Look up artifact metadata, scoped to `tenant_id`. Returns
    /// `ArtifactNotFound` if the id is unknown, soft-deleted, or owned by a
    /// different tenant — we collapse those cases (mirroring the
    /// agent/session lookups) so the caller in tenant A cannot probe
    /// tenant B's artifact ids by error-code differential.
    pub async fn get_artifact(&self, tenant_id: &Id, artifact_id: &Id) -> Result<Artifact> {
        for attempt in 0..16 {
            self.ensure_tenant_cached(tenant_id).await?;
            self.ensure_artifact_cached(tenant_id, artifact_id).await?;
            let result = self.get_artifact_from_cache(tenant_id, artifact_id).await;
            match result {
                Err(e) if Self::metadata_cache_miss_retryable(e.code) && attempt < 15 => continue,
                result => return result,
            }
        }
        unreachable!("bounded retry loop returns")
    }

    async fn get_artifact_from_cache(&self, tenant_id: &Id, artifact_id: &Id) -> Result<Artifact> {
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        s.artifacts
            .get(artifact_id.as_str())
            .filter(|a| {
                tenant_live
                    && a.deleted_at_ms.is_none()
                    && a.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::ArtifactNotFound, format!("{}", artifact_id)))
    }

    pub async fn list_artifacts(&self, tenant_id: &Id) -> Result<Vec<Artifact>> {
        self.get_tenant(tenant_id).await?;
        let mut artifacts = self
            .authoritative_tenant_artifacts(tenant_id)
            .await?
            .into_iter()
            .filter(|artifact| {
                artifact.deleted_at_ms.is_none()
                    && artifact.tenant_id.as_str() == tenant_id.as_str()
            })
            .collect::<Vec<_>>();
        artifacts.sort_by(|a, b| {
            b.created_at_ms
                .cmp(&a.created_at_ms)
                .then_with(|| a.artifact_id.as_str().cmp(b.artifact_id.as_str()))
        });
        Ok(artifacts)
    }

    /// Read the bytes for an artifact. Same scoping rules as `get_artifact`.
    /// Once raw is purged (V2.4 derivative-only lifecycle) the bytes are
    /// gone but the metadata stays — a `get_artifact_blob` against a
    /// purged artifact returns `ArtifactNotFound`.
    pub async fn get_artifact_blob(&self, tenant_id: &Id, artifact_id: &Id) -> Result<Vec<u8>> {
        let art = self.get_artifact(tenant_id, artifact_id).await?;
        if art.raw_purged_at_ms.is_some() {
            return Err(Error::new(
                ErrorCode::ArtifactNotFound,
                format!("{}", artifact_id),
            ));
        }
        let current_key = self.resolve_key(tenant_id, KeyOperation::ContentRead)?;
        let stored_bytes = self
            .content
            .get(tenant_id.as_str(), artifact_id.as_str())
            .await
            .map_err(|e| Error::new(ErrorCode::Internal, format!("content get failed: {e}")))?;
        let fallback_key =
            key_from_parts(&art.content_key_id, &art.content_key_version).unwrap_or(current_key);
        let plaintext = self.open_content(
            tenant_id.as_str(),
            "artifact",
            artifact_id.as_str(),
            fallback_key,
            stored_bytes,
        )?;
        let actual_sha256 = sha256_hex(&plaintext);
        if actual_sha256 != art.sha256 {
            return Err(Error::new(
                ErrorCode::HashMismatch,
                format!(
                    "artifact {} content hashed to {actual_sha256} but metadata expected {}",
                    artifact_id, art.sha256
                ),
            ));
        }
        Ok(plaintext)
    }

    pub async fn verify_artifact_lifecycle_for_tenant(
        &self,
        tenant_id: &Id,
    ) -> Result<ArtifactLifecycleVerificationReport> {
        let artifacts = self.list_artifacts(tenant_id).await?;
        let mut report = ArtifactLifecycleVerificationReport {
            tenant_id: Some(tenant_id.clone()),
            ..Default::default()
        };

        for artifact in artifacts {
            report.artifacts_checked += 1;
            if artifact.raw_purged_at_ms.is_some() {
                match self
                    .content
                    .head(tenant_id.as_str(), artifact.artifact_id.as_str())
                    .await
                {
                    Ok(false) => report.raw_purged += 1,
                    Ok(true) => {
                        report.unexpected_raw_after_purge += 1;
                        report.issues.push(ArtifactLifecycleIssue {
                            artifact_id: artifact.artifact_id.clone(),
                            issue: "unexpected_raw_after_purge".into(),
                            message:
                                "artifact metadata is derivative-only but raw content still exists"
                                    .into(),
                        });
                    }
                    Err(error) => {
                        report.read_errors += 1;
                        report.issues.push(ArtifactLifecycleIssue {
                            artifact_id: artifact.artifact_id.clone(),
                            issue: "content_head_error".into(),
                            message: error.to_string(),
                        });
                    }
                }
                continue;
            }

            match self
                .get_artifact_blob(tenant_id, &artifact.artifact_id)
                .await
            {
                Ok(_) => report.raw_present += 1,
                Err(error) if error.code == ErrorCode::ArtifactNotFound => {
                    report.missing_raw += 1;
                    report.issues.push(ArtifactLifecycleIssue {
                        artifact_id: artifact.artifact_id.clone(),
                        issue: "missing_raw".into(),
                        message: "artifact metadata expects raw content but content is missing"
                            .into(),
                    });
                }
                Err(error) if error.code == ErrorCode::HashMismatch => {
                    report.hash_mismatch += 1;
                    report.issues.push(ArtifactLifecycleIssue {
                        artifact_id: artifact.artifact_id.clone(),
                        issue: "hash_mismatch".into(),
                        message: error.message,
                    });
                }
                Err(error) => {
                    report.read_errors += 1;
                    report.issues.push(ArtifactLifecycleIssue {
                        artifact_id: artifact.artifact_id.clone(),
                        issue: "content_read_error".into(),
                        message: error.message,
                    });
                }
            }
        }

        Ok(report)
    }

    pub async fn delete_artifact(&self, tenant_id: &Id, artifact_id: &Id) -> Result<()> {
        let mut s = self.state.lock().await;
        let live = s.artifacts.get(artifact_id.as_str()).is_some_and(|a| {
            a.deleted_at_ms.is_none() && a.tenant_id.as_str() == tenant_id.as_str()
        });
        if !live {
            return Err(Error::new(
                ErrorCode::ArtifactNotFound,
                format!("{}", artifact_id),
            ));
        }

        let updated = {
            let a = s
                .artifacts
                .get_mut(artifact_id.as_str())
                .expect("presence checked above");
            a.deleted_at_ms = Some(crate::now_ms());
            a.clone()
        };
        if let Some(p) = &self.persist {
            if let Err(e) = p.put_artifact(&updated) {
                if let Some(a) = s.artifacts.get_mut(artifact_id.as_str()) {
                    a.deleted_at_ms = None;
                }
                drop(s);
                return Err(internal(e));
            }
        }
        drop(s);
        self.invalidate_tenant_aggregates(tenant_id).await;
        Ok(())
    }

    pub async fn sweep_lifecycle_at(&self, now_ms: u64) -> Result<LifecycleSweepReport> {
        self.sweep_lifecycle_inner(None, now_ms).await
    }

    pub async fn sweep_lifecycle_for_tenant_at(
        &self,
        tenant_id: &Id,
        now_ms: u64,
    ) -> Result<LifecycleSweepReport> {
        {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", tenant_id),
                ));
            }
        }
        self.sweep_lifecycle_inner(Some(tenant_id), now_ms).await
    }

    async fn sweep_lifecycle_inner(
        &self,
        tenant_scope: Option<&Id>,
        now_ms: u64,
    ) -> Result<LifecycleSweepReport> {
        let mut report = LifecycleSweepReport::default();
        let scanned_sessions = match tenant_scope {
            Some(tenant_id) => self.authoritative_tenant_sessions(tenant_id).await?,
            None => self.authoritative_all_sessions().await?,
        };
        let scanned_artifacts = match tenant_scope {
            Some(tenant_id) => self.authoritative_tenant_artifacts(tenant_id).await?,
            None => self.authoritative_all_artifacts().await?,
        };
        let sweep_tenant_ids: Vec<Id> = scanned_sessions
            .iter()
            .map(|session| session.tenant_id.clone())
            .chain(scanned_artifacts.iter().map(|a| a.tenant_id.clone()))
            .collect();
        self.ensure_tenants_cached(sweep_tenant_ids).await?;
        let (session_candidates, artifact_candidates) =
            {
                let s = self.state.lock().await;
                let mut candidates = Vec::new();
                for session in &scanned_sessions {
                    if session.deleted_at_ms.is_some() {
                        continue;
                    }
                    if tenant_scope
                        .is_some_and(|tenant_id| session.tenant_id.as_str() != tenant_id.as_str())
                    {
                        continue;
                    }
                    let policy = policy_for_tenant_in_state(&s, &session.tenant_id);
                    let idle_ms = now_ms.saturating_sub(session.last_active_at_ms);
                    let target = lifecycle_target(session.lifecycle, idle_ms, &policy);
                    if target != session.lifecycle {
                        candidates.push((session.clone(), target, policy));
                    }
                }

                let artifacts =
                    scanned_artifacts
                        .iter()
                        .filter(|a| a.deleted_at_ms.is_none() && a.raw_purged_at_ms.is_none())
                        .filter(|a| {
                            tenant_scope
                                .map(|tenant_id| a.tenant_id.as_str() == tenant_id.as_str())
                                .unwrap_or(true)
                        })
                        .filter(|a| {
                            let policy = policy_for_tenant_in_state(&s, &a.tenant_id);
                            !policy.retention.legal_hold
                                && policy.lifecycle.derivative_only_after_ms.is_some_and(
                                    |threshold| now_ms.saturating_sub(a.created_at_ms) >= threshold,
                                )
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                (candidates, artifacts)
            };

        for (session, target, policy) in session_candidates {
            if !lifecycle_work_allowed(&mut report, &policy) {
                break;
            }
            self.transition_session_lifecycle(&session.tenant_id, &session.session_id, target)
                .await?;
            let updated = {
                let mut s = self.state.lock().await;
                let current = s
                    .sessions
                    .get_mut(session.session_id.as_str())
                    .expect("session exists after lifecycle transition");
                current.policy_version = policy.version;
                current.clone()
            };
            if let Some(p) = &self.persist {
                p.put_session(&updated).map_err(internal)?;
            }
            lifecycle_report_action(&mut report, session.lifecycle, target);
            report.sessions_transitioned += 1;
            report.work_units += 1;
        }

        for artifact in artifact_candidates {
            let policy = self.resolve_policy_for_tenant(&artifact.tenant_id).await?;
            if !lifecycle_work_allowed(&mut report, &policy) {
                break;
            }
            self.content
                .delete(artifact.tenant_id.as_str(), artifact.artifact_id.as_str())
                .await
                .map_err(|e| {
                    Error::new(ErrorCode::Internal, format!("content delete failed: {e}"))
                })?;

            let mut s = self.state.lock().await;
            let Some(current) = s.artifacts.get_mut(artifact.artifact_id.as_str()) else {
                continue;
            };
            if current.deleted_at_ms.is_some()
                || current.raw_purged_at_ms.is_some()
                || current.tenant_id.as_str() != artifact.tenant_id.as_str()
            {
                continue;
            }
            current.raw_purged_at_ms = Some(now_ms);
            let updated = current.clone();
            if let Some(p) = &self.persist {
                p.put_artifact(&updated).map_err(internal)?;
            }
            report.artifacts_raw_purged += 1;
            report.work_units += 1;
        }

        Ok(report)
    }

    pub async fn gc_sweep_at(&self, now_ms: u64) -> Result<GcSweepReport> {
        self.gc_sweep_inner(None, now_ms).await
    }

    pub async fn gc_sweep_for_tenant_at(
        &self,
        tenant_id: &Id,
        now_ms: u64,
    ) -> Result<GcSweepReport> {
        {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", tenant_id),
                ));
            }
        }
        self.gc_sweep_inner(Some(tenant_id), now_ms).await
    }

    pub async fn run_policy_worker_once_at(
        &self,
        now_ms: u64,
        max_tenants: usize,
    ) -> Result<PolicyWorkerReport> {
        let mut scheduled = {
            let all_tenants = self.authoritative_all_tenants().await?;
            let mut tenants: Vec<(Id, Priority)> = all_tenants
                .iter()
                .map(|tenant| {
                    let policy = RuntimePolicy::from_json(&tenant.policy);
                    (tenant.tenant_id.clone(), policy.priority)
                })
                .collect();
            tenants.sort_by(|(left_id, left_priority), (right_id, right_priority)| {
                priority_rank(*right_priority)
                    .cmp(&priority_rank(*left_priority))
                    .then_with(|| left_id.as_str().cmp(right_id.as_str()))
            });
            tenants
        };

        let tenants_considered = scheduled.len();
        scheduled.truncate(max_tenants);

        let mut report = PolicyWorkerReport {
            tenants_considered,
            tenants_scheduled: scheduled.len(),
            ..PolicyWorkerReport::default()
        };

        for (tenant_id, priority) in scheduled {
            let tenant_live = {
                let s = self.state.lock().await;
                s.tenants
                    .get(tenant_id.as_str())
                    .is_some_and(|tenant| tenant.deleted_at_ms.is_none())
            };
            let lifecycle = if tenant_live {
                self.sweep_lifecycle_for_tenant_at(&tenant_id, now_ms)
                    .await?
            } else {
                LifecycleSweepReport::default()
            };
            let gc = self.gc_sweep_inner(Some(&tenant_id), now_ms).await?;
            report.lifecycle.sessions_transitioned += lifecycle.sessions_transitioned;
            report.lifecycle.artifacts_raw_purged += lifecycle.artifacts_raw_purged;
            report.lifecycle.seal_segment_actions += lifecycle.seal_segment_actions;
            report.lifecycle.move_to_cold_actions += lifecycle.move_to_cold_actions;
            report.lifecycle.archive_actions += lifecycle.archive_actions;
            report.lifecycle.hydrate_actions += lifecycle.hydrate_actions;
            report.lifecycle.work_units += lifecycle.work_units;
            report.lifecycle.soft_work_limit_exceeded |= lifecycle.soft_work_limit_exceeded;
            report.lifecycle.hard_work_limit_hit |= lifecycle.hard_work_limit_hit;
            report.gc.tenants_purged += gc.tenants_purged;
            report.gc.agents_purged += gc.agents_purged;
            report.gc.sessions_purged += gc.sessions_purged;
            report.gc.events_purged += gc.events_purged;
            report.gc.idempotency_purged += gc.idempotency_purged;
            report.gc.records_purged += gc.records_purged;
            report.gc.artifacts_purged += gc.artifacts_purged;
            report.gc.changefeed_delete_events += gc.changefeed_delete_events;
            report.gc.changefeed_purge_events += gc.changefeed_purge_events;
            report.gc.work_units += gc.work_units;
            report.gc.soft_work_limit_exceeded |= gc.soft_work_limit_exceeded;
            report.gc.hard_work_limit_hit |= gc.hard_work_limit_hit;
            report.tenants.push(TenantWorkerReport {
                tenant_id,
                priority,
                lifecycle,
                gc,
            });
        }

        Ok(report)
    }

    async fn gc_sweep_inner(
        &self,
        tenant_scope: Option<&Id>,
        now_ms: u64,
    ) -> Result<GcSweepReport> {
        let mut report = GcSweepReport::default();
        // GC's purge logic walks the cached maps; make them complete first.
        self.warm_metadata_caches_for_scope(tenant_scope).await?;
        let idempotency_candidates = {
            let s = self.state.lock().await;
            let mut out = Vec::new();
            let persisted = self
                .persist
                .as_ref()
                .map(|p| p.scan_all_idempotency().map_err(internal))
                .transpose()?;
            let records: Vec<(String, IdempotencyRecord)> = if let Some(records) = persisted {
                records
            } else {
                self.idem.entries()
            };
            for (full_key, record) in records {
                let Some((tenant_id_raw, _)) = full_key.split_once("::") else {
                    continue;
                };
                if tenant_scope.is_some_and(|tenant_id| tenant_id.as_str() != tenant_id_raw) {
                    continue;
                }
                let Some(tenant) = s.tenants.get(tenant_id_raw) else {
                    continue;
                };
                let policy = RuntimePolicy::from_json(&tenant.policy);
                if policy.retention.legal_hold
                    || now_ms.saturating_sub(record.created_at_ms) < policy.retention.retention_ms
                {
                    continue;
                }
                out.push(full_key);
            }
            out
        };
        for full_key in idempotency_candidates {
            if let Some(p) = &self.persist {
                p.delete_idempotency(&full_key).map_err(internal)?;
            }
            self.idem.remove(&full_key);
            report.idempotency_purged += 1;
            report.work_units += 1;
        }

        {
            let mut s = self.state.lock().await;
            let agent_ids: Vec<String> = s
                .agents
                .iter()
                .filter_map(|(id, agent)| {
                    if tenant_scope
                        .is_some_and(|tenant_id| agent.tenant_id.as_str() != tenant_id.as_str())
                    {
                        return None;
                    }
                    let tenant = s.tenants.get(agent.tenant_id.as_str())?;
                    let policy = RuntimePolicy::from_json(&tenant.policy);
                    let deleted_at = agent.deleted_at_ms.or(tenant.deleted_at_ms)?;
                    if policy.retention.legal_hold
                        || now_ms.saturating_sub(deleted_at) < policy.retention.retention_ms
                    {
                        return None;
                    }
                    Some(id.clone())
                })
                .collect();

            for agent_id in agent_ids {
                let Some(agent) = s.agents.get(&agent_id).cloned() else {
                    continue;
                };
                let policy = policy_for_tenant_in_state(&s, &agent.tenant_id);
                if !gc_work_allowed(&mut report, &policy) {
                    break;
                }
                let session_ids: Vec<String> = s
                    .sessions
                    .iter()
                    .filter(|(_, session)| {
                        session.tenant_id.as_str() == agent.tenant_id.as_str()
                            && session.agent_id.as_str() == agent.agent_id.as_str()
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                for session_id in session_ids {
                    let Some(session) = s.sessions.get(&session_id).cloned() else {
                        continue;
                    };
                    let tenant_deleted_at = s
                        .tenants
                        .get(session.tenant_id.as_str())
                        .and_then(|tenant| tenant.deleted_at_ms);
                    let deleted_at_ms = first_deletion_ms(
                        now_ms,
                        &[
                            session.deleted_at_ms,
                            agent.deleted_at_ms,
                            tenant_deleted_at,
                        ],
                    );
                    let reason = gc_tombstone_reason(session.deleted_at_ms, tenant_deleted_at);
                    if let Some(p) = &self.persist {
                        report.events_purged += p
                            .purge_session_events(
                                session.tenant_id.as_str(),
                                session.session_id.as_str(),
                                reason,
                                deleted_at_ms,
                            )
                            .map_err(internal)?;
                        p.purge_session(&session, reason, deleted_at_ms)
                            .map_err(internal)?;
                    } else {
                        let lo = (session.session_id.as_str().to_string(), 0);
                        let hi = (session.session_id.as_str().to_string(), u64::MAX);
                        let event_ids: Vec<String> = s
                            .events
                            .range(lo..=hi)
                            .map(|(_, event)| event.event_id.as_str().to_string())
                            .collect();
                        for event_id in event_ids {
                            if let Some((_, seq)) = s.event_by_id.remove(&event_id) {
                                s.events
                                    .remove(&(session.session_id.as_str().to_string(), seq));
                                report.events_purged += 1;
                            }
                        }
                    }
                    s.sessions.remove(&session_id);
                    if let Some(alias) = session.alias.as_deref() {
                        s.session_aliases
                            .remove(&scoped_alias_key(session.tenant_id.as_str(), alias));
                    }
                    report.sessions_purged += 1;
                    report.changefeed_purge_events += 1;
                    report.work_units += 1;
                }
                if let Some(p) = &self.persist {
                    let tenant_deleted_at = s
                        .tenants
                        .get(agent.tenant_id.as_str())
                        .and_then(|tenant| tenant.deleted_at_ms);
                    let deleted_at_ms =
                        first_deletion_ms(now_ms, &[agent.deleted_at_ms, tenant_deleted_at]);
                    let reason = gc_tombstone_reason(agent.deleted_at_ms, tenant_deleted_at);
                    p.purge_agent(&agent, reason, deleted_at_ms)
                        .map_err(internal)?;
                }
                s.agents.remove(&agent_id);
                if let Some(alias) = agent.alias.as_deref() {
                    s.agent_aliases
                        .remove(&scoped_alias_key(agent.tenant_id.as_str(), alias));
                }
                report.agents_purged += 1;
                report.changefeed_purge_events += 1;
                report.work_units += 1;
            }
        }

        {
            let mut s = self.state.lock().await;
            let session_ids: Vec<String> = s
                .sessions
                .iter()
                .filter_map(|(id, session)| {
                    if tenant_scope
                        .is_some_and(|tenant_id| session.tenant_id.as_str() != tenant_id.as_str())
                    {
                        return None;
                    }
                    let tenant = s.tenants.get(session.tenant_id.as_str())?;
                    let policy = RuntimePolicy::from_json(&tenant.policy);
                    let agent_deleted_at = s
                        .agents
                        .get(session.agent_id.as_str())
                        .and_then(|agent| agent.deleted_at_ms);
                    let deleted_at = session
                        .deleted_at_ms
                        .or(agent_deleted_at)
                        .or(tenant.deleted_at_ms)?;
                    if policy.retention.legal_hold
                        || now_ms.saturating_sub(deleted_at) < policy.retention.retention_ms
                    {
                        return None;
                    }
                    Some(id.clone())
                })
                .collect();

            for session_id in session_ids {
                let Some(session) = s.sessions.get(&session_id).cloned() else {
                    continue;
                };
                let tenant_deleted_at = s
                    .tenants
                    .get(session.tenant_id.as_str())
                    .and_then(|tenant| tenant.deleted_at_ms);
                let agent_deleted_at = s
                    .agents
                    .get(session.agent_id.as_str())
                    .and_then(|agent| agent.deleted_at_ms);
                let deleted_at_ms = first_deletion_ms(
                    now_ms,
                    &[session.deleted_at_ms, agent_deleted_at, tenant_deleted_at],
                );
                let reason = gc_tombstone_reason(session.deleted_at_ms, tenant_deleted_at);
                let policy = policy_for_tenant_in_state(&s, &session.tenant_id);
                if !gc_work_allowed(&mut report, &policy) {
                    break;
                }
                if let Some(p) = &self.persist {
                    report.events_purged += p
                        .purge_session_events(
                            session.tenant_id.as_str(),
                            session.session_id.as_str(),
                            reason,
                            deleted_at_ms,
                        )
                        .map_err(internal)?;
                    p.purge_session(&session, reason, deleted_at_ms)
                        .map_err(internal)?;
                } else {
                    let lo = (session.session_id.as_str().to_string(), 0);
                    let hi = (session.session_id.as_str().to_string(), u64::MAX);
                    let event_ids: Vec<String> = s
                        .events
                        .range(lo..=hi)
                        .map(|(_, event)| event.event_id.as_str().to_string())
                        .collect();
                    for event_id in event_ids {
                        if let Some((_, seq)) = s.event_by_id.remove(&event_id) {
                            s.events
                                .remove(&(session.session_id.as_str().to_string(), seq));
                            report.events_purged += 1;
                        }
                    }
                }
                s.sessions.remove(&session_id);
                if let Some(alias) = session.alias.as_deref() {
                    s.session_aliases
                        .remove(&scoped_alias_key(session.tenant_id.as_str(), alias));
                }
                report.sessions_purged += 1;
                report.changefeed_purge_events += 1;
                report.work_units += 1;
            }
        }

        {
            let mut s = self.state.lock().await;
            let record_ids: Vec<String> = s
                .records
                .iter()
                .filter_map(|(id, r)| {
                    if tenant_scope
                        .is_some_and(|tenant_id| r.tenant_id.as_str() != tenant_id.as_str())
                    {
                        return None;
                    }
                    let tenant = s.tenants.get(r.tenant_id.as_str())?;
                    let policy = RuntimePolicy::from_json(&tenant.policy);
                    let deleted_at = r.deleted_at_ms.or(tenant.deleted_at_ms)?;
                    if policy.retention.legal_hold
                        || now_ms.saturating_sub(deleted_at) < policy.retention.retention_ms
                    {
                        return None;
                    }
                    Some(id.clone())
                })
                .collect();

            for record_id in record_ids {
                let Some(record) = s.records.get(&record_id).cloned() else {
                    continue;
                };
                let policy = policy_for_tenant_in_state(&s, &record.tenant_id);
                if !gc_work_allowed(&mut report, &policy) {
                    break;
                }
                if let Some(p) = &self.persist {
                    let tenant_deleted_at = s
                        .tenants
                        .get(record.tenant_id.as_str())
                        .and_then(|tenant| tenant.deleted_at_ms);
                    let deleted_at_ms =
                        first_deletion_ms(now_ms, &[record.deleted_at_ms, tenant_deleted_at]);
                    let reason = gc_tombstone_reason(record.deleted_at_ms, tenant_deleted_at);
                    p.purge_record(&record, reason, deleted_at_ms)
                        .map_err(internal)?;
                }
                s.records.remove(&record_id);
                if let Some(doc) = s.record_to_doc.remove(&record_id) {
                    s.doc_to_record.remove(&doc);
                }
                report.records_purged += 1;
                report.changefeed_purge_events += 1;
                report.work_units += 1;
            }
        }

        let artifact_candidates = {
            let s = self.state.lock().await;
            s.artifacts
                .values()
                .filter_map(|a| {
                    if tenant_scope
                        .is_some_and(|tenant_id| a.tenant_id.as_str() != tenant_id.as_str())
                    {
                        return None;
                    }
                    let tenant = s.tenants.get(a.tenant_id.as_str())?;
                    let policy = RuntimePolicy::from_json(&tenant.policy);
                    let deleted_at = a.deleted_at_ms.or(tenant.deleted_at_ms)?;
                    if policy.retention.legal_hold
                        || now_ms.saturating_sub(deleted_at) < policy.retention.retention_ms
                    {
                        return None;
                    }
                    Some(a.clone())
                })
                .collect::<Vec<_>>()
        };

        for artifact in artifact_candidates {
            let policy = {
                let s = self.state.lock().await;
                policy_for_tenant_in_state(&s, &artifact.tenant_id)
            };
            if !gc_work_allowed(&mut report, &policy) {
                break;
            }
            self.content
                .delete(artifact.tenant_id.as_str(), artifact.artifact_id.as_str())
                .await
                .map_err(|e| {
                    Error::new(ErrorCode::Internal, format!("content delete failed: {e}"))
                })?;

            let mut s = self.state.lock().await;
            let Some(current) = s.artifacts.get(artifact.artifact_id.as_str()).cloned() else {
                continue;
            };
            let tenant_deleted = s
                .tenants
                .get(current.tenant_id.as_str())
                .and_then(|tenant| tenant.deleted_at_ms)
                .is_some();
            if (current.deleted_at_ms.is_none() && !tenant_deleted)
                || current.tenant_id.as_str() != artifact.tenant_id.as_str()
            {
                continue;
            }
            if let Some(p) = &self.persist {
                let tenant_deleted_at = s
                    .tenants
                    .get(current.tenant_id.as_str())
                    .and_then(|tenant| tenant.deleted_at_ms);
                let deleted_at_ms =
                    first_deletion_ms(now_ms, &[current.deleted_at_ms, tenant_deleted_at]);
                let reason = gc_tombstone_reason(current.deleted_at_ms, tenant_deleted_at);
                p.purge_artifact(&current, reason, deleted_at_ms)
                    .map_err(internal)?;
            }
            s.artifacts.remove(artifact.artifact_id.as_str());
            report.artifacts_purged += 1;
            report.changefeed_purge_events += 1;
            report.work_units += 1;
        }

        {
            let mut s = self.state.lock().await;
            let tenant_ids: Vec<String> = s
                .tenants
                .iter()
                .filter_map(|(id, tenant)| {
                    if tenant_scope
                        .is_some_and(|tenant_id| tenant.tenant_id.as_str() != tenant_id.as_str())
                    {
                        return None;
                    }
                    let deleted_at = tenant.deleted_at_ms?;
                    let policy = RuntimePolicy::from_json(&tenant.policy);
                    if policy.retention.legal_hold
                        || now_ms.saturating_sub(deleted_at) < policy.retention.retention_ms
                    {
                        return None;
                    }
                    Some(id.clone())
                })
                .collect();

            for tenant_id in tenant_ids {
                let Some(tenant) = s.tenants.get(&tenant_id).cloned() else {
                    continue;
                };
                let policy = RuntimePolicy::from_json(&tenant.policy);
                if !gc_work_allowed(&mut report, &policy) {
                    break;
                }
                let has_children = s
                    .agents
                    .values()
                    .any(|agent| agent.tenant_id.as_str() == tenant.tenant_id.as_str())
                    || s.sessions
                        .values()
                        .any(|session| session.tenant_id.as_str() == tenant.tenant_id.as_str())
                    || s.records
                        .values()
                        .any(|record| record.tenant_id.as_str() == tenant.tenant_id.as_str())
                    || s.artifacts
                        .values()
                        .any(|artifact| artifact.tenant_id.as_str() == tenant.tenant_id.as_str());
                if has_children {
                    continue;
                }
                if let Some(p) = &self.persist {
                    let deleted_at_ms = tenant.deleted_at_ms.unwrap_or(now_ms);
                    p.purge_tenant(&tenant, "tenant_purge", deleted_at_ms)
                        .map_err(internal)?;
                }
                s.tenants.remove(&tenant_id);
                if !tenant.external_key.is_empty() {
                    s.tenant_aliases.remove(&tenant.external_key);
                }
                report.tenants_purged += 1;
                report.changefeed_purge_events += 1;
                report.work_units += 1;
            }
        }

        Ok(report)
    }

    /// Test-only convenience that creates a tenant + agent + session in one shot.
    pub async fn bootstrap_test_session(&self) -> (Tenant, Agent, Session) {
        let t = self
            .create_tenant(NewTenant {
                external_key: "test".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let a = self
            .create_agent(NewAgent {
                tenant_id: t.tenant_id.clone(),
                alias: None,
                config: serde_json::json!({}),
            })
            .await
            .unwrap();
        let s = self
            .create_session(NewSession {
                tenant_id: t.tenant_id.clone(),
                agent_id: a.agent_id.clone(),
                alias: None,
            })
            .await
            .unwrap();
        (t, a, s)
    }

    pub async fn append(&self, req: AppendRequest) -> Result<AppendResponse> {
        Ok(self
            .append_with_policy(req, None, None, None)
            .await?
            .appended)
    }

    pub async fn append_with_backpressure(
        &self,
        req: AppendRequest,
    ) -> Result<(AppendResponse, BackpressureReport)> {
        let tenant_id = req.tenant_id.clone();
        let session_id = req.session_id.clone();
        let committed = self.append_with_policy(req, None, None, None).await?;
        let backpressure = if committed.backpressure_complete {
            committed.backpressure
        } else {
            self.backpressure_for_tenant_and_session(&tenant_id, &session_id)
                .await?
        };
        Ok((committed.appended, backpressure))
    }

    /// Append several events to ONE session in a single committed write batch.
    /// This is the bulk-ingest path (session-log importers, replay loaders): it
    /// preserves event order, assigns contiguous seqs, and commits every event
    /// through one persist write batch — far fewer round-trips and write
    /// batches than N single appends.
    ///
    /// Every request must target the same `(tenant, session)` and use the
    /// simple append shape (no artifact/source/credential refs and no per-event
    /// idempotency key); callers needing those append one event at a time.
    pub async fn append_batch(&self, reqs: Vec<AppendRequest>) -> Result<Vec<AppendResponse>> {
        if reqs.is_empty() {
            return Ok(Vec::new());
        }
        let tenant_id = reqs[0].tenant_id.clone();
        let session_id = reqs[0].session_id.clone();
        let agent_id = reqs[0].agent_id.clone();
        for req in &reqs {
            if req.tenant_id.as_str() != tenant_id.as_str()
                || req.session_id.as_str() != session_id.as_str()
            {
                return Err(Error::new(
                    ErrorCode::PolicyDenied,
                    "append_batch requires every event to target the same tenant and session",
                ));
            }
            if !req.artifact_refs.is_empty()
                || !req.source_refs.is_empty()
                || !req.credential_refs.is_empty()
                || req.idempotency_key.is_some()
            {
                return Err(Error::new(
                    ErrorCode::PolicyDenied,
                    "append_batch does not support artifact/source/credential refs or per-event \
                     idempotency keys; use append for those",
                ));
            }
        }
        self.ensure_tenant_cached(&tenant_id).await?;
        self.ensure_agent_cached(&tenant_id, &agent_id).await?;
        self.ensure_session_cached(&tenant_id, &session_id).await?;

        // Drive the same coalesced-commit machinery the concurrent append queue
        // uses, but with the whole ordered batch in one shot: one session lock,
        // one prepare pass, one write batch.
        let mut jobs = Vec::with_capacity(reqs.len());
        let mut receivers = Vec::with_capacity(reqs.len());
        for req in reqs {
            let (done, rx) = oneshot::channel();
            jobs.push(SimpleAppendJob {
                req,
                fetch_recent: None,
                context_max_bytes: None,
                done,
            });
            receivers.push(rx);
        }
        self.process_simple_append_batch(jobs).await;

        let mut out = Vec::with_capacity(receivers.len());
        for rx in receivers {
            let committed = rx.await.map_err(|_| {
                Error::new(
                    ErrorCode::Internal,
                    "session append batch dropped a response",
                )
            })??;
            out.push(committed.appended);
        }
        Ok(out)
    }

    async fn append_with_policy(
        &self,
        req: AppendRequest,
        resolved_policy: Option<RuntimePolicy>,
        fetch_recent: Option<usize>,
        context_max_bytes: Option<usize>,
    ) -> Result<AppendCommitResponse> {
        for attempt in 0..16 {
            self.ensure_tenant_cached(&req.tenant_id).await?;
            self.ensure_agent_cached(&req.tenant_id, &req.agent_id)
                .await?;
            self.ensure_session_cached(&req.tenant_id, &req.session_id)
                .await?;
            // Source-ref validation reads record rows under the lock; make
            // sure referenced records are resident first (lazy mode).
            for source_ref in &req.source_refs {
                if let Some(record_id) = &source_ref.record_id {
                    if record_id.kind() == crate::id::IdKind::Record {
                        self.ensure_record_cached(&req.tenant_id, record_id).await?;
                    }
                }
            }
            let result = self
                .append_with_policy_attempt(
                    req.clone(),
                    resolved_policy.clone(),
                    fetch_recent,
                    context_max_bytes,
                )
                .await;
            match result {
                Err(e) if Self::metadata_cache_miss_retryable(e.code) && attempt < 15 => continue,
                result => return result,
            }
        }
        unreachable!("bounded retry loop returns")
    }

    async fn append_with_policy_attempt(
        &self,
        req: AppendRequest,
        resolved_policy: Option<RuntimePolicy>,
        fetch_recent: Option<usize>,
        context_max_bytes: Option<usize>,
    ) -> Result<AppendCommitResponse> {
        if self.simple_append_coalescing_allowed(
            &req,
            resolved_policy.as_ref(),
            fetch_recent,
            context_max_bytes,
        ) {
            return self
                .append_with_simple_session_coalescing(req, fetch_recent, context_max_bytes)
                .await;
        }
        self.append_with_policy_direct(req, resolved_policy, fetch_recent, context_max_bytes)
            .await
    }

    fn simple_append_coalescing_allowed(
        &self,
        req: &AppendRequest,
        resolved_policy: Option<&RuntimePolicy>,
        fetch_recent: Option<usize>,
        context_max_bytes: Option<usize>,
    ) -> bool {
        let supported_response_shape = matches!(
            (fetch_recent, context_max_bytes),
            (None, None) | (Some(_), Some(_))
        );
        self.persist.is_some()
            && resolved_policy.is_none()
            && supported_response_shape
            && req.idempotency_key.is_none()
            && req.artifact_refs.is_empty()
            && req.source_refs.is_empty()
            && req.credential_refs.is_empty()
            && !env_flag_disabled_runtime("AGENTDB_SESSION_APPEND_COALESCE")
    }

    async fn append_with_simple_session_coalescing(
        &self,
        req: AppendRequest,
        fetch_recent: Option<usize>,
        context_max_bytes: Option<usize>,
    ) -> Result<AppendCommitResponse> {
        let queue = self.session_append_queue(&req.session_id);
        let (done, rx) = oneshot::channel();
        let mut start_runner = false;
        {
            let mut state = queue.inner.lock().await;
            state.jobs.push_back(SimpleAppendJob {
                req,
                fetch_recent,
                context_max_bytes,
                done,
            });
            if !state.running {
                state.running = true;
                start_runner = true;
            }
        }

        if start_runner {
            self.run_simple_append_queue(queue).await;
        }

        rx.await.map_err(|_| {
            Error::new(
                ErrorCode::Internal,
                "session append coalescer dropped response",
            )
        })?
    }

    async fn run_simple_append_queue(&self, queue: Arc<SessionAppendQueue>) {
        let max_batch = std::env::var("AGENTDB_SESSION_APPEND_BATCH_MAX")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64);
        let wait_us = std::env::var("AGENTDB_SESSION_APPEND_BATCH_WAIT_US")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        loop {
            let mut jobs = Vec::new();
            {
                let mut state = queue.inner.lock().await;
                while jobs.len() < max_batch {
                    let Some(job) = state.jobs.pop_front() else {
                        break;
                    };
                    jobs.push(job);
                }
                if jobs.is_empty() {
                    state.running = false;
                    if state.jobs.is_empty() {
                        return;
                    }
                    state.running = true;
                    continue;
                }
            }
            if wait_us > 0 && jobs.len() < max_batch {
                tokio::time::sleep(std::time::Duration::from_micros(wait_us)).await;
                let mut state = queue.inner.lock().await;
                while jobs.len() < max_batch {
                    let Some(job) = state.jobs.pop_front() else {
                        break;
                    };
                    jobs.push(job);
                }
            }
            self.process_simple_append_batch(jobs).await;
        }
    }

    async fn process_simple_append_batch(&self, jobs: Vec<SimpleAppendJob>) {
        if jobs.is_empty() {
            return;
        }
        let coalesce_profile = session_append_coalesce_profile();
        let total_started = coalesce_profile.map(|_| Instant::now());
        let mut coalesce_sample = SessionAppendCoalesceSample {
            jobs: jobs.len(),
            ..SessionAppendCoalesceSample::default()
        };
        let session_id = jobs[0].req.session_id.clone();
        let session_commit_lock = self.session_commit_lock(&session_id);
        let lock_wait_started = coalesce_profile.map(|_| Instant::now());
        let _session_commit_guard = session_commit_lock.lock().await;
        if let Some(started) = lock_wait_started {
            coalesce_sample.lock_wait_nanos = started.elapsed().as_nanos();
        }

        if let Err(err) = self.refresh_replicated_state().await {
            for job in jobs {
                let _ = job.done.send(Err(err.clone()));
            }
            return;
        }
        let persisted_session_max_seq = match self.persist.as_ref() {
            Some(persist) => {
                match persist.max_event_seq(jobs[0].req.tenant_id.as_str(), session_id.as_str()) {
                    Ok(max_seq) => max_seq,
                    Err(err) => {
                        let err = internal(err);
                        for job in jobs {
                            let _ = job.done.send(Err(err.clone()));
                        }
                        return;
                    }
                }
            }
            None => None,
        };

        let mut prepared = Vec::with_capacity(jobs.len());
        let mut final_session_snapshot: Option<Session> = None;
        let mut persist_session_checkpoint = false;

        let prepare_lock_wait_started = coalesce_profile.map(|_| Instant::now());
        {
            let mut s = self.state.lock().await;
            if let Some(started) = prepare_lock_wait_started {
                coalesce_sample.prepare_lock_wait_nanos = started.elapsed().as_nanos();
            }
            let prepare_work_started = coalesce_profile.map(|_| Instant::now());
            if let Some(max_seq) = persisted_session_max_seq {
                if let Err(err) = reconcile_session_next_seq_from_persisted_events(
                    &mut s,
                    &jobs[0].req.tenant_id,
                    &session_id,
                    max_seq,
                ) {
                    for job in jobs {
                        let _ = job.done.send(Err(err.clone()));
                    }
                    return;
                }
            }
            let mut policy_cache: BTreeMap<(String, String), RuntimePolicy> = BTreeMap::new();
            let mut metadata_key_cache: BTreeMap<String, KeyResolution> = BTreeMap::new();
            for job in jobs {
                let SimpleAppendJob {
                    req,
                    fetch_recent,
                    context_max_bytes,
                    done,
                } = job;

                let result = self.prepare_simple_append_under_state(
                    &s,
                    req,
                    fetch_recent,
                    context_max_bytes,
                    final_session_snapshot.take(),
                    &mut policy_cache,
                    &mut metadata_key_cache,
                );
                match result {
                    Ok((prepared_append, session_snapshot, checkpoint)) => {
                        persist_session_checkpoint |= checkpoint;
                        final_session_snapshot = Some(session_snapshot);
                        prepared.push((prepared_append, done));
                    }
                    Err((err, session_snapshot)) => {
                        final_session_snapshot = session_snapshot;
                        let _ = done.send(Err(err));
                    }
                }
            }
            if let Some(started) = prepare_work_started {
                coalesce_sample.prepare_work_nanos = started.elapsed().as_nanos();
            }
        }

        if prepared.is_empty() {
            return;
        }

        let event_refs = prepared
            .iter()
            .map(|(prepared, _)| &prepared.envelope)
            .collect::<Vec<_>>();
        coalesce_sample.events = event_refs.len();
        let session_checkpoint = persist_session_checkpoint
            .then_some(final_session_snapshot.as_ref())
            .flatten();
        if let Some(p) = &self.persist {
            let durable_write_started = coalesce_profile.map(|_| Instant::now());
            let durable_result = if env_flag_disabled_runtime("AGENTDB_APPEND_LOG_FAST_PATH") {
                let events = event_refs
                    .iter()
                    .map(|event| (*event).clone())
                    .collect::<Vec<_>>();
                p.append_many_batch_async(&events, session_checkpoint).await
            } else {
                p.append_many_log_refs_async(&event_refs, session_checkpoint)
                    .await
            };
            if let Err(err) = durable_result {
                drop(event_refs);
                let err = internal(err);
                for (_, done) in prepared {
                    let _ = done.send(Err(err.clone()));
                }
                return;
            }
            if let Some(started) = durable_write_started {
                coalesce_sample.durable_write_nanos = started.elapsed().as_nanos();
            }
        }
        drop(event_refs);

        let persist_enabled = self.persist.is_some();
        let update_state_lock_wait_started = coalesce_profile.map(|_| Instant::now());
        let mut s = self.state.lock().await;
        if let Some(started) = update_state_lock_wait_started {
            coalesce_sample.update_state_lock_wait_nanos = started.elapsed().as_nanos();
        }
        let update_state_work_started = coalesce_profile.map(|_| Instant::now());
        let mut completions = Vec::with_capacity(prepared.len());
        if let Some(session_snapshot) = final_session_snapshot {
            s.sessions
                .insert(session_id.as_str().to_string(), session_snapshot);
        }
        for (prepared_append, done) in prepared {
            let envelope = prepared_append.envelope;
            if !persist_enabled {
                s.event_by_id.insert(
                    envelope.event_id.as_str().to_string(),
                    (envelope.session_id.as_str().to_string(), envelope.seq),
                );
            }
            insert_runtime_event_cache(&mut s, persist_enabled, session_id.as_str(), envelope);

            let (recent, recent_complete) = if let Some(last) = prepared_append.fetch_recent {
                recent_for_appended_event_in_state(
                    &s,
                    session_id.as_str(),
                    prepared_append.appended.seq,
                    last,
                )
            } else {
                (Vec::new(), false)
            };
            let backpressure = backpressure_for_tenant_session_in_state(
                &s,
                &prepared_append.policy,
                &prepared_append.tenant_id,
                &prepared_append.session_after_append,
            );
            completions.push((
                done,
                AppendCommitResponse {
                    appended: prepared_append.appended,
                    recent,
                    recent_complete,
                    backpressure,
                    backpressure_complete: true,
                },
            ));
        }
        if let Some(started) = update_state_work_started {
            coalesce_sample.update_state_work_nanos = started.elapsed().as_nanos();
        }
        drop(s);
        for (done, response) in completions {
            let _ = done.send(Ok(response));
        }
        if let (Some(profile), Some(started)) = (coalesce_profile, total_started) {
            coalesce_sample.total_nanos = started.elapsed().as_nanos();
            profile.record(coalesce_sample);
        }
    }

    #[allow(clippy::result_large_err, clippy::too_many_arguments)]
    fn prepare_simple_append_under_state(
        &self,
        s: &State,
        mut req: AppendRequest,
        fetch_recent: Option<usize>,
        context_max_bytes: Option<usize>,
        prior_session_snapshot: Option<Session>,
        policy_cache: &mut BTreeMap<(String, String), RuntimePolicy>,
        metadata_key_cache: &mut BTreeMap<String, KeyResolution>,
    ) -> std::result::Result<(PreparedSimpleAppend, Session, bool), (Error, Option<Session>)> {
        sanitize_append_payload(&req.kind, &mut req.payload);
        let payload_len = serde_json::to_vec(&req.payload)
            .map(|v| v.len())
            .unwrap_or(usize::MAX);
        let policy_cache_key = (
            req.tenant_id.as_str().to_string(),
            req.agent_id.as_str().to_string(),
        );
        let policy = match policy_cache.get(&policy_cache_key) {
            Some(policy) => policy.clone(),
            None => {
                let policy = resolve_policy_for_agent_in_state(s, &req.tenant_id, &req.agent_id)
                    .map_err(|err| (err, prior_session_snapshot.clone()))?;
                policy_cache.insert(policy_cache_key, policy.clone());
                policy
            }
        };
        if let Some(max_bytes) = context_max_bytes {
            if let Some(max) = policy.quota.max_context_bytes {
                if max_bytes > max {
                    return Err((
                        Error::new(
                            ErrorCode::PolicyDenied,
                            format!(
                                "policy_version {} denies append_and_fetch: context byte request {max_bytes} exceeds quota {max}",
                                policy.version
                            ),
                        ),
                        prior_session_snapshot,
                    ));
                }
            }
        }
        validate_append_secret_handling_policy(&req.payload, &policy)
            .map_err(|err| (err, prior_session_snapshot.clone()))?;
        if payload_len > policy.artifact_size.max_inline_event_bytes {
            return Err((
                Error::new(
                    ErrorCode::EventTooLarge,
                    format!(
                        "inline event payload {payload_len} bytes exceeds {} byte limit",
                        policy.artifact_size.max_inline_event_bytes
                    ),
                )
                .with_suggestion(
                    "store the payload as an artifact and reference it via artifact_refs",
                ),
                prior_session_snapshot,
            ));
        }

        let mut snapshot = match prior_session_snapshot {
            Some(snapshot) => snapshot,
            None => s
                .sessions
                .get(req.session_id.as_str())
                .cloned()
                .ok_or_else(|| {
                    (
                        Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id)),
                        None,
                    )
                })?,
        };
        if snapshot.deleted_at_ms.is_some() || snapshot.tenant_id.as_str() != req.tenant_id.as_str()
        {
            return Err((
                Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id)),
                Some(snapshot),
            ));
        }
        if snapshot.agent_id.as_str() != req.agent_id.as_str() {
            return Err((
                Error::new(ErrorCode::AgentNotFound, format!("{}", req.agent_id)),
                Some(snapshot),
            ));
        }
        if let Some(max) = policy.quota.max_events_per_session {
            if snapshot.next_seq >= max {
                return Err((
                    Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies append: session event quota {max} exhausted",
                            policy.version
                        ),
                    ),
                    Some(snapshot),
                ));
            }
        }

        let metadata_key = match metadata_key_cache.get(req.tenant_id.as_str()) {
            Some(key) => key.clone(),
            None => {
                let key = self
                    .resolve_key(&req.tenant_id, KeyOperation::MetadataWrite)
                    .map_err(|err| (err, Some(snapshot.clone())))?;
                metadata_key_cache.insert(req.tenant_id.as_str().to_string(), key.clone());
                key
            }
        };
        let prior_lifecycle = snapshot.lifecycle;
        let prior_policy_version = snapshot.policy_version;
        if snapshot.lifecycle != Lifecycle::Warm {
            snapshot.lifecycle = Lifecycle::Warm;
            snapshot.warm_head_from_seq = snapshot.next_seq;
        }
        let seq = snapshot.next_seq;
        snapshot.next_seq += 1;
        snapshot.last_active_at_ms = crate::now_ms();
        snapshot.policy_version = policy.version;
        let persist_session_checkpoint =
            prior_lifecycle != Lifecycle::Warm || prior_policy_version != policy.version;

        let envelope = EventEnvelope::create(NewEvent {
            tenant_id: req.tenant_id.clone(),
            agent_id: req.agent_id.clone(),
            session_id: req.session_id.clone(),
            seq,
            kind: req.kind,
            payload: req.payload,
            artifact_refs: Vec::new(),
            source_refs: Vec::new(),
            credential_refs: Vec::new(),
            idempotency_key: None,
            policy_version: policy.version,
            metadata_key_id: Some(metadata_key.key_id),
            metadata_key_version: metadata_key.key_version,
        });
        let appended = AppendResponse {
            event_id: envelope.event_id.clone(),
            seq,
        };
        Ok((
            PreparedSimpleAppend {
                tenant_id: req.tenant_id,
                envelope,
                appended,
                policy,
                session_after_append: snapshot.clone(),
                fetch_recent,
            },
            snapshot,
            persist_session_checkpoint,
        ))
    }

    async fn append_with_policy_direct(
        &self,
        mut req: AppendRequest,
        resolved_policy: Option<RuntimePolicy>,
        fetch_recent: Option<usize>,
        context_max_bytes: Option<usize>,
    ) -> Result<AppendCommitResponse> {
        let path_profile = append_path_profile();
        let total_started = path_profile.map(|_| Instant::now());
        let mut path_sample = AppendPathSample::default();
        sanitize_append_payload(&req.kind, &mut req.payload);

        let refresh_started = path_profile.map(|_| Instant::now());
        self.refresh_replicated_state().await?;
        if let Some(started) = refresh_started {
            path_sample.refresh_nanos = started.elapsed().as_nanos();
        }

        // Compose the full idempotency scope key BEFORE locking state, so we can
        // short-circuit cheaply on retry.
        let idem_full_key = req
            .idempotency_key
            .as_ref()
            .map(|k| format!("{}::{}::{}::{}", req.tenant_id, "append", req.session_id, k));

        // Optimistic check before locking — fast path for retries that find a
        // committed result without contending on the state mutex.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.lookup_idempotency_record(full)? {
                return self.replay_append_idempotency_record(&rec, &req).await;
            }
        }

        // Inline payload size guard. Done after the idempotency short-circuit
        // (a retry that already committed should replay, not re-validate) but
        // before we lock state, so a misbehaving caller can't churn the mutex
        // by hammering oversized payload serialization.
        let payload_len = serde_json::to_vec(&req.payload)
            .map(|v| v.len())
            .unwrap_or(usize::MAX);

        let session_commit_lock = self.session_commit_lock(&req.session_id);
        let session_lock_wait_started = path_profile.map(|_| Instant::now());
        let _session_commit_guard = session_commit_lock.lock().await;
        if let Some(started) = session_lock_wait_started {
            path_sample.session_lock_wait_nanos = started.elapsed().as_nanos();
        }
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.lookup_idempotency_record(full)? {
                return self.replay_append_idempotency_record(&rec, &req).await;
            }
        }
        let persisted_session_max_seq = self
            .persist
            .as_ref()
            .map(|persist| persist.max_event_seq(req.tenant_id.as_str(), req.session_id.as_str()))
            .transpose()
            .map_err(internal)?
            .flatten();
        let initial_state_lock_wait_started = path_profile.map(|_| Instant::now());
        let mut s = self.state.lock().await;
        if let Some(started) = initial_state_lock_wait_started {
            path_sample.initial_state_lock_wait_nanos = started.elapsed().as_nanos();
        }
        let initial_state_work_started = path_profile.map(|_| Instant::now());

        // Re-check under the state lock to close the concurrent-append race:
        // another task could have committed our key between the optimistic check
        // and the lock acquisition above.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.lookup_idempotency_record(full)? {
                drop(s);
                return self.replay_append_idempotency_record(&rec, &req).await;
            }
        }
        if let Some(max_seq) = persisted_session_max_seq {
            reconcile_session_next_seq_from_persisted_events(
                &mut s,
                &req.tenant_id,
                &req.session_id,
                max_seq,
            )?;
        }
        if let Some(persist) = self.persist.as_ref() {
            advance_session_next_seq_past_persisted_collision(
                &mut s,
                persist,
                &req.tenant_id,
                &req.session_id,
            )?;
        }

        let policy = match resolved_policy {
            Some(policy) => policy,
            None => resolve_policy_for_agent_in_state(&s, &req.tenant_id, &req.agent_id)?,
        };
        if let Some(max_bytes) = context_max_bytes {
            if let Some(max) = policy.quota.max_context_bytes {
                if max_bytes > max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies append_and_fetch: context byte request {max_bytes} exceeds quota {max}",
                            policy.version
                        ),
                    ));
                }
            }
        }
        validate_append_secret_handling_policy(&req.payload, &policy)?;
        if payload_len > policy.artifact_size.max_inline_event_bytes {
            return Err(Error::new(
                ErrorCode::EventTooLarge,
                format!(
                    "inline event payload {payload_len} bytes exceeds {} byte limit",
                    policy.artifact_size.max_inline_event_bytes
                ),
            )
            .with_suggestion(
                "store the payload as an artifact and reference it via artifact_refs",
            ));
        }

        // Validate session + agent membership without mutating yet, so the
        // artifact_refs check below can run in the right error-code order
        // (session/agent mismatches surface as their dedicated NotFound
        // before we ever look at artifacts).
        {
            let session = s.sessions.get(req.session_id.as_str()).ok_or_else(|| {
                Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id))
            })?;
            if session.deleted_at_ms.is_some() {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{}", req.session_id),
                ));
            }
            // Tenant isolation: a session id from tenant B is "not found" from
            // tenant A's perspective, otherwise an attacker who learns a
            // cross-tenant session id could append events into another
            // tenant's session (and worse: those events would persist under
            // the attacker's tenant key prefix, splitting the session).
            if session.tenant_id.as_str() != req.tenant_id.as_str() {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{}", req.session_id),
                ));
            }
            // Agent must match the session's recorded agent. We surface this
            // as `AgentNotFound` (more precise than reusing SessionNotFound)
            // so callers get an actionable error.
            if session.agent_id.as_str() != req.agent_id.as_str() {
                return Err(Error::new(
                    ErrorCode::AgentNotFound,
                    format!("{}", req.agent_id),
                ));
            }
            if let Some(max) = policy.quota.max_events_per_session {
                if session.next_seq >= max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies append: session event quota {max} exhausted",
                            policy.version
                        ),
                    ));
                }
            }
        }

        // Validate every artifact_ref points to a live artifact owned by the
        // same tenant before we mutate session state. We collapse "wrong
        // kind", "unknown id", "soft-deleted", and "owned by another tenant"
        // into a single `ArtifactNotFound` (mirroring `get_artifact`) so a
        // caller in tenant A can't probe tenant B's artifact ids by
        // error-code differential.
        for art_ref in &req.artifact_refs {
            let known = (art_ref.artifact_id.kind() == crate::id::IdKind::Artifact)
                && s.artifacts
                    .get(art_ref.artifact_id.as_str())
                    .is_some_and(|a| {
                        a.deleted_at_ms.is_none() && a.tenant_id.as_str() == req.tenant_id.as_str()
                    });
            if !known {
                return Err(Error::new(
                    ErrorCode::ArtifactNotFound,
                    format!("{}", art_ref.artifact_id),
                ));
            }
        }

        // V2.2: validate every `source_refs[i].record_id` points to a live
        // record in the same tenant. Records are V2.2's domain so they're
        // strictly checked here (mirroring the artifact_refs check above).
        //
        // Other `SourceRef` variants are intentionally NOT validated:
        // - `artifact_id`: the canonical artifact link is the
        //   caller-explicit `artifact_refs: Vec<ArtifactRef>` field, which
        //   is already validated above. `source_refs.artifact_id` is a
        //   provenance hint, not a hard reference, so we don't double-walk.
        // - `session_id` + `seq`: validating would require a per-session
        //   `max_seq` lookup; events are append-only and a future V2.5
        //   fork-protection pass will revisit cross-session refs.
        // - `event_id`: `lookup_event_by_id` exists but V2.2 keeps the
        //   validation surface narrow to records only.
        for sr in &req.source_refs {
            if let Some(rec_id) = &sr.record_id {
                let exists = (rec_id.kind() == crate::id::IdKind::Record)
                    && s.records.get(rec_id.as_str()).is_some_and(|r| {
                        r.deleted_at_ms.is_none() && r.tenant_id.as_str() == req.tenant_id.as_str()
                    });
                if !exists {
                    return Err(Error::new(ErrorCode::RecordNotFound, format!("{}", rec_id)));
                }
            }
        }

        let metadata_key = self.resolve_key(&req.tenant_id, KeyOperation::MetadataWrite)?;

        let (seq, persist_session_checkpoint, session_snapshot) = {
            // Mint the seq from the current session snapshot but do not make
            // the mutation visible until the durable batch succeeds. The
            // per-session commit lock above preserves same-session ordering
            // while allowing unrelated sessions to persist concurrently.
            let session = s
                .sessions
                .get(req.session_id.as_str())
                .expect("session presence checked above");
            let prior_lifecycle = session.lifecycle;
            let prior_policy_version = session.policy_version;
            let mut snapshot = session.clone();
            if snapshot.lifecycle != Lifecycle::Warm {
                snapshot.lifecycle = Lifecycle::Warm;
                snapshot.warm_head_from_seq = snapshot.next_seq;
            }
            let seq = snapshot.next_seq;
            snapshot.next_seq += 1;
            snapshot.last_active_at_ms = crate::now_ms();
            snapshot.policy_version = policy.version;
            let persist_session_checkpoint =
                prior_lifecycle != Lifecycle::Warm || prior_policy_version != policy.version;
            (seq, persist_session_checkpoint, snapshot)
        };

        let envelope = EventEnvelope::create(NewEvent {
            tenant_id: req.tenant_id.clone(),
            agent_id: req.agent_id.clone(),
            session_id: req.session_id.clone(),
            seq,
            kind: req.kind.clone(),
            payload: req.payload.clone(),
            artifact_refs: req.artifact_refs.clone(),
            source_refs: req.source_refs.clone(),
            credential_refs: req.credential_refs.clone(),
            idempotency_key: req.idempotency_key.clone(),
            policy_version: policy.version,
            metadata_key_id: Some(metadata_key.key_id),
            metadata_key_version: metadata_key.key_version,
        });
        let event_id = envelope.event_id.clone();

        let resp = AppendResponse { event_id, seq };

        // Build the idempotency record up-front so both the in-memory and KV
        // commit paths can use it. The hash covers the entire request shape
        // (not just payload) so that a retry under the same idempotency key
        // with a different `kind`, agent, session, or refs is detected as a
        // conflict rather than silently replaying the original result.
        let idem_record = idem_full_key.as_ref().map(|_| IdempotencyRecord {
            payload_hash: hash_request(
                &req.tenant_id,
                &req.agent_id,
                &req.session_id,
                &req.kind,
                &req.payload,
                &req.artifact_refs,
                &req.source_refs,
                &req.credential_refs,
            ),
            result_json: serde_json::to_string(&resp).unwrap(),
            created_at_ms: crate::now_ms(),
        });

        if let Some(started) = initial_state_work_started {
            path_sample.initial_state_work_nanos = started.elapsed().as_nanos();
        }
        drop(s);

        // Atomic durable commit. The expensive storage call happens outside
        // the global state mutex; same-session ordering is protected by the
        // session commit lock held for the whole append. This keeps
        // read-after-write semantics after the call returns, but lets appends
        // for different sessions make storage progress in parallel.
        if let Some(p) = &self.persist {
            let pair = idem_full_key
                .as_ref()
                .zip(idem_record.as_ref())
                .map(|(k, r)| (k.as_str(), r));
            let session_checkpoint = persist_session_checkpoint.then_some(&session_snapshot);
            let durable_write_started = path_profile.map(|_| Instant::now());
            if let Err(e) = p
                .append_batch_async(&envelope, session_checkpoint, pair)
                .await
            {
                return Err(internal(e));
            }
            if let Some(started) = durable_write_started {
                path_sample.durable_write_nanos = started.elapsed().as_nanos();
            }
        }

        let next_seq_after_append = session_snapshot.next_seq;
        let persist_enabled = self.persist.is_some();
        let update_state_lock_wait_started = path_profile.map(|_| Instant::now());
        let mut s = self.state.lock().await;
        if let Some(started) = update_state_lock_wait_started {
            path_sample.update_state_lock_wait_nanos = started.elapsed().as_nanos();
        }
        let update_state_work_started = path_profile.map(|_| Instant::now());
        s.sessions
            .insert(req.session_id.as_str().to_string(), session_snapshot);
        // In-memory backend: keep the `event_by_id` map populated. The
        // persisted backend resolves event IDs through the durable secondary
        // index to avoid unbounded in-process growth on large histories.
        if !persist_enabled {
            s.event_by_id.insert(
                envelope.event_id.as_str().to_string(),
                (envelope.session_id.as_str().to_string(), envelope.seq),
            );
        }
        insert_runtime_event_cache(&mut s, persist_enabled, req.session_id.as_str(), envelope);

        let (recent, recent_complete) = if let Some(last) = fetch_recent {
            let last_seq_count = u64::try_from(last).unwrap_or(u64::MAX);
            let from_seq = resp.seq.saturating_add(1).saturating_sub(last_seq_count);
            let lo = (req.session_id.as_str().to_string(), from_seq);
            let hi = (req.session_id.as_str().to_string(), u64::MAX);
            let mut recent = s
                .events
                .range(lo..=hi)
                .take(last)
                .map(|(_, event)| event.clone())
                .collect::<Vec<_>>();
            if recent.len() > last {
                let drop_n = recent.len() - last;
                recent.drain(0..drop_n);
            }
            let expected = usize::try_from(next_seq_after_append.saturating_sub(from_seq))
                .unwrap_or(usize::MAX)
                .min(last);
            let complete = recent.len() >= expected;
            (recent, complete)
        } else {
            (Vec::new(), false)
        };

        let backpressure = {
            let session = s
                .sessions
                .get(req.session_id.as_str())
                .expect("session was inserted above");
            backpressure_for_tenant_session_in_state(&s, &policy, &req.tenant_id, session)
        };

        // Record the idempotency result before releasing the per-session
        // commit lock, so a same-session retry with the same key observes this
        // record on its under-lock re-check and replays instead of committing a
        // second event. Lock order is (state -> idem) and the idempotency store
        // never reaches back into state, so there is no deadlock risk despite
        // holding both briefly.
        if let (Some(full), Some(rec)) = (idem_full_key, idem_record) {
            self.idem.put(full, rec);
        }
        if let Some(started) = update_state_work_started {
            path_sample.update_state_work_nanos = started.elapsed().as_nanos();
        }
        drop(s);

        if let (Some(profile), Some(started)) = (path_profile, total_started) {
            path_sample.total_nanos = started.elapsed().as_nanos();
            profile.record(path_sample);
        }

        Ok(AppendCommitResponse {
            appended: resp,
            recent,
            recent_complete,
            backpressure,
            backpressure_complete: true,
        })
    }

    /// Append an event and return it together with up to `last` recent events
    /// from the session in ascending seq order.
    ///
    /// # Parameters
    /// - `last`: maximum number of recent events to return.
    /// - `max_bytes`: byte budget for returned recent event envelopes. The
    ///   runtime keeps the newest events that fit and drops older context.
    pub async fn append_and_fetch(
        &self,
        req: AppendRequest,
        last: usize,
        max_bytes: usize,
    ) -> Result<AppendAndFetchResponse> {
        Ok(self
            .append_and_fetch_with_backpressure(req, last, max_bytes)
            .await?
            .0)
    }

    pub async fn append_and_fetch_with_backpressure(
        &self,
        req: AppendRequest,
        last: usize,
        max_bytes: usize,
    ) -> Result<(AppendAndFetchResponse, BackpressureReport)> {
        let tenant_id = req.tenant_id.clone();
        let session_id = req.session_id.clone();
        let committed = self
            .append_with_policy(req, None, Some(last), Some(max_bytes))
            .await?;

        // Fetch only the tail window needed for context. A full-session fetch
        // makes append+fetch latency grow with session length and serializes
        // far more storage work than the API requires.
        let last_seq_count = u64::try_from(last).unwrap_or(u64::MAX);
        let from_seq = committed
            .appended
            .seq
            .saturating_add(1)
            .saturating_sub(last_seq_count);
        let mut recent = if last == 0 || committed.recent_complete {
            committed.recent
        } else {
            self.fetch_from_seq(FetchRequest {
                tenant_id: tenant_id.clone(),
                session_id: session_id.clone(),
                from_seq,
                limit: last,
            })
            .await?
        };
        if recent.len() > last {
            let drop_n = recent.len() - last;
            recent.drain(0..drop_n);
        }
        trim_recent_to_budget(&mut recent, max_bytes);

        let backpressure = if committed.backpressure_complete {
            committed.backpressure
        } else {
            self.backpressure_for_tenant_and_session(&tenant_id, &session_id)
                .await?
        };

        Ok((
            AppendAndFetchResponse {
                event_id: committed.appended.event_id,
                seq: committed.appended.seq,
                recent,
            },
            backpressure,
        ))
    }

    pub async fn build_context(&self, req: ContextRequest) -> Result<ContextResponse> {
        let session = self.get_session(&req.tenant_id, &req.session_id).await?;
        let include_events = req.include.contains(&ContextInclude::RecentEvents);
        let include_records = req.include.contains(&ContextInclude::Records);
        let include_policy = req.include.contains(&ContextInclude::Policy);

        let mut response = ContextResponse {
            session_id: req.session_id.clone(),
            events: Vec::new(),
            records: Vec::new(),
            policy: None,
            bytes_used: 0,
            truncated: false,
        };

        if include_events {
            let mut events = self
                .fetch_from_seq(FetchRequest {
                    tenant_id: req.tenant_id.clone(),
                    session_id: req.session_id.clone(),
                    from_seq: 0,
                    limit: usize::MAX,
                })
                .await?;
            if events.len() > req.max_events {
                events.drain(0..events.len() - req.max_events);
                response.truncated = true;
            }
            for event in events {
                if push_budgeted(&mut response.bytes_used, req.max_bytes, &event) {
                    response.events.push(event);
                } else {
                    response.truncated = true;
                    break;
                }
            }
        }

        if include_records {
            if let Some(record_query) = req.record_query {
                let records = self.search_records(record_query).await?;
                for record in records {
                    if push_budgeted(&mut response.bytes_used, req.max_bytes, &record) {
                        response.records.push(record);
                    } else {
                        response.truncated = true;
                        break;
                    }
                }
            }
        }

        if include_policy {
            let policy = self
                .resolve_policy_for_agent(&req.tenant_id, &session.agent_id)
                .await?;
            if push_budgeted(&mut response.bytes_used, req.max_bytes, &policy) {
                response.policy = Some(policy);
            } else {
                response.truncated = true;
            }
        }

        Ok(response)
    }

    pub async fn export_watch(&self, req: ExportWatchRequest) -> Result<Vec<ExportFeedItem>> {
        self.export_watch_through(req, None).await
    }

    /// Read an export page no later than a captured tenant high-watermark.
    /// The optional boundary is distinct from `from_cursor`: callers resume
    /// after `from_cursor` but must not observe commits after `through_cursor`.
    pub async fn export_watch_through(
        &self,
        req: ExportWatchRequest,
        through_cursor: Option<&str>,
    ) -> Result<Vec<ExportFeedItem>> {
        validate_export_include(&req.include)?;
        if req.limit == 0 {
            return Ok(Vec::new());
        }
        self.ensure_tenant_cached(&req.tenant_id).await?;
        if let Some(session_id) = req.session_id.as_ref() {
            self.ensure_session_cached(&req.tenant_id, session_id)
                .await?;
        }
        if let Some(agent_id) = req.agent_id.as_ref() {
            self.ensure_agent_cached(&req.tenant_id, agent_id).await?;
        }
        if let Some(persist) = &self.persist {
            let scope_validation = {
                let s = self.state.lock().await;
                validate_export_consumer_scope_in_state(
                    &s,
                    &req.tenant_id,
                    req.scope,
                    req.agent_id.as_ref(),
                    req.session_id.as_ref(),
                )
            };
            if let Err(error) = scope_validation {
                let has_retained_ledger = persist
                    .export_ledger_high_watermark(req.tenant_id.as_str())
                    .map_err(internal)?
                    .is_some();
                if !has_retained_ledger || !export_scope_has_required_ids(&req) {
                    return Err(error);
                }
            }
            self.validate_persistent_export_cursor(&req).await?;
            if let Some(through_cursor) = through_cursor {
                let boundary_request = ExportWatchRequest {
                    tenant_id: req.tenant_id.clone(),
                    scope: WatchScope::Tenant,
                    session_id: None,
                    agent_id: None,
                    include: Vec::new(),
                    from_cursor: Some(through_cursor.to_string()),
                    limit: 1,
                };
                self.validate_persistent_export_cursor(&boundary_request)
                    .await?;
            }
            let mut out = Vec::new();
            let cursor_context = self.changefeed_cursor_context()?;
            let agent_id = (req.scope == WatchScope::Agent).then(|| {
                req.agent_id
                    .as_ref()
                    .expect("validated export agent scope has agent_id")
                    .as_str()
            });
            let session_id = (req.scope == WatchScope::Session).then(|| {
                req.session_id
                    .as_ref()
                    .expect("validated export session scope has session_id")
                    .as_str()
            });
            persist
                .visit_export_ledger_items(
                    req.tenant_id.as_str(),
                    agent_id,
                    session_id,
                    req.from_cursor.as_deref(),
                    |cursor, item| {
                        if through_cursor.is_some_and(|boundary| cursor.as_str() > boundary) {
                            return true;
                        }
                        if !export_item_matches_scope(
                            &item,
                            req.scope,
                            req.agent_id.as_ref(),
                            req.session_id.as_ref(),
                        ) || !export_item_matches_include(&item, &req.include)
                        {
                            return false;
                        }
                        out.push(ExportFeedItem {
                            cursor,
                            cell_id: cursor_context.cell_id.clone(),
                            placement_scope_id: cursor_context.placement_scope_id.clone(),
                            tablet_id: cursor_context.tablet_id,
                            tablet_epoch: cursor_context.tablet_epoch,
                            applied_index: cursor_context.applied_index,
                            item,
                        });
                        out.len() >= req.limit
                    },
                )
                .map_err(internal)?;
            return Ok(out);
        }

        // In-memory export ledger: read the append-ordered ledger the same way
        // the persist branch reads its durable ledger — scope + `ledger:` cursor
        // bounds + include filter — so both backends behave identically. The
        // `through_cursor` boundary is a `ledger:` high-watermark string.
        if let Some(through_cursor) = through_cursor {
            if parse_export_ledger_cursor_for_runtime(through_cursor).is_none() {
                return Err(Error::new(
                    ErrorCode::InvalidCursor,
                    "in-memory export high watermark must use the ledger: format",
                ));
            }
        }
        self.validate_in_memory_export_cursor(&req).await?;
        let cursor_context = self.changefeed_cursor_context()?;
        let mut out = Vec::new();
        let s = self.state.lock().await;
        validate_export_consumer_scope_in_state(
            &s,
            &req.tenant_id,
            req.scope,
            req.agent_id.as_ref(),
            req.session_id.as_ref(),
        )?;
        if let Some(ledger) = s.export_ledger.get(req.tenant_id.as_str()) {
            for (&seq, item) in ledger.iter() {
                let cursor = in_memory_export_ledger_cursor(req.tenant_id.as_str(), seq, item);
                if req
                    .from_cursor
                    .as_deref()
                    .is_some_and(|from| cursor.as_str() <= from)
                {
                    continue;
                }
                if through_cursor.is_some_and(|boundary| cursor.as_str() > boundary) {
                    break;
                }
                if !export_item_matches_scope(
                    item,
                    req.scope,
                    req.agent_id.as_ref(),
                    req.session_id.as_ref(),
                ) || !export_item_matches_include(item, &req.include)
                {
                    continue;
                }
                out.push(ExportFeedItem {
                    cursor,
                    cell_id: cursor_context.cell_id.clone(),
                    placement_scope_id: cursor_context.placement_scope_id.clone(),
                    tablet_id: cursor_context.tablet_id,
                    tablet_epoch: cursor_context.tablet_epoch,
                    applied_index: cursor_context.applied_index,
                    item: item.clone(),
                });
                if out.len() >= req.limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Cursor validation for the in-memory export ledger, mirroring
    /// `validate_persistent_export_cursor`: the resume cursor must use the
    /// `ledger:` format, belong to this tenant (and session, for session
    /// scope), and still be within the retained ledger range.
    async fn validate_in_memory_export_cursor(&self, req: &ExportWatchRequest) -> Result<()> {
        let Some(cursor) = req.from_cursor.as_deref() else {
            return Ok(());
        };
        let Some(parts) = parse_export_ledger_cursor_for_runtime(cursor) else {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "in-memory export cursor must use the ledger: format",
            ));
        };
        if parts.tenant_id != req.tenant_id.as_str() {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "export cursor belongs to a different tenant",
            ));
        }
        if req.scope == WatchScope::Session
            && req.session_id.as_ref().is_some_and(|session_id| {
                parts
                    .session_id
                    .is_some_and(|cursor_session_id| cursor_session_id != session_id.as_str())
            })
        {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "export cursor belongs to a different session",
            ));
        }
        let s = self.state.lock().await;
        let ledger = s.export_ledger.get(req.tenant_id.as_str());
        if let Some((&oldest_seq, oldest_item)) = ledger.and_then(|ledger| ledger.iter().next()) {
            let oldest =
                in_memory_export_ledger_cursor(req.tenant_id.as_str(), oldest_seq, oldest_item);
            if cursor < oldest.as_str() {
                return Err(Error::new(
                    ErrorCode::InvalidCursor,
                    format!(
                        "export cursor is older than the retained export ledger; oldest_available_cursor={oldest}"
                    ),
                ));
            }
        }
        let exists = ledger.is_some_and(|ledger| {
            ledger.iter().any(|(&seq, item)| {
                in_memory_export_ledger_cursor(req.tenant_id.as_str(), seq, item) == cursor
            })
        });
        if !exists {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "export cursor was not found in the retained export ledger",
            ));
        }
        Ok(())
    }

    async fn validate_persistent_export_cursor(&self, req: &ExportWatchRequest) -> Result<()> {
        let Some(cursor) = req.from_cursor.as_deref() else {
            return Ok(());
        };
        let Some(parts) = parse_export_ledger_cursor_for_runtime(cursor) else {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "persistent export cursor must use the ledger: format",
            ));
        };
        if parts.tenant_id != req.tenant_id.as_str() {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "export cursor belongs to a different tenant",
            ));
        }
        if req.scope == WatchScope::Session
            && req.session_id.as_ref().is_some_and(|session_id| {
                parts
                    .session_id
                    .is_some_and(|cursor_session_id| cursor_session_id != session_id.as_str())
            })
        {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "export cursor belongs to a different session",
            ));
        }
        if let Some(oldest) = self.export_oldest_available_cursor(&req.tenant_id).await? {
            if cursor < oldest.as_str() {
                return Err(Error::new(
                    ErrorCode::InvalidCursor,
                    format!(
                        "export cursor is older than the retained export ledger; oldest_available_cursor={oldest}"
                    ),
                ));
            }
        }
        let exists = self
            .persist
            .as_ref()
            .expect("persistent cursor validation requires persistence")
            .get_export_ledger_item(req.tenant_id.as_str(), cursor)
            .map_err(internal)?
            .is_some();
        if !exists {
            return Err(Error::new(
                ErrorCode::InvalidCursor,
                "export cursor does not identify a retained ledger item",
            ));
        }
        Ok(())
    }

    /// Resolve an `event_id` to its `(session_id, seq)` location.
    ///
    /// KV-backed runtime resolves through the persisted `event_by_id`
    /// secondary index. In-memory runtime resolves through an in-process
    /// map populated on `append`; the map is reset on every restart, which
    /// is fine — the in-memory backend doesn't persist anyway.
    ///
    /// In both branches the lookup is tenant-scoped: an event_id created
    /// under tenant A is `None` to a caller passing tenant B, so a probe
    /// for cross-tenant event ids cannot leak existence.
    pub async fn lookup_event_by_id(
        &self,
        tenant_id: &Id,
        event_id: &Id,
    ) -> Result<Option<(Id, u64)>> {
        if let Some(p) = &self.persist {
            // KV-backed: persist is the single source of truth. The
            // KV `event_by_id` key is itself tenant-scoped, so a
            // mismatched tenant simply won't find the row.
            let raw = p
                .lookup_event_by_id(tenant_id.as_str(), event_id.as_str())
                .map_err(internal)?;
            let raw = match raw {
                Some(location) => Some(location),
                None => p
                    .lookup_event_by_id_in_append_log(tenant_id.as_str(), event_id.as_str())
                    .map_err(internal)?,
            };
            let Some((sid, seq)) = raw else {
                return Ok(None);
            };
            let session_id =
                Id::parse(&sid).map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?;
            return Ok(Some((session_id, seq)));
        }

        // In-memory backend: walk the event into its session, confirm
        // the session belongs to the requesting tenant.
        let s = self.state.lock().await;
        let Some((sid_str, seq)) = s.event_by_id.get(event_id.as_str()) else {
            return Ok(None);
        };
        let Some(sess) = s.sessions.get(sid_str.as_str()) else {
            // Session was deleted out from under the event_by_id
            // entry; treat as not-found rather than leak the orphan.
            return Ok(None);
        };
        if sess.tenant_id.as_str() != tenant_id.as_str() {
            return Ok(None);
        }
        Ok(Some((sess.session_id.clone(), *seq)))
    }

    /// Return events from `from_seq` (inclusive) up to `limit` count, in
    /// ascending seq order. Returns `SessionNotFound` if the session does not
    /// exist. Returns an empty vec if `from_seq` is past the session's last
    /// committed seq.
    pub async fn fetch_from_seq(&self, req: FetchRequest) -> Result<Vec<EventEnvelope>> {
        self.refresh_replicated_state().await?;
        for attempt in 0..16 {
            self.ensure_tenant_cached(&req.tenant_id).await?;
            self.ensure_session_cached(&req.tenant_id, &req.session_id)
                .await?;
            let result = self.fetch_from_seq_attempt(req.clone()).await;
            match result {
                Err(e) if Self::metadata_cache_miss_retryable(e.code) && attempt < 15 => continue,
                result => return result,
            }
        }
        unreachable!("bounded retry loop returns")
    }

    async fn fetch_from_seq_attempt(&self, req: FetchRequest) -> Result<Vec<EventEnvelope>> {
        let (session, hot_in_memory) = {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{}", req.session_id),
                ));
            }
            let session = s.sessions.get(req.session_id.as_str()).ok_or_else(|| {
                Error::new(ErrorCode::SessionNotFound, format!("{}", req.session_id))
            })?;
            if session.deleted_at_ms.is_some()
                || session.tenant_id.as_str() != req.tenant_id.as_str()
            {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{}", req.session_id),
                ));
            }
            let lo = (
                req.session_id.as_str().to_string(),
                req.from_seq.max(session.warm_head_from_seq),
            );
            let hi = (req.session_id.as_str().to_string(), u64::MAX);
            let hot = s
                .events
                .range(lo..=hi)
                .take(req.limit)
                .map(|(_, v)| v.clone())
                .collect::<Vec<_>>();
            (session.clone(), hot)
        };

        let mut out = Vec::new();
        if req.from_seq >= session.warm_head_from_seq {
            let expected_hot = usize::try_from(session.next_seq.saturating_sub(req.from_seq))
                .unwrap_or(usize::MAX)
                .min(req.limit);
            if hot_cache_covers_from(&hot_in_memory, req.from_seq, expected_hot) {
                out.extend(hot_in_memory.into_iter().take(req.limit));
                return Ok(out);
            }
        }

        // Archive and sealed segments are NOT mutually exclusive: sealing a
        // reopened warm head on top of an archived session keeps the archive
        // manifest AND pushes new segments above it. The merged view must
        // read archive, then segments, then the warm head — treating these
        // as alternatives makes post-archive segments invisible, and the
        // next archive compaction (which reads through this path) would
        // write a bundle without them and delete their blobs: silent,
        // permanent history loss. Caught by the S15 lifecycle-chaos harness.
        if let Some(manifest) = &session.archive_manifest {
            let mut archived = self.read_session_archive_events(manifest).await?;
            archived.retain(|event| event.seq >= req.from_seq);
            out.extend(archived);
            if out.len() >= req.limit {
                out.truncate(req.limit);
                return Ok(out);
            }
        }
        let mut segments = session.sealed_segments.clone();
        segments.sort_by_key(|segment| segment.seq_start);
        for segment in segments {
            if segment.seq_end < req.from_seq {
                continue;
            }
            let mut events = self.read_session_segment_events(&segment).await?;
            if req.from_seq > segment.seq_start {
                events.retain(|event| event.seq >= req.from_seq);
            }
            out.extend(events);
            if out.len() >= req.limit {
                out.truncate(req.limit);
                return Ok(out);
            }
        }

        let remaining = req.limit.saturating_sub(out.len());
        if remaining == 0 {
            return Ok(out);
        }
        let hot_from_seq = req.from_seq.max(session.warm_head_from_seq);
        if session.archive_manifest.is_none() && session.sealed_segments.is_empty() {
            let expected_hot = usize::try_from(session.next_seq.saturating_sub(hot_from_seq))
                .unwrap_or(usize::MAX)
                .min(remaining);
            if hot_cache_covers_from(&hot_in_memory, hot_from_seq, expected_hot) {
                out.extend(hot_in_memory.into_iter().take(remaining));
                if out.len() > req.limit {
                    out.truncate(req.limit);
                }
                return Ok(out);
            }
        }
        let hot = if self.persist.is_some() {
            let mut hot = self
                .read_hot_events_from_persist(
                    &req.tenant_id,
                    &req.session_id,
                    hot_from_seq,
                    remaining,
                )
                .await?;
            for event in hot_in_memory {
                if !hot.iter().any(|existing| existing.seq == event.seq) {
                    hot.push(event);
                }
            }
            hot.sort_by_key(|event| event.seq);
            hot.truncate(remaining);
            hot
        } else {
            hot_in_memory
        };
        out.extend(hot);
        if out.len() > req.limit {
            out.truncate(req.limit);
        }
        Ok(out)
    }

    /// Return up to `limit` latest events for a session in ascending seq order.
    ///
    /// This is the common agent-context read path. It computes the lower bound
    /// from the authoritative session `next_seq` and then reuses
    /// `fetch_from_seq`, so callers avoid stale fixture-side high-watermarks
    /// and do not need to scan from the beginning of the session.
    pub async fn fetch_recent(
        &self,
        tenant_id: Id,
        session_id: Id,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.refresh_replicated_state().await?;
        for attempt in 0..16 {
            self.ensure_tenant_cached(&tenant_id).await?;
            self.ensure_session_cached(&tenant_id, &session_id).await?;
            let result = self
                .fetch_recent_attempt(tenant_id.clone(), session_id.clone(), limit)
                .await;
            match result {
                Err(e) if Self::metadata_cache_miss_retryable(e.code) && attempt < 15 => continue,
                result => return result,
            }
        }
        unreachable!("bounded retry loop returns")
    }

    async fn fetch_recent_attempt(
        &self,
        tenant_id: Id,
        session_id: Id,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>> {
        let (session, from_seq, hot_in_memory) = {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{}", session_id),
                ));
            }
            let session = s
                .sessions
                .get(session_id.as_str())
                .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))?;
            if session.deleted_at_ms.is_some() || session.tenant_id.as_str() != tenant_id.as_str() {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{}", session_id),
                ));
            }
            let limit = u64::try_from(limit).unwrap_or(u64::MAX);
            let from_seq = session.next_seq.saturating_sub(limit);
            let lo = (
                session_id.as_str().to_string(),
                from_seq.max(session.warm_head_from_seq),
            );
            let hi = (session_id.as_str().to_string(), u64::MAX);
            let hot = s
                .events
                .range(lo..=hi)
                .take(usize::try_from(limit).unwrap_or(usize::MAX))
                .map(|(_, v)| v.clone())
                .collect::<Vec<_>>();
            (session.clone(), from_seq, hot)
        };

        if from_seq >= session.warm_head_from_seq {
            let expected_hot = usize::try_from(session.next_seq.saturating_sub(from_seq))
                .unwrap_or(usize::MAX)
                .min(limit);
            if hot_cache_covers_from(&hot_in_memory, from_seq, expected_hot) {
                return Ok(hot_in_memory.into_iter().take(limit).collect());
            }
        }

        if self.persist.is_some() {
            let hot_from_seq = from_seq.max(session.warm_head_from_seq);
            let mut hot = self
                .read_hot_events_from_persist(&tenant_id, &session_id, hot_from_seq, limit)
                .await?;
            // Prefer in-memory writes that may not yet have been flushed into
            // the persisted hot scan result, but avoid duplicate sequence
            // numbers if both sources contain the same tail event.
            for event in hot_in_memory {
                if !hot.iter().any(|existing| existing.seq == event.seq) {
                    hot.push(event);
                }
            }
            hot.sort_by_key(|event| event.seq);
            if hot.len() > limit {
                let drop_n = hot.len() - limit;
                hot.drain(0..drop_n);
            }
            Ok(hot)
        } else {
            let mut hot = hot_in_memory;
            if hot.len() > limit {
                let drop_n = hot.len() - limit;
                hot.drain(0..drop_n);
            }
            Ok(hot)
        }
    }
}

/// V2.2 search request for `Runtime::search_records`. Either `text`,
/// `embedding`, or both may be set; if both are absent, the runtime
/// returns every live record under the `(tenant_id, namespace)` scope
/// up to `limit`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetadataFilterOp {
    Eq,
    Neq,
    In,
    Exists,
    Gte,
    Lte,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetadataFilterCondition {
    pub path: String,
    pub op: MetadataFilterOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MetadataFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub all: Vec<MetadataFilterCondition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub any: Vec<MetadataFilterCondition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub none: Vec<MetadataFilterCondition>,
}

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub tenant_id: Id,
    pub namespace: Vec<String>,
    /// Optional caller-provided placement/sharding key. When present, record
    /// search uses the owner-local LSI-like index. When absent, record search
    /// uses the GSI-like global index and may lag unless a visibility token
    /// forces owner-local repair.
    pub sharding_key: Option<String>,
    pub text: Option<String>,
    pub embedding: Option<Vec<f32>>,
    /// Post-index tag filter. A record matches when **every** tag in
    /// this list is present in `SearchableRecord::tags`.
    pub tags: Vec<String>,
    /// Post-index source filter. A record matches when its `source`
    /// is `Some(s)` and `s` matches every populated field of the
    /// filter (unset fields don't constrain).
    pub source_filter: Option<SourceRef>,
    /// Optional post-index metadata filter. This intentionally supports a small
    /// scalar predicate subset rather than becoming a general JSON query engine.
    pub metadata_filter: Option<MetadataFilter>,
    /// Include logically superseded records. Search excludes them by default
    /// while preserving direct reads for audit/history workflows.
    pub include_superseded: bool,
    /// Drop matches below this query-local score.
    pub min_score: Option<f32>,
    /// Inclusive lower bound on record creation time.
    pub created_after_ms: Option<u64>,
    /// Inclusive upper bound on record creation time.
    pub created_before_ms: Option<u64>,
    /// Increment access counters for returned matches after ranking.
    pub touch: bool,
    pub limit: usize,
    /// Tenant-scoped searchable-record commit sequence that this read must
    /// observe. If the local index watermark is behind, `search_records`
    /// repairs from committed base records before replying.
    pub min_visible_record_commit_seq: Option<u64>,
}

impl SearchRequest {
    /// Convenience: text-only search with no filters.
    pub fn text_only(
        tenant_id: Id,
        namespace: Vec<String>,
        query: impl Into<String>,
        limit: usize,
    ) -> Self {
        Self {
            tenant_id,
            namespace,
            sharding_key: None,
            text: Some(query.into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit,
            min_visible_record_commit_seq: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RecordSearchScoreComponents {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordSearchMatch {
    pub record: SearchableRecord,
    pub score: f32,
    pub rank: usize,
    pub components: RecordSearchScoreComponents,
}

/// A scoped, text-only search over the durable event log. Unlike record search
/// (a Labs index feature), this is a core capability: an agent asking about its
/// own past sessions. It is an unindexed scan over the events in scope, so
/// callers narrow with `session_id`/`agent_id` for bounded cost.
#[derive(Debug, Clone)]
pub struct EventSearchRequest {
    pub tenant_id: Id,
    /// Scope to a single session. Takes precedence over `agent_id`.
    pub session_id: Option<Id>,
    /// Scope to one agent's sessions when `session_id` is absent.
    pub agent_id: Option<Id>,
    /// Whitespace-tokenized, case-insensitive query matched against event kind
    /// and payload text. An empty query returns no matches.
    pub query: String,
    /// Optional exact `kind` filter applied before text matching.
    pub kind: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSearchMatch {
    pub event: EventEnvelope,
    /// Number of distinct query tokens present in the event's searchable text.
    pub score: u32,
}

#[derive(Debug, Clone, Default)]
struct CandidateScore {
    score: f32,
    text: Option<f32>,
    vector: Option<f32>,
}

impl CandidateScore {
    fn with_text(score: f32) -> Self {
        Self {
            score,
            text: Some(score),
            vector: None,
        }
    }

    fn with_vector(score: f32) -> Self {
        Self {
            score,
            text: None,
            vector: Some(score),
        }
    }

    fn merge_text(&mut self, score: f32) {
        self.text = Some(self.text.map_or(score, |old| old.max(score)));
        self.score = self.score.max(score);
    }

    fn merge_vector(&mut self, score: f32) {
        self.vector = Some(self.vector.map_or(score, |old| old.max(score)));
        self.score = self.score.max(score);
    }

    fn components(&self) -> RecordSearchScoreComponents {
        RecordSearchScoreComponents {
            text: self.text,
            vector: self.vector,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommitScope {
    Session(Id),
    Tenant(Id),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitToken {
    pub placement_id: Option<i64>,
    pub scope: CommitScope,
    pub seq: u64,
}

#[derive(Debug, Clone)]
pub struct WriteRecordResult {
    pub record: SearchableRecord,
    pub commit_token: CommitToken,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordIndexStats {
    pub local_index_watermark: u64,
    pub global_index_watermark: u64,
    pub next_record_commit_seq: u64,
    pub local_index_lag_tokens: u64,
    pub global_index_lag_tokens: u64,
    pub local_index_repairs_total: u64,
    pub local_index_repair_candidates_total: u64,
    pub record_index_worker_failures_total: u64,
    pub global_index_apply_failures_total: u64,
    pub record_index_outbox_skipped_total: u64,
    #[serde(default)]
    pub snapshot_loaded_at_startup: bool,
    #[serde(default)]
    pub snapshot_last_watermark: u64,
    #[serde(default)]
    pub snapshot_writes_total: u64,
}

impl Runtime {
    /// Create and persist a V2.2 [`SearchableRecord`]. The record is
    /// indexed into the per-(tenant, namespace) text and vector indexes
    /// according to its `policy` *before* it is persisted, so a search
    /// against the in-memory cache cannot see a record that hasn't yet
    /// hit the indexes. The persist step is last; on persist failure we
    /// roll the indexes back to keep the runtime consistent with disk.
    pub async fn write_record(&self, req: NewRecord) -> Result<SearchableRecord> {
        Ok(self.write_record_with_commit(req).await?.record)
    }

    pub async fn write_record_with_commit(&self, req: NewRecord) -> Result<WriteRecordResult> {
        self.refresh_replicated_state().await?;
        let record = SearchableRecord::create(req);
        self.ensure_tenant_cached(&record.tenant_id).await?;
        let policy = self.resolve_policy_for_tenant(&record.tenant_id).await?;

        // Validate tenant exists. We hold the state lock only for the
        // tenant existence check; the index operations and persist
        // happen after we drop it.
        {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(record.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", record.tenant_id),
                ));
            }
            if let Some(external_id) = record.external_id.as_deref() {
                if let Some(existing_id) = s
                    .record_external_ids
                    .get(&record_external_key(&record.tenant_id, external_id))
                {
                    if let Some(existing) = s.records.get(existing_id).filter(|r| {
                        r.deleted_at_ms.is_none()
                            && !r.is_expired()
                            && r.tenant_id.as_str() == record.tenant_id.as_str()
                    }) {
                        let seq = s.record_commit_seq.get(existing_id).copied().unwrap_or(0);
                        return Ok(WriteRecordResult {
                            record: existing.clone(),
                            commit_token: CommitToken {
                                placement_id: None,
                                scope: CommitScope::Tenant(record.tenant_id.clone()),
                                seq,
                            },
                        });
                    }
                }
            }
            if let Some(max) = policy.quota.max_records {
                let live_count = s
                    .records
                    .values()
                    .filter(|r| {
                        r.tenant_id.as_str() == record.tenant_id.as_str()
                            && r.deleted_at_ms.is_none()
                            && !r.is_expired()
                    })
                    .count();
                if live_count >= max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies record write: record quota {max} exhausted",
                            policy.version
                        ),
                    ));
                }
            }
        }

        // Mint the DocId and index the record. We do this before
        // persisting so a successful indexes-then-persist failure path
        // gives us a single thing to roll back (the index inserts) —
        // we can't selectively delete a single doc from the BM25
        // postings list cheaply, so on persist failure we tolerate the
        // residual index entry: the corresponding record_id never gets
        // committed to `state.records`, so search will look it up,
        // find nothing, and skip it (the post-filter step rejects
        // unknown record_ids).
        let mut s = self.state.lock().await;
        let doc_id = mint_doc_id(&mut s.next_doc_seq);
        {
            let mut text = self.text_index.lock().expect("text index lock");
            let mut vec = self.vector_index.lock().expect("vector index lock");
            index_record_into(&mut text, &mut vec, &record, doc_id);
        }

        // Reserve the commit sequence and keep the state lock through durable
        // persistence and cache publication. Metadata snapshot backfill reads
        // its ledger high-watermark before taking this same lock; releasing it
        // between commit and publication could let a snapshot skip the record
        // and then tail from a cursor that already includes its create row.
        let commit_seq = next_record_commit_seq(&mut s);
        if let Some(p) = &self.persist {
            p.put_record_with_commit_seq_async(&record, Some(commit_seq))
                .await
                .map_err(internal)?;
        }
        s.records
            .insert(record.record_id.as_str().to_string(), record.clone());
        s.doc_to_record
            .insert(doc_id, record.record_id.as_str().to_string());
        s.record_to_doc
            .insert(record.record_id.as_str().to_string(), doc_id);
        s.record_commit_seq
            .insert(record.record_id.as_str().to_string(), commit_seq);
        if let Some(external_id) = record.external_id.as_deref() {
            s.record_external_ids.insert(
                record_external_key(&record.tenant_id, external_id),
                record.record_id.as_str().to_string(),
            );
        }
        s.local_record_index_watermark = s.local_record_index_watermark.max(commit_seq);
        if self.persist.is_none() {
            if let Err(error) = self
                .global_record_index
                .apply(global_index_mutation_for_record(
                    &record,
                    commit_seq,
                    GlobalIndexMutationOp::Upsert,
                ))
            {
                s.global_record_index_apply_failures_total += 1;
                drop(s);
                return Err(error);
            }
            s.global_record_index_watermark = s.global_record_index_watermark.max(commit_seq);
        }
        let commit_token = CommitToken {
            placement_id: None,
            scope: CommitScope::Tenant(record.tenant_id.clone()),
            seq: commit_seq,
        };
        Ok(WriteRecordResult {
            record,
            commit_token,
        })
    }

    /// Look up a record by primary key. Returns `RecordNotFound` if
    /// the record is unknown, soft-deleted, expired, or owned by a
    /// different tenant. The four cases collapse into a single error
    /// code (mirroring `get_artifact`) so a caller in tenant A cannot
    /// probe tenant B's record_ids by error-code differential.
    pub async fn get_record(&self, tenant_id: &Id, record_id: &Id) -> Result<SearchableRecord> {
        self.get_record_with_options(tenant_id, record_id, false)
            .await
    }

    pub async fn get_record_with_options(
        &self,
        tenant_id: &Id,
        record_id: &Id,
        include_deleted: bool,
    ) -> Result<SearchableRecord> {
        self.refresh_replicated_state().await?;
        self.ensure_tenant_cached(tenant_id).await?;
        self.ensure_record_cached(tenant_id, record_id).await?;
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        s.records
            .get(record_id.as_str())
            .filter(|r| {
                tenant_live
                    && (include_deleted || r.deleted_at_ms.is_none())
                    && r.tenant_id.as_str() == tenant_id.as_str()
                    && (include_deleted || !r.is_expired())
            })
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::RecordNotFound, format!("{}", record_id)))
    }

    pub async fn list_records(&self, tenant_id: &Id) -> Result<Vec<SearchableRecord>> {
        self.refresh_replicated_state().await?;
        self.get_tenant(tenant_id).await?;
        let mut records = self
            .authoritative_tenant_records(tenant_id)
            .await?
            .into_iter()
            .filter(|record| {
                record.tenant_id.as_str() == tenant_id.as_str()
                    && record.deleted_at_ms.is_none()
                    && !record.is_expired()
                    && record.status == RecordStatus::Active
            })
            .collect::<Vec<_>>();
        records.sort_by(|a, b| {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.record_id.as_str().cmp(b.record_id.as_str()))
        });
        Ok(records)
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub async fn simulate_record_index_lag_for_test(&self, record_id: &Id, watermark: u64) {
        let mut s = self.state.lock().await;
        if let Some(doc) = s.record_to_doc.remove(record_id.as_str()) {
            s.doc_to_record.remove(&doc);
        }
        s.local_record_index_watermark = watermark;
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub async fn simulate_record_state_gap_for_test(
        &self,
        record_id: &Id,
        local_watermark: u64,
        global_watermark: u64,
    ) {
        let mut s = self.state.lock().await;
        s.records.remove(record_id.as_str());
        s.record_commit_seq.remove(record_id.as_str());
        if let Some(doc) = s.record_to_doc.remove(record_id.as_str()) {
            s.doc_to_record.remove(&doc);
        }
        s.local_record_index_watermark = local_watermark;
        s.global_record_index_watermark = global_watermark;
    }

    /// Like [`simulate_record_state_gap_for_test`] but also purges the
    /// persisted record row, modelling the case where GC removed the
    /// record entirely while outbox mutations are still pending. Used by
    /// the regression test that pins the head-of-line-blocking fix for
    /// `apply_record_index_outbox_once`.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub async fn simulate_record_full_purge_for_test(
        &self,
        record_id: &Id,
        local_watermark: u64,
        global_watermark: u64,
    ) -> Result<()> {
        let snapshot = {
            let s = self.state.lock().await;
            s.records.get(record_id.as_str()).cloned()
        };
        self.simulate_record_state_gap_for_test(record_id, local_watermark, global_watermark)
            .await;
        if let (Some(record), Some(persist)) = (snapshot, self.persist.as_ref()) {
            persist
                .purge_record(&record, "test-simulated-purge", 0)
                .map_err(internal)?;
        }
        Ok(())
    }

    /// Overwrite the persisted record-index snapshot with undecodable bytes,
    /// for the corruption-fallback regression test.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn corrupt_record_index_snapshot_for_test(&self) -> Result<()> {
        let Some(p) = &self.persist else {
            return Ok(());
        };
        p.put_raw_record_index_snapshot_bytes(b"{not json")
            .map_err(internal)
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn record_index_outbox_counts_for_test(&self) -> Result<(usize, usize)> {
        let Some(p) = &self.persist else {
            return Ok((0, 0));
        };
        let mutations = p.scan_all_record_index_mutations().map_err(internal)?;
        let mut upserts = 0usize;
        let mut deletes = 0usize;
        for mutation in mutations {
            match mutation.op {
                RecordIndexMutationOp::Upsert => upserts += 1,
                RecordIndexMutationOp::Delete => deletes += 1,
            }
        }
        Ok((upserts, deletes))
    }

    pub async fn record_index_stats(&self) -> RecordIndexStats {
        let s = self.state.lock().await;
        let head = s.next_record_commit_seq;
        RecordIndexStats {
            local_index_watermark: s.local_record_index_watermark,
            global_index_watermark: s.global_record_index_watermark,
            next_record_commit_seq: head,
            local_index_lag_tokens: head.saturating_sub(s.local_record_index_watermark),
            global_index_lag_tokens: head.saturating_sub(s.global_record_index_watermark),
            local_index_repairs_total: s.record_index_repairs_total,
            local_index_repair_candidates_total: s.record_index_repair_candidates_total,
            record_index_worker_failures_total: s.record_index_worker_failures_total,
            global_index_apply_failures_total: s.global_record_index_apply_failures_total,
            record_index_outbox_skipped_total: s.record_index_outbox_skipped_total,
            snapshot_loaded_at_startup: s.record_index_snapshot_loaded,
            snapshot_last_watermark: s.last_record_index_snapshot_watermark,
            snapshot_writes_total: s.record_index_snapshot_writes_total,
        }
    }

    /// Persist a record-index snapshot if the local watermark has advanced
    /// at least `AGENTDB_RECORD_INDEX_SNAPSHOT_MIN_ADVANCE` (default 256)
    /// commits past the last snapshot — or unconditionally with `force`.
    /// Returns whether a snapshot was written. No-ops (Ok(false)) without a
    /// persist layer, on replicated runtimes, when snapshots are disabled,
    /// or when the global index backend cannot dump itself.
    pub async fn write_record_index_snapshot(&self, force: bool) -> Result<bool> {
        let Some(p) = &self.persist else {
            return Ok(false);
        };
        if self.tablet_router.is_some() || !record_index_snapshots_enabled() {
            return Ok(false);
        }
        let min_advance: u64 = std::env::var("AGENTDB_RECORD_INDEX_SNAPSHOT_MIN_ADVANCE")
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .unwrap_or(256);
        let snapshot = {
            let s = self.state.lock().await;
            if !force
                && s.local_record_index_watermark
                    .saturating_sub(s.last_record_index_snapshot_watermark)
                    < min_advance.max(1)
            {
                return Ok(false);
            }
            let Some(global) = self.global_record_index.snapshot() else {
                return Ok(false);
            };
            // Lock order matches the write paths: state, then text, then
            // vector — held only while cloning, so the capture is a
            // consistent point-in-time view.
            let text = self.text_index.lock().expect("text index lock");
            let vector = self.vector_index.lock().expect("vector index lock");
            crate::persist::RecordIndexSnapshot {
                format_version: crate::persist::RECORD_INDEX_SNAPSHOT_FORMAT_VERSION,
                watermark: s.local_record_index_watermark,
                global_watermark: s.global_record_index_watermark,
                next_doc_seq: s.next_doc_seq,
                next_record_commit_seq: s.next_record_commit_seq,
                text: text.to_snapshot(),
                vector: vector.to_snapshot(),
                global,
                doc_to_record: s
                    .doc_to_record
                    .iter()
                    .map(|(doc_id, record_id)| {
                        (doc_id.session_internal_id, doc_id.seq, record_id.clone())
                    })
                    .collect(),
                record_external_ids: s
                    .record_external_ids
                    .iter()
                    .map(|(key, record_id)| (key.clone(), record_id.clone()))
                    .collect(),
            }
        };
        let watermark = snapshot.watermark;
        p.put_record_index_snapshot(&snapshot).map_err(internal)?;
        let mut s = self.state.lock().await;
        s.last_record_index_snapshot_watermark =
            s.last_record_index_snapshot_watermark.max(watermark);
        s.record_index_snapshot_writes_total += 1;
        Ok(true)
    }

    /// Point-read a record by its caller-stable external id. Resolves
    /// through the external-id map (with the usual KV fallback via the
    /// upsert dedupe machinery's source of truth: the record row itself).
    pub async fn get_record_by_external_id(
        &self,
        tenant_id: &Id,
        external_id: &str,
    ) -> Result<Option<SearchableRecord>> {
        self.ensure_tenant_cached(tenant_id).await?;
        let mapped = {
            let s = self.state.lock().await;
            s.record_external_ids
                .get(&record_external_key(tenant_id, external_id))
                .cloned()
        };
        let Some(record_id_str) = mapped else {
            return Ok(None);
        };
        let Ok(record_id) = Id::parse_kind(&record_id_str, crate::id::IdKind::Record) else {
            return Ok(None);
        };
        self.ensure_record_cached(tenant_id, &record_id).await?;
        let s = self.state.lock().await;
        Ok(s.records
            .get(record_id.as_str())
            .filter(|r| {
                r.tenant_id.as_str() == tenant_id.as_str()
                    && r.deleted_at_ms.is_none()
                    && !r.is_expired()
            })
            .cloned())
    }

    pub async fn upsert_record_with_commit(&self, req: NewRecord) -> Result<WriteRecordResult> {
        self.refresh_replicated_state().await?;
        self.ensure_tenant_cached(&req.tenant_id).await?;
        // Resolve the external-id mapping first, then make sure the mapped
        // row is cached BEFORE applying the liveness filter — in lazy mode
        // an unresident row must not look like "no existing record" (that
        // would create a duplicate under the same external id).
        let candidate_id = if let Some(external_id) = req.external_id.as_deref() {
            let s = self.state.lock().await;
            s.record_external_ids
                .get(&record_external_key(&req.tenant_id, external_id))
                .cloned()
        } else {
            None
        };
        if let Some(candidate) = candidate_id.as_deref() {
            if let Ok(record_id) = Id::parse_kind(candidate, IdKind::Record) {
                self.ensure_record_cached(&req.tenant_id, &record_id)
                    .await?;
            }
        }
        let existing_id = if let Some(candidate) = candidate_id {
            let s = self.state.lock().await;
            Some(candidate).filter(|record_id| {
                s.records.get(record_id).is_some_and(|record| {
                    record.tenant_id.as_str() == req.tenant_id.as_str()
                        && record.deleted_at_ms.is_none()
                        && !record.is_expired()
                })
            })
        } else {
            None
        };
        let Some(existing_id) = existing_id else {
            return self.write_record_with_commit(req).await;
        };
        let record_id = Id::parse_kind(&existing_id, IdKind::Record)
            .map_err(|e| Error::new(ErrorCode::RecordNotFound, e.to_string()))?;
        self.update_record_with_commit(RecordUpdate {
            tenant_id: req.tenant_id,
            record_id,
            expected_version: None,
            namespace: Some(req.namespace),
            scope: Some(req.scope),
            text: Some(req.text),
            metadata: Some(req.metadata),
            tags: Some(req.tags),
            source: Some(req.source),
            embedding: Some(req.embedding),
            policy: Some(req.policy),
            status: Some(RecordStatus::Active),
            expires_at_ms: Some(req.expires_at_ms),
        })
        .await
    }

    pub async fn update_record_with_commit(
        &self,
        update: RecordUpdate,
    ) -> Result<WriteRecordResult> {
        self.refresh_replicated_state().await?;
        self.ensure_tenant_cached(&update.tenant_id).await?;
        self.ensure_record_cached(&update.tenant_id, &update.record_id)
            .await?;
        let mut s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(update.tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        if !tenant_live {
            return Err(Error::new(
                ErrorCode::TenantNotFound,
                format!("{}", update.tenant_id),
            ));
        }
        let current = s
            .records
            .get(update.record_id.as_str())
            .filter(|r| {
                r.tenant_id.as_str() == update.tenant_id.as_str()
                    && r.deleted_at_ms.is_none()
                    && !r.is_expired()
            })
            .cloned()
            .ok_or_else(|| {
                Error::new(ErrorCode::RecordNotFound, format!("{}", update.record_id))
            })?;
        if let Some(expected) = update.expected_version {
            if current.version != expected {
                return Err(Error::new(
                    ErrorCode::VersionConflict,
                    format!(
                        "record {} version mismatch: expected {}, got {}",
                        update.record_id, expected, current.version
                    ),
                ));
            }
        }

        let mut updated = current.clone();
        if let Some(namespace) = update.namespace {
            updated.namespace = namespace;
        }
        if let Some(scope) = update.scope {
            updated.scope = scope;
        }
        if let Some(text) = update.text {
            updated.text = text;
        }
        if let Some(metadata) = update.metadata {
            updated.metadata = metadata;
        }
        if let Some(tags) = update.tags {
            updated.tags = tags;
        }
        if let Some(source) = update.source {
            updated.source = source;
        }
        if let Some(embedding) = update.embedding {
            updated.embedding = embedding;
        }
        if let Some(policy) = update.policy {
            updated.policy = policy;
        }
        if let Some(status) = update.status {
            updated.status = status;
        }
        if let Some(expires_at_ms) = update.expires_at_ms {
            updated.expires_at_ms = expires_at_ms;
        }
        updated.version = updated
            .version
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorCode::Internal, "record version overflow"))?;
        updated.updated_at_ms = crate::now_ms();

        let commit_seq = next_record_commit_seq(&mut s);
        if let Some(p) = &self.persist {
            p.put_record_with_commit_seq(&updated, Some(commit_seq))
                .map_err(internal)?;
        } else if let Err(error) = self
            .global_record_index
            .apply(global_index_mutation_for_record(
                &updated,
                commit_seq,
                if updated.deleted_at_ms.is_none() {
                    GlobalIndexMutationOp::Upsert
                } else {
                    GlobalIndexMutationOp::Delete
                },
            ))
        {
            s.global_record_index_apply_failures_total += 1;
            return Err(error);
        } else {
            s.global_record_index_watermark = s.global_record_index_watermark.max(commit_seq);
        }

        if let Some(doc) = s.record_to_doc.remove(updated.record_id.as_str()) {
            s.doc_to_record.remove(&doc);
        }
        if updated.deleted_at_ms.is_none() && !updated.is_expired() {
            let doc_id = mint_doc_id(&mut s.next_doc_seq);
            {
                let mut text = self.text_index.lock().expect("text index lock");
                let mut vec = self.vector_index.lock().expect("vector index lock");
                index_record_into(&mut text, &mut vec, &updated, doc_id);
            }
            s.doc_to_record
                .insert(doc_id, updated.record_id.as_str().to_string());
            s.record_to_doc
                .insert(updated.record_id.as_str().to_string(), doc_id);
        }
        s.record_commit_seq
            .insert(updated.record_id.as_str().to_string(), commit_seq);
        s.local_record_index_watermark = s.local_record_index_watermark.max(commit_seq);
        s.records
            .insert(updated.record_id.as_str().to_string(), updated.clone());
        let commit_token = CommitToken {
            placement_id: None,
            scope: CommitScope::Tenant(updated.tenant_id.clone()),
            seq: commit_seq,
        };
        Ok(WriteRecordResult {
            record: updated,
            commit_token,
        })
    }

    pub async fn touch_record(&self, tenant_id: &Id, record_id: &Id) -> Result<SearchableRecord> {
        self.refresh_replicated_state().await?;
        let mut touched = self
            .touch_records_by_ids(tenant_id, &[record_id.as_str().to_string()])
            .await?;
        touched
            .pop()
            .ok_or_else(|| Error::new(ErrorCode::RecordNotFound, format!("{}", record_id)))
    }

    async fn touch_records_by_ids(
        &self,
        tenant_id: &Id,
        record_ids: &[String],
    ) -> Result<Vec<SearchableRecord>> {
        for record_id in record_ids {
            if let Ok(record_id) = Id::parse_kind(record_id, crate::id::IdKind::Record) {
                self.ensure_record_cached(tenant_id, &record_id).await?;
            }
        }
        let mut s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        if !tenant_live {
            return Err(Error::new(
                ErrorCode::TenantNotFound,
                format!("{}", tenant_id),
            ));
        }
        let mut out = Vec::with_capacity(record_ids.len());
        for record_id in record_ids {
            let record = s
                .records
                .get(record_id)
                .filter(|r| {
                    r.tenant_id.as_str() == tenant_id.as_str()
                        && r.deleted_at_ms.is_none()
                        && !r.is_expired()
                })
                .cloned()
                .ok_or_else(|| Error::new(ErrorCode::RecordNotFound, record_id.clone()))?;
            let mut updated = record;
            updated.access_count = updated
                .access_count
                .checked_add(1)
                .ok_or_else(|| Error::new(ErrorCode::Internal, "record access_count overflow"))?;
            updated.updated_at_ms = crate::now_ms();
            if let Some(p) = &self.persist {
                p.put_record_with_commit_seq(&updated, None)
                    .map_err(internal)?;
            }
            s.records.insert(record_id.clone(), updated.clone());
            out.push(updated);
        }
        Ok(out)
    }

    async fn maybe_touch_search_matches(
        &self,
        req: &SearchRequest,
        matches: Vec<RecordSearchMatch>,
    ) -> Result<Vec<RecordSearchMatch>> {
        if !req.touch || matches.is_empty() {
            return Ok(matches);
        }
        let ids = matches
            .iter()
            .map(|m| m.record.record_id.as_str().to_string())
            .collect::<Vec<_>>();
        let touched = self.touch_records_by_ids(&req.tenant_id, &ids).await?;
        let by_id = touched
            .into_iter()
            .map(|record| (record.record_id.as_str().to_string(), record))
            .collect::<BTreeMap<_, _>>();
        Ok(matches
            .into_iter()
            .map(|mut m| {
                if let Some(record) = by_id.get(m.record.record_id.as_str()) {
                    m.record = record.clone();
                }
                m
            })
            .collect())
    }

    pub async fn set_tenant_meta(
        &self,
        tenant_id: &Id,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<TenantMetaEntry> {
        self.refresh_replicated_state().await?;
        self.ensure_tenant_cached(tenant_id).await?;
        let key = key.into();
        let entry = {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", tenant_id),
                ));
            }
            TenantMetaEntry {
                tenant_id: tenant_id.clone(),
                key,
                value,
                updated_at_ms: crate::now_ms(),
            }
        };
        if let Some(p) = &self.persist {
            p.put_tenant_meta(&entry).map_err(internal)?;
        }
        let mut s = self.state.lock().await;
        s.tenant_meta
            .insert(tenant_meta_key(&entry.tenant_id, &entry.key), entry.clone());
        Ok(entry)
    }

    pub async fn get_tenant_meta(&self, tenant_id: &Id, key: &str) -> Result<TenantMetaEntry> {
        self.refresh_replicated_state().await?;
        self.ensure_tenant_cached(tenant_id).await?;
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        if !tenant_live {
            return Err(Error::new(
                ErrorCode::TenantNotFound,
                format!("{}", tenant_id),
            ));
        }
        s.tenant_meta
            .get(&tenant_meta_key(tenant_id, key))
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::RecordNotFound, key.to_string()))
    }

    pub async fn apply_record_index_outbox_once(&self) -> Result<usize> {
        let Some(p) = &self.persist else {
            return Ok(0);
        };
        let mut mutations = p.scan_all_record_index_mutations().map_err(internal)?;
        mutations.sort_by_key(|mutation| mutation.commit_seq);

        // Mutations at or below BOTH watermarks are fully reflected in the
        // local and global indexes — trim their outbox rows so the per-drain
        // scan stays proportional to new mutations, not to history. (The
        // outbox is a catch-up queue, not a rebuild source: full rebuilds
        // scan the record rows.)
        let floor = {
            let s = self.state.lock().await;
            s.local_record_index_watermark
                .min(s.global_record_index_watermark)
        };
        let (drained, pending): (Vec<_>, Vec<_>) = mutations
            .into_iter()
            .partition(|mutation| mutation.commit_seq <= floor);
        // Trimming is a KV WRITE: on a replicated runtime it must go through
        // the raft leader. Followers skip it silently — the leader trims the
        // same rows and the deletions replicate down. Any other trim failure
        // is logged but never blocks the drain (trim is an optimization; the
        // watermark floor already makes re-scanned rows cheap no-ops).
        if let Err(error) = p.delete_record_index_mutations(&drained) {
            let detail = format!("{error:?}");
            if detail.contains("not leader") {
                tracing::trace!(
                    drained = drained.len(),
                    "skipping record index outbox trim on non-leader"
                );
            } else {
                tracing::warn!(?error, "record index outbox trim failed");
            }
        }

        let mut applied = 0usize;
        for mutation in pending {
            let persisted_record = p
                .get_record(&mutation.tenant_id, &mutation.record_id)
                .map_err(internal)?;
            let mut s = self.state.lock().await;
            // Replicated applies land commit sequences this node never
            // minted; keep the local counter monotone so a newly elected
            // leader cannot reuse them.
            s.next_record_commit_seq = s.next_record_commit_seq.max(mutation.commit_seq);
            if matches!(mutation.op, RecordIndexMutationOp::Upsert)
                && !s.records.contains_key(&mutation.record_id)
            {
                let Some(record) = persisted_record.clone() else {
                    // Record is gone from both state.records and persist
                    // (typically purged by GC out from under the queue).
                    // Skip past this mutation rather than head-of-line-block
                    // every subsequent record's index visibility. Advance
                    // both watermarks past the broken commit_seq so we do
                    // not re-encounter it on every wake. Bump a counter so
                    // operators can detect the underlying divergence.
                    s.record_index_outbox_skipped_total += 1;
                    let skipped_seq = mutation.commit_seq;
                    s.local_record_index_watermark =
                        s.local_record_index_watermark.max(skipped_seq);
                    s.global_record_index_watermark =
                        s.global_record_index_watermark.max(skipped_seq);
                    continue;
                };
                s.record_commit_seq
                    .insert(mutation.record_id.clone(), mutation.commit_seq);
                s.records.insert(mutation.record_id.clone(), record);
            }
            let mut advanced = false;
            if mutation.commit_seq > s.global_record_index_watermark {
                let global_mutation = match mutation.op {
                    RecordIndexMutationOp::Upsert => match s.records.get(&mutation.record_id) {
                        Some(record) => Some(global_index_mutation_for_record(
                            record,
                            mutation.commit_seq,
                            GlobalIndexMutationOp::Upsert,
                        )),
                        None => {
                            // Defensive: hydration above either inserted the
                            // record or skipped+continued. Reaching None here
                            // means state diverged under the lock — treat as
                            // the same unreparable case rather than stall.
                            s.record_index_outbox_skipped_total += 1;
                            let skipped_seq = mutation.commit_seq;
                            s.local_record_index_watermark =
                                s.local_record_index_watermark.max(skipped_seq);
                            s.global_record_index_watermark =
                                s.global_record_index_watermark.max(skipped_seq);
                            continue;
                        }
                    },
                    RecordIndexMutationOp::Delete => Some(GlobalIndexMutation {
                        tenant_id: mutation.tenant_id.clone(),
                        record_id: mutation.record_id.clone(),
                        namespace: vec![],
                        placement_id: 0,
                        commit_seq: mutation.commit_seq,
                        op: GlobalIndexMutationOp::Delete,
                        terms: vec![],
                    }),
                };
                if let Some(global_mutation) = global_mutation {
                    if let Err(error) = self.global_record_index.apply(global_mutation) {
                        s.global_record_index_apply_failures_total += 1;
                        drop(s);
                        return Err(error);
                    }
                }
                s.global_record_index_watermark = mutation.commit_seq;
                advanced = true;
            }

            if mutation.commit_seq <= s.local_record_index_watermark {
                if advanced {
                    applied += 1;
                }
                continue;
            }

            match mutation.op {
                RecordIndexMutationOp::Upsert => {
                    let Some(record) = s.records.get(&mutation.record_id).cloned() else {
                        // Defensive: state diverged under the lock. Skip
                        // rather than stall, advance both watermarks so we
                        // do not retry, and signal via the counter.
                        s.record_index_outbox_skipped_total += 1;
                        let skipped_seq = mutation.commit_seq;
                        s.local_record_index_watermark =
                            s.local_record_index_watermark.max(skipped_seq);
                        s.global_record_index_watermark =
                            s.global_record_index_watermark.max(skipped_seq);
                        continue;
                    };
                    if record.deleted_at_ms.is_some() || record.is_expired() {
                        if let Some(doc) = s.record_to_doc.remove(&mutation.record_id) {
                            s.doc_to_record.remove(&doc);
                        }
                        s.local_record_index_watermark = mutation.commit_seq;
                        applied += 1;
                        continue;
                    }
                    if !s.record_to_doc.contains_key(&mutation.record_id) {
                        let doc_id = mint_doc_id(&mut s.next_doc_seq);
                        {
                            let mut text = self.text_index.lock().expect("text index lock");
                            let mut vec = self.vector_index.lock().expect("vector index lock");
                            index_record_into(&mut text, &mut vec, &record, doc_id);
                        }
                        s.doc_to_record.insert(doc_id, mutation.record_id.clone());
                        s.record_to_doc.insert(mutation.record_id.clone(), doc_id);
                    }
                    // Followers learn external-id claims only through this
                    // queue; the current row is authoritative.
                    if let Some(external_id) = record.external_id.as_deref() {
                        s.record_external_ids.insert(
                            record_external_key(&record.tenant_id, external_id),
                            mutation.record_id.clone(),
                        );
                    }
                }
                RecordIndexMutationOp::Delete => {
                    if let Some(doc) = s.record_to_doc.remove(&mutation.record_id) {
                        s.doc_to_record.remove(&doc);
                    }
                    // Release the external id, but only if the mapping still
                    // points at this record — a newer record may have
                    // reclaimed it since.
                    match persisted_record
                        .as_ref()
                        .and_then(|r| r.external_id.as_deref())
                    {
                        Some(external_id) => {
                            let key = record_external_key(
                                &persisted_record.as_ref().expect("checked above").tenant_id,
                                external_id,
                            );
                            if s.record_external_ids.get(&key) == Some(&mutation.record_id) {
                                s.record_external_ids.remove(&key);
                            }
                        }
                        None => {
                            // Row already purged: scrub any mapping still
                            // pointing at it (rare — purge follows retention).
                            s.record_external_ids
                                .retain(|_, mapped| mapped != &mutation.record_id);
                        }
                    }
                }
            }
            s.local_record_index_watermark = mutation.commit_seq;
            advanced = true;
            if advanced {
                applied += 1;
            }
        }
        Ok(applied)
    }

    pub fn spawn_record_index_worker(
        self: Arc<Self>,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        // Idempotency retention piggybacks on this worker: roughly every five
        // minutes of ticks, expire old records from the in-memory table and
        // delete their durable rows.
        let sweep_every = (std::time::Duration::from_secs(300).as_millis()
            / interval.as_millis().max(1))
        .max(1) as u64;
        let cache_sweep_ms: u128 = std::env::var("AGENTDB_METADATA_CACHE_SWEEP_MS")
            .ok()
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(60_000);
        let cache_sweep_every = (cache_sweep_ms / interval.as_millis().max(1)).max(1) as u64;
        tokio::spawn(async move {
            let mut ticks: u64 = 0;
            loop {
                if let Err(error) = self.apply_record_index_outbox_once().await {
                    let mut s = self.state.lock().await;
                    s.record_index_worker_failures_total += 1;
                    drop(s);
                    tracing::warn!(?error, "record index outbox replay failed");
                }
                if let Err(error) = self.write_record_index_snapshot(false).await {
                    let mut s = self.state.lock().await;
                    s.record_index_worker_failures_total += 1;
                    drop(s);
                    tracing::warn!(?error, "record index snapshot write failed");
                }
                ticks += 1;
                if ticks.is_multiple_of(sweep_every) {
                    self.sweep_idempotency_retention();
                }
                if ticks.is_multiple_of(cache_sweep_every) {
                    self.sweep_metadata_cache().await;
                }
                tokio::time::sleep(interval).await;
            }
        })
    }

    /// Expire idempotency records past the retention TTL (and beyond the
    /// entry cap) from the in-memory table, and delete the matching durable
    /// rows so restarts do not resurrect them.
    pub fn sweep_idempotency_retention(&self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let removed = self.idem.evict(
            now_ms,
            crate::idempotency::idempotency_ttl_ms(),
            crate::idempotency::idempotency_max_entries(),
        );
        if removed.is_empty() {
            return;
        }
        if let Some(p) = &self.persist {
            let mut delete_errors = 0usize;
            for key in &removed {
                if p.delete_idempotency(key).is_err() {
                    delete_errors += 1;
                }
            }
            tracing::info!(
                expired = removed.len(),
                delete_errors,
                "idempotency retention sweep removed expired records"
            );
        }
    }

    /// Scoped, text-only search over the durable event log. Resolves the scope
    /// sessions (one session, one agent's sessions, or the whole tenant),
    /// scans their events newest-session-first up to a bounded budget, and
    /// ranks matches by how many query tokens they contain, then recency.
    ///
    /// This is an unindexed scan by design — the event log is the durable
    /// substrate, and a narrow scope (session/agent) keeps it cheap. The scan
    /// is bounded by [`EVENT_SEARCH_MAX_SCAN`]; a tenant-wide search over a very
    /// large history is best-effort over the newest events within that budget.
    pub async fn search_events(&self, req: EventSearchRequest) -> Result<Vec<EventSearchMatch>> {
        self.ensure_tenant_cached(&req.tenant_id).await?;
        if req.limit == 0 {
            return Ok(Vec::new());
        }
        let tokens: Vec<String> = req
            .query
            .to_lowercase()
            .split_whitespace()
            .map(str::to_string)
            .collect();
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        // Resolve the sessions in scope, newest first, so a bounded scan spends
        // its budget on the most recent history.
        let mut sessions: Vec<Session> = if let Some(session_id) = req.session_id.as_ref() {
            self.ensure_session_cached(&req.tenant_id, session_id).await?;
            let session = self.get_session(&req.tenant_id, session_id).await?;
            vec![session]
        } else {
            if let Some(agent_id) = req.agent_id.as_ref() {
                // 404 an unknown agent rather than silently returning nothing.
                self.get_agent(&req.tenant_id, agent_id).await?;
            }
            self.authoritative_tenant_sessions(&req.tenant_id)
                .await?
                .into_iter()
                .filter(|session| {
                    session.deleted_at_ms.is_none()
                        && session.tenant_id.as_str() == req.tenant_id.as_str()
                        && req
                            .agent_id
                            .as_ref()
                            .is_none_or(|agent_id| session.agent_id.as_str() == agent_id.as_str())
                })
                .collect()
        };
        sessions.sort_by_key(|session| std::cmp::Reverse(session.created_at_ms));

        let mut matches: Vec<EventSearchMatch> = Vec::new();
        let mut scanned = 0usize;
        'sessions: for session in sessions {
            let events = self
                .fetch_from_seq(FetchRequest {
                    tenant_id: req.tenant_id.clone(),
                    session_id: session.session_id.clone(),
                    from_seq: 0,
                    limit: usize::MAX,
                })
                .await?;
            for event in events {
                if scanned >= EVENT_SEARCH_MAX_SCAN {
                    break 'sessions;
                }
                scanned += 1;
                if event.deleted_at_ms.is_some() {
                    continue;
                }
                if req.kind.as_ref().is_some_and(|kind| &event.kind != kind) {
                    continue;
                }
                let haystack = event_search_haystack(&event);
                let score = tokens
                    .iter()
                    .filter(|token| haystack.contains(token.as_str()))
                    .count() as u32;
                if score == 0 {
                    continue;
                }
                matches.push(EventSearchMatch { event, score });
            }
        }

        // Rank by token coverage, then recency, then seq for a stable order.
        matches.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then(right.event.created_at_ms.cmp(&left.event.created_at_ms))
                .then(right.event.seq.cmp(&left.event.seq))
        });
        matches.truncate(req.limit);
        Ok(matches)
    }

    /// Search the (tenant, namespace) scope and return records only. Call
    /// [`Runtime::search_record_matches`] when ranking/debug metadata is needed.
    pub async fn search_records(&self, req: SearchRequest) -> Result<Vec<SearchableRecord>> {
        self.ensure_tenant_cached(&req.tenant_id).await?;
        Ok(self
            .search_record_matches(req)
            .await?
            .into_iter()
            .map(|m| m.record)
            .collect())
    }

    /// Search the (tenant, namespace) scope. If `text` is set, run a
    /// BM25 text search; if `embedding` is set, run a cosine
    /// nearest-neighbor search; if both, run both and union by record
    /// (keeping the higher score per record_id). If neither is set,
    /// return every live record in the namespace, capped at `limit`.
    ///
    /// Tag, source, and metadata post-filters are applied after the index step.
    /// Soft-deleted and expired records are filtered out. Scores are query-local
    /// ranking aids; callers should not treat them as stable durable values.
    pub async fn search_record_matches(
        &self,
        req: SearchRequest,
    ) -> Result<Vec<RecordSearchMatch>> {
        self.refresh_replicated_state().await?;
        // Public funnel: the API calls this directly (not search_records),
        // so the tenant row must be pulled through the KV fallback here —
        // both branches below check the cached map. Found by the S18
        // SIGKILL harness: a cold lazy restart 404'd the first search.
        self.ensure_tenant_cached(&req.tenant_id).await?;
        if req.sharding_key.is_none() {
            return self.search_record_matches_global(req).await;
        }
        self.search_record_matches_local(req).await
    }

    async fn search_record_matches_global(
        &self,
        req: SearchRequest,
    ) -> Result<Vec<RecordSearchMatch>> {
        {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
        }
        let terms = req.text.as_deref().map(repair_tokens).unwrap_or_default();
        let index_limit = search_index_candidate_limit(&req);
        let result = self.global_record_index.search(GlobalIndexQuery {
            tenant_id: req.tenant_id.as_str().to_string(),
            namespace: req.namespace.clone(),
            terms,
            min_visible_commit_seq: req.min_visible_record_commit_seq,
            limit: index_limit,
        })?;

        if result.stale {
            return self.search_record_matches_local(req).await;
        }

        // Resolve global-index hits whose rows are not cached (lazy mode).
        let missing_hit_ids: Vec<Id> = {
            let s = self.state.lock().await;
            result
                .hits
                .iter()
                .filter(|hit| !s.records.contains_key(hit.record_id.as_str()))
                .filter_map(|hit| Id::parse_kind(&hit.record_id, crate::id::IdKind::Record).ok())
                .collect()
        };
        for record_id in &missing_hit_ids {
            self.ensure_record_cached(&req.tenant_id, record_id).await?;
        }
        let s = self.state.lock().await;
        let mut candidates = Vec::with_capacity(result.hits.len());
        for hit in result.hits {
            let Some(record) = s.records.get(&hit.record_id).cloned() else {
                continue;
            };
            if !record_matches_repair_query(&record, &req) {
                continue;
            }
            candidates.push((record.clone(), repair_score(&record, &req)));
        }
        drop(s);
        candidates.sort_by(|(_, a), (_, b)| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let matches = filter_search_candidates(candidates, &req);
        self.maybe_touch_search_matches(&req, matches).await
    }

    async fn search_record_matches_local(
        &self,
        req: SearchRequest,
    ) -> Result<Vec<RecordSearchMatch>> {
        // Validate tenant exists. Tenant scope leaks would defeat the
        // (tenant, namespace) isolation the index `path` already
        // provides, so reject upfront.
        {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
        }

        let path = format!("{}::{}", req.tenant_id.as_str(), req.namespace.join("/"));

        // Allow some slack so post-filtering by tags/source can still
        // produce `limit` matches even when a few top hits get filtered
        // out. Capped to avoid a denial-of-service vector via huge
        // search limits.
        let slack = search_index_candidate_limit(&req);

        // Collect index hits as (doc_id, score). When neither text
        // nor embedding is supplied we skip the indexes and return
        // every live record in the namespace (subject to filters and
        // limit) by walking the in-memory record cache directly.
        let mut hits: BTreeMap<DocId, CandidateScore> = BTreeMap::new();
        if let Some(query) = req.text.as_deref() {
            let text = self.text_index.lock().expect("text index lock");
            for (doc, score) in text.search(&path, query, slack) {
                hits.entry(doc)
                    .and_modify(|s| s.merge_text(score))
                    .or_insert_with(|| CandidateScore::with_text(score));
            }
        }
        if let Some(emb) = req.embedding.as_deref() {
            let vec = self.vector_index.lock().expect("vector index lock");
            for (doc, score) in vec.knn(&path, emb, slack) {
                hits.entry(doc)
                    .and_modify(|s| s.merge_vector(score))
                    .or_insert_with(|| CandidateScore::with_vector(score));
            }
        }

        // Lazy-mode row resolution: the search indexes are complete, but the
        // record rows may not be resident. Brute-force and repair paths walk
        // rows by tenant, so pre-warm the tenant's rows for those; the
        // index-driven path only needs the matched ids, resolved below.
        if self.hydration_mode == HydrationMode::Lazy
            && ((req.text.is_none() && req.embedding.is_none())
                || req.min_visible_record_commit_seq.is_some())
        {
            let scanned = self.authoritative_tenant_records(&req.tenant_id).await?;
            let mut s = self.state.lock().await;
            for row in scanned {
                s.records
                    .entry(row.record_id.as_str().to_string())
                    .or_insert(row);
            }
        }
        // Resolve index hits whose rows are not cached (targeted point
        // reads; a no-op when everything is resident).
        let missing_hit_ids: Vec<Id> = {
            let s = self.state.lock().await;
            hits.keys()
                .filter_map(|doc| s.doc_to_record.get(doc))
                .filter(|rid| !s.records.contains_key(rid.as_str()))
                .filter_map(|rid| Id::parse_kind(rid, crate::id::IdKind::Record).ok())
                .collect()
        };
        for record_id in &missing_hit_ids {
            self.ensure_record_cached(&req.tenant_id, record_id).await?;
        }

        let mut s = self.state.lock().await;
        let repair_range = req
            .min_visible_record_commit_seq
            .filter(|required| *required > s.local_record_index_watermark)
            .map(|required| (s.local_record_index_watermark, required));

        // Build the candidate list. Index-driven path: walk hits in
        // descending-score order and resolve each DocId to a record.
        // Brute-force path: walk every record in the namespace. We
        // distinguish the two by whether `text` or `embedding` was set.
        let mut candidates: Vec<(SearchableRecord, CandidateScore)> = if req.text.is_some()
            || req.embedding.is_some()
        {
            let mut ranked: Vec<(DocId, CandidateScore)> = hits.into_iter().collect();
            ranked.sort_by(|(_, a), (_, b)| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            ranked
                .into_iter()
                .filter_map(|(doc, score)| {
                    let rid = s.doc_to_record.get(&doc)?;
                    let r = s.records.get(rid)?;
                    Some((r.clone(), score))
                })
                .collect()
        } else {
            s.records
                .values()
                .filter(|r| {
                    r.tenant_id.as_str() == req.tenant_id.as_str() && r.namespace == req.namespace
                })
                .map(|r| {
                    (
                        r.clone(),
                        CandidateScore {
                            score: 1.0,
                            ..CandidateScore::default()
                        },
                    )
                })
                .collect()
        };
        if let Some((applied, required)) = repair_range {
            s.record_index_repairs_total += 1;
            let mut repaired_candidates = 0u64;
            let mut seen: BTreeSet<String> = candidates
                .iter()
                .map(|(r, _)| r.record_id.as_str().to_string())
                .collect();
            for r in s.records.values() {
                let Some(commit_seq) = s.record_commit_seq.get(r.record_id.as_str()) else {
                    continue;
                };
                if *commit_seq <= applied || *commit_seq > required {
                    continue;
                }
                if !record_matches_repair_query(r, &req) {
                    continue;
                }
                if seen.insert(r.record_id.as_str().to_string()) {
                    candidates.push((r.clone(), repair_score(r, &req)));
                    repaired_candidates += 1;
                }
            }
            s.record_index_repair_candidates_total += repaired_candidates;
            candidates.sort_by(|(_, a), (_, b)| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        drop(s);

        let matches = filter_search_candidates(candidates, &req);
        self.maybe_touch_search_matches(&req, matches).await
    }

    /// Soft-delete a record: set `deleted_at_ms = now`, persist the
    /// updated row, and remove the entry from the text + vector
    /// indexes (and the DocId mappings). Returns `RecordNotFound` if
    /// the record is unknown or owned by a different tenant.
    ///
    /// Note that V2.2 does NOT support text-index removal cheaply
    /// (the BM25 backend's `insert` is one-way). We "remove" by
    /// dropping the DocId mapping; future searches still touch the
    /// stale postings entry but the unresolvable DocId gets filtered
    /// out at the resolve step. V2.4 will replace this with a proper
    /// tombstoning pass.
    pub async fn delete_record(&self, tenant_id: &Id, record_id: &Id) -> Result<()> {
        self.refresh_replicated_state().await?;
        self.ensure_tenant_cached(tenant_id).await?;
        self.ensure_record_cached(tenant_id, record_id).await?;
        let mut s = self.state.lock().await;
        let live = s.records.get(record_id.as_str()).is_some_and(|r| {
            r.deleted_at_ms.is_none() && r.tenant_id.as_str() == tenant_id.as_str()
        });
        if !live {
            return Err(Error::new(
                ErrorCode::RecordNotFound,
                format!("{}", record_id),
            ));
        }

        // Mutate the record in place so the persisted shape matches
        // the in-memory shape after the persist step.
        let updated = {
            let r = s
                .records
                .get_mut(record_id.as_str())
                .expect("presence checked above");
            r.deleted_at_ms = Some(crate::now_ms());
            r.clone()
        };

        if let Some(p) = &self.persist {
            let commit_seq = next_record_commit_seq(&mut s);
            if let Err(e) = p.put_record_with_commit_seq(&updated, Some(commit_seq)) {
                // Roll back the soft-delete in the cache so the
                // in-memory state stays consistent with disk.
                if let Some(r) = s.records.get_mut(record_id.as_str()) {
                    r.deleted_at_ms = None;
                }
                drop(s);
                return Err(internal(e));
            }
            s.record_commit_seq
                .insert(record_id.as_str().to_string(), commit_seq);
            s.local_record_index_watermark = commit_seq;
        } else {
            let commit_seq = next_record_commit_seq(&mut s);
            s.record_commit_seq
                .insert(record_id.as_str().to_string(), commit_seq);
            s.local_record_index_watermark = commit_seq;
            if let Err(error) = self.global_record_index.apply(GlobalIndexMutation {
                tenant_id: tenant_id.as_str().to_string(),
                record_id: record_id.as_str().to_string(),
                namespace: vec![],
                placement_id: 0,
                commit_seq,
                op: GlobalIndexMutationOp::Delete,
                terms: vec![],
            }) {
                s.global_record_index_apply_failures_total += 1;
                drop(s);
                return Err(error);
            }
            s.global_record_index_watermark = commit_seq;
        }

        if let Some(doc) = s.record_to_doc.remove(record_id.as_str()) {
            s.doc_to_record.remove(&doc);
        }
        if let Some(external_id) = updated.external_id.as_deref() {
            s.record_external_ids
                .remove(&record_external_key(&updated.tenant_id, external_id));
        }
        Ok(())
    }
}

fn usage_percent(usage: u64, soft_limit: u64) -> Option<u64> {
    if soft_limit == 0 {
        return (usage > 0).then_some(100);
    }
    if usage < soft_limit {
        return None;
    }
    Some(usage.saturating_mul(100).saturating_div(soft_limit))
}

fn first_deletion_ms(now_ms: u64, candidates: &[Option<u64>]) -> u64 {
    candidates
        .iter()
        .copied()
        .flatten()
        .next()
        .unwrap_or(now_ms)
}

fn gc_tombstone_reason(
    object_deleted_at: Option<u64>,
    tenant_deleted_at: Option<u64>,
) -> &'static str {
    if object_deleted_at.is_none() && tenant_deleted_at.is_some() {
        "tenant_purge"
    } else {
        "retention_gc"
    }
}

fn resolve_policy_for_agent_in_state(
    s: &State,
    tenant_id: &Id,
    agent_id: &Id,
) -> Result<RuntimePolicy> {
    let tenant = s
        .tenants
        .get(tenant_id.as_str())
        .filter(|t| t.deleted_at_ms.is_none())
        .ok_or_else(|| Error::new(ErrorCode::TenantNotFound, format!("{}", tenant_id)))?;
    let agent = s
        .agents
        .get(agent_id.as_str())
        .filter(|a| a.deleted_at_ms.is_none() && a.tenant_id.as_str() == tenant_id.as_str())
        .ok_or_else(|| Error::new(ErrorCode::AgentNotFound, format!("{}", agent_id)))?;
    let base = RuntimePolicy::from_json(&tenant.policy);
    let override_json = agent
        .config
        .get("policy")
        .unwrap_or(&serde_json::Value::Null);
    Ok(base.merge_override(override_json))
}

/// Aggregates for the in-lock hot path: the fresh cache entry when present
/// (kept warm by the async callers), else map-derived counts — exact in full
/// hydration, a lower bound in lazy mode (advisory reporting only; quota
/// enforcement always goes through `tenant_aggregates_cached` with ZERO age).
fn tenant_aggregates_in_state(s: &State, tenant_id: &Id) -> TenantAggregates {
    if let Some((agg, at)) = s.tenant_aggregate_cache.get(tenant_id.as_str()) {
        if at.elapsed() <= std::time::Duration::from_secs(2) {
            return *agg;
        }
    }
    TenantAggregates {
        active_sessions: s
            .sessions
            .values()
            .filter(|sess| {
                sess.tenant_id.as_str() == tenant_id.as_str()
                    && sess.deleted_at_ms.is_none()
                    && sess.status == crate::session::Status::Active
            })
            .count(),
        live_agents: s
            .agents
            .values()
            .filter(|a| a.tenant_id.as_str() == tenant_id.as_str() && a.deleted_at_ms.is_none())
            .count(),
        live_artifacts: s
            .artifacts
            .values()
            .filter(|a| a.tenant_id.as_str() == tenant_id.as_str() && a.deleted_at_ms.is_none())
            .count(),
        live_artifact_bytes: s
            .artifacts
            .values()
            .filter(|a| a.tenant_id.as_str() == tenant_id.as_str() && a.deleted_at_ms.is_none())
            .map(|a| a.bytes)
            .sum(),
        live_records: s
            .records
            .values()
            .filter(|r| {
                r.tenant_id.as_str() == tenant_id.as_str()
                    && r.deleted_at_ms.is_none()
                    && !r.is_expired()
            })
            .count(),
    }
}

fn backpressure_for_tenant_session_in_state(
    s: &State,
    policy: &RuntimePolicy,
    tenant_id: &Id,
    session: &Session,
) -> BackpressureReport {
    let agg = tenant_aggregates_in_state(s, tenant_id);
    let mut report = BackpressureReport::default();

    if let Some(soft) = policy.quota.soft_max_active_sessions {
        report.push_if_over("active_sessions", agg.active_sessions as u64, soft as u64);
    }
    if let Some(soft) = policy.quota.soft_max_agents {
        report.push_if_over("agents", agg.live_agents as u64, soft as u64);
    }
    if let Some(soft) = policy.quota.soft_max_records {
        report.push_if_over("records", agg.live_records as u64, soft as u64);
    }
    if let Some(soft) = policy.quota.soft_max_artifacts {
        report.push_if_over("artifacts", agg.live_artifacts as u64, soft as u64);
    }
    if let Some(soft) = policy.quota.soft_max_artifact_storage_bytes {
        report.push_if_over("artifact_storage", agg.live_artifact_bytes, soft);
    }
    if let Some(soft) = policy.quota.soft_max_events_per_session {
        report.push_if_over("session_events", session.next_seq, soft);
    }

    report
}

fn recent_for_appended_event_in_state(
    s: &State,
    session_id: &str,
    appended_seq: u64,
    last: usize,
) -> (Vec<EventEnvelope>, bool) {
    let last_seq_count = u64::try_from(last).unwrap_or(u64::MAX);
    let from_seq = appended_seq
        .saturating_add(1)
        .saturating_sub(last_seq_count);
    let lo = (session_id.to_string(), from_seq);
    let hi = (session_id.to_string(), appended_seq);
    let mut recent = s
        .events
        .range(lo..=hi)
        .take(last)
        .map(|(_, event)| event.clone())
        .collect::<Vec<_>>();
    if recent.len() > last {
        let drop_n = recent.len() - last;
        recent.drain(0..drop_n);
    }
    let expected = usize::try_from(appended_seq.saturating_add(1).saturating_sub(from_seq))
        .unwrap_or(usize::MAX)
        .min(last);
    let complete = recent.len() >= expected;
    (recent, complete)
}

fn trim_recent_to_budget(recent: &mut Vec<EventEnvelope>, max_bytes: usize) {
    if max_bytes == 0 {
        recent.clear();
        return;
    }
    let upper_bound = recent.iter().try_fold(0usize, |used, event| {
        used.checked_add(event_json_size_upper_bound(event))
    });
    if upper_bound.is_some_and(|bytes| bytes <= max_bytes) {
        return;
    }

    let mut used = 0usize;
    let mut keep_from = recent.len();
    for (idx, event) in recent.iter().enumerate().rev() {
        let event_bytes = serde_json::to_vec(event)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if used.saturating_add(event_bytes) > max_bytes {
            break;
        }
        used += event_bytes;
        keep_from = idx;
    }
    if keep_from > 0 {
        recent.drain(0..keep_from);
    }
}

fn event_json_size_upper_bound(event: &EventEnvelope) -> usize {
    // This intentionally overestimates. It is only used to prove that a recent
    // context is under budget and can skip the exact serialization pass.
    let mut total = 2usize; // object braces
    total = add_json_field_upper_bound(
        total,
        "tenant_id",
        json_str_upper_bound(event.tenant_id.as_str()),
    );
    total = add_json_field_upper_bound(
        total,
        "agent_id",
        json_str_upper_bound(event.agent_id.as_str()),
    );
    total = add_json_field_upper_bound(
        total,
        "session_id",
        json_str_upper_bound(event.session_id.as_str()),
    );
    total = add_json_field_upper_bound(
        total,
        "event_id",
        json_str_upper_bound(event.event_id.as_str()),
    );
    total = add_json_field_upper_bound(total, "seq", 20);
    total = add_json_field_upper_bound(total, "created_at_ms", 20);
    total = add_json_field_upper_bound(total, "kind", json_str_upper_bound(event.kind.as_str()));
    total = add_json_field_upper_bound(
        total,
        "runtime_version",
        json_str_upper_bound(event.runtime_version.as_str()),
    );
    total = add_json_field_upper_bound(total, "payload", json_value_upper_bound(&event.payload));
    total = add_json_field_upper_bound(
        total,
        "artifact_refs",
        json_array_upper_bound(
            event
                .artifact_refs
                .iter()
                .map(artifact_ref_json_upper_bound),
        ),
    );
    total = add_json_field_upper_bound(
        total,
        "source_refs",
        json_array_upper_bound(event.source_refs.iter().map(source_ref_json_upper_bound)),
    );
    if let Some(idempotency_key) = event.idempotency_key.as_deref() {
        total = add_json_field_upper_bound(
            total,
            "idempotency_key",
            json_str_upper_bound(idempotency_key),
        );
    }
    total = add_json_field_upper_bound(total, "policy_version", 20);
    if let Some(key_id) = event.metadata_key_id.as_deref() {
        total = add_json_field_upper_bound(total, "metadata_key_id", json_str_upper_bound(key_id));
    }
    if let Some(key_version) = event.metadata_key_version.as_deref() {
        total = add_json_field_upper_bound(
            total,
            "metadata_key_version",
            json_str_upper_bound(key_version),
        );
    }
    total = add_json_field_upper_bound(
        total,
        "deleted_at_ms",
        event.deleted_at_ms.map(|_| 20).unwrap_or(4),
    );
    total
}

fn add_json_field_upper_bound(current: usize, field: &str, value_size: usize) -> usize {
    current
        .saturating_add(1) // comma or harmless overestimate for first field
        .saturating_add(json_str_upper_bound(field))
        .saturating_add(1) // colon
        .saturating_add(value_size)
}

fn artifact_ref_json_upper_bound(reference: &ArtifactRef) -> usize {
    let mut total = 2usize;
    total = add_json_field_upper_bound(
        total,
        "artifact_id",
        json_str_upper_bound(reference.artifact_id.as_str()),
    );
    if let Some(content_type) = reference.content_type.as_deref() {
        total =
            add_json_field_upper_bound(total, "content_type", json_str_upper_bound(content_type));
    }
    if reference.bytes.is_some() {
        total = add_json_field_upper_bound(total, "bytes", 20);
    }
    if let Some(sha256) = reference.sha256.as_deref() {
        total = add_json_field_upper_bound(total, "sha256", json_str_upper_bound(sha256));
    }
    total
}

fn source_ref_json_upper_bound(reference: &SourceRef) -> usize {
    let mut total = 2usize;
    if let Some(session_id) = reference.session_id.as_ref() {
        total = add_json_field_upper_bound(
            total,
            "session_id",
            json_str_upper_bound(session_id.as_str()),
        );
    }
    if reference.seq.is_some() {
        total = add_json_field_upper_bound(total, "seq", 20);
    }
    if let Some(event_id) = reference.event_id.as_ref() {
        total =
            add_json_field_upper_bound(total, "event_id", json_str_upper_bound(event_id.as_str()));
    }
    if let Some(artifact_id) = reference.artifact_id.as_ref() {
        total = add_json_field_upper_bound(
            total,
            "artifact_id",
            json_str_upper_bound(artifact_id.as_str()),
        );
    }
    if let Some(record_id) = reference.record_id.as_ref() {
        total = add_json_field_upper_bound(
            total,
            "record_id",
            json_str_upper_bound(record_id.as_str()),
        );
    }
    total
}

fn json_array_upper_bound(items: impl IntoIterator<Item = usize>) -> usize {
    let mut total = 2usize;
    for item in items {
        total = total.saturating_add(1).saturating_add(item);
    }
    total
}

fn json_value_upper_bound(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(value) => {
            if *value {
                4
            } else {
                5
            }
        }
        serde_json::Value::Number(number) => number.to_string().len(),
        serde_json::Value::String(value) => json_str_upper_bound(value),
        serde_json::Value::Array(values) => {
            json_array_upper_bound(values.iter().map(json_value_upper_bound))
        }
        serde_json::Value::Object(values) => {
            let mut total = 2usize;
            for (key, value) in values {
                total = total
                    .saturating_add(1)
                    .saturating_add(json_str_upper_bound(key))
                    .saturating_add(1)
                    .saturating_add(json_value_upper_bound(value));
            }
            total
        }
    }
}

fn json_str_upper_bound(value: &str) -> usize {
    value.len().saturating_mul(6).saturating_add(2)
}

fn hot_cache_covers_from(events: &[EventEnvelope], from_seq: u64, expected: usize) -> bool {
    expected == 0
        || (events.len() >= expected && events.first().is_some_and(|event| event.seq == from_seq))
}

fn push_budgeted<T: Serialize>(used: &mut usize, max_bytes: usize, value: &T) -> bool {
    let bytes = serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if used.saturating_add(bytes) > max_bytes {
        return false;
    }
    *used += bytes;
    true
}

/// Lowercased searchable text for an event: its `kind` plus its JSON payload
/// rendered as text. Substring token matching over this powers `search_events`.
fn event_search_haystack(event: &EventEnvelope) -> String {
    let mut haystack = event.kind.to_lowercase();
    haystack.push(' ');
    haystack.push_str(&event.payload.to_string().to_lowercase());
    haystack
}

fn validate_export_include(include: &[String]) -> Result<()> {
    for item in include {
        if !matches!(
            item.as_str(),
            "events"
                | "event"
                | "tenants"
                | "tenant"
                | "agents"
                | "agent"
                | "sessions"
                | "session"
                | "artifacts"
                | "artifact"
                | "records"
                | "record"
                | "fork_edges"
                | "fork_edge"
        ) {
            return Err(Error::new(
                ErrorCode::PolicyDenied,
                format!("unsupported export include: {item}"),
            ));
        }
    }
    Ok(())
}

fn validate_tenant_live_in_state(state: &State, tenant_id: &Id) -> Result<()> {
    let live = state
        .tenants
        .get(tenant_id.as_str())
        .is_some_and(|tenant| tenant.deleted_at_ms.is_none());
    if live {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::TenantNotFound,
            format!("{tenant_id}"),
        ))
    }
}

fn validate_export_consumer_scope_in_state(
    state: &State,
    tenant_id: &Id,
    scope: WatchScope,
    agent_id: Option<&Id>,
    session_id: Option<&Id>,
) -> Result<()> {
    validate_tenant_live_in_state(state, tenant_id)?;
    match scope {
        WatchScope::Tenant => Ok(()),
        WatchScope::Agent => {
            let Some(agent_id) = agent_id else {
                return Err(Error::new(
                    ErrorCode::AgentNotFound,
                    "export consumer agent scope requires agent_id",
                ));
            };
            let live = state.agents.get(agent_id.as_str()).is_some_and(|agent| {
                agent.deleted_at_ms.is_none() && agent.tenant_id.as_str() == tenant_id.as_str()
            });
            if live {
                Ok(())
            } else {
                Err(Error::new(ErrorCode::AgentNotFound, format!("{agent_id}")))
            }
        }
        WatchScope::Session => {
            let Some(session_id) = session_id else {
                return Err(Error::new(
                    ErrorCode::SessionNotFound,
                    "export consumer session scope requires session_id",
                ));
            };
            let live = state
                .sessions
                .get(session_id.as_str())
                .is_some_and(|session| {
                    session.deleted_at_ms.is_none()
                        && session.tenant_id.as_str() == tenant_id.as_str()
                });
            if live {
                Ok(())
            } else {
                Err(Error::new(
                    ErrorCode::SessionNotFound,
                    format!("{session_id}"),
                ))
            }
        }
    }
}

fn export_ledger_item_from_event(event: EventEnvelope) -> ExportLedgerItem {
    ExportLedgerItem {
        op: "insert".into(),
        object_type: "event".into(),
        tenant_id: event.tenant_id.clone(),
        agent_id: Some(event.agent_id.clone()),
        session_id: Some(event.session_id.clone()),
        object_id: event.event_id.as_str().to_string(),
        seq: Some(event.seq),
        created_at_ms: event.created_at_ms,
        data: serde_json::to_value(&event).expect("event serializes"),
        tombstone: None,
    }
}

fn export_ledger_item_from_fork_edge(
    tenant_id: &Id,
    agent_id: Option<Id>,
    edge: ForkEdge,
) -> ExportLedgerItem {
    ExportLedgerItem {
        op: "insert".into(),
        object_type: "fork_edge".into(),
        tenant_id: tenant_id.clone(),
        agent_id,
        session_id: Some(edge.parent_session_id.clone()),
        object_id: edge.child_session_id.as_str().to_string(),
        seq: None,
        created_at_ms: edge.created_at_ms,
        data: serde_json::to_value(&edge).expect("fork edge serializes"),
        tombstone: None,
    }
}

fn export_item_matches_scope(
    item: &ExportLedgerItem,
    scope: WatchScope,
    agent_id: Option<&Id>,
    session_id: Option<&Id>,
) -> bool {
    match scope {
        WatchScope::Tenant => true,
        WatchScope::Agent => agent_id.is_some_and(|agent_id| {
            item.agent_id
                .as_ref()
                .is_some_and(|item_agent_id| item_agent_id.as_str() == agent_id.as_str())
        }),
        WatchScope::Session => session_id.is_some_and(|session_id| {
            item.session_id
                .as_ref()
                .is_some_and(|item_session_id| item_session_id.as_str() == session_id.as_str())
        }),
    }
}

fn export_scope_has_required_ids(req: &ExportWatchRequest) -> bool {
    match req.scope {
        WatchScope::Tenant => true,
        WatchScope::Agent => req.agent_id.is_some(),
        WatchScope::Session => req.session_id.is_some(),
    }
}

fn export_item_matches_include(item: &ExportLedgerItem, include: &[String]) -> bool {
    include.is_empty()
        || include
            .iter()
            .any(|include| export_include_matches_type(include, &item.object_type))
}

fn export_include_matches_type(include: &str, object_type: &str) -> bool {
    match include {
        "events" => object_type == "event",
        "tenants" => object_type == "tenant",
        "agents" => object_type == "agent",
        "sessions" => object_type == "session",
        "artifacts" => object_type == "artifact",
        "records" => object_type == "record",
        "fork_edges" => object_type == "fork_edge",
        other => other == object_type,
    }
}

struct RuntimeExportLedgerCursorParts<'a> {
    tenant_id: &'a str,
    session_id: Option<&'a str>,
}

fn parse_export_ledger_cursor_for_runtime(
    cursor: &str,
) -> Option<RuntimeExportLedgerCursorParts<'_>> {
    let raw = cursor.strip_prefix("ledger:")?;
    let parts = raw.split('/').collect::<Vec<_>>();
    if parts.len() == 6
        && parts[1] == "v2"
        && parts[2].parse::<u64>().is_ok()
        && parts[3] == "event"
    {
        return Some(RuntimeExportLedgerCursorParts {
            tenant_id: parts[0],
            session_id: Some(parts[4]),
        });
    }
    if parts.len() == 6
        && parts[1] == "v2"
        && parts[2].parse::<u64>().is_ok()
        && parts[3] == "object"
    {
        return Some(RuntimeExportLedgerCursorParts {
            tenant_id: parts[0],
            session_id: None,
        });
    }
    if parts.len() == 6 && parts[4] == "event" && parts[3].parse::<u64>().is_ok() {
        return Some(RuntimeExportLedgerCursorParts {
            tenant_id: parts[0],
            session_id: Some(parts[2]),
        });
    }
    if parts.len() == 5 && parts[2].parse::<u64>().is_ok() {
        return Some(RuntimeExportLedgerCursorParts {
            tenant_id: parts[0],
            session_id: None,
        });
    }
    None
}

/// Compare a record's `source` against a search filter. The filter
/// only constrains the fields it sets — every populated filter field
/// must match the record's corresponding field.
fn source_ref_matches(rec: &SourceRef, filter: &SourceRef) -> bool {
    fn match_id(r: &Option<Id>, f: &Option<Id>) -> bool {
        match (r, f) {
            (_, None) => true,
            (Some(rid), Some(fid)) => rid.as_str() == fid.as_str(),
            (None, Some(_)) => false,
        }
    }
    fn match_u64(r: &Option<u64>, f: &Option<u64>) -> bool {
        match (r, f) {
            (_, None) => true,
            (Some(rv), Some(fv)) => rv == fv,
            (None, Some(_)) => false,
        }
    }
    match_id(&rec.session_id, &filter.session_id)
        && match_u64(&rec.seq, &filter.seq)
        && match_id(&rec.event_id, &filter.event_id)
        && match_id(&rec.artifact_id, &filter.artifact_id)
        && match_id(&rec.record_id, &filter.record_id)
}

fn search_index_candidate_limit(req: &SearchRequest) -> usize {
    let base = req.limit.max(1);
    let has_post_filter = !req.tags.is_empty()
        || req.source_filter.is_some()
        || req.metadata_filter.is_some()
        || req.created_after_ms.is_some()
        || req.created_before_ms.is_some();
    if has_post_filter {
        base.saturating_mul(10).clamp(base, 1024)
    } else {
        base
    }
}

fn filter_search_candidates(
    candidates: Vec<(SearchableRecord, CandidateScore)>,
    req: &SearchRequest,
) -> Vec<RecordSearchMatch> {
    let mut out = Vec::new();
    for (r, score) in candidates {
        if r.tenant_id.as_str() != req.tenant_id.as_str() {
            continue;
        }
        if r.namespace != req.namespace {
            continue;
        }
        if r.deleted_at_ms.is_some() || r.is_expired() {
            continue;
        }
        if !req.include_superseded && r.status == RecordStatus::Superseded {
            continue;
        }
        if let Some(after) = req.created_after_ms {
            if r.created_at_ms < after {
                continue;
            }
        }
        if let Some(before) = req.created_before_ms {
            if r.created_at_ms > before {
                continue;
            }
        }
        if !req.tags.iter().all(|t| r.tags.iter().any(|rt| rt == t)) {
            continue;
        }
        if let Some(filter) = &req.source_filter {
            let Some(rs) = r.source.as_ref() else {
                continue;
            };
            if !source_ref_matches(rs, filter) {
                continue;
            }
        }
        if let Some(filter) = &req.metadata_filter {
            if !metadata_filter_matches(&r.metadata, filter) {
                continue;
            }
        }
        if let Some(min_score) = req.min_score {
            if score.score < min_score {
                continue;
            }
        }
        out.push(RecordSearchMatch {
            record: r,
            score: score.score,
            rank: out.len() + 1,
            components: score.components(),
        });
        if out.len() >= req.limit {
            break;
        }
    }
    out
}

/// Mint a fresh `DocId` from the per-runtime counter. The pair
/// `(0, seq)` plays the role `agentdb-index` reserves for
/// `(session_internal_id, seq)` in event-driven uses. We don't have
/// session_internal_ids for records, so we keep the first half pinned
/// to `0` and let the second half be the unique counter — combined
/// with the per-(tenant, namespace) `path` isolation already enforced
/// by the indexes themselves, that's sufficient.
fn mint_doc_id(next: &mut u64) -> DocId {
    let seq = *next;
    *next = next.checked_add(1).expect("doc_id counter overflow");
    DocId::new(0, seq)
}

fn next_record_commit_seq(state: &mut State) -> u64 {
    state.next_record_commit_seq = state
        .next_record_commit_seq
        .checked_add(1)
        .expect("record commit sequence overflow");
    state.next_record_commit_seq
}

fn record_external_key(tenant_id: &Id, external_id: &str) -> String {
    format!("{}::{external_id}", tenant_id.as_str())
}

fn tenant_meta_key(tenant_id: &Id, key: &str) -> String {
    format!("{}::{key}", tenant_id.as_str())
}

/// Index a record's text + (optional) embedding into the per-path
/// indexes. Shared between `Runtime::write_record` and the hydration
/// loop so a record that survives a restart is indexed exactly the
/// same way it was on first write.
fn index_record_into(
    text: &mut InMemoryInverted,
    vector: &mut HnswVectorIndex,
    r: &SearchableRecord,
    doc_id: DocId,
) {
    let path = format!("{}::{}", r.tenant_id.as_str(), r.namespace_path());
    if let Some(t) = record_indexable_text(r) {
        if !t.is_empty() {
            text.insert(&path, doc_id, &t);
        }
    }
    if r.policy.vector_allowed {
        if let Some(emb) = r.embedding.as_ref() {
            // Vector index only fails on dim-mismatch within a path;
            // tracking the error per record would require widening
            // the runtime API. For V2.2 we log and drop on mismatch
            // so a single bad record can't poison the whole index.
            if let Err(e) = vector.insert(&path, doc_id, emb) {
                tracing::warn!(
                    record_id = r.record_id.as_str(),
                    path = path.as_str(),
                    error = %e,
                    "vector index insert failed; record will be text-searchable only"
                );
            }
        }
    }
}

fn record_indexable_text(r: &SearchableRecord) -> Option<String> {
    match r.policy.mode {
        IndexingMode::NoIndexing => None,
        IndexingMode::MetadataOnly => Some(extract_metadata_strings(&r.metadata, &r.tags)),
        IndexingMode::SelectedFields => Some(extract_selected_fields(
            &r.metadata,
            &r.policy.fields,
            &r.tags,
        )),
        IndexingMode::SummaryOnly => r
            .text
            .as_ref()
            .map(|s| s.chars().take(256).collect::<String>()),
    }
}

fn global_index_mutation_for_record(
    r: &SearchableRecord,
    commit_seq: u64,
    op: GlobalIndexMutationOp,
) -> GlobalIndexMutation {
    GlobalIndexMutation {
        tenant_id: r.tenant_id.as_str().to_string(),
        record_id: r.record_id.as_str().to_string(),
        namespace: r.namespace.clone(),
        placement_id: 0,
        commit_seq,
        op,
        terms: record_indexable_text(r)
            .map(|text| repair_tokens(&text))
            .unwrap_or_default(),
    }
}

fn record_matches_repair_query(r: &SearchableRecord, req: &SearchRequest) -> bool {
    if r.tenant_id.as_str() != req.tenant_id.as_str() || r.namespace != req.namespace {
        return false;
    }
    if r.deleted_at_ms.is_some() || r.is_expired() {
        return false;
    }
    if !req.include_superseded && r.status == RecordStatus::Superseded {
        return false;
    }
    if let Some(after) = req.created_after_ms {
        if r.created_at_ms < after {
            return false;
        }
    }
    if let Some(before) = req.created_before_ms {
        if r.created_at_ms > before {
            return false;
        }
    }
    if let Some(query) = req.text.as_deref() {
        let Some(text) = record_indexable_text(r) else {
            return false;
        };
        if !repair_text_matches_query(&text, query) {
            return false;
        }
    }
    if let Some(query) = req.embedding.as_deref() {
        let Some(embedding) = r.embedding.as_ref() else {
            return false;
        };
        if !r.policy.vector_allowed || embedding.len() != query.len() {
            return false;
        }
    }
    if let Some(filter) = &req.metadata_filter {
        if !metadata_filter_matches(&r.metadata, filter) {
            return false;
        }
    }
    true
}

fn repair_score(r: &SearchableRecord, req: &SearchRequest) -> CandidateScore {
    let mut score = CandidateScore::default();
    if let Some(query) = req.text.as_deref() {
        if let Some(text) = record_indexable_text(r) {
            score.merge_text(repair_text_score(&text, query));
        }
    }
    if let Some(query) = req.embedding.as_deref() {
        if r.policy.vector_allowed {
            if let Some(embedding) = r.embedding.as_ref() {
                score.merge_vector(cosine_similarity(embedding, query));
            }
        }
    }
    score
}

fn metadata_filter_matches(metadata: &serde_json::Value, filter: &MetadataFilter) -> bool {
    filter
        .all
        .iter()
        .all(|condition| metadata_condition_matches(metadata, condition))
        && (filter.any.is_empty()
            || filter
                .any
                .iter()
                .any(|condition| metadata_condition_matches(metadata, condition)))
        && filter
            .none
            .iter()
            .all(|condition| !metadata_condition_matches(metadata, condition))
}

fn metadata_condition_matches(
    metadata: &serde_json::Value,
    condition: &MetadataFilterCondition,
) -> bool {
    let value = metadata_path_value(metadata, &condition.path);
    match condition.op {
        MetadataFilterOp::Exists => {
            let expected = condition
                .value
                .as_ref()
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            value.is_some() == expected
        }
        MetadataFilterOp::Eq => value.is_some_and(|v| {
            condition
                .value
                .as_ref()
                .is_some_and(|expected| json_scalar_eq(v, expected))
        }),
        MetadataFilterOp::Neq => value.is_some_and(|v| {
            condition
                .value
                .as_ref()
                .is_some_and(|expected| !json_scalar_eq(v, expected))
        }),
        MetadataFilterOp::In => {
            let Some(actual) = value else {
                return false;
            };
            condition
                .value
                .as_ref()
                .and_then(serde_json::Value::as_array)
                .is_some_and(|values| {
                    values
                        .iter()
                        .any(|expected| json_scalar_eq(actual, expected))
                })
        }
        MetadataFilterOp::Gte => value.is_some_and(|v| {
            condition.value.as_ref().is_some_and(|expected| {
                json_scalar_cmp(v, expected).is_some_and(|ord| {
                    matches!(ord, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                })
            })
        }),
        MetadataFilterOp::Lte => value.is_some_and(|v| {
            condition.value.as_ref().is_some_and(|expected| {
                json_scalar_cmp(v, expected).is_some_and(|ord| {
                    matches!(ord, std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                })
            })
        }),
    }
}

fn metadata_path_value<'a>(
    metadata: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    if path.is_empty() {
        return None;
    }
    let mut current = metadata;
    for part in path.split('.') {
        if part.is_empty() {
            return None;
        }
        current = current.get(part)?;
    }
    Some(current)
}

fn json_scalar_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(an), serde_json::Value::Number(bn)) => {
            match (an.as_f64(), bn.as_f64()) {
                (Some(a), Some(b)) => (a - b).abs() < f64::EPSILON,
                _ => an == bn,
            }
        }
        _ => a == b,
    }
}

fn json_scalar_cmp(a: &serde_json::Value, b: &serde_json::Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (serde_json::Value::Number(an), serde_json::Value::Number(bn)) => {
            an.as_f64()?.partial_cmp(&bn.as_f64()?)
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn repair_text_matches_query(text: &str, query: &str) -> bool {
    let tokens = repair_tokens(text);
    if tokens.is_empty() {
        return false;
    }
    repair_tokens(query)
        .into_iter()
        .all(|q| tokens.iter().any(|t| t == &q))
}

fn repair_text_score(text: &str, query: &str) -> f32 {
    let tokens = repair_tokens(text);
    repair_tokens(query)
        .into_iter()
        .filter(|q| tokens.iter().any(|t| t == q))
        .count() as f32
}

fn repair_tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| t.len() >= 2)
        .map(|t| t.to_ascii_lowercase())
        .collect()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a.sqrt() * norm_b.sqrt())
}

/// Concatenate every string value reachable from `metadata` plus
/// every tag, separated by spaces. Used by `IndexingMode::MetadataOnly`.
fn extract_metadata_strings(metadata: &serde_json::Value, tags: &[String]) -> String {
    let mut out = String::new();
    for tag in tags {
        out.push_str(tag);
        out.push(' ');
    }
    walk_strings(metadata, &mut out);
    out
}

fn walk_strings(v: &serde_json::Value, out: &mut String) {
    match v {
        serde_json::Value::String(s) => {
            out.push_str(s);
            out.push(' ');
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                walk_strings(v, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                walk_strings(v, out);
            }
        }
        _ => {}
    }
}

/// Concatenate the values of the named top-level fields in `metadata`
/// (string-typed only) plus the tags. Used by
/// `IndexingMode::SelectedFields`.
fn extract_selected_fields(
    metadata: &serde_json::Value,
    fields: &[String],
    tags: &[String],
) -> String {
    let mut out = String::new();
    for tag in tags {
        out.push_str(tag);
        out.push(' ');
    }
    if let serde_json::Value::Object(map) = metadata {
        for f in fields {
            if let Some(serde_json::Value::String(s)) = map.get(f) {
                out.push_str(s);
                out.push(' ');
            }
        }
    }
    out
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use agentdb_storage::{InMemoryContentBackend, InMemoryKvStore};

    use super::*;

    #[tokio::test]
    async fn append_replays_persisted_idempotency_after_cache_miss() {
        let kv = Arc::new(InMemoryKvStore::new());
        let content = Arc::new(InMemoryContentBackend::default());
        let rt = Runtime::with_kv(kv, content);
        let (tenant, agent, session) = rt.bootstrap_test_session().await;

        let mut req = AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "idempotent",
            serde_json::json!({"v": 1}),
        );
        req.idempotency_key = Some("durable-retry".into());

        let first = rt.append(req.clone()).await.unwrap();
        assert_eq!(first.seq, 0);

        let full_key = format!(
            "{}::append::{}::durable-retry",
            tenant.tenant_id, session.session_id
        );
        assert!(rt
            .persist
            .as_ref()
            .unwrap()
            .get_idempotency(&full_key)
            .unwrap()
            .is_some());
        rt.idem.remove(&full_key);

        let replay = rt.append(req.clone()).await.unwrap();
        assert_eq!(replay.seq, first.seq);
        assert_eq!(replay.event_id, first.event_id);

        rt.idem.remove(&full_key);
        let mut conflict = req;
        conflict.payload = serde_json::json!({"v": 2});
        let err = rt.append(conflict).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::IdempotencyConflict);

        let events = rt
            .fetch_from_seq(FetchRequest {
                tenant_id: tenant.tenant_id,
                session_id: session.session_id,
                from_seq: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 0);
    }

    #[tokio::test]
    async fn append_idempotency_replay_requires_visible_event() {
        let kv = Arc::new(InMemoryKvStore::new());
        let content = Arc::new(InMemoryContentBackend::default());
        let rt = Runtime::with_kv(kv, content);
        let (tenant, agent, session) = rt.bootstrap_test_session().await;

        let mut req = AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "idempotent",
            serde_json::json!({"v": 1}),
        );
        req.idempotency_key = Some("durable-retry-not-visible".into());

        let first = rt.append(req.clone()).await.unwrap();
        assert_eq!(first.seq, 0);
        let full_key = format!(
            "{}::append::{}::durable-retry-not-visible",
            tenant.tenant_id, session.session_id
        );
        rt.idem.remove(&full_key);
        assert_eq!(
            rt.persist
                .as_ref()
                .unwrap()
                .delete_events_from_seq(tenant.tenant_id.as_str(), session.session_id.as_str(), 0)
                .unwrap(),
            1
        );

        let err = rt.append(req).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("is not yet visible"));
    }

    #[tokio::test]
    async fn append_idempotency_replay_repairs_session_visibility_gap() {
        let kv = Arc::new(InMemoryKvStore::new());
        let content = Arc::new(InMemoryContentBackend::default());
        let rt = Runtime::with_kv(kv, content);
        let (tenant, agent, session) = rt.bootstrap_test_session().await;

        let mut req = AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "idempotent",
            serde_json::json!({"v": 1}),
        );
        req.idempotency_key = Some("durable-retry-state-gap".into());

        let first = rt.append(req.clone()).await.unwrap();
        assert_eq!(first.seq, 0);
        let full_key = format!(
            "{}::append::{}::durable-retry-state-gap",
            tenant.tenant_id, session.session_id
        );
        rt.idem.remove(&full_key);
        {
            let mut state = rt.state.lock().await;
            state
                .events
                .remove(&(session.session_id.as_str().to_string(), 0));
            state
                .sessions
                .get_mut(session.session_id.as_str())
                .unwrap()
                .next_seq = 0;
        }

        let replay = rt.append(req).await.unwrap();
        assert_eq!(replay.seq, 0);
        let events = rt
            .fetch_from_seq(FetchRequest {
                tenant_id: tenant.tenant_id.clone(),
                session_id: session.session_id.clone(),
                from_seq: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 0);
    }

    #[tokio::test]
    async fn append_does_not_reuse_persisted_seq_after_stale_session_state() {
        let kv = Arc::new(InMemoryKvStore::new());
        let content = Arc::new(InMemoryContentBackend::default());
        let rt = Runtime::with_kv(kv, content);
        let (tenant, agent, session) = rt.bootstrap_test_session().await;

        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"seq": 0}),
        ))
        .await
        .unwrap();
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"seq": 1}),
        ))
        .await
        .unwrap();

        {
            let mut state = rt.state.lock().await;
            state
                .sessions
                .get_mut(session.session_id.as_str())
                .unwrap()
                .next_seq = 1;
        }

        let mut req = AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"seq": 2}),
        );
        req.idempotency_key = Some("after-stale-state".into());
        let appended = rt.append(req.clone()).await.unwrap();
        assert_eq!(appended.seq, 2);

        rt.idem.remove(&format!(
            "{}::append::{}::after-stale-state",
            tenant.tenant_id, session.session_id
        ));
        {
            let mut state = rt.state.lock().await;
            state
                .sessions
                .get_mut(session.session_id.as_str())
                .unwrap()
                .next_seq = 2;
        }
        let replay = rt.append(req).await.unwrap();
        assert_eq!(replay.seq, 2);
        assert_eq!(replay.event_id, appended.event_id);

        let events = rt
            .fetch_from_seq(FetchRequest {
                tenant_id: tenant.tenant_id,
                session_id: session.session_id,
                from_seq: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(
            events.iter().map(|event| event.seq).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(events[2].payload, serde_json::json!({"seq": 2}));
    }

    fn test_record(tenant_id: Id, external_id: Option<&str>, text: &str) -> NewRecord {
        NewRecord {
            tenant_id,
            external_id: external_id.map(str::to_string),
            namespace: vec!["memory".into()],
            scope: serde_json::json!({"agent_id": "agent-a"}),
            text: Some(text.into()),
            metadata: serde_json::json!({"kind": "memory"}),
            tags: vec!["memory".into()],
            source: None,
            embedding: None,
            policy: crate::record::IndexingPolicy {
                mode: crate::record::IndexingMode::SummaryOnly,
                fields: vec![],
                vector_allowed: false,
            },
            expires_at_ms: None,
        }
    }

    #[tokio::test]
    async fn record_external_id_is_idempotent_and_upsert_updates_existing_row() {
        let kv = Arc::new(InMemoryKvStore::new());
        let content = Arc::new(InMemoryContentBackend::default());
        let rt = Runtime::with_kv(kv, content);
        let (tenant, _, _) = rt.bootstrap_test_session().await;

        let first = rt
            .write_record_with_commit(test_record(
                tenant.tenant_id.clone(),
                Some("mem-1"),
                "first value",
            ))
            .await
            .unwrap();
        let replay = rt
            .write_record_with_commit(test_record(
                tenant.tenant_id.clone(),
                Some("mem-1"),
                "ignored duplicate",
            ))
            .await
            .unwrap();
        assert_eq!(replay.record.record_id, first.record.record_id);
        assert_eq!(replay.record.text.as_deref(), Some("first value"));
        assert_eq!(replay.commit_token.seq, first.commit_token.seq);

        let updated = rt
            .upsert_record_with_commit(test_record(
                tenant.tenant_id.clone(),
                Some("mem-1"),
                "updated value",
            ))
            .await
            .unwrap();
        assert_eq!(updated.record.record_id, first.record.record_id);
        assert_eq!(updated.record.text.as_deref(), Some("updated value"));
        assert_eq!(updated.record.version, first.record.version + 1);
    }

    #[tokio::test]
    async fn record_update_version_status_search_and_touch_support_memory_semantics() {
        let kv = Arc::new(InMemoryKvStore::new());
        let content = Arc::new(InMemoryContentBackend::default());
        let rt = Runtime::with_kv(kv, content);
        let (tenant, _, _) = rt.bootstrap_test_session().await;
        let written = rt
            .write_record_with_commit(test_record(
                tenant.tenant_id.clone(),
                Some("mem-2"),
                "active memory",
            ))
            .await
            .unwrap();

        let touched = rt
            .search_record_matches(SearchRequest {
                tenant_id: tenant.tenant_id.clone(),
                namespace: vec!["memory".into()],
                sharding_key: Some("local".into()),
                text: Some("active".into()),
                embedding: None,
                tags: vec![],
                source_filter: None,
                metadata_filter: None,
                include_superseded: false,
                min_score: Some(0.0),
                created_after_ms: None,
                created_before_ms: None,
                touch: true,
                limit: 10,
                min_visible_record_commit_seq: None,
            })
            .await
            .unwrap();
        assert_eq!(touched.len(), 1);
        assert_eq!(touched[0].record.access_count, 1);

        let err = rt
            .update_record_with_commit(RecordUpdate {
                tenant_id: tenant.tenant_id.clone(),
                record_id: written.record.record_id.clone(),
                expected_version: Some(99),
                namespace: None,
                scope: None,
                text: None,
                metadata: None,
                tags: None,
                source: None,
                embedding: None,
                policy: None,
                status: Some(RecordStatus::Superseded),
                expires_at_ms: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::VersionConflict);

        let superseded = rt
            .update_record_with_commit(RecordUpdate {
                tenant_id: tenant.tenant_id.clone(),
                record_id: written.record.record_id.clone(),
                expected_version: Some(written.record.version),
                namespace: None,
                scope: None,
                text: None,
                metadata: None,
                tags: None,
                source: None,
                embedding: None,
                policy: None,
                status: Some(RecordStatus::Superseded),
                expires_at_ms: None,
            })
            .await
            .unwrap();
        assert_eq!(superseded.record.status, RecordStatus::Superseded);

        let default_search = rt
            .search_record_matches(SearchRequest {
                tenant_id: tenant.tenant_id.clone(),
                namespace: vec!["memory".into()],
                sharding_key: Some("local".into()),
                text: Some("active".into()),
                embedding: None,
                tags: vec![],
                source_filter: None,
                metadata_filter: None,
                include_superseded: false,
                min_score: Some(0.0),
                created_after_ms: None,
                created_before_ms: None,
                touch: false,
                limit: 10,
                min_visible_record_commit_seq: None,
            })
            .await
            .unwrap();
        assert!(default_search.is_empty());

        let history_search = rt
            .search_record_matches(SearchRequest {
                tenant_id: tenant.tenant_id.clone(),
                namespace: vec!["memory".into()],
                sharding_key: Some("local".into()),
                text: None,
                embedding: None,
                tags: vec![],
                source_filter: None,
                metadata_filter: None,
                include_superseded: true,
                min_score: Some(1.0),
                created_after_ms: Some(written.record.created_at_ms),
                created_before_ms: Some(crate::now_ms()),
                touch: false,
                limit: 10,
                min_visible_record_commit_seq: None,
            })
            .await
            .unwrap();
        assert_eq!(history_search.len(), 1);
        assert_eq!(history_search[0].record.status, RecordStatus::Superseded);
    }

    #[tokio::test]
    async fn tenant_meta_is_persisted() {
        let kv = Arc::new(InMemoryKvStore::new());
        let content = Arc::new(InMemoryContentBackend::default());
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let (tenant, _, _) = rt.bootstrap_test_session().await;
        rt.set_tenant_meta(
            &tenant.tenant_id,
            "db0.cursor",
            serde_json::json!({"offset": 42}),
        )
        .await
        .unwrap();

        let restarted = Runtime::with_kv(kv, content);
        let meta = restarted
            .get_tenant_meta(&tenant.tenant_id, "db0.cursor")
            .await
            .unwrap();
        assert_eq!(meta.value, serde_json::json!({"offset": 42}));
    }
}

/// Verify a stored idempotency record matches the incoming request and replay
/// the original response, or surface a conflict if any field that affects
/// the commit differs.
fn verify_idem_replay(rec: &IdempotencyRecord, req: &AppendRequest) -> Result<AppendResponse> {
    let h = hash_request(
        &req.tenant_id,
        &req.agent_id,
        &req.session_id,
        &req.kind,
        &req.payload,
        &req.artifact_refs,
        &req.source_refs,
        &req.credential_refs,
    );
    if rec.payload_hash != h {
        return Err(Error::new(
            ErrorCode::IdempotencyConflict,
            "request differs for the same idempotency key",
        ));
    }
    let resp: AppendResponse = serde_json::from_str(&rec.result_json).unwrap();
    Ok(resp)
}


/// Map a storage-layer error into a V2 `Internal` error. The persist layer
/// returns `agentdb_storage::Error`; surfacing it as `Internal` keeps the
/// Most storage errors surface as `Internal`; Raft leadership misses are
/// retry-routing signals, not generic server faults.
fn internal(e: agentdb_storage::Error) -> Error {
    let message = e.to_string();
    if message.contains("not leader for tablet") {
        return Error::new(ErrorCode::RaftNotLeader, message)
            .with_suggestion("retry the request against the raft leader advertised by the API");
    }
    Error::new(ErrorCode::Internal, message)
}

fn key_from_parts(key_id: &Option<String>, key_version: &Option<String>) -> Option<KeyResolution> {
    key_id.as_ref().map(|key_id| KeyResolution {
        key_id: key_id.clone(),
        key_version: key_version.clone(),
    })
}

fn kms_error(e: KmsError) -> Error {
    match e {
        KmsError::KeyUnavailable {
            tenant_id,
            operation,
            reason,
        } => Error::new(
            ErrorCode::KeyUnavailable,
            format!("tenant {tenant_id} key unavailable for {operation}: {reason}"),
        )
        .with_suggestion("restore or rotate the tenant encryption key before retrying"),
        KmsError::Other(message) => Error::new(ErrorCode::Internal, message),
    }
}
