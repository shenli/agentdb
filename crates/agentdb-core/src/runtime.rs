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

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use agentdb_index::doc_id::DocId;
use agentdb_index::text::{InMemoryInverted, TextIndex};
use agentdb_index::vector::{HnswVectorIndex, VectorIndex};
use agentdb_storage::content::{sha256_hex, ContentBackend, InMemoryContentBackend};
use agentdb_storage::content_crypto::{decode_keyed_envelope, encode_keyed_envelope};
use agentdb_storage::{
    AllowAllKmsKeyResolver, ContentCryptoAdapter, ContentCryptoContext,
    IdentityContentCryptoAdapter, IdentityMetadataCryptoAdapter, KeyOperation, KeyResolution,
    KmsError, KmsKeyResolver, KvIter, KvSnapshot, KvStore, MetadataCryptoAdapter, ReplicatedLog,
    TabletKvStore, WriteOp,
};

use crate::agent::{Agent, NewAgent};
use crate::artifact::{Artifact, NewArtifact};
use crate::errors::{Error, ErrorCode, Result};
use crate::event::{ArtifactRef, EventEnvelope, NewEvent, SourceRef};
use crate::global_index::{
    GlobalIndexMutation, GlobalIndexMutationOp, GlobalIndexQuery, GlobalRecordIndex,
    InMemoryGlobalRecordIndex,
};
use crate::id::{Id, IdKind};
use crate::idempotency::{hash_json, hash_request, IdempotencyRecord, IdempotencyStore};
use crate::persist::{Persist, RecordIndexMutationOp};
use crate::policy::{Priority, RuntimePolicy};
use crate::record::{IndexingMode, NewRecord, SearchableRecord};
use crate::segment::{
    SessionArchiveFile, SessionArchiveManifest, SessionSegment, SessionSegmentFile,
};
use crate::session::{ForkRef, Lifecycle, NewSession, Session};
use crate::skill::{AgentSkillBinding, BindSkillRequest, NewSkill, Skill};
use crate::tablet_state::{
    LoggedTabletStateMachine, TabletCommand, TabletSnapshotShardFilter, TabletStateMachine,
    TabletStateSnapshot, TabletStorageStats, TabletWriteOp,
};
use crate::tenant::{NewTenant, Tenant};

pub const RUNTIME_PRIMARY_TABLET_ID: i64 = 1;

tokio::task_local! {
    static CURRENT_RUNTIME_TABLET_ID: i64;
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
    Skills,
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
    pub skills: Vec<AgentSkillBinding>,
    pub policy: Option<RuntimePolicy>,
    pub bytes_used: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchScope {
    Session,
    Tenant,
    Agent,
}

#[derive(Debug, Clone)]
pub struct WatchRequest {
    pub tenant_id: Id,
    pub scope: WatchScope,
    pub session_id: Option<Id>,
    pub agent_id: Option<Id>,
    pub from_cursor: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangefeedItem {
    pub cursor: String,
    pub event: EventEnvelope,
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
    pub skills_purged: usize,
    pub skill_bindings_purged: usize,
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

#[derive(Default, Debug)]
struct State {
    tenants: BTreeMap<String, Tenant>,
    agents: BTreeMap<String, Agent>,
    sessions: BTreeMap<String, Session>,
    /// Keyed by artifact_id_str. Hydrated at startup from
    /// `Persist::scan_all_artifacts`; updated on every `create_artifact`.
    artifacts: BTreeMap<String, Artifact>,
    /// Keyed by (session_id, seq) for ordered scan. Only populated in the
    /// in-memory backend; the KV backend reads events back from storage on
    /// demand instead of carrying a duplicate copy in memory.
    events: BTreeMap<(String, u64), EventEnvelope>,
    /// Maps `event_id -> (session_id_str, seq)` for in-memory `event_by_id`
    /// lookup. Only populated in the in-memory backend (the KV backend
    /// resolves through the `event_by_id` secondary index in storage).
    /// Reset on every restart, which is fine — the in-memory backend
    /// doesn't persist anyway.
    event_by_id: BTreeMap<String, (String, u64)>,
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
    /// V2.3 skills, keyed by skill_id string.
    skills: BTreeMap<String, Skill>,
    /// Uniqueness map for `(tenant_id, skill_name, version) -> skill_id`.
    skill_uniques: BTreeMap<String, String>,
    /// Agent skill bindings keyed by `(agent_id, skill_id)`.
    agent_skill_bindings: BTreeMap<(String, String), AgentSkillBinding>,
    /// V2.2 searchable records, keyed by `record_id` string. Hydrated at
    /// startup from `Persist::scan_all_records`; updated on every
    /// `write_record` / `delete_record`.
    records: BTreeMap<String, SearchableRecord>,
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
    /// Counter behind [`mint_doc_id`]. Lives in `State` so the same
    /// state mutex serializes counter increments alongside record
    /// inserts.
    next_doc_seq: u64,
}

fn scoped_alias_key(tenant_id: &str, alias: &str) -> String {
    format!("{tenant_id}::{alias}")
}

fn skill_unique_state_key(tenant_id: &str, name: &str, version: &str) -> String {
    format!("{tenant_id}::{name}::{version}")
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

fn priority_rank(p: Priority) -> u8 {
    match p {
        Priority::Low => 0,
        Priority::Normal => 1,
        Priority::High => 2,
        Priority::Critical => 3,
    }
}

pub struct Runtime {
    state: Mutex<State>,
    session_commit_locks: StdMutex<BTreeMap<String, Weak<Mutex<()>>>>,
    idem: IdempotencyStore,
    persist: Option<Persist>,
    content: Arc<dyn ContentBackend>,
    content_crypto: Arc<dyn ContentCryptoAdapter>,
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

#[derive(Clone)]
struct RuntimeTabletKvStore {
    store: Arc<TabletKvStore>,
    state_machine: Arc<dyn TabletStateMachine>,
}

impl RuntimeTabletKvStore {
    fn new(
        kv: Arc<dyn KvStore + Send + Sync>,
        tablet_id: i64,
        state_machine: Arc<dyn TabletStateMachine>,
    ) -> agentdb_storage::Result<Self> {
        let store = Arc::new(TabletKvStore::new(kv, tablet_id)?);
        state_machine.apply_committed()?;
        Ok(Self {
            store,
            state_machine,
        })
    }

    fn single_node(
        kv: Arc<dyn KvStore + Send + Sync>,
        tablet_id: i64,
    ) -> agentdb_storage::Result<Self> {
        let state_machine = Arc::new(LoggedTabletStateMachine::single_node(
            kv.clone(),
            tablet_id,
        )?);
        Self::new(kv, tablet_id, state_machine)
    }

    fn logged(
        kv: Arc<dyn KvStore + Send + Sync>,
        tablet_id: i64,
        log: Arc<dyn ReplicatedLog + Send + Sync>,
    ) -> agentdb_storage::Result<(Self, Arc<LoggedTabletStateMachine>)> {
        let tablet_store = TabletKvStore::new(kv.clone(), tablet_id)?;
        let state_machine = Arc::new(LoggedTabletStateMachine::new(tablet_id, log, tablet_store)?);
        let kv_store = Self::new(kv, tablet_id, state_machine.clone())?;
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
    kv: Arc<dyn KvStore + Send + Sync>,
    default_tablet_id: i64,
    tablets: RwLock<BTreeMap<i64, RuntimeTabletHandle>>,
}

impl RuntimeTabletRouterKvStore {
    fn new(
        kv: Arc<dyn KvStore + Send + Sync>,
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
            let (kv_store, state_machine) =
                RuntimeTabletKvStore::logged(kv.clone(), tablet_id, log)?;
            tablets.insert(
                tablet_id,
                RuntimeTabletHandle {
                    kv_store: Arc::new(kv_store),
                    state_machine,
                },
            );
        }
        Ok(Self {
            kv,
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
        let (kv_store, state_machine) =
            RuntimeTabletKvStore::logged(self.kv.clone(), tablet_id, log)?;
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
            let snapshot = handle.state_machine.export_snapshot()?;
            serde_json::to_vec(&snapshot).map_err(|err| {
                agentdb_storage::Error::Other(format!(
                    "encode tablet {tablet_id} runtime snapshot: {err}"
                ))
            })
        })
    }

    fn export_snapshot_payload_for_shard_range(
        &self,
        source_tablet_id: i64,
        filter: TabletSnapshotShardFilter,
    ) -> agentdb_storage::Result<Vec<u8>> {
        self.with_handle(source_tablet_id, |handle| {
            let snapshot = handle
                .state_machine
                .export_snapshot_for_shard_range(filter)?;
            serde_json::to_vec(&snapshot).map_err(|err| {
                agentdb_storage::Error::Other(format!(
                    "encode tablet {source_tablet_id} filtered runtime snapshot: {err}"
                ))
            })
        })
    }

    fn install_snapshot_payload(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> agentdb_storage::Result<()> {
        let snapshot: TabletStateSnapshot = serde_json::from_slice(payload).map_err(|err| {
            agentdb_storage::Error::Other(format!(
                "decode tablet {tablet_id} runtime snapshot: {err}"
            ))
        })?;
        self.with_handle(tablet_id, |handle| {
            handle.state_machine.install_snapshot(&snapshot)
        })
    }

    fn install_snapshot_payload_merge(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> agentdb_storage::Result<()> {
        let snapshot: TabletStateSnapshot = serde_json::from_slice(payload).map_err(|err| {
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

fn hydrate_runtime_from_persist(persist: &Persist) -> RuntimeHydration {
    let mut state = State::default();
    for t in persist.scan_all_tenants().expect("hydrate tenants") {
        state.tenants.insert(t.tenant_id.as_str().to_string(), t);
    }
    for a in persist.scan_all_agents().expect("hydrate agents") {
        state.agents.insert(a.agent_id.as_str().to_string(), a);
    }
    for mut s in persist.scan_all_sessions().expect("hydrate sessions") {
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
    for skill in persist.scan_all_skills().expect("hydrate skills") {
        if skill.deleted_at_ms.is_none() {
            state
                .skills
                .insert(skill.skill_id.as_str().to_string(), skill);
        }
    }
    for ((tenant, name, version), skill_id) in persist
        .scan_all_skill_uniques()
        .expect("hydrate skill uniqueness")
    {
        state
            .skill_uniques
            .insert(skill_unique_state_key(&tenant, &name, &version), skill_id);
    }
    for binding in persist
        .scan_all_agent_skill_bindings()
        .expect("hydrate agent skill bindings")
    {
        if binding.deleted_at_ms.is_none() {
            state.agent_skill_bindings.insert(
                (
                    binding.agent_id.as_str().to_string(),
                    binding.skill_id.as_str().to_string(),
                ),
                binding,
            );
        }
    }

    let idempotency_records = persist
        .scan_all_idempotency()
        .expect("hydrate idempotency records");
    let mut hydrated_record_commit_seq: BTreeMap<String, u64> = persist
        .scan_all_record_commit_seqs()
        .expect("hydrate record commit sequences")
        .into_iter()
        .collect();
    if let Some(max_commit_seq) = hydrated_record_commit_seq.values().copied().max() {
        state.next_record_commit_seq = max_commit_seq;
        state.local_record_index_watermark = max_commit_seq;
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
        state.records.insert(r.record_id.as_str().to_string(), r);
    }

    RuntimeHydration {
        state,
        idempotency_records,
        text_index,
        vector_index,
        global_record_index,
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
        }
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
        let (content_key, ciphertext) = match decode_keyed_envelope(&stored_bytes)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("segment envelope failed: {e}")))?
        {
            Some((recorded_key, ciphertext)) => (recorded_key, ciphertext),
            None => (
                key_from_parts(&segment.content_key_id, &segment.content_key_version)
                    .unwrap_or(current_key),
                stored_bytes,
            ),
        };
        let crypto_context = ContentCryptoContext {
            tenant_id: segment.tenant_id.as_str(),
            artifact_id: segment.segment_id.as_str(),
            key: &content_key,
        };
        let plaintext = self
            .content_crypto
            .decrypt(&crypto_context, &ciphertext)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("segment decrypt failed: {e}")))?;
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
        let crypto_context = ContentCryptoContext {
            tenant_id: tenant_id.as_str(),
            artifact_id: segment_id.as_str(),
            key: &content_key,
        };
        let encrypted_bytes = self
            .content_crypto
            .encrypt(&crypto_context, &plaintext)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("segment encrypt failed: {e}")))?;
        let stored_bytes = encode_keyed_envelope(&content_key, &encrypted_bytes);
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
        let (content_key, ciphertext) = match decode_keyed_envelope(&stored_bytes)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("archive envelope failed: {e}")))?
        {
            Some((recorded_key, ciphertext)) => (recorded_key, ciphertext),
            None => (
                key_from_parts(&manifest.content_key_id, &manifest.content_key_version)
                    .unwrap_or(current_key),
                stored_bytes,
            ),
        };
        let crypto_context = ContentCryptoContext {
            tenant_id: manifest.tenant_id.as_str(),
            artifact_id: manifest.manifest_id.as_str(),
            key: &content_key,
        };
        let plaintext = self
            .content_crypto
            .decrypt(&crypto_context, &ciphertext)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("archive decrypt failed: {e}")))?;
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
        let crypto_context = ContentCryptoContext {
            tenant_id: tenant_id.as_str(),
            artifact_id: manifest_id.as_str(),
            key: &content_key,
        };
        let encrypted_bytes = self
            .content_crypto
            .encrypt(&crypto_context, &plaintext)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("archive encrypt failed: {e}")))?;
        let stored_bytes = encode_keyed_envelope(&content_key, &encrypted_bytes);
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
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            idem: IdempotencyStore::default(),
            persist: None,
            content: Arc::new(InMemoryContentBackend::default()),
            content_crypto: Arc::new(IdentityContentCryptoAdapter),
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
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            idem: IdempotencyStore::default(),
            persist: None,
            content: Arc::new(InMemoryContentBackend::default()),
            content_crypto: Arc::new(IdentityContentCryptoAdapter),
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
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            idem: IdempotencyStore::default(),
            persist: None,
            content: Arc::new(InMemoryContentBackend::default()),
            content_crypto,
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
        Self::with_kv_kms_content_metadata_and_replicated_logs(
            kv,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            Arc::new(IdentityMetadataCryptoAdapter),
            logs,
        )
    }

    pub fn with_kv_and_replicated_logs(
        kv: Arc<dyn KvStore + Send + Sync>,
        content: Arc<dyn ContentBackend>,
        logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>>,
    ) -> Arc<Self> {
        Self::with_kv_kms_content_metadata_and_replicated_logs(
            kv,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(IdentityContentCryptoAdapter),
            Arc::new(IdentityMetadataCryptoAdapter),
            logs,
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
        let tablet_router = Arc::new(
            RuntimeTabletRouterKvStore::new(kv, RUNTIME_PRIMARY_TABLET_ID, logs)
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
        let hydration = hydrate_runtime_from_persist(&persist);
        let idem = IdempotencyStore::default();
        idem.extend(hydration.idempotency_records);

        Arc::new(Self {
            state: Mutex::new(hydration.state),
            session_commit_locks: StdMutex::new(BTreeMap::new()),
            idem,
            persist: Some(persist),
            content,
            content_crypto,
            kms,
            text_index: StdMutex::new(hydration.text_index),
            vector_index: StdMutex::new(hydration.vector_index),
            global_record_index: hydration.global_record_index,
            tablet_router,
        })
    }

    pub async fn with_tablet_context<F, T>(&self, tablet_id: i64, future: F) -> T
    where
        F: Future<Output = T>,
    {
        CURRENT_RUNTIME_TABLET_ID.scope(tablet_id, future).await
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

    pub async fn install_tablet_snapshot_payload_and_rehydrate(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> Result<bool> {
        if !self.install_tablet_snapshot_payload(tablet_id, payload)? {
            return Ok(false);
        }
        self.rehydrate_from_persist().await?;
        Ok(true)
    }

    pub async fn install_tablet_snapshot_payload_merge_and_rehydrate(
        &self,
        tablet_id: i64,
        payload: &[u8],
    ) -> Result<bool> {
        if !self.install_tablet_snapshot_payload_merge(tablet_id, payload)? {
            return Ok(false);
        }
        self.rehydrate_from_persist().await?;
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

    async fn rehydrate_from_persist(&self) -> Result<()> {
        let Some(persist) = self.persist.as_ref() else {
            return Ok(());
        };
        let hydration = hydrate_runtime_from_persist(persist);
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
            self.rehydrate_from_persist().await?;
        }
        Ok(())
    }

    fn resolve_key(&self, tenant_id: &Id, operation: KeyOperation) -> Result<KeyResolution> {
        self.kms
            .resolve_tenant_key(tenant_id.as_str(), operation)
            .map_err(kms_error)
    }

    /// Look up a tenant by id. Returns `TenantNotFound` if the id doesn't
    /// match a live (non-soft-deleted) tenant.
    pub async fn get_tenant(&self, tenant_id: &Id) -> Result<Tenant> {
        self.refresh_replicated_state().await?;
        let s = self.state.lock().await;
        s.tenants
            .get(tenant_id.as_str())
            .filter(|t| t.deleted_at_ms.is_none())
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::TenantNotFound, format!("{}", tenant_id)))
    }

    pub async fn list_tenants(&self) -> Vec<Tenant> {
        let s = self.state.lock().await;
        s.tenants
            .values()
            .filter(|t| t.deleted_at_ms.is_none())
            .cloned()
            .collect()
    }

    /// Look up an agent by id, scoped to the given tenant. Returns
    /// `AgentNotFound` if the id is unknown or belongs to a different tenant.
    pub async fn get_agent(&self, tenant_id: &Id, agent_id: &Id) -> Result<Agent> {
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
    }

    pub async fn list_agents(&self, tenant_id: &Id) -> Result<Vec<Agent>> {
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
        let mut agents: Vec<Agent> = s
            .agents
            .values()
            .filter(|agent| {
                agent.deleted_at_ms.is_none() && agent.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
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
        Ok(())
    }

    /// Look up a session by id, scoped to the given tenant. Returns
    /// `SessionNotFound` if the id is unknown or belongs to a different tenant.
    pub async fn get_session(&self, tenant_id: &Id, session_id: &Id) -> Result<Session> {
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
            .ok_or_else(|| Error::new(ErrorCode::SessionNotFound, format!("{}", session_id)))
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
        let s = self.state.lock().await;
        let live_tenants: Vec<Tenant> = s
            .tenants
            .values()
            .filter(|tenant| tenant.deleted_at_ms.is_none())
            .cloned()
            .collect();

        let mut tenant_summaries = Vec::with_capacity(live_tenants.len());
        let mut lifecycle_counts = LifecycleCounts::default();
        let mut session_count = 0usize;
        let mut active_session_count = 0usize;
        let mut sealed_segment_count = 0usize;
        let mut archive_manifest_count = 0usize;
        let mut sealed_bytes = 0u64;
        let mut archived_bytes = 0u64;

        for tenant in &live_tenants {
            let sessions: Vec<Session> = s
                .sessions
                .values()
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
        let mut sessions: Vec<Session> = s
            .sessions
            .values()
            .filter(|sess| {
                sess.deleted_at_ms.is_none() && sess.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
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
        Ok(())
    }

    pub async fn create_tenant(&self, req: NewTenant) -> Result<Tenant> {
        let t = {
            let mut s = self.state.lock().await;
            // External keys are tenant-name slots; a duplicate is an
            // unrecoverable client error (not a retry), so we surface it as
            // AliasConflict rather than IdempotencyConflict.
            if !req.external_key.is_empty() && s.tenant_aliases.contains_key(&req.external_key) {
                return Err(Error::new(
                    ErrorCode::AliasConflict,
                    format!("tenant external_key already in use: {}", req.external_key),
                ));
            }
            let t = Tenant::create(req);
            s.tenants
                .insert(t.tenant_id.as_str().to_string(), t.clone());
            if !t.external_key.is_empty() {
                s.tenant_aliases
                    .insert(t.external_key.clone(), t.tenant_id.as_str().to_string());
            }
            t
        };
        if let Some(p) = &self.persist {
            p.put_tenant(&t).map_err(internal)?;
        }
        Ok(t)
    }

    pub async fn create_agent(&self, req: NewAgent) -> Result<Agent> {
        self.refresh_replicated_state().await?;
        let a = {
            let mut s = self.state.lock().await;
            let policy = active_tenant_policy_for_id(&s, &req.tenant_id).ok_or_else(|| {
                Error::new(ErrorCode::TenantNotFound, format!("{}", req.tenant_id))
            })?;
            if let Some(max) = policy.quota.max_agents {
                let live_agents = s
                    .agents
                    .values()
                    .filter(|a| {
                        a.tenant_id.as_str() == req.tenant_id.as_str() && a.deleted_at_ms.is_none()
                    })
                    .count();
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
            if !s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none())
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
        self.refresh_replicated_state().await?;
        let sess = {
            let mut s = self.state.lock().await;
            let policy = active_tenant_policy_for_id(&s, &req.tenant_id).ok_or_else(|| {
                Error::new(ErrorCode::TenantNotFound, format!("{}", req.tenant_id))
            })?;
            if let Some(max) = policy.quota.max_active_sessions {
                let active_sessions = s
                    .sessions
                    .values()
                    .filter(|sess| {
                        sess.tenant_id.as_str() == req.tenant_id.as_str()
                            && sess.deleted_at_ms.is_none()
                            && sess.status == crate::session::Status::Active
                    })
                    .count();
                if active_sessions >= max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies session create: active session quota {max} exhausted",
                            policy.version
                        ),
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
        Ok(sess)
    }

    pub async fn fork_session(&self, req: ForkSessionRequest) -> Result<Session> {
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
            if req.at_seq >= parent.next_seq {
                return Err(Error::new(
                    ErrorCode::InvalidCursor,
                    format!(
                        "fork at_seq {} is outside parent session next_seq {}",
                        req.at_seq, parent.next_seq
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
                at_seq: req.at_seq,
            });
            child.policy_version = parent.policy_version;
            s.sessions
                .insert(child.session_id.as_str().to_string(), child.clone());
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
            p.put_session(&child).map_err(internal)?;
        }
        Ok(child)
    }

    pub async fn replay_session(&self, tenant_id: &Id, session_id: &Id) -> Result<ReplayResponse> {
        let mut segments: Vec<(Id, Option<u64>)> = Vec::new();
        let mut current_id = session_id.clone();
        let mut current_cutoff = None;
        loop {
            let session = self.get_session(tenant_id, &current_id).await?;
            segments.push((session.session_id.clone(), current_cutoff));
            let Some(parent) = session.forked_from else {
                break;
            };
            current_id = parent.session_id.clone();
            current_cutoff = Some(parent.at_seq);
        }

        segments.reverse();
        let mut events = Vec::new();
        for (sid, cutoff) in segments {
            let mut segment = self
                .fetch_from_seq(FetchRequest {
                    tenant_id: tenant_id.clone(),
                    session_id: sid,
                    from_seq: 0,
                    limit: usize::MAX,
                })
                .await?;
            if let Some(max_seq) = cutoff {
                segment.retain(|event| event.seq <= max_seq);
            }
            events.extend(segment);
        }
        Ok(ReplayResponse {
            session_id: session_id.clone(),
            events,
        })
    }

    /// Register a V2.3 skill. `(tenant_id, name, version)` is unique.
    /// Artifact refs are validated against live artifacts in the same tenant.
    pub async fn register_skill(
        &self,
        req: NewSkill,
        idempotency_key: Option<String>,
    ) -> Result<Skill> {
        let idem_full_key = idempotency_key.as_ref().map(|k| {
            format!(
                "{}::{}::{}::{}",
                req.tenant_id, "skill_register", req.name, k
            )
        });
        let req_hash = hash_skill_register_request(&req);
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                return verify_json_replay(&rec, req_hash);
            }
        }

        let skill = {
            let mut s = self.state.lock().await;
            if let Some(ref full) = idem_full_key {
                if let Some(rec) = self.idem.lookup(full) {
                    drop(s);
                    return verify_json_replay(&rec, req_hash);
                }
            }
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
            let policy = policy_for_tenant_in_state(&s, &req.tenant_id);
            if let Some(max) = policy.quota.max_skills {
                let live_skills = s
                    .skills
                    .values()
                    .filter(|skill| {
                        skill.tenant_id.as_str() == req.tenant_id.as_str()
                            && skill.deleted_at_ms.is_none()
                    })
                    .count();
                if live_skills >= max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies skill register: skill quota {max} exhausted",
                            policy.version
                        ),
                    ));
                }
            }
            let unique = skill_unique_state_key(req.tenant_id.as_str(), &req.name, &req.version);
            if s.skill_uniques.contains_key(&unique) {
                return Err(Error::new(
                    ErrorCode::AliasConflict,
                    format!(
                        "skill version already registered: {}@{}",
                        req.name, req.version
                    ),
                ));
            }
            for art_ref in &req.artifact_refs {
                let exists = art_ref.artifact_id.kind() == crate::id::IdKind::Artifact
                    && s.artifacts
                        .get(art_ref.artifact_id.as_str())
                        .is_some_and(|a| {
                            a.deleted_at_ms.is_none()
                                && a.tenant_id.as_str() == req.tenant_id.as_str()
                        });
                if !exists {
                    return Err(Error::new(
                        ErrorCode::ArtifactNotFound,
                        format!("{}", art_ref.artifact_id),
                    ));
                }
            }
            let skill = Skill::create(req);
            if let Some(p) = &self.persist {
                p.put_skill(&skill).map_err(internal)?;
            }
            s.skill_uniques
                .insert(unique, skill.skill_id.as_str().to_string());
            s.skills
                .insert(skill.skill_id.as_str().to_string(), skill.clone());
            skill
        };

        if let Some(full) = idem_full_key {
            let rec = IdempotencyRecord {
                payload_hash: req_hash,
                result_json: serde_json::to_string(&skill).unwrap(),
                created_at_ms: crate::now_ms(),
            };
            if let Some(p) = &self.persist {
                p.put_idempotency(&full, &rec).map_err(internal)?;
            }
            self.idem.put(full, rec);
        }
        Ok(skill)
    }

    pub async fn get_skill(&self, tenant_id: &Id, skill_id: &Id) -> Result<Skill> {
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        s.skills
            .get(skill_id.as_str())
            .filter(|skill| {
                tenant_live
                    && skill.deleted_at_ms.is_none()
                    && skill.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::SkillNotFound, format!("{}", skill_id)))
    }

    pub async fn list_skills(&self, tenant_id: &Id) -> Result<Vec<Skill>> {
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
        let mut skills = s
            .skills
            .values()
            .filter(|skill| {
                skill.deleted_at_ms.is_none() && skill.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
            .collect::<Vec<_>>();
        skills.sort_by(|a, b| {
            a.name
                .cmp(&b.name)
                .then_with(|| a.version.cmp(&b.version))
                .then_with(|| a.skill_id.as_str().cmp(b.skill_id.as_str()))
        });
        Ok(skills)
    }

    pub async fn delete_skill(&self, tenant_id: &Id, skill_id: &Id) -> Result<()> {
        let mut s = self.state.lock().await;
        let live = s.skills.get(skill_id.as_str()).is_some_and(|skill| {
            skill.deleted_at_ms.is_none() && skill.tenant_id.as_str() == tenant_id.as_str()
        });
        if !live {
            return Err(Error::new(
                ErrorCode::SkillNotFound,
                format!("{}", skill_id),
            ));
        }

        let now = crate::now_ms();
        let updated_skill = {
            let skill = s
                .skills
                .get_mut(skill_id.as_str())
                .expect("presence checked above");
            skill.deleted_at_ms = Some(now);
            skill.clone()
        };
        let binding_keys: Vec<(String, String)> = s
            .agent_skill_bindings
            .iter()
            .filter(|(_, binding)| {
                binding.tenant_id.as_str() == tenant_id.as_str()
                    && binding.skill_id.as_str() == skill_id.as_str()
                    && binding.deleted_at_ms.is_none()
            })
            .map(|(key, _)| key.clone())
            .collect();
        let mut updated_bindings = Vec::new();
        for key in binding_keys {
            if let Some(binding) = s.agent_skill_bindings.get_mut(&key) {
                binding.deleted_at_ms = Some(now);
                updated_bindings.push(binding.clone());
            }
        }

        if let Some(p) = &self.persist {
            if let Err(e) = p.put_skill(&updated_skill) {
                if let Some(skill) = s.skills.get_mut(skill_id.as_str()) {
                    skill.deleted_at_ms = None;
                }
                for binding in &updated_bindings {
                    if let Some(current) = s.agent_skill_bindings.get_mut(&(
                        binding.agent_id.as_str().to_string(),
                        binding.skill_id.as_str().to_string(),
                    )) {
                        current.deleted_at_ms = None;
                    }
                }
                drop(s);
                return Err(internal(e));
            }
            for binding in &updated_bindings {
                if let Err(e) = p.put_agent_skill_binding(binding) {
                    drop(s);
                    return Err(internal(e));
                }
            }
        }
        Ok(())
    }

    /// Bind an exact skill version to an agent. Rebinding the same skill id is
    /// idempotent even without an Idempotency-Key.
    pub async fn bind_skill(&self, req: BindSkillRequest) -> Result<AgentSkillBinding> {
        let idem_full_key = req.idempotency_key.as_ref().map(|k| {
            format!(
                "{}::{}::{}::{}",
                req.tenant_id, "skill_bind", req.agent_id, k
            )
        });
        let req_hash = hash_skill_bind_request(&req);
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                return verify_json_replay(&rec, req_hash);
            }
        }

        let binding = {
            let mut s = self.state.lock().await;
            if let Some(ref full) = idem_full_key {
                if let Some(rec) = self.idem.lookup(full) {
                    drop(s);
                    return verify_json_replay(&rec, req_hash);
                }
            }
            let agent = s
                .agents
                .get(req.agent_id.as_str())
                .filter(|a| {
                    a.deleted_at_ms.is_none() && a.tenant_id.as_str() == req.tenant_id.as_str()
                })
                .ok_or_else(|| Error::new(ErrorCode::AgentNotFound, format!("{}", req.agent_id)))?;
            let skill = s
                .skills
                .get(req.skill_id.as_str())
                .filter(|skill| {
                    skill.deleted_at_ms.is_none()
                        && skill.tenant_id.as_str() == req.tenant_id.as_str()
                })
                .ok_or_else(|| Error::new(ErrorCode::SkillNotFound, format!("{}", req.skill_id)))?;
            let key = (
                agent.agent_id.as_str().to_string(),
                skill.skill_id.as_str().to_string(),
            );
            if let Some(existing) = s.agent_skill_bindings.get(&key) {
                return Ok(existing.clone());
            }
            let policy = policy_for_tenant_in_state(&s, &req.tenant_id);
            if let Some(max) = policy.quota.max_skill_bindings_per_agent {
                let live_bindings = s
                    .agent_skill_bindings
                    .values()
                    .filter(|binding| {
                        binding.tenant_id.as_str() == req.tenant_id.as_str()
                            && binding.agent_id.as_str() == req.agent_id.as_str()
                            && binding.deleted_at_ms.is_none()
                    })
                    .count();
                if live_bindings >= max {
                    return Err(Error::new(
                        ErrorCode::PolicyDenied,
                        format!(
                            "policy_version {} denies skill bind: skill binding quota {max} exhausted",
                            policy.version
                        ),
                    ));
                }
            }
            let binding =
                AgentSkillBinding::create(req.tenant_id.clone(), req.agent_id.clone(), skill);
            if let Some(p) = &self.persist {
                p.put_agent_skill_binding(&binding).map_err(internal)?;
            }
            s.agent_skill_bindings.insert(key, binding.clone());
            binding
        };

        if let Some(full) = idem_full_key {
            let rec = IdempotencyRecord {
                payload_hash: req_hash,
                result_json: serde_json::to_string(&binding).unwrap(),
                created_at_ms: crate::now_ms(),
            };
            if let Some(p) = &self.persist {
                p.put_idempotency(&full, &rec).map_err(internal)?;
            }
            self.idem.put(full, rec);
        }
        Ok(binding)
    }

    pub async fn list_agent_skills(
        &self,
        tenant_id: &Id,
        agent_id: &Id,
    ) -> Result<Vec<AgentSkillBinding>> {
        let s = self.state.lock().await;
        let tenant_live = s
            .tenants
            .get(tenant_id.as_str())
            .is_some_and(|t| t.deleted_at_ms.is_none());
        let agent_live = s.agents.get(agent_id.as_str()).is_some_and(|a| {
            tenant_live && a.deleted_at_ms.is_none() && a.tenant_id.as_str() == tenant_id.as_str()
        });
        if !agent_live {
            return Err(Error::new(
                ErrorCode::AgentNotFound,
                format!("{}", agent_id),
            ));
        }
        Ok(s.agent_skill_bindings
            .values()
            .filter(|b| {
                b.deleted_at_ms.is_none()
                    && b.tenant_id.as_str() == tenant_id.as_str()
                    && b.agent_id.as_str() == agent_id.as_str()
            })
            .cloned()
            .collect())
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

    pub async fn backpressure_for_tenant(&self, tenant_id: &Id) -> Result<BackpressureReport> {
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
            let active_sessions = s
                .sessions
                .values()
                .filter(|sess| {
                    sess.tenant_id.as_str() == tenant_id.as_str()
                        && sess.deleted_at_ms.is_none()
                        && sess.status == crate::session::Status::Active
                })
                .count();
            report.push_if_over("active_sessions", active_sessions as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_agents {
            let live_agents = s
                .agents
                .values()
                .filter(|a| a.tenant_id.as_str() == tenant_id.as_str() && a.deleted_at_ms.is_none())
                .count();
            report.push_if_over("agents", live_agents as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_records {
            let live_records = s
                .records
                .values()
                .filter(|r| {
                    r.tenant_id.as_str() == tenant_id.as_str()
                        && r.deleted_at_ms.is_none()
                        && !r.is_expired()
                })
                .count();
            report.push_if_over("records", live_records as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_artifacts {
            let live_artifacts = s
                .artifacts
                .values()
                .filter(|a| a.tenant_id.as_str() == tenant_id.as_str() && a.deleted_at_ms.is_none())
                .count();
            report.push_if_over("artifacts", live_artifacts as u64, soft as u64);
        }

        if let Some(soft) = policy.quota.soft_max_artifact_storage_bytes {
            let live_bytes: u64 = s
                .artifacts
                .values()
                .filter(|a| a.tenant_id.as_str() == tenant_id.as_str() && a.deleted_at_ms.is_none())
                .map(|a| a.bytes)
                .sum();
            report.push_if_over("artifact_storage", live_bytes, soft);
        }

        if let Some(soft) = policy.quota.soft_max_skills {
            let live_skills = s
                .skills
                .values()
                .filter(|skill| {
                    skill.tenant_id.as_str() == tenant_id.as_str() && skill.deleted_at_ms.is_none()
                })
                .count();
            report.push_if_over("skills", live_skills as u64, soft as u64);
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
        if let Some(soft) = policy.quota.soft_max_skill_bindings_per_agent {
            let live_bindings = s
                .agent_skill_bindings
                .values()
                .filter(|binding| {
                    binding.tenant_id.as_str() == tenant_id.as_str()
                        && binding.agent_id.as_str() == session.agent_id.as_str()
                        && binding.deleted_at_ms.is_none()
                })
                .count();
            report.push_if_over("skill_bindings", live_bindings as u64, soft as u64);
        }
        Ok(report)
    }

    /// Create an artifact: write the bytes to the content backend, persist
    /// the metadata, return the metadata record. Tenant must exist.
    ///
    /// On success the bytes are durable (or in-memory equivalent) and the
    /// metadata is committed atomically with the secondary `by_sha256` index.
    ///
    /// Atomicity caveat: the content write and the metadata write are
    /// **separate** persistence steps. If the metadata write fails after the
    /// content write succeeds we have an orphan blob; V2.4 GC will reap it
    /// (TODO: link the reaper plan once it lands). This task accepts the
    /// orphan window in exchange for a simpler V2.1 surface.
    pub async fn create_artifact(&self, req: NewArtifact, bytes: Vec<u8>) -> Result<Artifact> {
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
            if !s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|t| t.deleted_at_ms.is_none())
            {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
            if let Some(max) = policy.quota.max_artifacts {
                let live_artifacts = s
                    .artifacts
                    .values()
                    .filter(|a| {
                        a.tenant_id.as_str() == req.tenant_id.as_str() && a.deleted_at_ms.is_none()
                    })
                    .count();
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
                let live_bytes: u64 = s
                    .artifacts
                    .values()
                    .filter(|a| {
                        a.tenant_id.as_str() == req.tenant_id.as_str() && a.deleted_at_ms.is_none()
                    })
                    .map(|a| a.bytes)
                    .sum();
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

        let crypto_context = ContentCryptoContext {
            tenant_id: req.tenant_id.as_str(),
            artifact_id: artifact_id.as_str(),
            key: &content_key,
        };
        let encrypted_bytes = self
            .content_crypto
            .encrypt(&crypto_context, &bytes)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("content encrypt failed: {e}")))?;
        let stored_bytes = encode_keyed_envelope(&content_key, &encrypted_bytes);
        let _put = self
            .content
            .put(req.tenant_id.as_str(), artifact_id.as_str(), &stored_bytes)
            .await
            .map_err(|e| Error::new(ErrorCode::Internal, format!("content put failed: {e}")))?;

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

        // Persist metadata, then update the in-memory cache. If persist
        // fails the bytes are an orphan (see atomicity caveat above).
        if let Some(p) = &self.persist {
            p.put_artifact(&artifact).map_err(internal)?;
        }
        let mut s = self.state.lock().await;
        s.artifacts
            .insert(artifact.artifact_id.as_str().to_string(), artifact.clone());
        Ok(artifact)
    }

    /// Look up artifact metadata, scoped to `tenant_id`. Returns
    /// `ArtifactNotFound` if the id is unknown, soft-deleted, or owned by a
    /// different tenant — we collapse those cases (mirroring the
    /// agent/session lookups) so the caller in tenant A cannot probe
    /// tenant B's artifact ids by error-code differential.
    pub async fn get_artifact(&self, tenant_id: &Id, artifact_id: &Id) -> Result<Artifact> {
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
        let mut artifacts = s
            .artifacts
            .values()
            .filter(|artifact| {
                artifact.deleted_at_ms.is_none()
                    && artifact.tenant_id.as_str() == tenant_id.as_str()
            })
            .cloned()
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
        let (content_key, ciphertext) = match decode_keyed_envelope(&stored_bytes)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("content envelope failed: {e}")))?
        {
            Some((recorded_key, ciphertext)) => (recorded_key, ciphertext),
            None => (
                key_from_parts(&art.content_key_id, &art.content_key_version)
                    .unwrap_or(current_key),
                stored_bytes,
            ),
        };
        let crypto_context = ContentCryptoContext {
            tenant_id: tenant_id.as_str(),
            artifact_id: artifact_id.as_str(),
            key: &content_key,
        };
        let plaintext = self
            .content_crypto
            .decrypt(&crypto_context, &ciphertext)
            .map_err(|e| Error::new(ErrorCode::Internal, format!("content decrypt failed: {e}")))?;
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
        let (session_candidates, artifact_candidates) =
            {
                let s = self.state.lock().await;
                let mut candidates = Vec::new();
                for session in s.sessions.values() {
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
                    s.artifacts
                        .values()
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
            let s = self.state.lock().await;
            let mut tenants: Vec<(Id, Priority)> = s
                .tenants
                .values()
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
            report.gc.skills_purged += gc.skills_purged;
            report.gc.skill_bindings_purged += gc.skill_bindings_purged;
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
                    if let Some(p) = &self.persist {
                        report.events_purged += p
                            .delete_session_events(
                                session.tenant_id.as_str(),
                                session.session_id.as_str(),
                            )
                            .map_err(internal)?;
                        p.delete_session(&session).map_err(internal)?;
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
                    p.delete_agent(&agent).map_err(internal)?;
                }
                let binding_keys: Vec<(String, String)> = s
                    .agent_skill_bindings
                    .iter()
                    .filter(|(_, binding)| {
                        binding.tenant_id.as_str() == agent.tenant_id.as_str()
                            && binding.agent_id.as_str() == agent.agent_id.as_str()
                    })
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in binding_keys {
                    let Some(binding) = s.agent_skill_bindings.get(&key).cloned() else {
                        continue;
                    };
                    if let Some(p) = &self.persist {
                        p.delete_agent_skill_binding(&binding).map_err(internal)?;
                    }
                    s.agent_skill_bindings.remove(&key);
                    report.skill_bindings_purged += 1;
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
                let policy = policy_for_tenant_in_state(&s, &session.tenant_id);
                if !gc_work_allowed(&mut report, &policy) {
                    break;
                }
                if let Some(p) = &self.persist {
                    report.events_purged += p
                        .delete_session_events(
                            session.tenant_id.as_str(),
                            session.session_id.as_str(),
                        )
                        .map_err(internal)?;
                    p.delete_session(&session).map_err(internal)?;
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
            let skill_ids: Vec<String> = s
                .skills
                .iter()
                .filter_map(|(id, skill)| {
                    if tenant_scope
                        .is_some_and(|tenant_id| skill.tenant_id.as_str() != tenant_id.as_str())
                    {
                        return None;
                    }
                    let tenant = s.tenants.get(skill.tenant_id.as_str())?;
                    let policy = RuntimePolicy::from_json(&tenant.policy);
                    let deleted_at = skill.deleted_at_ms.or(tenant.deleted_at_ms)?;
                    if policy.retention.legal_hold
                        || now_ms.saturating_sub(deleted_at) < policy.retention.retention_ms
                    {
                        return None;
                    }
                    Some(id.clone())
                })
                .collect();

            for skill_id in skill_ids {
                let Some(skill) = s.skills.get(&skill_id).cloned() else {
                    continue;
                };
                let policy = policy_for_tenant_in_state(&s, &skill.tenant_id);
                if !gc_work_allowed(&mut report, &policy) {
                    break;
                }
                let binding_keys: Vec<(String, String)> = s
                    .agent_skill_bindings
                    .iter()
                    .filter(|(_, binding)| {
                        binding.tenant_id.as_str() == skill.tenant_id.as_str()
                            && binding.skill_id.as_str() == skill.skill_id.as_str()
                    })
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in binding_keys {
                    let Some(binding) = s.agent_skill_bindings.get(&key).cloned() else {
                        continue;
                    };
                    if let Some(p) = &self.persist {
                        p.delete_agent_skill_binding(&binding).map_err(internal)?;
                    }
                    s.agent_skill_bindings.remove(&key);
                    report.skill_bindings_purged += 1;
                }
                if let Some(p) = &self.persist {
                    p.delete_skill(&skill).map_err(internal)?;
                }
                s.skill_uniques.remove(&skill_unique_state_key(
                    skill.tenant_id.as_str(),
                    &skill.name,
                    &skill.version,
                ));
                s.skills.remove(&skill_id);
                report.skills_purged += 1;
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
                    p.delete_record(&record).map_err(internal)?;
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
                p.delete_artifact(&current).map_err(internal)?;
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
                        .any(|artifact| artifact.tenant_id.as_str() == tenant.tenant_id.as_str())
                    || s.skills
                        .values()
                        .any(|skill| skill.tenant_id.as_str() == tenant.tenant_id.as_str());
                if has_children {
                    continue;
                }
                if let Some(p) = &self.persist {
                    p.delete_tenant(&tenant).map_err(internal)?;
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
        self.refresh_replicated_state().await?;

        // Compose the full idempotency scope key BEFORE locking state, so we can
        // short-circuit cheaply on retry.
        let idem_full_key = req
            .idempotency_key
            .as_ref()
            .map(|k| format!("{}::{}::{}::{}", req.tenant_id, "append", req.session_id, k));

        // Optimistic check before locking — fast path for retries that find a
        // committed result without contending on the state mutex.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                return verify_idem_replay(&rec, &req);
            }
        }

        let policy = self
            .resolve_policy_for_agent(&req.tenant_id, &req.agent_id)
            .await?;

        // Inline payload size guard. Done after the idempotency short-circuit
        // (a retry that already committed should replay, not re-validate) but
        // before we lock state, so a misbehaving caller can't churn the mutex
        // by hammering oversized payloads. The MAX_INLINE_EVENT_BYTES limit is
        let payload_len = serde_json::to_vec(&req.payload)
            .map(|v| v.len())
            .unwrap_or(usize::MAX);
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

        let session_commit_lock = self.session_commit_lock(&req.session_id);
        let _session_commit_guard = session_commit_lock.lock().await;
        let s = self.state.lock().await;

        // Re-check under the state lock to close the concurrent-append race:
        // another task could have committed our key between the optimistic check
        // and the lock acquisition above.
        if let Some(ref full) = idem_full_key {
            if let Some(rec) = self.idem.lookup(full) {
                drop(s);
                return verify_idem_replay(&rec, &req);
            }
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
            if let Some(skill_id) = &sr.skill_id {
                let exists = (skill_id.kind() == crate::id::IdKind::Skill)
                    && s.skills.get(skill_id.as_str()).is_some_and(|skill| {
                        skill.deleted_at_ms.is_none()
                            && skill.tenant_id.as_str() == req.tenant_id.as_str()
                    });
                if !exists {
                    return Err(Error::new(
                        ErrorCode::SkillNotFound,
                        format!("{}", skill_id),
                    ));
                }
            }
        }

        if req.kind == "skill_used" {
            let skill_id_raw = req
                .payload
                .get("skill_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::SkillNotFound,
                        "skill_used payload must include skill_id",
                    )
                })?;
            let version = req
                .payload
                .get("version")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::SkillNotFound,
                        "skill_used payload must include version",
                    )
                })?;
            let skill_id = Id::parse_kind(skill_id_raw, crate::id::IdKind::Skill)
                .map_err(|_| Error::new(ErrorCode::SkillNotFound, skill_id_raw))?;
            let exact = s.skills.get(skill_id.as_str()).is_some_and(|skill| {
                skill.deleted_at_ms.is_none()
                    && skill.tenant_id.as_str() == req.tenant_id.as_str()
                    && skill.version == version
            });
            if !exact {
                return Err(Error::new(ErrorCode::SkillNotFound, skill_id_raw));
            }
        }

        let metadata_key = self.resolve_key(&req.tenant_id, KeyOperation::MetadataWrite)?;

        let (seq, _prior_policy_version, session_snapshot) = {
            // Mint the seq from the current session snapshot but do not make
            // the mutation visible until the durable batch succeeds. The
            // per-session commit lock above preserves same-session ordering
            // while allowing unrelated sessions to persist concurrently.
            let session = s
                .sessions
                .get(req.session_id.as_str())
                .expect("session presence checked above");
            let mut snapshot = session.clone();
            if snapshot.lifecycle != Lifecycle::Warm {
                snapshot.lifecycle = Lifecycle::Warm;
                snapshot.warm_head_from_seq = snapshot.next_seq;
            }
            let seq = snapshot.next_seq;
            let pv = snapshot.policy_version;
            snapshot.next_seq += 1;
            snapshot.last_active_at_ms = crate::now_ms();
            snapshot.policy_version = policy.version;
            (seq, pv, snapshot)
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
            ),
            result_json: serde_json::to_string(&resp).unwrap(),
            created_at_ms: crate::now_ms(),
        });

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
            if let Err(e) = p.append_batch(&envelope, &session_snapshot, pair) {
                return Err(internal(e));
            }
        }

        let mut s = self.state.lock().await;
        s.sessions
            .insert(req.session_id.as_str().to_string(), session_snapshot);
        // In-memory backend: keep the events map populated, and mirror the
        // `event_by_id` secondary-index entry that the KV backend writes via
        // `append_batch`. The persisted backend keeps this in memory too so
        // same-process reads can avoid a storage lookup.
        let key = (req.session_id.as_str().to_string(), seq);
        s.event_by_id.insert(
            envelope.event_id.as_str().to_string(),
            (envelope.session_id.as_str().to_string(), envelope.seq),
        );
        s.events.insert(key, envelope);

        // Record the idempotency result before releasing the per-session
        // commit lock, so a same-session retry with the same key observes this
        // record on its under-lock re-check and replays instead of committing a
        // second event. Lock order is (state -> idem) and the idempotency store
        // never reaches back into state, so there is no deadlock risk despite
        // holding both briefly.
        if let (Some(full), Some(rec)) = (idem_full_key, idem_record) {
            self.idem.put(full, rec);
        }
        drop(s);

        Ok(resp)
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
        let tenant_id = req.tenant_id.clone();
        let session_id = req.session_id.clone();
        let policy = self
            .resolve_policy_for_agent(&req.tenant_id, &req.agent_id)
            .await?;
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
        let appended = self.append(req).await?;

        // Fetch only the tail window needed for context. A full-session fetch
        // makes append+fetch latency grow with session length and serializes
        // far more storage work than the API requires.
        let last_seq_count = u64::try_from(last).unwrap_or(u64::MAX);
        let from_seq = appended
            .seq
            .saturating_add(1)
            .saturating_sub(last_seq_count);
        let mut recent = if last == 0 {
            Vec::new()
        } else {
            self.fetch_from_seq(FetchRequest {
                tenant_id,
                session_id,
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

        Ok(AppendAndFetchResponse {
            event_id: appended.event_id,
            seq: appended.seq,
            recent,
        })
    }

    pub async fn build_context(&self, req: ContextRequest) -> Result<ContextResponse> {
        let session = self.get_session(&req.tenant_id, &req.session_id).await?;
        let include_events = req.include.contains(&ContextInclude::RecentEvents);
        let include_records = req.include.contains(&ContextInclude::Records);
        let include_skills = req.include.contains(&ContextInclude::Skills);
        let include_policy = req.include.contains(&ContextInclude::Policy);

        let mut response = ContextResponse {
            session_id: req.session_id.clone(),
            events: Vec::new(),
            records: Vec::new(),
            skills: Vec::new(),
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

        if include_skills {
            let skills = self
                .list_agent_skills(&req.tenant_id, &session.agent_id)
                .await?;
            for skill in skills {
                if push_budgeted(&mut response.bytes_used, req.max_bytes, &skill) {
                    response.skills.push(skill);
                } else {
                    response.truncated = true;
                    break;
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

    pub async fn watch(&self, req: WatchRequest) -> Result<Vec<ChangefeedItem>> {
        let session_ids: Vec<Id> = {
            let s = self.state.lock().await;
            let tenant_live = s
                .tenants
                .get(req.tenant_id.as_str())
                .is_some_and(|tenant| tenant.deleted_at_ms.is_none());
            if !tenant_live {
                return Err(Error::new(
                    ErrorCode::TenantNotFound,
                    format!("{}", req.tenant_id),
                ));
            }
            match req.scope {
                WatchScope::Session => {
                    let session_id = req.session_id.clone().ok_or_else(|| {
                        Error::new(ErrorCode::SessionNotFound, "watch session_id is required")
                    })?;
                    let live = s.sessions.get(session_id.as_str()).is_some_and(|session| {
                        session.deleted_at_ms.is_none()
                            && session.tenant_id.as_str() == req.tenant_id.as_str()
                    });
                    if !live {
                        return Err(Error::new(
                            ErrorCode::SessionNotFound,
                            format!("{}", session_id),
                        ));
                    }
                    vec![session_id]
                }
                WatchScope::Tenant => s
                    .sessions
                    .values()
                    .filter(|session| {
                        session.deleted_at_ms.is_none()
                            && session.tenant_id.as_str() == req.tenant_id.as_str()
                    })
                    .map(|session| session.session_id.clone())
                    .collect(),
                WatchScope::Agent => {
                    let agent_id = req.agent_id.clone().ok_or_else(|| {
                        Error::new(ErrorCode::AgentNotFound, "watch agent_id is required")
                    })?;
                    let live = s.agents.get(agent_id.as_str()).is_some_and(|agent| {
                        agent.deleted_at_ms.is_none()
                            && agent.tenant_id.as_str() == req.tenant_id.as_str()
                    });
                    if !live {
                        return Err(Error::new(
                            ErrorCode::AgentNotFound,
                            format!("{}", agent_id),
                        ));
                    }
                    s.sessions
                        .values()
                        .filter(|session| {
                            session.deleted_at_ms.is_none()
                                && session.tenant_id.as_str() == req.tenant_id.as_str()
                                && session.agent_id.as_str() == agent_id.as_str()
                        })
                        .map(|session| session.session_id.clone())
                        .collect()
                }
            }
        };

        let mut events = Vec::new();
        for session_id in session_ids {
            events.extend(
                self.fetch_from_seq(FetchRequest {
                    tenant_id: req.tenant_id.clone(),
                    session_id,
                    from_seq: 0,
                    limit: usize::MAX,
                })
                .await?,
            );
        }
        events.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.session_id.as_str().cmp(right.session_id.as_str()))
                .then_with(|| left.seq.cmp(&right.seq))
        });

        let mut out = Vec::new();
        for event in events {
            let cursor = event_cursor(&event);
            if req
                .from_cursor
                .as_ref()
                .is_some_and(|from| cursor.as_str() <= from.as_str())
            {
                continue;
            }
            out.push(ChangefeedItem { cursor, event });
            if out.len() >= req.limit {
                break;
            }
        }
        Ok(out)
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
        if let Some(manifest) = &session.archive_manifest {
            let mut archived = self.read_session_archive_events(manifest).await?;
            archived.retain(|event| event.seq >= req.from_seq);
            out.extend(archived);
            if out.len() >= req.limit {
                out.truncate(req.limit);
                return Ok(out);
            }
        } else {
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
        }

        let remaining = req.limit.saturating_sub(out.len());
        if remaining == 0 {
            return Ok(out);
        }
        let hot_from_seq = req.from_seq.max(session.warm_head_from_seq);
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
        let from_seq = {
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
            session.next_seq.saturating_sub(limit)
        };
        self.fetch_from_seq(FetchRequest {
            tenant_id,
            session_id,
            from_seq,
            limit,
        })
        .await
    }
}

/// V2.2 search request for `Runtime::search_records`. Either `text`,
/// `embedding`, or both may be set; if both are absent, the runtime
/// returns every live record under the `(tenant_id, namespace)` scope
/// up to `limit`.
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
            limit,
            min_visible_record_commit_seq: None,
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
        let record = SearchableRecord::create(req);
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

        // Persist with the state lock still held — concurrent
        // `write_record`s for the same tenant + namespace mint distinct
        // DocIds (counter is under the lock) and the persist order
        // matches the index-insert order.
        let commit_seq = next_record_commit_seq(&mut s);
        if let Some(p) = &self.persist {
            if let Err(e) = p.put_record_with_commit_seq(&record, Some(commit_seq)) {
                // Persist failed: drop the cache + DocId mapping we
                // were about to install. The index inserts above are
                // not rolled back — see the long comment above.
                drop(s);
                return Err(internal(e));
            }
        }
        s.records
            .insert(record.record_id.as_str().to_string(), record.clone());
        s.doc_to_record
            .insert(doc_id, record.record_id.as_str().to_string());
        s.record_to_doc
            .insert(record.record_id.as_str().to_string(), doc_id);
        s.record_commit_seq
            .insert(record.record_id.as_str().to_string(), commit_seq);
        s.local_record_index_watermark = commit_seq;
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
            s.global_record_index_watermark = commit_seq;
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
        }
    }

    pub async fn apply_record_index_outbox_once(&self) -> Result<usize> {
        let Some(p) = &self.persist else {
            return Ok(0);
        };
        let mut mutations = p.scan_all_record_index_mutations().map_err(internal)?;
        mutations.sort_by_key(|mutation| mutation.commit_seq);

        let mut applied = 0usize;
        for mutation in mutations {
            let mut s = self.state.lock().await;
            let mut advanced = false;
            if mutation.commit_seq > s.global_record_index_watermark {
                let global_mutation = match mutation.op {
                    RecordIndexMutationOp::Upsert => {
                        s.records.get(&mutation.record_id).map(|record| {
                            global_index_mutation_for_record(
                                record,
                                mutation.commit_seq,
                                GlobalIndexMutationOp::Upsert,
                            )
                        })
                    }
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
                        s.local_record_index_watermark = mutation.commit_seq;
                        applied += 1;
                        continue;
                    };
                    if record.deleted_at_ms.is_some() || record.is_expired() {
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
                }
                RecordIndexMutationOp::Delete => {
                    if let Some(doc) = s.record_to_doc.remove(&mutation.record_id) {
                        s.doc_to_record.remove(&doc);
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
        tokio::spawn(async move {
            loop {
                if let Err(error) = self.apply_record_index_outbox_once().await {
                    let mut s = self.state.lock().await;
                    s.record_index_worker_failures_total += 1;
                    drop(s);
                    tracing::warn!(?error, "record index outbox replay failed");
                }
                tokio::time::sleep(interval).await;
            }
        })
    }

    /// Search the (tenant, namespace) scope. If `text` is set, run a
    /// BM25 text search; if `embedding` is set, run a cosine
    /// nearest-neighbor search; if both, run both and union by record
    /// (keeping the higher score per record_id). If neither is set,
    /// return every live record in the namespace, capped at `limit`.
    ///
    /// Tag and source post-filters are applied after the index step.
    /// Soft-deleted and expired records are filtered out.
    pub async fn search_records(&self, req: SearchRequest) -> Result<Vec<SearchableRecord>> {
        if req.sharding_key.is_none() {
            return self.search_records_global(req).await;
        }
        self.search_records_local(req).await
    }

    async fn search_records_global(&self, req: SearchRequest) -> Result<Vec<SearchableRecord>> {
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
        let result = self.global_record_index.search(GlobalIndexQuery {
            tenant_id: req.tenant_id.as_str().to_string(),
            namespace: req.namespace.clone(),
            terms,
            min_visible_commit_seq: req.min_visible_record_commit_seq,
            limit: req.limit.max(1),
        })?;

        if result.stale {
            return self.search_records_local(req).await;
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
        candidates.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));

        Ok(filter_search_candidates(candidates, &req))
    }

    async fn search_records_local(&self, req: SearchRequest) -> Result<Vec<SearchableRecord>> {
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
        let slack = req.limit.saturating_mul(2).clamp(req.limit, 1024);

        // Collect index hits as (doc_id, score). When neither text
        // nor embedding is supplied we skip the indexes and return
        // every live record in the namespace (subject to filters and
        // limit) by walking the in-memory record cache directly.
        let mut hits: BTreeMap<DocId, f32> = BTreeMap::new();
        if let Some(query) = req.text.as_deref() {
            let text = self.text_index.lock().expect("text index lock");
            for (doc, score) in text.search(&path, query, slack) {
                hits.entry(doc)
                    .and_modify(|s| *s = s.max(score))
                    .or_insert(score);
            }
        }
        if let Some(emb) = req.embedding.as_deref() {
            let vec = self.vector_index.lock().expect("vector index lock");
            for (doc, score) in vec.knn(&path, emb, slack) {
                hits.entry(doc)
                    .and_modify(|s| *s = s.max(score))
                    .or_insert(score);
            }
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
        let mut candidates: Vec<(SearchableRecord, f32)> = if req.text.is_some()
            || req.embedding.is_some()
        {
            let mut ranked: Vec<(DocId, f32)> = hits.into_iter().collect();
            ranked.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
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
                .map(|r| (r.clone(), 0.0))
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
            candidates
                .sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        }
        drop(s);

        Ok(filter_search_candidates(candidates, &req))
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

fn trim_recent_to_budget(recent: &mut Vec<EventEnvelope>, max_bytes: usize) {
    if max_bytes == 0 {
        recent.clear();
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

fn event_cursor(event: &EventEnvelope) -> String {
    format!(
        "{:020}:{}:{:020}",
        event.created_at_ms, event.session_id, event.seq
    )
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
        && match_id(&rec.skill_id, &filter.skill_id)
}

fn filter_search_candidates(
    candidates: Vec<(SearchableRecord, f32)>,
    req: &SearchRequest,
) -> Vec<SearchableRecord> {
    let mut out = Vec::new();
    for (r, _score) in candidates {
        if r.tenant_id.as_str() != req.tenant_id.as_str() {
            continue;
        }
        if r.namespace != req.namespace {
            continue;
        }
        if r.deleted_at_ms.is_some() || r.is_expired() {
            continue;
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
        out.push(r);
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
    true
}

fn repair_score(r: &SearchableRecord, req: &SearchRequest) -> f32 {
    let mut score = 0.0f32;
    if let Some(query) = req.text.as_deref() {
        if let Some(text) = record_indexable_text(r) {
            score = score.max(repair_text_score(&text, query));
        }
    }
    if let Some(query) = req.embedding.as_deref() {
        if r.policy.vector_allowed {
            if let Some(embedding) = r.embedding.as_ref() {
                score = score.max(cosine_similarity(embedding, query));
            }
        }
    }
    score
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

fn hash_skill_register_request(req: &NewSkill) -> u64 {
    hash_json(&serde_json::json!({
        "tenant_id": req.tenant_id.as_str(),
        "name": req.name,
        "version": req.version,
        "manifest": req.manifest,
        "artifact_refs": req.artifact_refs,
    }))
}

fn hash_skill_bind_request(req: &BindSkillRequest) -> u64 {
    hash_json(&serde_json::json!({
        "tenant_id": req.tenant_id.as_str(),
        "agent_id": req.agent_id.as_str(),
        "skill_id": req.skill_id.as_str(),
    }))
}

fn verify_json_replay<T>(rec: &IdempotencyRecord, expected_hash: u64) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if rec.payload_hash != expected_hash {
        return Err(Error::new(
            ErrorCode::IdempotencyConflict,
            "request differs for the same idempotency key",
        ));
    }
    Ok(serde_json::from_str(&rec.result_json).unwrap())
}

/// Map a storage-layer error into a V2 `Internal` error. The persist layer
/// returns `agentdb_storage::Error`; surfacing it as `Internal` keeps the
/// Error code surface stable.
fn internal(e: agentdb_storage::Error) -> Error {
    Error::new(ErrorCode::Internal, e.to_string())
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
