//! Shared HTTP handler state.
//!
//! Holds the runtime handle so axum extractors can pull it off the router.
//! Cloning `AppState` is cheap — `Runtime` is wrapped in `Arc`.

use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    Arc, OnceLock,
};
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::Runtime;
use agentdb_meta::MetaStore;
use agentdb_storage::{AsyncMirrorContentBackend, RaftRsSnapshotTransfer, RaftRsWireMessage};

use crate::raft_ingress::RaftRsGroupRegistry;
use crate::routing_http::RouteRuntime;

#[derive(Debug)]
pub struct ApiJsonResponseProfile {
    interval_ms: u64,
    last_log_ms: AtomicU64,
    responses: AtomicU64,
    bytes: AtomicU64,
    serialize_nanos: AtomicU64,
}

impl ApiJsonResponseProfile {
    fn from_env() -> Option<Self> {
        let interval_ms = std::env::var("AGENTDB_API_JSON_PROFILE_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|value| *value > 0)?;
        Some(Self {
            interval_ms,
            last_log_ms: AtomicU64::new(now_ms()),
            responses: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            serialize_nanos: AtomicU64::new(0),
        })
    }

    pub fn record(&self, bytes: usize, serialize_nanos: u128) {
        self.responses.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.serialize_nanos.fetch_add(
            serialize_nanos.min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );

        let now = now_ms();
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

        let responses = self.responses.swap(0, Ordering::Relaxed);
        if responses == 0 {
            return;
        }
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        let serialize_nanos = self.serialize_nanos.swap(0, Ordering::Relaxed);
        tracing::info!(
            responses,
            bytes,
            avg_bytes = bytes as f64 / responses as f64,
            avg_serialize_us = serialize_nanos as f64 / responses as f64 / 1_000.0,
            "api json response profile"
        );
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

pub trait RaftRsIngress: Send + Sync {
    fn step_wire_message(&self, message: RaftRsWireMessage) -> agentdb_storage::Result<()>;

    fn install_snapshot_transfer(
        &self,
        to: u64,
        transfer: RaftRsSnapshotTransfer,
    ) -> agentdb_storage::Result<Vec<u8>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProxyPolicy {
    pub timeout_ms: u64,
    pub max_response_bytes: u64,
}

impl Default for ProxyPolicy {
    fn default() -> Self {
        Self {
            timeout_ms: 5_000,
            max_response_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct DrainState {
    node_id: Arc<str>,
    draining: Arc<AtomicBool>,
    drained_at_ms: Arc<AtomicI64>,
}

impl DrainState {
    fn new(node_id: impl Into<Arc<str>>) -> Self {
        Self {
            node_id: node_id.into(),
            draining: Arc::new(AtomicBool::new(false)),
            drained_at_ms: Arc::new(AtomicI64::new(0)),
        }
    }

    fn with_node_id(&self, node_id: impl Into<Arc<str>>) -> Self {
        Self {
            node_id: node_id.into(),
            draining: self.draining.clone(),
            drained_at_ms: self.drained_at_ms.clone(),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    pub fn drained_at_ms(&self) -> Option<i64> {
        let value = self.drained_at_ms.load(Ordering::SeqCst);
        (value > 0).then_some(value)
    }

    pub fn activate(&self, now_ms: i64) -> i64 {
        self.draining.store(true, Ordering::SeqCst);
        let existing = self.drained_at_ms.load(Ordering::SeqCst);
        if existing > 0 {
            existing
        } else {
            self.drained_at_ms.store(now_ms, Ordering::SeqCst);
            now_ms
        }
    }

    pub fn deactivate(&self) {
        self.draining.store(false, Ordering::SeqCst);
        self.drained_at_ms.store(0, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct SchedulerStatus {
    leader_node_id: Arc<OnceLock<String>>,
    leader_epoch: Arc<AtomicI64>,
    leader_expires_at_ms: Arc<AtomicI64>,
    is_leader: Arc<AtomicBool>,
    dead_nodes_marked_total: Arc<AtomicU64>,
    tablet_reassignments_total: Arc<AtomicU64>,
    local_tablet_log_compactions_total: Arc<AtomicU64>,
    local_tablet_log_compaction_failures_total: Arc<AtomicU64>,
    local_tablet_log_compaction_last_tick_at_ms: Arc<AtomicI64>,
    last_tick_at_ms: Arc<AtomicI64>,
}

impl SchedulerStatus {
    pub fn new() -> Self {
        Self {
            leader_node_id: Arc::new(OnceLock::new()),
            leader_epoch: Arc::new(AtomicI64::new(0)),
            leader_expires_at_ms: Arc::new(AtomicI64::new(0)),
            is_leader: Arc::new(AtomicBool::new(false)),
            dead_nodes_marked_total: Arc::new(AtomicU64::new(0)),
            tablet_reassignments_total: Arc::new(AtomicU64::new(0)),
            local_tablet_log_compactions_total: Arc::new(AtomicU64::new(0)),
            local_tablet_log_compaction_failures_total: Arc::new(AtomicU64::new(0)),
            local_tablet_log_compaction_last_tick_at_ms: Arc::new(AtomicI64::new(0)),
            last_tick_at_ms: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn snapshot(&self) -> SchedulerStatusSnapshot {
        SchedulerStatusSnapshot {
            leader_node_id: self.leader_node_id.get().cloned(),
            leader_epoch: self.leader_epoch.load(Ordering::SeqCst),
            leader_expires_at_ms: self.leader_expires_at_ms.load(Ordering::SeqCst),
            is_leader: self.is_leader.load(Ordering::SeqCst),
            dead_nodes_marked_total: self.dead_nodes_marked_total.load(Ordering::SeqCst),
            tablet_reassignments_total: self.tablet_reassignments_total.load(Ordering::SeqCst),
            local_tablet_log_compactions_total: self
                .local_tablet_log_compactions_total
                .load(Ordering::SeqCst),
            local_tablet_log_compaction_failures_total: self
                .local_tablet_log_compaction_failures_total
                .load(Ordering::SeqCst),
            local_tablet_log_compaction_last_tick_at_ms: self
                .local_tablet_log_compaction_last_tick_at_ms
                .load(Ordering::SeqCst),
            last_tick_at_ms: self.last_tick_at_ms.load(Ordering::SeqCst),
        }
    }

    pub fn update_leader(&self, node_id: &str, epoch: i64, expires_at_ms: i64, is_leader: bool) {
        let _ = self.leader_node_id.set(node_id.to_string());
        self.leader_epoch.store(epoch, Ordering::SeqCst);
        self.leader_expires_at_ms
            .store(expires_at_ms, Ordering::SeqCst);
        self.is_leader.store(is_leader, Ordering::SeqCst);
    }

    pub fn set_not_leader(&self) {
        self.is_leader.store(false, Ordering::SeqCst);
    }

    pub fn add_dead_nodes_marked(&self, n: u64) {
        self.dead_nodes_marked_total.fetch_add(n, Ordering::SeqCst);
    }

    pub fn add_tablet_reassignments(&self, n: u64) {
        self.tablet_reassignments_total
            .fetch_add(n, Ordering::SeqCst);
    }

    pub fn add_local_tablet_log_compactions(&self, n: u64, now_ms: i64) {
        self.local_tablet_log_compactions_total
            .fetch_add(n, Ordering::SeqCst);
        self.local_tablet_log_compaction_last_tick_at_ms
            .store(now_ms, Ordering::SeqCst);
    }

    pub fn add_local_tablet_log_compaction_failure(&self) {
        self.local_tablet_log_compaction_failures_total
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn set_last_tick_at_ms(&self, now_ms: i64) {
        self.last_tick_at_ms.store(now_ms, Ordering::SeqCst);
    }
}

impl Default for SchedulerStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct SchedulerStatusSnapshot {
    pub leader_node_id: Option<String>,
    pub leader_epoch: i64,
    pub leader_expires_at_ms: i64,
    pub is_leader: bool,
    pub dead_nodes_marked_total: u64,
    pub tablet_reassignments_total: u64,
    pub local_tablet_log_compactions_total: u64,
    pub local_tablet_log_compaction_failures_total: u64,
    pub local_tablet_log_compaction_last_tick_at_ms: i64,
    pub last_tick_at_ms: i64,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct TabletStoreStatus {
    pub layout: String,
    pub root_dir: Option<String>,
    pub tablets_per_domain: i64,
    pub max_open_domains: usize,
    pub domain_write_buffer_size_mib: usize,
    pub domain_max_write_buffers: i32,
    pub domain_parallelism: i32,
    pub domain_max_background_jobs: i32,
    pub domain_max_open_files: i32,
    pub domains: Vec<agentdb_storage::TabletStoreDomainMetrics>,
}

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<Runtime>,
    pub meta: Option<MetaStore>,
    pub auth_meta: Option<MetaStore>,
    pub routing: RouteRuntime,
    pub raft_ingress: Option<Arc<dyn RaftRsIngress>>,
    pub raft_registry: Option<Arc<RaftRsGroupRegistry>>,
    pub internal_auth_token: Option<Arc<str>>,
    pub internal_auth_previous_token: Option<Arc<str>>,
    pub api_auth_token: Option<Arc<str>>,
    pub api_auth_previous_token: Option<Arc<str>>,
    pub drain: DrainState,
    pub scheduler: Option<SchedulerStatus>,
    pub tablet_store: Option<TabletStoreStatus>,
    pub content_mirror: Option<Arc<AsyncMirrorContentBackend>>,
    pub recovery_backup: Option<Arc<RecoveryBackupStatus>>,
    pub proxy: reqwest::Client,
    pub proxy_policy: ProxyPolicy,
    pub api_json_response_profile: Option<Arc<ApiJsonResponseProfile>>,
    pub auth_cache: Arc<crate::auth::AuthTokenCache>,
    pub operational: Arc<crate::operational::OperationalRegistry>,
    /// Deterministic transport fault injection (test-only). `Some` only
    /// when the server started with `AGENTDB_TRANSPORT_FAULTS=1`.
    pub transport_faults: Option<Arc<crate::transport_faults::FaultTable>>,
    /// Linearizable-read barrier timeout. `Some` enables the Raft
    /// ReadIndex barrier on local reads of replicated tablets; `None`
    /// (single-node, or the `AGENTDB_READ_BARRIER=off` kill switch)
    /// serves reads without it.
    pub read_barrier_timeout: Option<std::time::Duration>,
    /// Advisory disk budget for the data directory in bytes (0 = unset, e.g.
    /// in-memory). Reported by `/api/storage/disk`; never blocks writes.
    pub disk_budget_bytes: u64,
    /// Directories whose on-disk size counts toward the disk budget (data dir
    /// and, if separate, the content dir).
    pub disk_usage_paths: Vec<std::path::PathBuf>,
}

impl AppState {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            meta: None,
            auth_meta: None,
            routing: RouteRuntime::disabled(),
            raft_ingress: None,
            raft_registry: None,
            internal_auth_token: None,
            internal_auth_previous_token: None,
            api_auth_token: None,
            api_auth_previous_token: None,
            drain: DrainState::new("single-node"),
            scheduler: None,
            tablet_store: None,
            content_mirror: None,
            recovery_backup: None,
            proxy: reqwest::Client::new(),
            proxy_policy: ProxyPolicy::default(),
            api_json_response_profile: ApiJsonResponseProfile::from_env().map(Arc::new),
            auth_cache: Arc::new(crate::auth::AuthTokenCache::from_env()),
            operational: Arc::new(crate::operational::OperationalRegistry::default()),
            transport_faults: None,
            read_barrier_timeout: None,
            disk_budget_bytes: 0,
            disk_usage_paths: Vec::new(),
        }
    }

    /// Set the advisory disk budget and the directories that count toward it.
    pub fn with_disk_budget(mut self, budget_bytes: u64, paths: Vec<std::path::PathBuf>) -> Self {
        self.disk_budget_bytes = budget_bytes;
        self.disk_usage_paths = paths;
        self
    }

    pub fn with_meta(mut self, meta: MetaStore) -> Self {
        self.auth_meta = Some(meta.clone());
        self.meta = Some(meta);
        self
    }

    pub fn with_auth_meta(mut self, meta: MetaStore) -> Self {
        self.auth_meta = Some(meta);
        self
    }

    pub fn with_routing(mut self, routing: RouteRuntime) -> Self {
        self.drain = self.drain.with_node_id(routing.local_node_id().to_string());
        self.routing = routing;
        self
    }

    pub fn with_raft_ingress(mut self, ingress: Arc<dyn RaftRsIngress>) -> Self {
        self.raft_ingress = Some(ingress);
        self
    }

    pub fn with_raft_registry(mut self, registry: Arc<RaftRsGroupRegistry>) -> Self {
        self.raft_ingress = Some(registry.clone());
        self.raft_registry = Some(registry);
        self
    }

    pub fn with_internal_auth_token(mut self, token: impl Into<Arc<str>>) -> Self {
        self.internal_auth_token = Some(token.into());
        self
    }

    pub fn with_internal_auth_previous_token(mut self, token: impl Into<Arc<str>>) -> Self {
        self.internal_auth_previous_token = Some(token.into());
        self
    }

    pub fn with_api_auth_token(mut self, token: impl Into<Arc<str>>) -> Self {
        self.api_auth_token = Some(token.into());
        self
    }

    pub fn with_api_auth_previous_token(mut self, token: impl Into<Arc<str>>) -> Self {
        self.api_auth_previous_token = Some(token.into());
        self
    }

    pub fn with_scheduler(mut self, scheduler: SchedulerStatus) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_tablet_store_status(mut self, tablet_store: TabletStoreStatus) -> Self {
        self.tablet_store = Some(tablet_store);
        self
    }

    pub fn with_recovery_backup(mut self, status: Arc<RecoveryBackupStatus>) -> Self {
        self.recovery_backup = Some(status);
        self
    }

    pub fn with_content_mirror(mut self, content_mirror: Arc<AsyncMirrorContentBackend>) -> Self {
        self.content_mirror = Some(content_mirror);
        self
    }

    pub fn with_proxy_policy(mut self, proxy_policy: ProxyPolicy) -> Self {
        self.proxy_policy = proxy_policy;
        self
    }
}

/// Point-in-time view of the recovery-manifest backup worker, served by
/// `GET /api/admin/recovery`. `recovery_lag` is derived from
/// `last_success_ms` by the route handler.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct RecoveryBackupSnapshot {
    pub destination: String,
    pub interval_ms: u64,
    pub last_attempt_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub last_error: Option<String>,
    pub last_entry_count: Option<u64>,
    pub last_payload_bytes: Option<u64>,
    pub consecutive_failures: u64,
    /// Content-derived id of the currently-published manifest (§4d): the
    /// authoritative "what is applied" answer a worker reconciles against.
    pub applied_change_id: Option<String>,
    /// Watermark of the currently-published manifest.
    pub applied_watermark: Option<u64>,
    /// Outcome of the last publish attempt: applied / already_applied /
    /// rejected_stale.
    pub last_publish_outcome: Option<String>,
}

/// Shared status handle: the server backup worker writes, the admin route
/// reads. A plain std Mutex — both sides only take it for field copies.
#[derive(Debug)]
pub struct RecoveryBackupStatus {
    inner: std::sync::Mutex<RecoveryBackupSnapshot>,
}

impl RecoveryBackupStatus {
    pub fn new(destination: impl Into<String>, interval_ms: u64) -> Self {
        Self {
            inner: std::sync::Mutex::new(RecoveryBackupSnapshot {
                destination: destination.into(),
                interval_ms,
                ..RecoveryBackupSnapshot::default()
            }),
        }
    }

    pub fn record_success(&self, now_ms: i64, entry_count: u64, payload_bytes: u64) {
        let mut g = self.inner.lock().expect("recovery status mutex poisoned");
        g.last_attempt_ms = Some(now_ms);
        g.last_success_ms = Some(now_ms);
        g.last_error = None;
        g.last_entry_count = Some(entry_count);
        g.last_payload_bytes = Some(payload_bytes);
        g.consecutive_failures = 0;
    }

    /// Record a reconcilable publish: the authoritative applied change id +
    /// watermark and the outcome of this attempt. Also counts as a success.
    pub fn record_publish(
        &self,
        now_ms: i64,
        entry_count: u64,
        payload_bytes: u64,
        applied_change_id: impl Into<String>,
        applied_watermark: u64,
        outcome: &str,
    ) {
        let mut g = self.inner.lock().expect("recovery status mutex poisoned");
        g.last_attempt_ms = Some(now_ms);
        g.last_success_ms = Some(now_ms);
        g.last_error = None;
        g.last_entry_count = Some(entry_count);
        g.last_payload_bytes = Some(payload_bytes);
        g.consecutive_failures = 0;
        g.applied_change_id = Some(applied_change_id.into());
        g.applied_watermark = Some(applied_watermark);
        g.last_publish_outcome = Some(outcome.to_string());
    }

    pub fn record_failure(&self, now_ms: i64, error: impl Into<String>) {
        let mut g = self.inner.lock().expect("recovery status mutex poisoned");
        g.last_attempt_ms = Some(now_ms);
        g.last_error = Some(error.into());
        g.consecutive_failures += 1;
    }

    pub fn snapshot(&self) -> RecoveryBackupSnapshot {
        self.inner
            .lock()
            .expect("recovery status mutex poisoned")
            .clone()
    }
}
