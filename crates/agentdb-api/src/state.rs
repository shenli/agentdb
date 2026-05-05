//! Shared HTTP handler state.
//!
//! Holds the runtime handle so axum extractors can pull it off the router.
//! Cloning `AppState` is cheap — `Runtime` is wrapped in `Arc`.

use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    Arc, OnceLock,
};

use agentdb_core::Runtime;
use agentdb_meta::MetaStore;

use crate::routing_http::RouteRuntime;

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
    drained_at_ms: Arc<OnceLock<i64>>,
}

impl DrainState {
    fn new(node_id: impl Into<Arc<str>>) -> Self {
        Self {
            node_id: node_id.into(),
            draining: Arc::new(AtomicBool::new(false)),
            drained_at_ms: Arc::new(OnceLock::new()),
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
        self.drained_at_ms.get().copied()
    }

    pub fn activate(&self, now_ms: i64) -> i64 {
        self.draining.store(true, Ordering::SeqCst);
        *self.drained_at_ms.get_or_init(|| now_ms)
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
            last_tick_at_ms: self.last_tick_at_ms.load(Ordering::SeqCst),
        }
    }

    pub fn update_leader(&self, node_id: &str, epoch: i64, expires_at_ms: i64, is_leader: bool) {
        let _ = self.leader_node_id.set(node_id.to_string());
        self.leader_epoch.store(epoch, Ordering::SeqCst);
        self.leader_expires_at_ms.store(expires_at_ms, Ordering::SeqCst);
        self.is_leader.store(is_leader, Ordering::SeqCst);
    }

    pub fn set_not_leader(&self) {
        self.is_leader.store(false, Ordering::SeqCst);
    }

    pub fn add_dead_nodes_marked(&self, n: u64) {
        self.dead_nodes_marked_total.fetch_add(n, Ordering::SeqCst);
    }

    pub fn add_tablet_reassignments(&self, n: u64) {
        self.tablet_reassignments_total.fetch_add(n, Ordering::SeqCst);
    }

    pub fn set_last_tick_at_ms(&self, now_ms: i64) {
        self.last_tick_at_ms.store(now_ms, Ordering::SeqCst);
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
    pub last_tick_at_ms: i64,
}

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<Runtime>,
    pub meta: Option<MetaStore>,
    pub routing: RouteRuntime,
    pub drain: DrainState,
    pub scheduler: Option<SchedulerStatus>,
    pub proxy: reqwest::Client,
    pub proxy_policy: ProxyPolicy,
}

impl AppState {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            meta: None,
            routing: RouteRuntime::disabled(),
            drain: DrainState::new("single-node"),
            scheduler: None,
            proxy: reqwest::Client::new(),
            proxy_policy: ProxyPolicy::default(),
        }
    }

    pub fn with_meta(mut self, meta: MetaStore) -> Self {
        self.meta = Some(meta);
        self
    }

    pub fn with_routing(mut self, routing: RouteRuntime) -> Self {
        self.drain = self.drain.with_node_id(routing.local_node_id().to_string());
        self.routing = routing;
        self
    }

    pub fn with_scheduler(mut self, scheduler: SchedulerStatus) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_proxy_policy(mut self, proxy_policy: ProxyPolicy) -> Self {
        self.proxy_policy = proxy_policy;
        self
    }
}
