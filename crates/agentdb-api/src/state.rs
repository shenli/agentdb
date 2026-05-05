//! Shared HTTP handler state.
//!
//! Holds the runtime handle so axum extractors can pull it off the router.
//! Cloning `AppState` is cheap — `Runtime` is wrapped in `Arc`.

use std::sync::{
    atomic::{AtomicBool, Ordering},
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
pub struct AppState {
    pub runtime: Arc<Runtime>,
    pub meta: Option<MetaStore>,
    pub routing: RouteRuntime,
    pub drain: DrainState,
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

    pub fn with_proxy_policy(mut self, proxy_policy: ProxyPolicy) -> Self {
        self.proxy_policy = proxy_policy;
        self
    }
}
