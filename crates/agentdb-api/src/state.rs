//! Shared HTTP handler state.
//!
//! Holds the runtime handle so axum extractors can pull it off the router.
//! Cloning `AppState` is cheap — `Runtime` is wrapped in `Arc`.

use std::sync::Arc;

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
pub struct AppState {
    pub runtime: Arc<Runtime>,
    pub meta: Option<MetaStore>,
    pub routing: RouteRuntime,
    pub proxy: reqwest::Client,
    pub proxy_policy: ProxyPolicy,
}

impl AppState {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            meta: None,
            routing: RouteRuntime::disabled(),
            proxy: reqwest::Client::new(),
            proxy_policy: ProxyPolicy::default(),
        }
    }

    pub fn with_meta(mut self, meta: MetaStore) -> Self {
        self.meta = Some(meta);
        self
    }

    pub fn with_routing(mut self, routing: RouteRuntime) -> Self {
        self.routing = routing;
        self
    }

    pub fn with_proxy_policy(mut self, proxy_policy: ProxyPolicy) -> Self {
        self.proxy_policy = proxy_policy;
        self
    }
}
