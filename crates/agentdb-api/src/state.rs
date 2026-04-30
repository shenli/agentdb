//! Shared HTTP handler state.
//!
//! Holds the runtime handle so axum extractors can pull it off the router.
//! Cloning `AppState` is cheap — `Runtime` is wrapped in `Arc`.

use std::sync::Arc;

use agentdb_core::Runtime;

use crate::routing_http::RouteRuntime;

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<Runtime>,
    pub routing: RouteRuntime,
    pub proxy: reqwest::Client,
}

impl AppState {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            routing: RouteRuntime::disabled(),
            proxy: reqwest::Client::new(),
        }
    }

    pub fn with_routing(mut self, routing: RouteRuntime) -> Self {
        self.routing = routing;
        self
    }
}
