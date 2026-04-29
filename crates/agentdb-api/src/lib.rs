//! AgentDB HTTP API.
//!
//! V1 routes were deleted in the v2.0 restructure. This crate is currently a
//! skeleton: [`AppState`] holds the runtime handle and [`router`] returns an
//! empty `axum::Router`. The next milestone restores HTTP endpoints targeting
//! the flat `agentdb_core::*` runtime.

use std::sync::Arc;

use axum::Router;

use agentdb_core::Runtime;

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<Runtime>,
}

pub fn router(state: AppState) -> Router {
    Router::new().with_state(state)
}
