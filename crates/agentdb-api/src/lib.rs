//! AgentDB HTTP API.
//!
//! V2 routes wrap `agentdb_core::Runtime` (tenant / agent / session / event
//! lifecycle plus artifacts, records, skills, policies, replay, subscribe, and
//! context).

mod auth;
pub mod dto;
pub mod errors;
pub mod internal_raft_http;
pub mod raft_ingress;
pub mod routes;
pub mod routing_http;
pub mod state;

pub use errors::ApiError;
pub use state::AppState;

use axum::{middleware, Router};

/// Build the V2 router with all runtime routes mounted under `/api/...`.
pub fn build_router(state: AppState) -> Router {
    routes::routes()
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::authorize_external_api,
        ))
        .with_state(state)
}

/// Backwards-compatible alias for callers that grew up calling `router(state)`.
/// New code should prefer `build_router`.
pub fn router(state: AppState) -> Router {
    build_router(state)
}
