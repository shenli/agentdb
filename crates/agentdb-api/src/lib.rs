//! AgentDB HTTP API.
//!
//! V2.0 routes wrap `agentdb_core::Runtime` (tenant / agent / session / event
//! lifecycle). Auth, SSE subscribe, and the artifact / record / skill / policy
//! surfaces are deliberately deferred to later milestones.

pub mod dto;
pub mod errors;
pub mod routes;
pub mod state;

pub use errors::ApiError;
pub use state::AppState;

use axum::Router;

/// Build the V2.0 router with all runtime routes mounted under `/api/...`.
pub fn build_router(state: AppState) -> Router {
    routes::routes().with_state(state)
}

/// Backwards-compatible alias for callers that grew up calling `router(state)`.
/// New code should prefer `build_router`.
pub fn router(state: AppState) -> Router {
    build_router(state)
}
