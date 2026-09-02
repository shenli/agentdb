//! AgentDB HTTP API.
//!
//! V2 routes wrap `agentdb_core::Runtime` (tenant / agent / session / event
//! lifecycle plus artifacts, records, policies, replay, subscribe, and
//! context).

mod auth;
pub mod dek_provider;
pub mod dto;
pub mod errors;
pub mod internal_raft_http;
pub mod operational;
pub mod raft_ingress;
pub mod routes;
pub mod routing_http;
pub mod state;
pub mod sysmeta;
pub mod transport_faults;

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
        .layer(middleware::from_fn(errors::envelope_plain_client_errors))
        .with_state(state)
}

/// Backwards-compatible alias for callers that grew up calling `router(state)`.
/// New code should prefer `build_router`.
pub fn router(state: AppState) -> Router {
    build_router(state)
}
