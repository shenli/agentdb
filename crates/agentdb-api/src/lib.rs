//! AgentDB HTTP API (native REST).

use std::sync::Arc;

use axum::Router;

pub mod admin;
pub mod dto;
pub mod error;
pub mod fork;
pub mod handlers;

pub use error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/api/tenants/:tenant_id/sessions/:session_id/append_and_fetch",
            axum::routing::post(handlers::append_and_fetch),
        )
        .route(
            "/api/admin/tier/force",
            axum::routing::post(admin::force_tier),
        )
        .route(
            "/api/sessions/:session_id/fork",
            axum::routing::post(fork::fork_handler),
        )
        .with_state(state)
}
