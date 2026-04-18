//! AgentDB HTTP API (native REST).

use std::sync::Arc;

use axum::Router;

pub mod dto;
pub mod error;
pub mod handlers;

pub use error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
}

pub fn router(state: AppState) -> Router {
    // Routes added in Task 12.
    Router::new().with_state(state)
}
