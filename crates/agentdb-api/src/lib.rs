//! AgentDB HTTP API (native REST).

use std::sync::Arc;

use axum::Router;

pub mod admin;
pub mod admin_boxes;
pub mod admin_sessions;
pub mod dto;
pub mod error;
pub mod fork;
pub mod handlers;
pub mod stream;

pub use error::ApiError;
pub use stream::{now_ms, StreamBus, StreamEvent, TickBox};

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
    pub stream: StreamBus,
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
        .route(
            "/api/admin/boxes",
            axum::routing::get(admin_boxes::list_boxes),
        )
        .route(
            "/api/admin/boxes/:id",
            axum::routing::get(admin_boxes::get_box),
        )
        .route(
            "/api/admin/boxes/:id/inject_fault",
            axum::routing::post(admin_boxes::inject_fault),
        )
        .route(
            "/api/admin/boxes/:id/quota",
            axum::routing::post(admin_boxes::set_quota),
        )
        .route(
            "/api/admin/sessions/:session_id",
            axum::routing::get(admin_sessions::get_session),
        )
        .route("/api/stream", axum::routing::get(stream::sse_handler))
        .with_state(state)
}
