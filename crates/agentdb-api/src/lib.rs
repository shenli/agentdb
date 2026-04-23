//! AgentDB HTTP API (native REST).

use std::sync::Arc;

use axum::Router;

pub mod admin;
pub mod admin_boxes;
pub mod admin_drain;
pub mod admin_nodes;
pub mod admin_scheduler;
pub mod admin_sessions;
pub mod admin_shards;
pub mod admin_tablets;
pub mod dto;
pub mod error;
pub mod fork;
pub mod handlers;
pub mod health;
pub mod hlc_header;
pub mod metrics_exposition;
pub mod stream;
pub mod webhook;

pub use error::{ApiError, ErrorCode, ErrorKind, ErrorResponse};
pub use stream::{now_ms, StreamBus, StreamEvent, TickBox};

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<agentdb_core::BoxRegistry>,
    pub tenants: Arc<agentdb_core::TenantRegistry>,
    pub sessions: Arc<agentdb_core::SessionRegistry>,
    pub stream: StreamBus,
    pub http: reqwest::Client,
}

/// OpenAPI 3.x spec for the native REST surface. Mongo wire protocol
/// and SSE event bodies are out of scope (see ARCHITECTURE.md).
#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "AgentDB REST API",
        description = "Agent-native operational database. See docs/agent-db.md for design and docs/ARCHITECTURE.md for implementation.",
        version = "0.1.0",
    ),
    paths(
        handlers::append_and_fetch,
        fork::fork_handler,
        admin::force_tier,
        admin::get_hlc,
        admin_boxes::list_boxes,
        admin_boxes::get_box,
        admin_boxes::inject_fault,
        admin_boxes::set_quota,
        admin_boxes::set_webhook,
        admin_drain::get_drain,
        admin_drain::post_drain,
        admin_nodes::list_nodes,
        admin_scheduler::get_scheduler,
        admin_sessions::get_session,
        admin_shards::get_shard_by_id,
        admin_shards::get_shard_by_session,
        admin_shards::list_shard_overrides,
        admin_shards::set_shard,
        admin_tablets::list_tablets,
        stream::sse_handler,
    ),
    components(schemas(
        dto::AppendAndFetchRequest,
        dto::AppendAndFetchResponse,
        dto::EventDto,
        admin::ForceTierRequest,
        admin::ForceTierResponse,
        admin::HlcStatus,
        admin_boxes::BoxListItem,
        admin_boxes::QuotaView,
        admin_boxes::InjectFaultRequest,
        admin_boxes::InjectFaultResponse,
        admin_boxes::QuotaRequest,
        admin_boxes::WebhookRequest,
        admin_boxes::WebhookResponse,
        admin_drain::DrainStatus,
        admin_drain::DrainUnavailable,
        admin_nodes::NodeView,
        admin_nodes::NodesUnavailable,
        admin_scheduler::LeaderView,
        admin_scheduler::SchedulerStatus,
        admin_scheduler::SchedulerUnavailable,
        admin_sessions::SessionQuery,
        admin_sessions::SessionDetail,
        admin_shards::SetShardRequest,
        admin_shards::ShardAssignment,
        admin_shards::ShardOverride,
        admin_shards::ShardOverridesList,
        admin_shards::ShardsUnavailable,
        admin_tablets::TabletView,
        admin_tablets::TabletsUnavailable,
        fork::ForkRequest,
        fork::ForkResponse,
        stream::StreamEvent,
        stream::TickBox,
        error::ErrorResponse,
        error::ErrorCode,
        error::ErrorKind,
        agentdb_core::MetricsSnapshot,
        agentdb_core::GaugeSnapshot,
        agentdb_core::Session,
        agentdb_core::Event,
        agentdb_core::ForkRef,
        agentdb_core::Tier,
    )),
    tags(
        (name = "primitives", description = "Agent-native write and read primitives"),
        (name = "admin", description = "Operator-only endpoints (no agent exposure)"),
        (name = "stream", description = "Server-Sent Events"),
    ),
)]
pub struct ApiDoc;

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
        .route("/api/admin/hlc", axum::routing::get(admin::get_hlc))
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
            "/api/admin/boxes/:id/webhook",
            axum::routing::post(admin_boxes::set_webhook),
        )
        .route(
            "/api/admin/sessions/:session_id",
            axum::routing::get(admin_sessions::get_session),
        )
        .route(
            "/api/admin/nodes",
            axum::routing::get(admin_nodes::list_nodes),
        )
        .route(
            "/api/admin/drain",
            axum::routing::get(admin_drain::get_drain).post(admin_drain::post_drain),
        )
        .route(
            "/api/admin/tablets",
            axum::routing::get(admin_tablets::list_tablets),
        )
        .route(
            "/api/admin/shards",
            axum::routing::get(admin_shards::get_shard_by_session),
        )
        .route(
            "/api/admin/shards/list",
            axum::routing::get(admin_shards::list_shard_overrides),
        )
        .route(
            "/api/admin/shards/:id",
            axum::routing::get(admin_shards::get_shard_by_id).post(admin_shards::set_shard),
        )
        .route(
            "/api/admin/scheduler",
            axum::routing::get(admin_scheduler::get_scheduler),
        )
        .route("/api/stream", axum::routing::get(stream::sse_handler))
        .route("/health/live", axum::routing::get(health::live))
        .route("/health/ready", axum::routing::get(health::ready))
        .route("/metrics", axum::routing::get(metrics_exposition::metrics))
        .route("/openapi.json", axum::routing::get(openapi_json))
        .nest_service("/dashboard", agentdb_dashboard::router())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            hlc_header::hlc_middleware,
        ))
        .with_state(state)
}

async fn openapi_json() -> axum::Json<utoipa::openapi::OpenApi> {
    use utoipa::OpenApi;
    axum::Json(ApiDoc::openapi())
}
