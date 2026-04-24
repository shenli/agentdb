//! AgentDB HTTP API (native REST).

use std::sync::Arc;

use axum::Router;

pub mod admin;
pub mod admin_audit;
pub mod admin_backup;
pub mod admin_boxes;
pub mod admin_drain;
pub mod admin_nodes;
pub mod admin_scheduler;
pub mod admin_sessions;
pub mod admin_shards;
pub mod admin_hot_tablets;
pub mod admin_slow_queries;
pub mod admin_tablets;
pub mod admin_tenant_quotas;
pub mod admin_users;
pub mod admin_auth_middleware;
pub mod admin_tokens;
pub mod auth_middleware;
pub mod auto_shed;
pub mod dto;
pub mod error;
pub mod fork;
pub mod forwarding;
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
    /// M19 auth config. `None` → no auth required; the Basic
    /// Auth middleware is a no-op. `Some` → `/api/tenants/*`
    /// endpoints require `Authorization: Basic <...>` that
    /// verifies against a user in the meta store.
    pub auth: Option<AuthConfig>,
    /// Admin-plane auth config (post-M25). `None` → admin
    /// endpoints stay open (dev mode, backward compat).
    /// `Some` → `/api/admin/*` requires a Bearer token that
    /// verifies against the admin_tokens table. Bootstrap
    /// exception: the first `POST /api/admin/tokens` is
    /// allowed without auth when no active tokens exist.
    pub admin_auth: Option<admin_auth_middleware::AdminAuthConfig>,
}

#[derive(Clone)]
pub struct AuthConfig {
    /// Handle on the meta store. The middleware pulls users
    /// from here; `None` is an invariant violation (Internal
    /// 500) rather than "auth disabled" because callers only
    /// set `auth = Some(...)` when they've attached a real
    /// meta store.
    pub meta: Option<Arc<agentdb_meta::MetaStore>>,
}

impl AppState {
    /// Convenience constructor that matches the pre-M19 shape.
    /// Existing tests use the full struct literal — the struct
    /// field is public so adding `auth: None` there is a
    /// one-line change; this helper exists for new call sites.
    pub fn no_auth(
        registry: Arc<agentdb_core::BoxRegistry>,
        tenants: Arc<agentdb_core::TenantRegistry>,
        sessions: Arc<agentdb_core::SessionRegistry>,
        stream: StreamBus,
        http: reqwest::Client,
    ) -> Self {
        Self {
            registry,
            tenants,
            sessions,
            stream,
            http,
            auth: None,
            admin_auth: None,
        }
    }
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
        admin_backup::post_backup,
        admin_audit::list_audit,
        admin_boxes::list_boxes,
        admin_boxes::get_box,
        admin_boxes::inject_fault,
        admin_boxes::set_quota,
        admin_boxes::set_webhook,
        admin_boxes::get_box_latency,
        admin_drain::get_drain,
        admin_drain::post_drain,
        admin_nodes::list_nodes,
        admin_scheduler::get_scheduler,
        admin_sessions::get_session,
        admin_sessions::migrate_session,
        admin_shards::get_shard_by_id,
        admin_shards::get_shard_by_session,
        admin_shards::list_shard_overrides,
        admin_shards::set_shard,
        admin_tablets::list_tablets,
        admin_hot_tablets::list_hot_tablets,
        admin_hot_tablets::list_tablet_load,
        admin_hot_tablets::shed_tablet,
        admin_hot_tablets::create_tablet,
        admin_tokens::mint_token,
        admin_tokens::list_tokens,
        admin_tokens::revoke_token,
        admin_slow_queries::list_slow_queries,
        admin_tenant_quotas::list_tenant_quotas,
        admin_tenant_quotas::get_tenant_quota,
        admin_tenant_quotas::put_tenant_quota,
        admin_tenant_quotas::delete_tenant_quota,
        admin_users::create_user,
        admin_users::list_users,
        admin_users::delete_user,
        stream::sse_handler,
    ),
    components(schemas(
        dto::AppendAndFetchRequest,
        dto::AppendAndFetchResponse,
        dto::EventDto,
        admin::ForceTierRequest,
        admin::ForceTierResponse,
        admin::HlcStatus,
        admin_audit::AuditRecordView,
        admin_backup::BackupRequest,
        admin_backup::BackupManifest,
        admin_boxes::BoxListItem,
        admin_boxes::QuotaView,
        admin_boxes::InjectFaultRequest,
        admin_boxes::InjectFaultResponse,
        admin_boxes::QuotaRequest,
        admin_boxes::WebhookRequest,
        admin_boxes::WebhookResponse,
        admin_boxes::BoxLatency,
        admin_boxes::HistogramView,
        admin_drain::DrainStatus,
        admin_drain::DrainUnavailable,
        admin_nodes::NodeView,
        admin_nodes::NodesUnavailable,
        admin_scheduler::LeaderView,
        admin_scheduler::SchedulerStatus,
        admin_scheduler::SchedulerUnavailable,
        admin_sessions::SessionQuery,
        admin_sessions::SessionDetail,
        admin_sessions::MigrateSessionRequest,
        admin_sessions::MigrateSessionResponse,
        admin_shards::SetShardRequest,
        admin_shards::ShardAssignment,
        admin_shards::ShardOverride,
        admin_shards::ShardOverridesList,
        admin_shards::ShardsUnavailable,
        admin_tablets::TabletView,
        admin_hot_tablets::HotTabletView,
        admin_hot_tablets::TabletLoadView,
        admin_hot_tablets::HotTabletsResponse,
        admin_hot_tablets::TabletLoadResponse,
        admin_hot_tablets::ShedRequest,
        admin_hot_tablets::ShedResponse,
        admin_hot_tablets::MigratedSessionSummary,
        admin_hot_tablets::SkippedSession,
        admin_hot_tablets::CreateTabletRequest,
        admin_hot_tablets::CreateTabletResponse,
        admin_tokens::MintTokenRequest,
        admin_tokens::MintTokenResponse,
        admin_tokens::TokenSummaryView,
        agentdb_meta::admin_tokens::AdminScope,
        admin_tenant_quotas::SetTenantQuotaRequest,
        admin_tenant_quotas::TenantQuotaView,
        admin_tablets::TabletsUnavailable,
        admin_users::CreateUserRequest,
        admin_users::CreateUserResponse,
        admin_users::UserListItem,
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
        agentdb_core::SlowQueryRecord,
        agentdb_core::SlowQueryKind,
    )),
    tags(
        (name = "primitives", description = "Agent-native write and read primitives"),
        (name = "admin", description = "Operator-only endpoints (no agent exposure)"),
        (name = "stream", description = "Server-Sent Events"),
    ),
)]
pub struct ApiDoc;

pub fn router(state: AppState) -> Router {
    // Routes that require Basic auth when AppState::auth is Some.
    // Split into a separate Router so the auth middleware only
    // sees tenant-scoped paths — admin-plane routes are a
    // separate subtree below with no auth layer.
    let tenant_routes: Router<AppState> = Router::new().route(
        "/api/tenants/:tenant_id/sessions/:session_id/append_and_fetch",
        axum::routing::post(handlers::append_and_fetch),
    );
    let tenant_routes = tenant_routes.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        auth_middleware::basic_auth_middleware,
    ));

    // Admin-plane routes. Every `/api/admin/*` endpoint goes
    // here; the bearer-token middleware layers over the
    // subtree so health/metrics/dashboard stay open.
    let admin_routes: Router<AppState> = Router::new()
        .route(
            "/api/admin/tier/force",
            axum::routing::post(admin::force_tier),
        )
        .route("/api/admin/hlc", axum::routing::get(admin::get_hlc))
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
            "/api/admin/boxes/:id/latency",
            axum::routing::get(admin_boxes::get_box_latency),
        )
        .route(
            "/api/admin/sessions/:session_id",
            axum::routing::get(admin_sessions::get_session),
        )
        .route(
            "/api/admin/sessions/:session_id/migrate",
            axum::routing::post(admin_sessions::migrate_session),
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
            "/api/admin/tablets/hot",
            axum::routing::get(admin_hot_tablets::list_hot_tablets),
        )
        .route(
            "/api/admin/tablets/load",
            axum::routing::get(admin_hot_tablets::list_tablet_load),
        )
        .route(
            "/api/admin/tablets/:id/shed",
            axum::routing::post(admin_hot_tablets::shed_tablet),
        )
        .route(
            "/api/admin/tablets/create",
            axum::routing::post(admin_hot_tablets::create_tablet),
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
        .route(
            "/api/admin/users",
            axum::routing::post(admin_users::create_user)
                .get(admin_users::list_users),
        )
        .route(
            "/api/admin/users/:user_id",
            axum::routing::delete(admin_users::delete_user),
        )
        .route(
            "/api/admin/tokens",
            axum::routing::post(admin_tokens::mint_token)
                .get(admin_tokens::list_tokens),
        )
        .route(
            "/api/admin/tokens/:token_id",
            axum::routing::delete(admin_tokens::revoke_token),
        )
        .route(
            "/api/admin/audit",
            axum::routing::get(admin_audit::list_audit),
        )
        .route(
            "/api/admin/slow-queries",
            axum::routing::get(admin_slow_queries::list_slow_queries),
        )
        .route(
            "/api/admin/backup",
            axum::routing::post(admin_backup::post_backup),
        )
        .route(
            "/api/admin/tenant-quotas",
            axum::routing::get(admin_tenant_quotas::list_tenant_quotas),
        )
        .route(
            "/api/admin/tenant-quotas/:tenant_id",
            axum::routing::get(admin_tenant_quotas::get_tenant_quota)
                .put(admin_tenant_quotas::put_tenant_quota)
                .delete(admin_tenant_quotas::delete_tenant_quota),
        );
    let admin_routes = admin_routes.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        admin_auth_middleware::admin_auth_middleware,
    ));

    Router::new()
        .merge(tenant_routes)
        .merge(admin_routes)
        .route(
            "/api/sessions/:session_id/fork",
            axum::routing::post(fork::fork_handler),
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
