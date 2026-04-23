use std::time::{Duration, Instant};

use agentdb_core::{FaultKind, InjectedFault};
use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ErrorResponse};
use crate::stream::{now_ms, StreamEvent};
use crate::AppState;

#[derive(Serialize, utoipa::ToSchema)]
pub struct BoxListItem {
    pub id: u16,
    pub num_tenants: u64,
    pub metrics: agentdb_core::MetricsSnapshot,
    pub quota: QuotaView,
    pub fault_active: bool,
    pub webhook_url: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct QuotaView {
    pub max_warm_sessions: Option<u64>,
    pub max_qps: Option<u64>,
    pub max_tenant_qps: Option<u64>,
    pub burst: Option<u64>,
}

#[utoipa::path(
    get,
    path = "/api/admin/boxes",
    tag = "admin",
    responses(
        (status = 200, description = "Per-box snapshot", body = Vec<BoxListItem>),
    ),
)]
pub async fn list_boxes(State(state): State<AppState>) -> Result<Json<Vec<BoxListItem>>, ApiError> {
    let mut out = Vec::new();
    for i in 0..state.registry.num_boxes() {
        let Some(b) = state.registry.box_by_id(i) else {
            continue;
        };
        let tenants = state.tenants.list_in_box(i)?.len() as u64;
        let metrics = b.metrics.snapshot();
        let q = *b.quota.read();
        let fault_active = b
            .fault
            .read()
            .as_ref()
            .map(|f| f.is_active())
            .unwrap_or(false);
        let webhook_url = b.webhook.read().clone();
        out.push(BoxListItem {
            id: i,
            num_tenants: tenants,
            metrics,
            quota: QuotaView {
                max_warm_sessions: q.max_warm_sessions,
                max_qps: q.max_qps,
                max_tenant_qps: q.max_tenant_qps,
                burst: q.burst,
            },
            fault_active,
            webhook_url,
        });
    }
    Ok(Json(out))
}

#[utoipa::path(
    get,
    path = "/api/admin/boxes/{id}",
    tag = "admin",
    params(("id" = u16, Path, description = "Box id")),
    responses(
        (status = 200, body = BoxListItem),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn get_box(
    State(state): State<AppState>,
    Path(id): Path<u16>,
) -> Result<Json<BoxListItem>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Box_,
            id: id.to_string(),
        })?;
    let tenants = state.tenants.list_in_box(id)?.len() as u64;
    let metrics = b.metrics.snapshot();
    let q = *b.quota.read();
    let fault_active = b
        .fault
        .read()
        .as_ref()
        .map(|f| f.is_active())
        .unwrap_or(false);
    let webhook_url = b.webhook.read().clone();
    Ok(Json(BoxListItem {
        id,
        num_tenants: tenants,
        metrics,
        quota: QuotaView {
            max_warm_sessions: q.max_warm_sessions,
            max_qps: q.max_qps,
            max_tenant_qps: q.max_tenant_qps,
            burst: q.burst,
        },
        fault_active,
        webhook_url,
    }))
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HistogramView {
    pub count: u64,
    pub sum_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
}

impl From<agentdb_core::HistogramSnapshot> for HistogramView {
    fn from(s: agentdb_core::HistogramSnapshot) -> Self {
        Self {
            count: s.count,
            sum_ns: s.sum_ns,
            p50_ns: s.p50(),
            p95_ns: s.p95(),
            p99_ns: s.p99(),
        }
    }
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct BoxLatency {
    pub box_id: u16,
    pub append: HistogramView,
    pub admission_wait: HistogramView,
}

#[utoipa::path(
    get,
    path = "/api/admin/boxes/{id}/latency",
    tag = "admin",
    params(("id" = u16, Path, description = "Box id")),
    responses(
        (status = 200, body = BoxLatency),
        (status = 404, description = "No such box"),
    ),
)]
pub async fn get_box_latency(
    State(state): State<AppState>,
    Path(id): Path<u16>,
) -> Result<Json<BoxLatency>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Box_,
            id: id.to_string(),
        })?;
    Ok(Json(BoxLatency {
        box_id: id,
        append: b.metrics.append_latency.snapshot().into(),
        admission_wait: b.metrics.admission_wait_latency.snapshot().into(),
    }))
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct InjectFaultRequest {
    pub kind: String,
    pub duration_s: u64,
    #[serde(default)]
    pub latency_ms: Option<u64>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct InjectFaultResponse {
    pub ok: bool,
    pub expires_in_s: u64,
}

#[utoipa::path(
    post,
    path = "/api/admin/boxes/{id}/inject_fault",
    tag = "admin",
    params(("id" = u16, Path)),
    request_body = InjectFaultRequest,
    responses(
        (status = 200, body = InjectFaultResponse),
        (status = 400, description = "Unknown fault kind", body = ErrorResponse),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn inject_fault(
    State(state): State<AppState>,
    Path(id): Path<u16>,
    Json(body): Json<InjectFaultRequest>,
) -> Result<Json<InjectFaultResponse>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Box_,
            id: id.to_string(),
        })?;
    let kind = match body.kind.to_ascii_lowercase().as_str() {
        "latency" => FaultKind::Latency {
            ms: body.latency_ms.unwrap_or(200),
        },
        "error" => FaultKind::Error,
        other => {
            return Err(agentdb_core::Error::Invalid {
                field: Some("kind".into()),
                message: format!("unknown fault kind '{other}'; expected 'latency' or 'error'"),
            }
            .into());
        }
    };
    let expires_at = Instant::now() + Duration::from_secs(body.duration_s);
    *b.fault.write() = Some(InjectedFault { kind, expires_at });
    b.metrics.record_fault();
    state.stream.emit(StreamEvent::FaultInjected {
        ts_ms: now_ms(),
        box_id: id,
        kind: body.kind.to_ascii_lowercase(),
        duration_s: body.duration_s,
    });
    Ok(Json(InjectFaultResponse {
        ok: true,
        expires_in_s: body.duration_s,
    }))
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct QuotaRequest {
    #[serde(default)]
    pub max_warm_sessions: Option<u64>,
    #[serde(default)]
    pub max_qps: Option<u64>,
    #[serde(default)]
    pub max_tenant_qps: Option<u64>,
    #[serde(default)]
    pub burst: Option<u64>,
}

#[utoipa::path(
    post,
    path = "/api/admin/boxes/{id}/quota",
    tag = "admin",
    params(("id" = u16, Path)),
    request_body = QuotaRequest,
    responses(
        (status = 200, body = QuotaView),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn set_quota(
    State(state): State<AppState>,
    Path(id): Path<u16>,
    Json(body): Json<QuotaRequest>,
) -> Result<Json<QuotaView>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Box_,
            id: id.to_string(),
        })?;
    {
        let mut q = b.quota.write();
        q.max_warm_sessions = body.max_warm_sessions;
        q.max_qps = body.max_qps;
        q.max_tenant_qps = body.max_tenant_qps;
        q.burst = body.burst;
    }
    b.rebuild_buckets();
    let q = *b.quota.read();
    Ok(Json(QuotaView {
        max_warm_sessions: q.max_warm_sessions,
        max_qps: q.max_qps,
        max_tenant_qps: q.max_tenant_qps,
        burst: q.burst,
    }))
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct WebhookRequest {
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct WebhookResponse {
    pub url: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/admin/boxes/{id}/webhook",
    tag = "admin",
    params(("id" = u16, Path)),
    request_body = WebhookRequest,
    responses(
        (status = 200, body = WebhookResponse),
        (status = 400, description = "Invalid URL", body = ErrorResponse),
        (status = 404, description = "Box not found", body = ErrorResponse),
    ),
)]
pub async fn set_webhook(
    State(state): State<AppState>,
    Path(id): Path<u16>,
    Json(body): Json<WebhookRequest>,
) -> Result<Json<WebhookResponse>, ApiError> {
    let b = state
        .registry
        .box_by_id(id)
        .ok_or_else(|| agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Box_,
            id: id.to_string(),
        })?;
    if let Some(ref u) = body.url {
        if !(u.starts_with("http://") || u.starts_with("https://")) {
            return Err(agentdb_core::Error::Invalid {
                field: Some("url".into()),
                message: format!("webhook url must start with http:// or https:// (got {u})"),
            }
            .into());
        }
    }
    *b.webhook.write() = body.url.clone();
    Ok(Json(WebhookResponse { url: body.url }))
}
