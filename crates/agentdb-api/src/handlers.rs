use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::Json;

use crate::dto::{AppendAndFetchRequest, AppendAndFetchResponse, EventDto};
use crate::error::{ApiError, ErrorResponse};
use crate::AppState;

#[utoipa::path(
    post,
    path = "/api/tenants/{tenant_id}/sessions/{session_id}/append_and_fetch",
    tag = "primitives",
    params(
        ("tenant_id" = String, Path, description = "External tenant id"),
        ("session_id" = String, Path, description = "External session id"),
    ),
    request_body = AppendAndFetchRequest,
    responses(
        (status = 200, description = "Event appended; last N events returned", body = AppendAndFetchResponse),
        (status = 400, description = "Malformed request", body = ErrorResponse),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
        (status = 503, description = "Fault injected", body = ErrorResponse),
    ),
)]
pub async fn append_and_fetch(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    Json(body): Json<AppendAndFetchRequest>,
) -> Result<Json<AppendAndFetchResponse>, ApiError> {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let out = match agentdb_core::append_and_fetch_with_actor(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &tenant,
        &session,
        body.event,
        body.actor_id,
        body.fetch_last_n,
        now_millis,
    )
    .await
    {
        Ok(o) => o,
        Err(agentdb_core::Error::TooManyRequests {
            scope,
            retry_after_ms,
        }) => {
            let box_id = state.registry.route_tenant(&tenant).id;
            state.stream.emit(crate::stream::StreamEvent::QuotaHit {
                ts_ms: crate::stream::now_ms(),
                box_id,
                kind: scope.as_str().into(),
                tenant: Some(tenant.clone()),
            });
            return Err(ApiError::Core(agentdb_core::Error::TooManyRequests {
                scope,
                retry_after_ms,
            }));
        }
        Err(e) => return Err(e.into()),
    };
    let recent = out.recent.into_iter().map(EventDto::from).collect();
    Ok(Json(AppendAndFetchResponse {
        seq: out.new_seq,
        recent,
    }))
}
