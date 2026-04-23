use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::fork::fork_session;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ErrorResponse};
use crate::forwarding::{self, PassthroughHeaders, FORWARDED_HEADER};
use crate::stream::StreamEvent;
use crate::AppState;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ForkRequest {
    pub tenant_id: String,
    pub new_session_id: String,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct ForkResponse {
    pub new_session_id: String,
    pub parent_session_id: String,
    pub parent_seq_at_fork: u64,
}

#[utoipa::path(
    post,
    path = "/api/sessions/{session_id}/fork",
    tag = "primitives",
    params(("session_id" = String, Path, description = "Parent session's external id")),
    request_body = ForkRequest,
    responses(
        (status = 200, description = "Child session created", body = ForkResponse),
        (status = 400, description = "Malformed request", body = ErrorResponse),
        (status = 404, description = "Parent tenant/session not found", body = ErrorResponse),
        (status = 421, description = "Routing drift on a forwarded request"),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
    ),
)]
pub async fn fork_handler(
    State(state): State<AppState>,
    Path(parent_session_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Decode the body early so we can pull tenant_id for
    // routing. The decoded struct isn't used on the forward
    // path, only on local — but we need the tenant_id to
    // decide where to route.
    let parsed: ForkRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return ApiError::Core(agentdb_core::Error::Invalid {
                field: None,
                message: format!("invalid JSON body: {e}"),
            })
            .into_response()
        }
    };

    let decision = state
        .registry
        .route_session_for_io(&parsed.tenant_id, &parent_session_id);
    let already_forwarded = headers.contains_key(FORWARDED_HEADER);

    if let agentdb_core::RouteDecision::Remote { target, tablet_id } = &decision {
        if already_forwarded {
            let local_node = state
                .registry
                .node_id()
                .unwrap_or("unknown")
                .to_string();
            return forwarding::misdirected_reply(&local_node, *tablet_id);
        }
        if let Some(b) = state.registry.box_by_id(0) {
            b.metrics.forwarded_reads_total.fetch_add(1, Ordering::Relaxed);
        }
        let passthrough = PassthroughHeaders::from_headers(&headers);
        match forwarding::forward_fork(
            &state.http,
            target,
            &parent_session_id,
            body,
            passthrough,
        )
        .await
        {
            Ok(resp) => return resp,
            Err(err) => {
                if let Some(b) = state.registry.box_by_id(0) {
                    b.metrics
                        .forwarded_read_failures_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                return (StatusCode::SERVICE_UNAVAILABLE, Json(err.to_body())).into_response();
            }
        }
    }

    let box_ = match decision {
        agentdb_core::RouteDecision::Local(b) => b,
        agentdb_core::RouteDecision::Remote { .. } => unreachable!(),
    };
    let guard = box_.tenant_write_lock(&parsed.tenant_id).lock_owned().await;
    let child = match fork_session(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &guard,
        &parsed.tenant_id,
        &parent_session_id,
        &parsed.new_session_id,
        now_millis(),
    ) {
        Ok(s) => s,
        Err(agentdb_core::Error::TooManyRequests {
            scope,
            retry_after_ms,
        }) => {
            let box_id = box_.id;
            state.stream.emit(StreamEvent::QuotaHit {
                ts_ms: crate::stream::now_ms(),
                box_id,
                kind: scope.as_str().to_string(),
                tenant: Some(parsed.tenant_id.clone()),
            });
            return ApiError::Core(agentdb_core::Error::TooManyRequests {
                scope,
                retry_after_ms,
            })
            .into_response();
        }
        Err(e) => return ApiError::Core(e).into_response(),
    };
    drop(guard);
    let fork = child
        .forked_from
        .expect("fork_session always sets forked_from");
    state.stream.emit(StreamEvent::ForkCreated {
        ts_ms: now_millis(),
        box_id: box_.id,
        parent: parent_session_id.clone(),
        child: child.external_id.clone(),
    });
    Json(ForkResponse {
        new_session_id: child.external_id,
        parent_session_id,
        parent_seq_at_fork: fork.parent_seq_at_fork,
    })
    .into_response()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
