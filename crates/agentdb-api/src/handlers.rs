use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::dto::{AppendAndFetchRequest, AppendAndFetchResponse, EventDto};
use crate::error::{ApiError, ErrorResponse};
use crate::forwarding::{self, PassthroughHeaders, FORWARDED_HEADER};
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
        (status = 421, description = "Routing drift on a forwarded request"),
        (status = 429, description = "Rate limit exceeded", body = ErrorResponse),
        (status = 503, description = "Fault injected, forwarding failed, or node draining", body = ErrorResponse),
    ),
)]
#[tracing::instrument(
    name = "rest.append_and_fetch",
    skip(state, headers, body),
    fields(
        tenant = %tenant,
        session = %session,
        forwarded = headers.contains_key(FORWARDED_HEADER),
    ),
)]
pub async fn append_and_fetch(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // M20 multi-node: decide whether to write locally or
    // forward before we deserialize, so we can ship the original
    // bytes to the peer without a round-trip through JSON.
    //
    // M26 final phase: route via the locator (where the
    // session canonically lives) before falling back to
    // shard-override / tenant-hash — keeps writes aligned
    // with the locator-driven reads from PR #74.
    let decision = state
        .registry
        .route_session_for_write(&tenant, &session)
        .await;
    let already_forwarded = headers.contains_key(FORWARDED_HEADER);

    if let agentdb_core::RouteDecision::Remote { target, tablet_id } = &decision {
        if already_forwarded {
            // A peer forwarded to us, and our own routing still
            // says Remote. Routing drift between nodes — surface
            // 421 so operators see it in logs rather than a
            // silent infinite loop.
            let local_node = state
                .registry
                .node_id()
                .unwrap_or("unknown")
                .to_string();
            return forwarding::misdirected_reply(&local_node, *tablet_id);
        }
        // Count the forward attempt on box 0 (admin-chosen
        // attribution — the actual write lands on a remote
        // box, so there's no natural local box to attribute to).
        let box_for_metrics = state.registry.box_by_id(0);
        if let Some(ref b) = box_for_metrics {
            b.metrics.forwarded_writes_total.fetch_add(1, Ordering::Relaxed);
        }
        let passthrough = PassthroughHeaders::from_headers(&headers);
        match forwarding::forward_append_and_fetch(
            &state.http,
            target,
            &tenant,
            &session,
            body,
            passthrough,
        )
        .await
        {
            Ok(resp) => return resp,
            Err(err) => {
                if let Some(b) = box_for_metrics {
                    b.metrics
                        .forwarded_write_failures_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                let body = err.to_body();
                let status = StatusCode::SERVICE_UNAVAILABLE;
                return (status, axum::Json(body)).into_response();
            }
        }
    }

    // Local path: deserialize the body we read as bytes and
    // fall through to the existing append_and_fetch.
    let parsed: AppendAndFetchRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            return ApiError::Core(agentdb_core::Error::Invalid {
                field: None,
                message: format!("invalid JSON body: {e}"),
            })
            .into_response()
        }
    };
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
        parsed.event,
        parsed.actor_id,
        parsed.fetch_last_n,
        now_millis,
    )
    .await
    {
        Ok(o) => o,
        Err(agentdb_core::Error::TooManyRequests {
            scope,
            retry_after_ms,
        }) => {
            // Attribute the QuotaHit event to the box that actually
            // rejected — matches the M20 session routing above.
            let box_id = state.registry.route_session(&tenant, &session).id;
            state.stream.emit(crate::stream::StreamEvent::QuotaHit {
                ts_ms: crate::stream::now_ms(),
                box_id,
                kind: scope.as_str().into(),
                tenant: Some(tenant.clone()),
            });
            return ApiError::Core(agentdb_core::Error::TooManyRequests {
                scope,
                retry_after_ms,
            })
            .into_response();
        }
        Err(e) => return ApiError::Core(e).into_response(),
    };
    let recent = out.recent.into_iter().map(EventDto::from).collect();
    axum::Json(AppendAndFetchResponse {
        seq: out.new_seq,
        recent,
    })
    .into_response()
}
