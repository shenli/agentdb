use std::sync::atomic::Ordering;

use agentdb_core::types::Session;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ErrorResponse};
use crate::forwarding::{self, PassthroughHeaders, FORWARDED_HEADER};
use crate::AppState;

#[derive(Deserialize, utoipa::ToSchema)]
pub struct SessionQuery {
    pub tenant_id: String,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct SessionDetail {
    pub session: Session,
    pub parent_external_id: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/admin/sessions/{session_id}",
    tag = "admin",
    params(
        ("session_id" = String, Path, description = "External session id"),
        ("tenant_id" = String, Query, description = "Tenant that owns the session"),
    ),
    responses(
        (status = 200, body = SessionDetail),
        (status = 404, description = "Tenant or session not found", body = ErrorResponse),
        (status = 421, description = "Routing drift on a forwarded request"),
    ),
)]
pub async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(q): Query<SessionQuery>,
    headers: HeaderMap,
) -> Response {
    // M20 multi-node: if the session's shard is owned by a
    // peer, the read must land on that peer — the session's
    // writes and metadata live there.
    let decision = state
        .registry
        .route_session_for_io(&q.tenant_id, &session_id);
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
        // Count forwarded reads on box 0 (admin attribution —
        // the read itself runs on the remote node's box).
        if let Some(b) = state.registry.box_by_id(0) {
            b.metrics.forwarded_reads_total.fetch_add(1, Ordering::Relaxed);
        }
        let passthrough = PassthroughHeaders::from_headers(&headers);
        match forwarding::forward_admin_session_get(
            &state.http,
            target,
            &session_id,
            &q.tenant_id,
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

    // Local path.
    let box_ = match decision {
        agentdb_core::RouteDecision::Local(b) => b,
        agentdb_core::RouteDecision::Remote { .. } => unreachable!("handled above"),
    };
    let result: Result<SessionDetail, ApiError> = (|| {
        let tenant = state
            .tenants
            .lookup(box_.id, &q.tenant_id)?
            .ok_or_else(|| agentdb_core::Error::NotFound {
                resource: agentdb_core::Resource::Tenant,
                id: q.tenant_id.clone(),
            })?;
        let session = state
            .sessions
            .lookup(box_.id, tenant.internal_id, &session_id)?
            .ok_or_else(|| agentdb_core::Error::NotFound {
                resource: agentdb_core::Resource::Session,
                id: session_id.clone(),
            })?;
        let parent_external_id = match &session.forked_from {
            Some(fork) => state
                .sessions
                .lookup_by_internal(box_.id, tenant.internal_id, fork.parent_session_internal_id)?
                .map(|s| s.external_id),
            None => None,
        };
        Ok(SessionDetail {
            session,
            parent_external_id,
        })
    })();
    match result {
        Ok(detail) => Json(detail).into_response(),
        Err(e) => e.into_response(),
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct MigrateSessionRequest {
    pub tenant_id: String,
    pub to_tablet_id: u16,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct MigrateSessionResponse {
    pub events_migrated: u64,
    pub from_tablet_id: u16,
    pub to_tablet_id: u16,
}

#[utoipa::path(
    post,
    path = "/api/admin/sessions/{session_id}/migrate",
    tag = "admin",
    params(("session_id" = String, Path, description = "Session to move")),
    request_body = MigrateSessionRequest,
    responses(
        (status = 200, body = MigrateSessionResponse),
        (status = 404, description = "Session or tenant not found"),
        (status = 409, description = "Session tier is cold/archive; migration refuses",
            body = ErrorResponse),
    ),
)]
pub async fn migrate_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(body): Json<MigrateSessionRequest>,
) -> Result<Json<MigrateSessionResponse>, ApiError> {
    let now_ms = crate::stream::now_ms();
    let result = agentdb_core::migrate_warm_session(
        &state.registry,
        state.registry.kv(),
        &state.tenants,
        &state.sessions,
        &body.tenant_id,
        &session_id,
        body.to_tablet_id,
        now_ms,
    )
    .await
    .map_err(|e| match e {
        agentdb_core::Error::Invalid {
            field: Some(ref f),
            ref message,
        } if f == "tier" => ApiError::Conflict(message.clone()),
        agentdb_core::Error::Invalid {
            field: Some(ref f),
            ref message,
        } if f == "forked_from" => ApiError::Conflict(message.clone()),
        other => ApiError::Core(other),
    })?;
    Ok(Json(MigrateSessionResponse {
        events_migrated: result.events_migrated,
        from_tablet_id: result.from_box_id,
        to_tablet_id: result.to_box_id,
    }))
}
