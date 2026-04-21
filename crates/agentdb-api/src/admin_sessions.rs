use agentdb_core::types::Session;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ErrorResponse};
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
    ),
)]
pub async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(q): Query<SessionQuery>,
) -> Result<Json<SessionDetail>, ApiError> {
    let box_ = state.registry.route_tenant(&q.tenant_id);
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
    Ok(Json(SessionDetail {
        session,
        parent_external_id,
    }))
}
