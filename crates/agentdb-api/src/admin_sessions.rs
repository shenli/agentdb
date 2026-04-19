use agentdb_core::types::Session;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct SessionQuery {
    pub tenant_id: String,
}

#[derive(Serialize)]
pub struct SessionDetail {
    pub session: Session,
    pub parent_external_id: Option<String>,
}

pub async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(q): Query<SessionQuery>,
) -> Result<Json<SessionDetail>, ApiError> {
    let box_ = state.registry.route_tenant(&q.tenant_id);
    let tenant = state
        .tenants
        .lookup(box_.id, &q.tenant_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("tenant {} not found", q.tenant_id)))?;
    let session = state
        .sessions
        .lookup(box_.id, tenant.internal_id, &session_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("session {session_id} not found")))?;
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
