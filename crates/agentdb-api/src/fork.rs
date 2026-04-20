use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::fork::fork_session;
use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::stream::StreamEvent;
use crate::AppState;

#[derive(Deserialize)]
pub struct ForkRequest {
    pub tenant_id: String,
    pub new_session_id: String,
}

#[derive(Serialize)]
pub struct ForkResponse {
    pub new_session_id: String,
    pub parent_session_id: String,
    pub parent_seq_at_fork: u64,
}

pub async fn fork_handler(
    State(state): State<AppState>,
    Path(parent_session_id): Path<String>,
    Json(body): Json<ForkRequest>,
) -> Result<Json<ForkResponse>, ApiError> {
    let box_ = state.registry.route_tenant(&body.tenant_id);
    let guard = box_.write_lock.lock().await;
    let child = match fork_session(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &guard,
        &body.tenant_id,
        &parent_session_id,
        &body.new_session_id,
        now_millis(),
    ) {
        Ok(s) => s,
        Err(agentdb_core::Error::TooManyRequests { scope }) => {
            let box_id = state.registry.route_tenant(&body.tenant_id).id;
            state.stream.emit(StreamEvent::QuotaHit {
                ts_ms: crate::stream::now_ms(),
                box_id,
                kind: scope.clone(),
                tenant: Some(body.tenant_id.clone()),
            });
            return Err(ApiError::Core(agentdb_core::Error::TooManyRequests {
                scope,
            }));
        }
        Err(e) => return Err(e.into()),
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
    Ok(Json(ForkResponse {
        new_session_id: child.external_id,
        parent_session_id,
        parent_seq_at_fork: fork.parent_seq_at_fork,
    }))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
