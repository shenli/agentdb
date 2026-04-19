use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::Json;

use crate::dto::{AppendAndFetchRequest, AppendAndFetchResponse, EventDto};
use crate::error::ApiError;
use crate::AppState;

pub async fn append_and_fetch(
    State(state): State<AppState>,
    Path((tenant, session)): Path<(String, String)>,
    Json(body): Json<AppendAndFetchRequest>,
) -> Result<Json<AppendAndFetchResponse>, ApiError> {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let out = agentdb_core::append_and_fetch(
        &state.registry,
        &state.tenants,
        &state.sessions,
        &tenant,
        &session,
        body.event,
        body.fetch_last_n,
        now_millis,
    )
    .await?;
    let recent = out.recent.into_iter().map(EventDto::from).collect();
    Ok(Json(AppendAndFetchResponse {
        seq: out.new_seq,
        recent,
    }))
}
