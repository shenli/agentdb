use agentdb_core::tiering::{cold_transition, reactivate};
use agentdb_core::types::Tier;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub struct ForceTierRequest {
    pub tenant_id: String,
    pub session_id: String,
    pub target: String,
}

#[derive(Serialize)]
pub struct ForceTierResponse {
    pub tier: String,
}

pub async fn force_tier(
    State(state): State<AppState>,
    Json(body): Json<ForceTierRequest>,
) -> Result<Json<ForceTierResponse>, ApiError> {
    let target = body.target.to_ascii_lowercase();
    match target.as_str() {
        "warm" | "cold" => {}
        other => {
            return Err(ApiError::BadRequest(format!(
                "unsupported target tier {other}; expected 'warm' or 'cold'"
            )));
        }
    }

    let box_ = state.registry.route_tenant(&body.tenant_id);
    // Admin endpoint: do NOT silently mint unknown tenants. A typo should 400,
    // not create a phantom tenant entry.
    let tenant = state
        .tenants
        .lookup(box_.id, &body.tenant_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("tenant {} not found", body.tenant_id)))?;
    let mut session = state
        .sessions
        .lookup(box_.id, tenant.internal_id, &body.session_id)?
        .ok_or_else(|| ApiError::BadRequest(format!("session {} not found", body.session_id)))?;

    let guard = box_.write_lock.lock().await;
    match target.as_str() {
        "cold" if session.tier == Tier::Warm => {
            cold_transition(
                state.registry.kv(),
                state.registry.object_store(),
                &state.sessions,
                &guard,
                &mut session,
            )
            .await?;
        }
        "cold" => { /* already cold — no-op */ }
        "warm" if session.tier == Tier::Cold => {
            reactivate(
                state.registry.kv(),
                state.registry.object_store(),
                &state.sessions,
                &guard,
                &mut session,
            )
            .await?;
        }
        "warm" => { /* already warm — no-op */ }
        _ => unreachable!(),
    }
    drop(guard);

    let tier_str = match session.tier {
        Tier::Warm => "warm",
        Tier::Cold => "cold",
        Tier::Archive => "archive",
    };
    Ok(Json(ForceTierResponse {
        tier: tier_str.into(),
    }))
}
