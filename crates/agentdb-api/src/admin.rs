use agentdb_core::types::Tier;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::stream::{now_ms, StreamEvent};
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
        "warm" | "cold" | "archive" => {}
        other => {
            return Err(agentdb_core::Error::Invalid {
                field: Some("target".into()),
                message: format!(
                    "unsupported target tier '{other}'; expected 'warm' | 'cold' | 'archive'"
                ),
            }
            .into());
        }
    }

    let box_ = state.registry.route_tenant(&body.tenant_id);
    let tenant = state
        .tenants
        .lookup(box_.id, &body.tenant_id)?
        .ok_or_else(|| agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Tenant,
            id: body.tenant_id.clone(),
        })?;
    let mut session = state
        .sessions
        .lookup(box_.id, tenant.internal_id, &body.session_id)?
        .ok_or_else(|| agentdb_core::Error::NotFound {
            resource: agentdb_core::Resource::Session,
            id: body.session_id.clone(),
        })?;

    let guard = box_.write_lock.lock().await;
    // Walk one hop at a time until we reach the target tier. Each hop
    // records metrics, emits SSE, and fires a webhook.
    //
    // TODO(m12-followup): if a multi-step transition fails on a later
    // hop (e.g. warm→cold succeeds, cold→archive errors), the session's
    // persisted tier has moved but no SSE/webhook fires for any step —
    // emission happens after the loop. Either emit per-step before
    // continuing, or roll back the committed hop. Low probability in
    // practice (cold_to_archive only fails on externally-mutated rows)
    // but the asymmetry with the sweep path is worth fixing.
    let mut steps: Vec<(Tier, Tier)> = Vec::new();
    loop {
        use agentdb_core::tiering::{
            archive_to_cold, cold_to_archive, cold_transition, reactivate,
        };
        let from = session.tier;
        let done = match (from, target.as_str()) {
            (Tier::Warm, "cold") => {
                cold_transition(
                    state.registry.kv(),
                    state.registry.object_store(),
                    &state.sessions,
                    &guard,
                    &mut session,
                )
                .await?;
                steps.push((Tier::Warm, Tier::Cold));
                true
            }
            (Tier::Warm, "archive") => {
                cold_transition(
                    state.registry.kv(),
                    state.registry.object_store(),
                    &state.sessions,
                    &guard,
                    &mut session,
                )
                .await?;
                steps.push((Tier::Warm, Tier::Cold));
                false
            }
            (Tier::Cold, "archive") => {
                cold_to_archive(&state.sessions, &guard, &mut session).await?;
                steps.push((Tier::Cold, Tier::Archive));
                true
            }
            (Tier::Cold, "warm") => {
                reactivate(
                    state.registry.kv(),
                    state.registry.object_store(),
                    &state.sessions,
                    &guard,
                    &mut session,
                )
                .await?;
                steps.push((Tier::Cold, Tier::Warm));
                true
            }
            (Tier::Archive, "cold") => {
                archive_to_cold(&state.sessions, &guard, &mut session).await?;
                steps.push((Tier::Archive, Tier::Cold));
                true
            }
            (Tier::Archive, "warm") => {
                archive_to_cold(&state.sessions, &guard, &mut session).await?;
                steps.push((Tier::Archive, Tier::Cold));
                false
            }
            _ => true, // already at target — no-op
        };
        if done {
            break;
        }
    }
    drop(guard);

    for &(from, to) in &steps {
        box_.metrics.record_tier_transition();
        box_.metrics.drop_tier(from);
        box_.metrics.bump_tier(to);
        state.stream.emit(StreamEvent::TierChange {
            ts_ms: now_ms(),
            box_id: box_.id,
            session_id: body.session_id.clone(),
            from: tier_str(from).into(),
            to: tier_str(to).into(),
        });
        crate::webhook::fire_tier_change(
            &state.http,
            &box_,
            &body.tenant_id,
            session.clone(),
            from,
            to,
        );
    }

    Ok(Json(ForceTierResponse {
        tier: tier_str(session.tier).into(),
    }))
}

fn tier_str(t: Tier) -> &'static str {
    match t {
        Tier::Warm => "warm",
        Tier::Cold => "cold",
        Tier::Archive => "archive",
    }
}
