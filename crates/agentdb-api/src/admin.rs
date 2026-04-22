use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::types::Tier;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ErrorResponse};
use crate::stream::{now_ms, StreamEvent};
use crate::AppState;

/// Process-global HLC health snapshot, served from `GET /api/admin/hlc`.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct HlcStatus {
    /// Current HLC physical component (nanoseconds since UNIX epoch).
    pub physical_ns: u64,
    /// Current HLC logical counter.
    pub logical: u32,
    /// Local wall clock at the time of this snapshot, nanoseconds since
    /// epoch. Compare against `physical_ns` to estimate `offset_ns`.
    pub wall_clock_ns: u64,
    /// `physical_ns - wall_clock_ns`. Positive when HLC is ahead of wall
    /// (pushed by peers); negative when wall has jumped ahead and HLC
    /// has not yet caught up. Absolute value > a few seconds warrants
    /// investigation.
    pub offset_ns: i64,
    /// Configured max-offset window in seconds. Inbound timestamps
    /// whose physical component exceeds `wall + max_offset` are rejected
    /// and counted in `rejects`.
    pub max_offset_secs: u64,
    /// Running count of inbound timestamps rejected by the max-offset
    /// guard. Any non-zero value is a signal; a sustained growth rate
    /// indicates a clock-skew problem or a rogue peer.
    pub rejects: u64,
}

#[utoipa::path(
    get,
    path = "/api/admin/hlc",
    tag = "admin",
    responses(
        (status = 200, description = "Process-global HLC status", body = HlcStatus),
    ),
)]
pub async fn get_hlc(State(state): State<AppState>) -> Json<HlcStatus> {
    let hlc = state.registry.hlc();
    let current = hlc.current();
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock must not precede UNIX epoch")
        .as_nanos() as u64;
    let offset_ns = current.physical_ns as i64 - wall as i64;
    Json(HlcStatus {
        physical_ns: current.physical_ns,
        logical: current.logical,
        wall_clock_ns: wall,
        offset_ns,
        max_offset_secs: hlc.max_offset_secs(),
        rejects: hlc.rejects_count(),
    })
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ForceTierRequest {
    pub tenant_id: String,
    pub session_id: String,
    pub target: String,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct ForceTierResponse {
    pub tier: String,
}

#[utoipa::path(
    post,
    path = "/api/admin/tier/force",
    tag = "admin",
    request_body = ForceTierRequest,
    responses(
        (status = 200, description = "Session transitioned to target tier", body = ForceTierResponse),
        (status = 400, description = "Unsupported target tier or malformed body", body = ErrorResponse),
        (status = 404, description = "Tenant or session not found", body = ErrorResponse),
    ),
)]
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
