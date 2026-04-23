//! M25 hot-tablet admin endpoints.
//!
//! - `GET /api/admin/tablets/hot` — currently-flagged tablets
//! - `GET /api/admin/tablets/load` — all tablets with
//!   instantaneous rate snapshot (hot and cool)
//! - `POST /api/admin/tablets/:id/shed` — bulk-move a fraction
//!   of the named tablet's shards to another tablet (manual
//!   load redistribution)
//!
//! All three are admin-plane; they stay open today (operator
//! trusted on loopback / private network). Admin-plane auth is
//! a separate future PR.

use std::sync::Arc;

use agentdb_core::{HotTablet, HotTabletDetector, TabletLoad};
use agentdb_meta::{shards as meta_shards, MetaStore};
use axum::extract::{Extension, Path};
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;

#[derive(Debug, Serialize, ToSchema)]
pub struct HotTabletView {
    pub tablet_id: i64,
    pub appends_per_sec: f64,
    pub admission_wait_ratio: f64,
    pub flagged_at_ms: i64,
}

impl From<HotTablet> for HotTabletView {
    fn from(h: HotTablet) -> Self {
        Self {
            tablet_id: h.tablet_id,
            appends_per_sec: h.appends_per_sec,
            admission_wait_ratio: h.admission_wait_ratio,
            flagged_at_ms: h.flagged_at_ms,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TabletLoadView {
    pub tablet_id: i64,
    pub appends_per_sec: f64,
    pub admission_wait_ratio: f64,
    pub is_hot: bool,
}

impl From<TabletLoad> for TabletLoadView {
    fn from(t: TabletLoad) -> Self {
        Self {
            tablet_id: t.tablet_id,
            appends_per_sec: t.appends_per_sec,
            admission_wait_ratio: t.admission_wait_ratio,
            is_hot: t.is_hot,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HotTabletsResponse {
    pub hot: Vec<HotTabletView>,
    /// Wall-clock ms of the last detector sample. Zero when
    /// this node isn't the scheduler leader.
    pub last_sample_ms: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TabletLoadResponse {
    pub tablets: Vec<TabletLoadView>,
    pub last_sample_ms: i64,
}

#[utoipa::path(
    get,
    path = "/api/admin/tablets/hot",
    tag = "admin",
    responses(
        (status = 200, description = "Currently-flagged tablets + last sample time", body = HotTabletsResponse),
        (status = 503, description = "Hot-tablet detector not configured on this server"),
    ),
)]
pub async fn list_hot_tablets(
    detector: Option<Extension<Arc<HotTabletDetector>>>,
) -> Result<Json<HotTabletsResponse>, ApiError> {
    let Some(Extension(d)) = detector else {
        return Err(ApiError::ServiceUnavailable(
            "hot-tablet detector not configured on this server".into(),
        ));
    };
    Ok(Json(HotTabletsResponse {
        hot: d.hot_tablets().into_iter().map(HotTabletView::from).collect(),
        last_sample_ms: d.last_sample_ms(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/admin/tablets/load",
    tag = "admin",
    responses(
        (status = 200, description = "All tablets with hot/cool status", body = TabletLoadResponse),
        (status = 503, description = "Hot-tablet detector not configured on this server"),
    ),
)]
pub async fn list_tablet_load(
    detector: Option<Extension<Arc<HotTabletDetector>>>,
) -> Result<Json<TabletLoadResponse>, ApiError> {
    let Some(Extension(d)) = detector else {
        return Err(ApiError::ServiceUnavailable(
            "hot-tablet detector not configured on this server".into(),
        ));
    };
    Ok(Json(TabletLoadResponse {
        tablets: d.all_tablets().into_iter().map(TabletLoadView::from).collect(),
        last_sample_ms: d.last_sample_ms(),
    }))
}

/// Request body for `POST /api/admin/tablets/create` (M25
/// dynamic tablet creation).
#[derive(Debug, Deserialize, Default, ToSchema)]
pub struct CreateTabletRequest {
    /// Explicit tablet id. When null, the server picks
    /// `max(existing) + 1` (simple and predictable at
    /// prototype scale).
    #[serde(default)]
    pub tablet_id: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CreateTabletResponse {
    pub tablet_id: i64,
    pub owner_node_id: String,
}

#[utoipa::path(
    post,
    path = "/api/admin/tablets/create",
    tag = "admin",
    request_body = CreateTabletRequest,
    responses(
        (status = 200, description = "New tablet created and owned by this node", body = CreateTabletResponse),
        (status = 409, description = "tablet_id already exists on this node"),
        (status = 503, description = "Meta store / node id not configured"),
    ),
)]
pub async fn create_tablet(
    meta: Option<Extension<Arc<MetaStore>>>,
    axum::extract::State(state): axum::extract::State<crate::AppState>,
    Json(body): Json<CreateTabletRequest>,
) -> Result<Json<CreateTabletResponse>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let Some(node_id) = state.registry.node_id().map(str::to_string) else {
        return Err(ApiError::ServiceUnavailable(
            "node id not configured on this server".into(),
        ));
    };

    // Pick tablet_id: explicit from body, else max(existing) + 1.
    let tablet_id = match body.tablet_id {
        Some(id) => id,
        None => {
            let existing = agentdb_meta::tablets::list_all(meta.pool())
                .await
                .map_err(|e| ApiError::Internal(format!("list tablets: {e}")))?;
            existing
                .iter()
                .map(|t| t.tablet_id)
                .max()
                .map(|m| m + 1)
                .unwrap_or(0)
        }
    };

    if tablet_id < 0 || tablet_id > u16::MAX as i64 {
        return Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("tablet_id".into()),
            message: format!("tablet_id {tablet_id} out of range 0..65535"),
        }));
    }

    // Delegate to BoxRegistry::add_box (idempotent meta
    // row + lease acquire + push under write lock).
    state
        .registry
        .add_box(tablet_id as u16)
        .await
        .map_err(|e| match e {
            agentdb_core::Error::Invalid { message, .. }
                if message.contains("already exists") =>
            {
                ApiError::Conflict(message)
            }
            other => ApiError::Core(other),
        })?;

    Ok(Json(CreateTabletResponse {
        tablet_id,
        owner_node_id: node_id,
    }))
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ShedRequest {
    /// Target tablet to move shards to. When `null`, the server
    /// picks the least-loaded tablet from the detector's
    /// snapshot (excluding the source).
    #[serde(default)]
    pub to_tablet_id: Option<i64>,
    /// Fraction of the source's currently-overridden shards to
    /// move. Clamped to (0, 1]. Defaults to 0.5 (half).
    #[serde(default)]
    pub fraction: Option<f64>,
    /// When true, after reassigning the shards the server
    /// enumerates sessions on the source tablet whose shard
    /// is in the moved set and migrates each warm one to
    /// the destination via
    /// [`agentdb_core::migrate_warm_session`]. Cold/archive
    /// sessions are reported in `skipped_sessions` and
    /// otherwise left alone. Default false for backward
    /// compatibility (previous callers don't expect data to
    /// move).
    #[serde(default)]
    pub migrate_data: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ShedResponse {
    pub source_tablet_id: i64,
    pub to_tablet_id: i64,
    pub moved_shards: Vec<i64>,
    /// Sessions whose warm state was rekeyed to the dest
    /// tablet. Only populated when `migrate_data == true`.
    #[serde(default)]
    pub migrated_sessions: Vec<MigratedSessionSummary>,
    /// Sessions on the moved shards that the migration
    /// refused (cold / archive / fork parent). Operators
    /// handle these out of band.
    #[serde(default)]
    pub skipped_sessions: Vec<SkippedSession>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MigratedSessionSummary {
    pub session_id: String,
    pub tenant_id: String,
    pub events_migrated: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SkippedSession {
    pub session_id: String,
    pub tenant_id: String,
    pub reason: String,
}

#[utoipa::path(
    post,
    path = "/api/admin/tablets/{id}/shed",
    tag = "admin",
    params(("id" = i64, Path, description = "Source tablet id")),
    request_body = ShedRequest,
    responses(
        (status = 200, description = "Shards moved", body = ShedResponse),
        (status = 400, description = "Request invalid or no viable destination"),
        (status = 503, description = "Meta / detector not configured"),
    ),
)]
pub async fn shed_tablet(
    meta: Option<Extension<Arc<MetaStore>>>,
    detector: Option<Extension<Arc<HotTabletDetector>>>,
    location: Option<Extension<Arc<agentdb_core::LocationProxy>>>,
    axum::extract::State(state): axum::extract::State<crate::AppState>,
    Path(tablet_id): Path<i64>,
    Json(body): Json<ShedRequest>,
) -> Result<Json<ShedResponse>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured on this server".into(),
        ));
    };
    let Some(Extension(detector)) = detector else {
        return Err(ApiError::ServiceUnavailable(
            "hot-tablet detector not configured on this server".into(),
        ));
    };

    let fraction = body.fraction.unwrap_or(0.5).clamp(0.0, 1.0);
    if fraction <= 0.0 {
        return Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("fraction".into()),
            message: "fraction must be > 0".into(),
        }));
    }

    // Resolve destination.
    let dest = match body.to_tablet_id {
        Some(t) if t == tablet_id => {
            return Err(ApiError::Core(agentdb_core::Error::Invalid {
                field: Some("to_tablet_id".into()),
                message: "destination must differ from source".into(),
            }));
        }
        Some(t) => t,
        None => {
            // Pick the least-loaded live tablet excluding
            // source. The detector's all_tablets() is keyed by
            // the local registry's box ids, so this is a
            // local-node decision — good for single-node
            // deployments, acceptable placeholder for multi-
            // node (operator can always pass an explicit
            // to_tablet_id).
            detector
                .all_tablets()
                .into_iter()
                .filter(|t| t.tablet_id != tablet_id)
                .min_by(|a, b| {
                    a.appends_per_sec
                        .partial_cmp(&b.appends_per_sec)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|t| t.tablet_id)
                .ok_or_else(|| {
                    ApiError::Core(agentdb_core::Error::Invalid {
                        field: None,
                        message: "no viable destination tablet (detector has no snapshot yet)"
                            .into(),
                    })
                })?
        }
    };

    let shards = meta_shards::list_by_tablet(meta.pool(), tablet_id)
        .await
        .map_err(|e| ApiError::Internal(format!("list shards: {e}")))?;
    if shards.is_empty() {
        return Ok(Json(ShedResponse {
            source_tablet_id: tablet_id,
            to_tablet_id: dest,
            moved_shards: vec![],
            migrated_sessions: vec![],
            skipped_sessions: vec![],
        }));
    }
    let n = ((shards.len() as f64) * fraction).ceil() as usize;
    let n = n.min(shards.len()).max(1);
    let moved: Vec<i64> = shards[..n].to_vec();
    let now = crate::stream::now_ms() as i64;
    for sid in &moved {
        meta_shards::set_assignment(meta.pool(), *sid, Some(dest), now)
            .await
            .map_err(|e| ApiError::Internal(format!("set_assignment {sid}: {e}")))?;
    }
    // Refresh the local proxy so subsequent writes see the move.
    if let Some(Extension(loc)) = location {
        if let Err(e) = loc.refresh().await {
            tracing::warn!(%e, "shed: post-move LocationProxy refresh failed");
        }
    }

    // Optional: migrate the warm data for every session on
    // the moved shards. Cold / fork-parent sessions refuse
    // inside migrate_warm_session; we capture the reason
    // and report it so operators can act on it out of band.
    let (migrated_sessions, skipped_sessions) = if body.migrate_data {
        migrate_sessions_for_shards(&state, tablet_id as u16, dest as u16, &moved).await
    } else {
        (vec![], vec![])
    };

    Ok(Json(ShedResponse {
        source_tablet_id: tablet_id,
        to_tablet_id: dest,
        moved_shards: moved,
        migrated_sessions,
        skipped_sessions,
    }))
}

/// Enumerate sessions on `from_box_id` whose `session_id`
/// hashes into any of `moved_shards`, then run
/// `migrate_warm_session` on each. Warm sessions land in
/// `migrated`; everything else (cold, archive, fork parent,
/// already-migrated) lands in `skipped` with a human-
/// readable reason.
///
/// This scan is O(sessions × tenants on source box). At
/// prototype scale that's fine — tens of tenants, hundreds
/// of sessions. For bigger deployments we'd index shards
/// by tablet in a separate meta table.
pub(crate) async fn migrate_sessions_for_shards(
    state: &crate::AppState,
    from_box_id: u16,
    to_box_id: u16,
    moved_shards: &[i64],
) -> (Vec<MigratedSessionSummary>, Vec<SkippedSession>) {
    let mut migrated = Vec::new();
    let mut skipped = Vec::new();
    let now_ms = crate::stream::now_ms();
    let moved_set: std::collections::HashSet<u16> = moved_shards
        .iter()
        .filter_map(|s| u16::try_from(*s).ok())
        .collect();
    let tenants = match state.tenants.list_in_box(from_box_id) {
        Ok(list) => list,
        Err(e) => {
            tracing::warn!(%e, from_box_id, "shed migrate: list tenants failed");
            return (migrated, skipped);
        }
    };
    for tenant_external in tenants {
        let Ok(Some(tenant)) = state.tenants.lookup(from_box_id, &tenant_external) else {
            continue;
        };
        let Ok(session_externals) =
            state.sessions.list_in_tenant(from_box_id, tenant.internal_id)
        else {
            continue;
        };
        for session_external in session_externals {
            let sid = agentdb_core::shard_id(&session_external);
            if !moved_set.contains(&sid) {
                continue;
            }
            match agentdb_core::migrate_warm_session(
                &state.registry,
                state.registry.kv(),
                &state.tenants,
                &state.sessions,
                &tenant_external,
                &session_external,
                to_box_id,
                now_ms,
            )
            .await
            {
                Ok(m) => migrated.push(MigratedSessionSummary {
                    session_id: session_external,
                    tenant_id: tenant_external.clone(),
                    events_migrated: m.events_migrated,
                }),
                Err(e) => {
                    skipped.push(SkippedSession {
                        session_id: session_external,
                        tenant_id: tenant_external.clone(),
                        reason: e.to_string(),
                    });
                }
            }
        }
    }
    (migrated, skipped)
}
