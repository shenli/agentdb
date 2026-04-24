//! Admin-triggered point-in-time backup.
//!
//! `POST /api/admin/backup` with `{ "out_dir": "/path/to/dir" }`.
//! The directory MUST NOT exist — the endpoint creates it
//! atomically so a partially-written backup never collides
//! with a pre-existing dir.
//!
//! `GET /api/admin/backup/status` returns last-run info about
//! the scheduled backup loop, when one is configured (the
//! "last success / last failure" shape). When no scheduler is
//! attached the fields are all null.
//!
//! The on-disk layout and primitive live in
//! `agentdb_core::backup`. This module is the axum wrapper.

use std::path::PathBuf;
use std::sync::Arc;

use agentdb_core::{BackupManifest, BackupSchedulerStatus};
use agentdb_meta::MetaStore;
use axum::extract::{Extension, State};
use axum::Json;
use serde::Deserialize;
use utoipa::ToSchema;

use crate::error::ApiError;
use crate::AppState;

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct BackupRequest {
    /// Absolute path to a directory that does NOT yet exist.
    pub out_dir: String,
}

#[utoipa::path(
    post,
    path = "/api/admin/backup",
    tag = "admin",
    request_body = BackupRequest,
    responses(
        (status = 200, description = "Backup complete; manifest returned", body = BackupManifest),
        (status = 400, description = "out_dir already exists, or is not an absolute path"),
        (status = 401, description = "Admin auth required"),
        (status = 500, description = "Checkpoint or VACUUM INTO failed"),
        (status = 503, description = "Meta store not configured"),
    ),
)]
pub async fn post_backup(
    State(state): State<AppState>,
    meta: Option<Extension<Arc<MetaStore>>>,
    Json(body): Json<BackupRequest>,
) -> Result<Json<BackupManifest>, ApiError> {
    let Some(Extension(meta)) = meta else {
        return Err(ApiError::ServiceUnavailable(
            "meta store not configured; nothing to back up".into(),
        ));
    };
    let out_dir = PathBuf::from(&body.out_dir);
    let manifest = agentdb_core::run_backup(
        state.registry.kv(),
        meta.as_ref(),
        &out_dir,
        state.registry.node_id(),
    )
    .await
    .map_err(ApiError::Core)?;
    Ok(Json(manifest))
}

#[utoipa::path(
    get,
    path = "/api/admin/backup/status",
    tag = "admin",
    responses(
        (status = 200, description = "Backup scheduler status (last success + last failure)", body = BackupSchedulerStatus),
        (status = 401, description = "Admin auth required"),
    ),
)]
pub async fn get_backup_status(
    State(state): State<AppState>,
) -> Result<Json<BackupSchedulerStatus>, ApiError> {
    // Every running scheduler pushes its status into the
    // BoxRegistry's OnceLock. When no scheduler is attached
    // (config unset; this node is a follower that never
    // elected leader), we hand back a default "never ran"
    // status rather than 404 — dashboards don't need to
    // special-case disabled mode.
    let status = state
        .registry
        .backup_status()
        .map(|arc| arc.snapshot())
        .unwrap_or_default();
    Ok(Json(status))
}
