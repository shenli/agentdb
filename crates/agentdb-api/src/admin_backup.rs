//! Admin-triggered point-in-time backup.
//!
//! `POST /api/admin/backup` with `{ "out_dir": "/path/to/dir" }`.
//! The directory MUST NOT exist — the endpoint creates it
//! atomically so a partially-written backup never collides with
//! a pre-existing dir. Contents:
//!
//!   out_dir/
//!     kv/             <- RocksDB checkpoint (hard-linked SSTs
//!                        where possible; safe to copy elsewhere)
//!     meta.sqlite     <- SQLite VACUUM INTO of the meta store
//!     manifest.json   <- timestamp, sizes, version
//!
//! Restore is **offline**: stop the server, copy the contents of
//! `kv/` into the target KV dir and `meta.sqlite` into the
//! target meta path, and restart. See `docs/ARCHITECTURE.md §
//! Point-in-time backup` for the full runbook.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use agentdb_meta::MetaStore;
use axum::extract::{Extension, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;
use crate::AppState;

/// Backup manifest version. Bump on a breaking change to the
/// directory layout. Restore helpers check this before trying
/// to copy files over.
pub const BACKUP_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct BackupRequest {
    /// Absolute path to a directory that does NOT yet exist.
    /// The endpoint creates it and writes `kv/`, `meta.sqlite`,
    /// and `manifest.json` into it.
    pub out_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackupManifest {
    pub version: u32,
    pub created_at_ms: i64,
    pub kv_bytes: u64,
    pub meta_bytes: u64,
    pub kv_path: String,
    pub meta_path: String,
    /// Node id of the process that produced this backup. `None`
    /// when a meta-less test registry produced it (no node
    /// identity).
    pub node_id: Option<String>,
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
    validate_dest(&out_dir)?;

    // Create the top-level dir. If this fails we don't leave
    // anything behind on disk.
    std::fs::create_dir_all(&out_dir).map_err(|e| {
        ApiError::Internal(format!("create backup dir {}: {e}", out_dir.display()))
    })?;

    // KV checkpoint. Path MUST NOT pre-exist; rocksdb
    // `create_checkpoint` enforces that — but we've just
    // created the parent, so the kv subpath is fresh.
    let kv_path = out_dir.join("kv");
    state
        .registry
        .kv()
        .checkpoint(&kv_path)
        .map_err(|e| ApiError::Internal(format!("kv checkpoint: {e}")))?;

    // Meta snapshot.
    let meta_path = out_dir.join("meta.sqlite");
    meta.backup(&meta_path)
        .await
        .map_err(|e| ApiError::Internal(format!("meta backup: {e}")))?;

    let kv_bytes = dir_size(&kv_path).unwrap_or(0);
    let meta_bytes = std::fs::metadata(&meta_path)
        .map(|m| m.len())
        .unwrap_or(0);

    let manifest = BackupManifest {
        version: BACKUP_MANIFEST_VERSION,
        created_at_ms: crate::stream::now_ms() as i64,
        kv_bytes,
        meta_bytes,
        kv_path: kv_path.to_string_lossy().into_owned(),
        meta_path: meta_path.to_string_lossy().into_owned(),
        node_id: state.registry.node_id().map(|s| s.to_string()),
    };

    let manifest_json = serde_json::to_vec_pretty(&manifest).map_err(|e| {
        ApiError::Internal(format!("serialize manifest: {e}"))
    })?;
    std::fs::write(out_dir.join("manifest.json"), manifest_json).map_err(|e| {
        ApiError::Internal(format!("write manifest.json: {e}"))
    })?;

    Ok(Json(manifest))
}

fn validate_dest(out_dir: &Path) -> Result<(), ApiError> {
    if !out_dir.is_absolute() {
        return Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("out_dir".into()),
            message: "out_dir must be an absolute path".into(),
        }));
    }
    if out_dir.exists() {
        return Err(ApiError::Core(agentdb_core::Error::Invalid {
            field: Some("out_dir".into()),
            message: format!(
                "out_dir already exists: {}. Backup endpoints never overwrite.",
                out_dir.display()
            ),
        }));
    }
    Ok(())
}

/// Recursively sum file sizes under `path`. Any stat error on a
/// child is swallowed — the byte count is informational on the
/// manifest, not a correctness signal.
fn dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let meta = std::fs::metadata(&p)?;
        if meta.is_file() {
            total += meta.len();
        } else if meta.is_dir() {
            for entry in std::fs::read_dir(&p)? {
                let entry = entry?;
                stack.push(entry.path());
            }
        }
    }
    Ok(total)
}
