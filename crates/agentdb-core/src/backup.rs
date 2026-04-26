//! Point-in-time backup primitive.
//!
//! Shared between the admin-triggered endpoint
//! (`POST /api/admin/backup`) and the [`BackupScheduler`]
//! background loop. One place that knows the on-disk layout:
//!
//!   out_dir/
//!     kv/             <- `KvStore::checkpoint` result
//!     meta.sqlite     <- `MetaStore::backup` VACUUM INTO file
//!     manifest.json   <- version + sizes + timestamps
//!
//! `out_dir` must not pre-exist — the contract is "no silent
//! overwrite", enforced here and by the underlying primitives.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use agentdb_meta::MetaStore;
use agentdb_storage::KvStore;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{Error, Result};

/// Backup manifest version. Bump on a breaking change to the
/// on-disk layout. Restore helpers check this before copying
/// files over.
pub const BACKUP_MANIFEST_VERSION: u32 = 1;

/// Metadata written alongside every backup. Identical whether
/// the backup came from the admin endpoint or the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackupManifest {
    pub version: u32,
    pub created_at_ms: i64,
    pub kv_bytes: u64,
    pub meta_bytes: u64,
    pub kv_path: String,
    pub meta_path: String,
    /// Node id that produced this backup, when available.
    pub node_id: Option<String>,
}

/// Run one backup into `out_dir`. Creates the directory; refuses
/// to overwrite. Writes `kv/`, `meta.sqlite`, and a pretty-printed
/// `manifest.json` (the same structure that's also returned from
/// the function for programmatic callers).
pub async fn run_backup(
    kv: &Arc<dyn KvStore>,
    meta: &MetaStore,
    out_dir: &Path,
    node_id: Option<&str>,
) -> Result<BackupManifest> {
    if !out_dir.is_absolute() {
        return Err(Error::Invalid {
            field: Some("out_dir".into()),
            message: "out_dir must be an absolute path".into(),
        });
    }
    if out_dir.exists() {
        return Err(Error::Invalid {
            field: Some("out_dir".into()),
            message: format!(
                "out_dir already exists: {}. Backup never overwrites.",
                out_dir.display()
            ),
        });
    }

    std::fs::create_dir_all(out_dir).map_err(|e| Error::Invalid {
        field: Some("out_dir".into()),
        message: format!("create {}: {e}", out_dir.display()),
    })?;

    let kv_path = out_dir.join("kv");
    kv.checkpoint(&kv_path).map_err(|e| Error::Invalid {
        field: Some("out_dir".into()),
        message: format!("kv checkpoint: {e}"),
    })?;

    let meta_path = out_dir.join("meta.sqlite");
    meta.backup(&meta_path)
        .await
        .map_err(|e| Error::Invalid {
            field: Some("out_dir".into()),
            message: format!("meta backup: {e}"),
        })?;

    let kv_bytes = dir_size(&kv_path).unwrap_or(0);
    let meta_bytes = std::fs::metadata(&meta_path)
        .map(|m| m.len())
        .unwrap_or(0);

    let manifest = BackupManifest {
        version: BACKUP_MANIFEST_VERSION,
        created_at_ms: now_ms(),
        kv_bytes,
        meta_bytes,
        kv_path: kv_path.to_string_lossy().into_owned(),
        meta_path: meta_path.to_string_lossy().into_owned(),
        node_id: node_id.map(|s| s.to_string()),
    };

    let manifest_json = serde_json::to_vec_pretty(&manifest).map_err(Error::from)?;
    std::fs::write(out_dir.join("manifest.json"), manifest_json).map_err(|e| {
        Error::Invalid {
            field: Some("out_dir".into()),
            message: format!("write manifest.json: {e}"),
        }
    })?;

    Ok(manifest)
}

/// List every backup dir directly under `base_dir` whose name
/// starts with `backup-`. Return ordered oldest → newest by
/// directory mtime (falling back to dir name if mtime is
/// unavailable). Used by the scheduler's retention pass; also
/// useful to status endpoints to answer "how many backups?".
pub fn list_backup_dirs(base_dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    if !base_dir.exists() {
        return Ok(Vec::new());
    }
    for entry in std::fs::read_dir(base_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with("backup-") {
            continue;
        }
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        out.push((path, mtime));
    }
    out.sort_by_key(|a| a.1);
    Ok(out.into_iter().map(|(p, _)| p).collect())
}

/// Delete all but the newest `retain` backup dirs under
/// `base_dir`. No-op when `retain == 0` (operators treat 0 as
/// "retention disabled" — keep everything).
pub fn prune_backup_dirs(base_dir: &Path, retain: usize) -> std::io::Result<Vec<PathBuf>> {
    if retain == 0 {
        return Ok(Vec::new());
    }
    let all = list_backup_dirs(base_dir)?;
    if all.len() <= retain {
        return Ok(Vec::new());
    }
    let to_remove_count = all.len() - retain;
    let to_remove: Vec<PathBuf> = all.into_iter().take(to_remove_count).collect();
    for p in &to_remove {
        std::fs::remove_dir_all(p)?;
    }
    Ok(to_remove)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Recursively sum file sizes under `path`. Stat errors on
/// children are swallowed — the byte count is informational on
/// the manifest, not a correctness signal.
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_backup_dir(base: &Path, name: &str) {
        let dir = base.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("marker"), name).unwrap();
    }

    #[test]
    fn list_finds_only_backup_prefixed_dirs_newest_last() {
        let tmp = TempDir::new().unwrap();
        make_backup_dir(tmp.path(), "backup-2000");
        // Space out mtimes so sort is deterministic.
        std::thread::sleep(std::time::Duration::from_millis(15));
        make_backup_dir(tmp.path(), "backup-3000");
        std::thread::sleep(std::time::Duration::from_millis(15));
        make_backup_dir(tmp.path(), "backup-4000");
        // Unrelated dir — must be ignored.
        std::fs::create_dir_all(tmp.path().join("not-a-backup")).unwrap();

        let dirs = list_backup_dirs(tmp.path()).unwrap();
        let names: Vec<&str> = dirs
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(names, vec!["backup-2000", "backup-3000", "backup-4000"]);
    }

    #[test]
    fn prune_keeps_newest_n_and_deletes_the_rest() {
        let tmp = TempDir::new().unwrap();
        for (i, n) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            make_backup_dir(tmp.path(), &format!("backup-{i:03}-{n}"));
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let removed = prune_backup_dirs(tmp.path(), 2).unwrap();
        let remaining = list_backup_dirs(tmp.path()).unwrap();
        let rem_names: Vec<&str> = remaining
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(rem_names.len(), 2);
        assert_eq!(removed.len(), 3);
        // The two kept should be the newest by insertion order.
        assert!(rem_names
            .iter()
            .all(|n| n.ends_with("-d") || n.ends_with("-e")));
    }

    #[test]
    fn prune_with_retain_zero_is_a_no_op() {
        let tmp = TempDir::new().unwrap();
        for n in ["a", "b", "c"] {
            make_backup_dir(tmp.path(), &format!("backup-{n}"));
        }
        let removed = prune_backup_dirs(tmp.path(), 0).unwrap();
        assert!(removed.is_empty());
        assert_eq!(list_backup_dirs(tmp.path()).unwrap().len(), 3);
    }

    #[test]
    fn prune_when_fewer_than_retain_is_a_no_op() {
        let tmp = TempDir::new().unwrap();
        make_backup_dir(tmp.path(), "backup-a");
        let removed = prune_backup_dirs(tmp.path(), 5).unwrap();
        assert!(removed.is_empty());
        assert_eq!(list_backup_dirs(tmp.path()).unwrap().len(), 1);
    }

    #[test]
    fn list_returns_empty_when_base_dir_missing() {
        let tmp = TempDir::new().unwrap();
        let missing = tmp.path().join("no-such-place");
        let dirs = list_backup_dirs(&missing).unwrap();
        assert!(dirs.is_empty());
    }
}
