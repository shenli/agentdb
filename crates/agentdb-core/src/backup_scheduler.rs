//! Periodic point-in-time backup.
//!
//! Runs on the scheduler leader so a multi-node cluster
//! doesn't produce duplicate backups. The tick interval,
//! destination dir, and retention count come from env
//! (`AGENTDB_BACKUP_INTERVAL_SECS`, `AGENTDB_BACKUP_DIR`,
//! `AGENTDB_BACKUP_RETAIN`). When the interval is unset,
//! the scheduler is not started at all.
//!
//! Each tick writes `<base>/backup-<ms>/` via
//! [`crate::backup::run_backup`] and then prunes the dir
//! so only the newest `retain` subdirs survive.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentdb_meta::MetaStore;
use agentdb_storage::KvStore;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use utoipa::ToSchema;

use crate::backup::{prune_backup_dirs, run_backup};
use crate::scheduler::LeaderElection;

/// Runtime config for the scheduler.
#[derive(Debug, Clone)]
pub struct BackupSchedulerConfig {
    pub base_dir: PathBuf,
    pub interval: Duration,
    /// Keep the newest `retain` backups; delete older ones
    /// after each run. `0` disables retention (operators prune
    /// by hand).
    pub retain: usize,
}

impl BackupSchedulerConfig {
    /// Parse from env vars. Returns `None` when the interval
    /// env isn't set (the scheduler is off).
    ///
    /// - `AGENTDB_BACKUP_INTERVAL_SECS` (required)
    /// - `AGENTDB_BACKUP_DIR` (required when interval is set)
    /// - `AGENTDB_BACKUP_RETAIN` (default 7)
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(secs) = std::env::var("AGENTDB_BACKUP_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return Ok(None);
        };
        if secs == 0 {
            return Err(
                "AGENTDB_BACKUP_INTERVAL_SECS must be > 0 or unset".into(),
            );
        }
        let Some(base_dir) = std::env::var("AGENTDB_BACKUP_DIR").ok().map(PathBuf::from)
        else {
            return Err(
                "AGENTDB_BACKUP_INTERVAL_SECS set but AGENTDB_BACKUP_DIR is missing".into(),
            );
        };
        if !base_dir.is_absolute() {
            return Err(format!(
                "AGENTDB_BACKUP_DIR must be absolute; got {}",
                base_dir.display()
            ));
        }
        let retain = std::env::var("AGENTDB_BACKUP_RETAIN")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(7);
        Ok(Some(Self {
            base_dir,
            interval: Duration::from_secs(secs),
            retain,
        }))
    }
}

/// Snapshot of the scheduler's recent activity. Surfaced via
/// `GET /api/admin/backup/status` so operators can answer "when
/// did the last backup succeed / fail?" without SSHing to the
/// box.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct BackupSchedulerStatus {
    /// `true` when the scheduler is configured and running on
    /// this process (leader-gated; followers report `false`).
    pub enabled: bool,
    /// Wall-clock ms of the most recent successful run.
    /// `None` before the first success.
    pub last_success_ms: Option<i64>,
    /// Manifest created_at of the most recent success.
    /// Present iff `last_success_ms` is present.
    pub last_manifest_created_ms: Option<i64>,
    /// Path (relative or absolute) of the most recent success.
    pub last_out_dir: Option<String>,
    /// Bytes on disk for the most recent success (kv + meta).
    pub last_total_bytes: Option<u64>,
    /// Wall-clock ms of the most recent failure.
    pub last_failure_ms: Option<i64>,
    /// Human-readable reason for the most recent failure.
    pub last_failure: Option<String>,
    /// Number of `backup-*` subdirs currently under
    /// `base_dir` (i.e. post-retention count after the last
    /// run).
    pub backups_on_disk: u64,
    pub runs_total: u64,
    pub failures_total: u64,
    pub base_dir: Option<String>,
    pub retain: Option<u64>,
    pub interval_secs: Option<u64>,
}

/// Shared status slot. The scheduler loop writes; readers
/// (admin status endpoint, /metrics exposition) take read
/// snapshots.
#[derive(Default)]
pub struct BackupSchedulerHandle {
    inner: RwLock<BackupSchedulerStatus>,
    runs_total: AtomicU64,
    failures_total: AtomicU64,
}

impl BackupSchedulerHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current snapshot — cheap clone; fine to call from a
    /// request handler.
    pub fn snapshot(&self) -> BackupSchedulerStatus {
        let mut s = self.inner.read().clone();
        s.runs_total = self.runs_total.load(Ordering::Relaxed);
        s.failures_total = self.failures_total.load(Ordering::Relaxed);
        s
    }

    /// Runs-total counter. Exposed for `/metrics`.
    pub fn runs_total(&self) -> u64 {
        self.runs_total.load(Ordering::Relaxed)
    }

    /// Failures counter. Exposed for `/metrics`.
    pub fn failures_total(&self) -> u64 {
        self.failures_total.load(Ordering::Relaxed)
    }

    fn record_success(
        &self,
        manifest: &crate::backup::BackupManifest,
        out_dir: &str,
        backups_on_disk: u64,
    ) {
        self.runs_total.fetch_add(1, Ordering::Relaxed);
        let mut w = self.inner.write();
        w.last_success_ms = Some(now_ms());
        w.last_manifest_created_ms = Some(manifest.created_at_ms);
        w.last_out_dir = Some(out_dir.to_string());
        w.last_total_bytes = Some(manifest.kv_bytes + manifest.meta_bytes);
        w.backups_on_disk = backups_on_disk;
    }

    fn record_failure(&self, reason: String) {
        self.failures_total.fetch_add(1, Ordering::Relaxed);
        let mut w = self.inner.write();
        w.last_failure_ms = Some(now_ms());
        w.last_failure = Some(reason);
    }

    fn initialize(&self, cfg: &BackupSchedulerConfig) {
        let mut w = self.inner.write();
        w.enabled = true;
        w.base_dir = Some(cfg.base_dir.to_string_lossy().into_owned());
        w.retain = Some(cfg.retain as u64);
        w.interval_secs = Some(cfg.interval.as_secs());
    }
}

/// Owns the scheduler's background task. Drop to abort.
pub struct BackupScheduler {
    handle: Arc<BackupSchedulerHandle>,
    _task: JoinHandle<()>,
}

impl BackupScheduler {
    /// Spawn the scheduler. Runs on `interval` ticks only when
    /// `election.is_leader()` is true — followers wake up,
    /// notice they aren't leader, and go back to sleep.
    pub fn start(
        kv: Arc<dyn KvStore>,
        meta: Arc<MetaStore>,
        election: Arc<LeaderElection>,
        node_id: Option<String>,
        cfg: BackupSchedulerConfig,
    ) -> Arc<Self> {
        let handle = Arc::new(BackupSchedulerHandle::new());
        handle.initialize(&cfg);
        let h = handle.clone();
        let task = tokio::spawn(run_loop(h, kv, meta, election, node_id, cfg));
        Arc::new(Self { handle, _task: task })
    }

    pub fn status_handle(&self) -> Arc<BackupSchedulerHandle> {
        self.handle.clone()
    }
}

async fn run_loop(
    handle: Arc<BackupSchedulerHandle>,
    kv: Arc<dyn KvStore>,
    meta: Arc<MetaStore>,
    election: Arc<LeaderElection>,
    node_id: Option<String>,
    cfg: BackupSchedulerConfig,
) {
    let mut ticker = tokio::time::interval(cfg.interval);
    // First tick fires immediately; skip it so a restart loop
    // doesn't hammer disk. Backups are scheduled, not
    // startup-triggered.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if !election.is_leader() {
            tracing::debug!("backup scheduler: not leader, skipping tick");
            continue;
        }
        run_one_tick(&handle, &kv, &meta, node_id.as_deref(), &cfg).await;
    }
}

async fn run_one_tick(
    handle: &Arc<BackupSchedulerHandle>,
    kv: &Arc<dyn KvStore>,
    meta: &Arc<MetaStore>,
    node_id: Option<&str>,
    cfg: &BackupSchedulerConfig,
) {
    // Dest dir stamped with wall-clock ms — monotonic enough
    // for retention's mtime sort, and human-readable at a
    // glance.
    let out = cfg.base_dir.join(format!("backup-{}", now_ms()));

    // Ensure base_dir exists before we try to create a child
    // under it. RocksDB's checkpoint rejects pre-existing
    // `out`, but we're creating `out` ourselves inside
    // `run_backup`.
    if let Err(e) = std::fs::create_dir_all(&cfg.base_dir) {
        handle.record_failure(format!("create base_dir {}: {e}", cfg.base_dir.display()));
        return;
    }

    match run_backup(kv, meta, &out, node_id).await {
        Ok(manifest) => {
            // Prune *after* a successful backup so a failed
            // prune never destroys the last good copy.
            if let Err(e) = prune_backup_dirs(&cfg.base_dir, cfg.retain) {
                tracing::warn!(error = %e, "backup retention prune failed");
            }
            let remaining = crate::backup::list_backup_dirs(&cfg.base_dir)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
            handle.record_success(&manifest, &out.to_string_lossy(), remaining);
            tracing::info!(
                out = %out.display(),
                kv_bytes = manifest.kv_bytes,
                meta_bytes = manifest.meta_bytes,
                backups_on_disk = remaining,
                "scheduled backup completed",
            );
        }
        Err(e) => {
            // Clean up a partial directory so retention counts
            // remain accurate.
            let _ = std::fs::remove_dir_all(&out);
            handle.record_failure(e.to_string());
            tracing::warn!(error = %e, "scheduled backup failed");
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
