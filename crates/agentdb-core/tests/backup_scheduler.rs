//! End-to-end: scheduled backup loop produces backups on
//! the configured interval, retention prunes older ones,
//! the status handle reflects what happened.

use std::sync::Arc;
use std::time::Duration;

use agentdb_core::{
    BackupScheduler, BackupSchedulerConfig, LeaderElection, LeaderElectionConfig,
};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use tempfile::TempDir;

async fn setup_leader() -> (tempfile::NamedTempFile, Arc<MetaStore>, Arc<LeaderElection>) {
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());

    // Short election intervals so the test becomes leader quickly.
    let cfg = LeaderElectionConfig {
        lease_duration: Duration::from_secs(5),
        renew_interval: Duration::from_millis(200),
        attempt_interval: Duration::from_millis(100),
    };
    let election = LeaderElection::start(meta.clone(), "test-node".into(), cfg);
    // Poll until we're elected — single-node race is usually
    // resolved in one attempt_interval tick.
    for _ in 0..50 {
        if election.is_leader() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        election.is_leader(),
        "test failed to elect single-node leader"
    );
    (meta_file, meta, election)
}

#[tokio::test]
async fn scheduler_produces_backups_on_tick_and_retains_newest() {
    let (_meta_file, meta, election) = setup_leader().await;
    let kv_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    // Write something so the kv checkpoint has content.
    kv.put(b"hello", b"world").unwrap();

    let base = TempDir::new().unwrap();
    let cfg = BackupSchedulerConfig {
        base_dir: base.path().to_path_buf(),
        interval: Duration::from_millis(150),
        retain: 2,
    };
    let scheduler = BackupScheduler::start(
        kv,
        meta.clone(),
        election.clone(),
        Some("test-node".into()),
        cfg,
    );
    let handle = scheduler.status_handle();

    // Each tick takes the full interval; wait long enough
    // for 4-5 ticks so retention actually has work to do.
    tokio::time::sleep(Duration::from_millis(900)).await;

    // At most `retain` subdirs should remain.
    let dirs = agentdb_core::list_backup_dirs(base.path()).unwrap();
    assert!(
        dirs.len() <= 2,
        "retention should cap dir count at 2; got {} ({:?})",
        dirs.len(),
        dirs.iter().map(|p| p.file_name().unwrap()).collect::<Vec<_>>(),
    );
    assert!(!dirs.is_empty(), "at least one backup should have landed");

    // Each surviving dir has the expected layout.
    for d in &dirs {
        assert!(d.join("kv").is_dir(), "kv/ missing under {}", d.display());
        assert!(d.join("meta.sqlite").is_file(), "meta.sqlite missing");
        assert!(d.join("manifest.json").is_file(), "manifest.json missing");
    }

    // Status reflects real activity.
    let snap = handle.snapshot();
    assert!(snap.enabled, "status.enabled should be true");
    assert!(snap.runs_total >= 2, "runs_total: {}", snap.runs_total);
    assert!(
        snap.last_success_ms.is_some(),
        "last_success_ms should be set"
    );
    assert!(snap.failures_total == 0, "unexpected failures: {}", snap.failures_total);
    assert!(snap.backups_on_disk >= 1);
    assert!(snap.base_dir.is_some());
    assert_eq!(snap.retain, Some(2));
}

#[tokio::test]
async fn status_records_failure_when_backup_cannot_run() {
    // Point the scheduler at a base_dir that can't be written
    // (parent is a file, not a dir). Every tick should fail;
    // `failures_total` + `last_failure` must reflect that,
    // and no rogue backup dir should survive.
    let (_meta_file, meta, election) = setup_leader().await;
    let kv_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());

    // base_dir is a *file* path, not a dir. create_dir_all
    // on a file refuses.
    let conflict_dir = TempDir::new().unwrap();
    let file_path = conflict_dir.path().join("this-is-a-file");
    std::fs::write(&file_path, b"blocking").unwrap();

    let cfg = BackupSchedulerConfig {
        base_dir: file_path,
        interval: Duration::from_millis(120),
        retain: 2,
    };
    let scheduler = BackupScheduler::start(
        kv,
        meta,
        election,
        Some("test-node".into()),
        cfg,
    );
    let handle = scheduler.status_handle();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let snap = handle.snapshot();
    assert!(
        snap.failures_total >= 1,
        "expected at least one failure; got runs={} fails={} last_failure={:?}",
        snap.runs_total,
        snap.failures_total,
        snap.last_failure,
    );
    assert!(snap.last_failure.is_some());
}

#[test]
fn config_from_env_requires_both_interval_and_dir() {
    // Emulate the bail path: interval set, dir missing.
    // Can't mutate env safely across parallel tests — isolate
    // via setting + unsetting in-scope. The env-var set is
    // best-effort (safety: SAFETY of SetEnv is fine at
    // test-time).
    // Use a serial pattern by checking both vars in one go.
    std::env::remove_var("AGENTDB_BACKUP_INTERVAL_SECS");
    std::env::remove_var("AGENTDB_BACKUP_DIR");
    assert!(matches!(
        BackupSchedulerConfig::from_env(),
        Ok(None)
    ));

    std::env::set_var("AGENTDB_BACKUP_INTERVAL_SECS", "60");
    std::env::remove_var("AGENTDB_BACKUP_DIR");
    let err = BackupSchedulerConfig::from_env().unwrap_err();
    assert!(
        err.contains("AGENTDB_BACKUP_DIR"),
        "error should name the missing var: {err}",
    );

    std::env::set_var("AGENTDB_BACKUP_DIR", "/abs/path");
    let parsed = BackupSchedulerConfig::from_env().unwrap().unwrap();
    assert_eq!(parsed.interval.as_secs(), 60);
    assert_eq!(parsed.base_dir.to_string_lossy(), "/abs/path");
    // Default retain = 7.
    assert_eq!(parsed.retain, 7);

    // Relative path is rejected.
    std::env::set_var("AGENTDB_BACKUP_DIR", "rel/path");
    let err = BackupSchedulerConfig::from_env().unwrap_err();
    assert!(err.contains("absolute"));

    std::env::remove_var("AGENTDB_BACKUP_INTERVAL_SECS");
    std::env::remove_var("AGENTDB_BACKUP_DIR");
}
