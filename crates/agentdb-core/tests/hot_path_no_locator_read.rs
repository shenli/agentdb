//! M27 hot-path correctness: a steady-state append must NOT
//! synchronously read `session_locators`. Writes route via
//! `LocationProxy` (override) or its tenant-hash fallback;
//! the locator is only touched by mint (`populate_session_locator`,
//! an UPSERT not a GET), fork lookups, admin, and fanout reads.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use agentdb_core::{append_and_fetch, BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_meta::session_locators::LOCATOR_GETS;
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn steady_state_append_does_not_read_locator() {
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            4,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "self".into(),
        )
        .await
        .unwrap(),
    );
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));

    // Warmup: first write mints the session (UPSERTs the locator,
    // which doesn't increment LOCATOR_GETS — only `get` does).
    append_and_fetch(
        &registry, &tenants, &sessions,
        "t-1", "s-hot", json!({"x": 0}), 0, 100,
    ).await.unwrap();

    let baseline = LOCATOR_GETS.load(Ordering::Relaxed);

    // 5 steady-state writes to the same session.
    for i in 1..=5 {
        append_and_fetch(
            &registry, &tenants, &sessions,
            "t-1", "s-hot", json!({"x": i}), 0, 100,
        ).await.unwrap();
    }

    let after = LOCATOR_GETS.load(Ordering::Relaxed);
    assert_eq!(
        after, baseline,
        "steady-state append must not call session_locators::get \
         (baseline={baseline}, after={after})"
    );
}
