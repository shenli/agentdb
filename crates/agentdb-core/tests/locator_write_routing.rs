//! M27: the locator is the canonical session-ownership record
//! but is not on the write hot path. This test verifies that
//! session mint stamps the locator with the box that ended up
//! hosting the session — so fork lookups, admin enumeration,
//! and fanout reads can rely on it.

use std::sync::Arc;

use agentdb_core::{append_and_fetch, BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn session_mint_stamps_locator_with_hosting_box() {
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
            kv.clone(), store, 4, Arc::new(HlcClock::default()), meta.clone(), "self".into(),
        ).await.unwrap(),
    );
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));

    // Mint via a real append.
    append_and_fetch(
        &registry, &tenants, &sessions,
        "t-1", "s-mint", json!({"hello": "world"}), 0, 100,
    ).await.unwrap();

    // Locator should now exist and point at the box where the session lives.
    let locator = agentdb_meta::session_locators::get(meta.pool(), "s-mint")
        .await
        .unwrap()
        .expect("populate_session_locator should have created the row on mint");

    // The session ended up on whichever box tenant-hash routed to.
    let expected_box = registry.route_tenant("t-1").id as i64;
    assert_eq!(
        locator.tablet_id,
        Some(expected_box),
        "locator's tablet_id must match the box that hosts the session"
    );
    assert_eq!(locator.tenant_external_id.as_deref(), Some("t-1"));
}
