//! M26 final phase: writes route via the session locator.
//!
//! Sets up a meta-backed registry, pre-populates a locator
//! row that pins a session to a specific box, and drives a
//! real `append_and_fetch` through the same primitive the
//! REST/Mongo write path uses. The event must land on the
//! locator-pinned box, even though tenant-hash routing for
//! that tenant resolves to a different one.

use std::sync::Arc;

use agentdb_core::{
    append_and_fetch, BoxRegistry, HlcClock, SessionRegistry, TenantRegistry,
};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn write_to_locator_pinned_session_lands_on_locator_box() {
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

    // Pick a box that is NOT the tenant-hash target. The
    // locator will pin "s-pinned" to this distinct box; if
    // the locator-first routing works, the write lands here
    // and not on the tenant-hash box.
    let tenant_hash_box = registry.route_tenant("t-1").id;
    let pinned_box: u16 = if tenant_hash_box == 3 { 0 } else { tenant_hash_box + 1 };
    assert_ne!(tenant_hash_box, pinned_box);

    agentdb_meta::session_locators::upsert(
        meta.pool(),
        "s-pinned",
        7,
        "t-1",
        Some(pinned_box as i64),
        agentdb_core::shard::shard_id("s-pinned") as i32,
        0,
    )
    .await
    .unwrap();

    // Drive the write through the real primitive.
    append_and_fetch(
        &registry,
        &tenants,
        &sessions,
        "t-1",
        "s-pinned",
        json!({"text": "locator-routed"}),
        0,
        100,
    )
    .await
    .unwrap();

    // Verify: the tenant exists on the pinned box, not the
    // tenant-hash box. Tenants get minted via
    // `tenants.get_or_mint(box_.id, ...)` inside the write
    // path — landing on the right box puts the row in the
    // right per-box KV slot.
    assert!(
        tenants.lookup(pinned_box, "t-1").unwrap().is_some(),
        "tenant should have been minted on the locator-pinned box ({pinned_box})"
    );
    assert!(
        tenants.lookup(tenant_hash_box, "t-1").unwrap().is_none(),
        "tenant should NOT have been minted on the tenant-hash box \
         ({tenant_hash_box}); locator-first routing failed"
    );

    // The session record likewise lives on the pinned box.
    let tenant_on_pinned = tenants.lookup(pinned_box, "t-1").unwrap().unwrap();
    let session = sessions
        .lookup(pinned_box, tenant_on_pinned.internal_id, "s-pinned")
        .unwrap()
        .expect("session record should exist on the pinned box");
    assert_eq!(session.box_id, pinned_box);
    assert_eq!(session.next_seq, 2, "one event committed → next_seq=2");
}
