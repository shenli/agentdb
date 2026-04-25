//! M26 Phase 3: Mongo wire `find` over a tenant whose
//! sessions live on more than one local box.
//!
//! Pre-Phase-3 behavior: an unpinned `find` anchored on the
//! tenant-hash box and only saw sessions that happened to land
//! there. Once a session moved (via shard override or future
//! tablet handoff), an unpinned find lost it silently. This
//! test pins down the contract: with the locator-driven
//! fanout in place, an unpinned find returns events from every
//! local box that hosts a session of the tenant.

use std::sync::Arc;

use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_meta::{shards as meta_shards, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use mongodb::options::ClientOptions;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

async fn spawn() -> (
    tempfile::NamedTempFile,
    TempDir,
    TempDir,
    Arc<MetaStore>,
    Arc<agentdb_core::LocationProxy>,
    u16,
) {
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    meta_shards::seed_shards(meta.pool(), agentdb_core::SHARD_COUNT as i64, 0)
        .await
        .unwrap();

    let data = TempDir::new().unwrap();
    let obj = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(data.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj.path()).unwrap());
    // `new_with_meta` wires the registry's meta_store, which is
    // what activates the locator-driven fanout in find_response.
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            4,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "node-a".into(),
        )
        .await
        .unwrap(),
    );
    let location = agentdb_core::LocationProxy::new(meta.clone()).await.unwrap();
    registry.attach_location(location.clone()).ok();

    let state = WireState::new(
        registry,
        Arc::new(TenantRegistry::new(kv.clone())),
        Arc::new(SessionRegistry::new(kv)),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = serve(listener, state).await;
    });
    (meta_file, data, obj, meta, location, port)
}

#[tokio::test]
async fn unpinned_find_fans_out_across_boxes_within_one_tenant() {
    // Two sessions of the same tenant routed to two different
    // boxes via shard overrides:
    //   - "s-on-zero": shard_id(s-on-zero) → tablet 0 (box 0)
    //   - "s-on-three": shard_id(s-on-three) → tablet 3 (box 3)
    // Both inserts succeed locally. An unpinned find on the
    // tenant must see events from BOTH sessions.
    let (_m, _d, _o, meta, location, port) = spawn().await;

    let session_a = "s-on-zero";
    let session_b = "s-on-three";
    let shard_a = agentdb_core::shard_id(session_a) as i64;
    let shard_b = agentdb_core::shard_id(session_b) as i64;
    // Pin each shard to a distinct local tablet/box so the
    // sessions definitively don't share storage. now=0 for the
    // override timestamp.
    meta_shards::set_assignment(meta.pool(), shard_a, Some(0), 0)
        .await
        .unwrap();
    meta_shards::set_assignment(meta.pool(), shard_b, Some(3), 0)
        .await
        .unwrap();
    // Force the LocationProxy to pick up the overrides
    // synchronously rather than wait on its periodic tick.
    location.refresh().await.unwrap();

    let opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .await
        .unwrap();
    let client = mongodb::Client::with_options(opts).unwrap();
    let coll = client
        .database("t-fanout")
        .collection::<bson::Document>("events");
    coll.insert_one(doc! { "session_id": session_a, "kind": "from-zero" })
        .await
        .unwrap();
    coll.insert_one(doc! { "session_id": session_b, "kind": "from-three" })
        .await
        .unwrap();

    // Sanity-check the locator captured both sessions on
    // distinct tablets.
    let loc_a = agentdb_meta::session_locators::get(meta.pool(), session_a)
        .await
        .unwrap()
        .expect("locator for session a");
    let loc_b = agentdb_meta::session_locators::get(meta.pool(), session_b)
        .await
        .unwrap()
        .expect("locator for session b");
    assert_ne!(
        loc_a.tablet_id, loc_b.tablet_id,
        "test setup expected sessions on different tablets; got a={:?} b={:?}",
        loc_a.tablet_id, loc_b.tablet_id
    );

    // Unpinned find: no session_id in the filter.
    use futures_util::TryStreamExt;
    let cursor = coll.find(doc! {}).await.unwrap();
    let docs: Vec<bson::Document> = cursor.try_collect().await.unwrap();
    let kinds: std::collections::HashSet<String> = docs
        .iter()
        .filter_map(|d| {
            d.get_document("payload")
                .ok()
                .and_then(|p| p.get_str("kind").ok())
                .map(|s| s.to_string())
        })
        .collect();
    assert!(
        kinds.contains("from-zero") && kinds.contains("from-three"),
        "fanout expected both kinds; got {kinds:?}"
    );
}
