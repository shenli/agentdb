//! Cross-node Mongo wire `find` fanout — end-to-end.
//!
//! Pre-PR contract: an unpinned `find` on a tenant whose
//! sessions live on more than one node returned only the
//! sessions whose locator pointed at a *locally-owned*
//! tablet; cross-node sessions were silently skipped (with a
//! tracing warn). This test pins down the new contract: with
//! the locator-driven cross-node fanout in place, an unpinned
//! find returns events from every session on the tenant,
//! including ones owned by peer nodes.
//!
//! Setup mirrors `cross_node_forward.rs`: two wire servers
//! sharing meta + object storage. Two sessions of the same
//! tenant; one routed to a tablet owned by node-a, one to a
//! tablet owned by node-b. Inserts target each session — the
//! cross-node insert exercises the existing wire-level
//! forward, populating the peer's KV and the shared
//! session_locators table. The unpinned find on node-a then
//! exercises the new pinned-find OP_MSG fanout to node-b.

use std::sync::Arc;

use agentdb_core::{
    BoxRegistry, HlcClock, LocationProxy, NodeAddressProxy, SessionRegistry, TenantRegistry,
    SHARD_COUNT,
};
use agentdb_meta::{
    nodes as meta_nodes, shards as meta_shards, tablets as meta_tablets, MetaStore,
};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use futures_util::TryStreamExt;
use mongodb::options::ClientOptions;
use mongodb::Client;
use tempfile::TempDir;

async fn client(port: u16) -> Client {
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    let mut options = ClientOptions::parse(&uri).await.unwrap();
    options.credential = None;
    Client::with_options(options).unwrap()
}

#[tokio::test]
async fn unpinned_find_via_node_a_includes_events_owned_by_node_b() {
    // Shared meta store: both nodes read/write the same
    // tablets/nodes/session_locators tables.
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta = Arc::new(
        MetaStore::open_sqlite(&meta_file.path().to_string_lossy())
            .await
            .unwrap(),
    );

    // Find a session id whose hash routes to tablet 0 (node-a)
    // and another whose hash routes to tablet 1 (node-b).
    let session_a = (0u64..1024)
        .map(|i| format!("s-on-a-{i}"))
        .find(|s| (agentdb_core::shard_id(s) as i64) % 4 == 0)
        .expect("find a session whose default shard hash routes to box 0");
    let session_b = (0u64..1024)
        .map(|i| format!("s-on-b-{i}"))
        .find(|s| (agentdb_core::shard_id(s) as i64) % 4 == 1)
        .expect("find a session whose default shard hash routes to box 1");
    let shard_a = agentdb_core::shard_id(&session_a) as i64;
    let shard_b = agentdb_core::shard_id(&session_b) as i64;

    meta_shards::seed_shards(meta.pool(), SHARD_COUNT as i64, 100)
        .await
        .unwrap();
    meta_tablets::get_or_create(meta.pool(), 0, 100)
        .await
        .unwrap();
    meta_tablets::get_or_create(meta.pool(), 1, 100)
        .await
        .unwrap();
    meta_tablets::assume_ownership(meta.pool(), 0, 0, "node-a", i64::MAX)
        .await
        .unwrap();
    meta_tablets::assume_ownership(meta.pool(), 1, 0, "node-b", i64::MAX)
        .await
        .unwrap();
    // Pin the two sessions to distinct tablets so they
    // unambiguously route to different nodes.
    meta_shards::set_assignment(meta.pool(), shard_a, Some(0), 200)
        .await
        .unwrap();
    meta_shards::set_assignment(meta.pool(), shard_b, Some(1), 200)
        .await
        .unwrap();

    // Bind both listeners up-front so we can register their
    // mongo addresses in the nodes table before the servers
    // serve any traffic.
    let port_b_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_b = port_b_listener.local_addr().unwrap().port();
    let port_a_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_a = port_a_listener.local_addr().unwrap().port();
    drop(port_b_listener);
    drop(port_a_listener);
    let addr_a = format!("127.0.0.1:{port_a}");
    let addr_b = format!("127.0.0.1:{port_b}");
    meta_nodes::upsert(
        meta.pool(),
        "node-a",
        &addr_a,
        Some(addr_a.as_str()),
        "alive",
        100,
        None,
    )
    .await
    .unwrap();
    meta_nodes::upsert(
        meta.pool(),
        "node-b",
        &addr_b,
        Some(addr_b.as_str()),
        "alive",
        100,
        None,
    )
    .await
    .unwrap();

    // Spawn each node with its own KV + object store but the
    // shared meta store — that's what lets node-a discover
    // session_b's locator (written by node-b) and decide to
    // forward the per-session find.
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let spawn = |meta: Arc<MetaStore>, node_id: String, port: u16| async move {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        // new_with_meta wires meta_store on the registry so
        // the locator-fanout branch in find_response actually
        // fires, AND tablets/locators populate on insert.
        let registry = Arc::new(
            BoxRegistry::new_with_meta(
                kv.clone(),
                store,
                4,
                Arc::new(HlcClock::default()),
                meta.clone(),
                node_id.clone(),
            )
            .await
            .unwrap(),
        );
        let location = LocationProxy::new(meta.clone()).await.unwrap();
        registry.attach_location(location).ok();
        let node_addr = NodeAddressProxy::new(meta.clone(), node_id.clone())
            .await
            .unwrap();
        registry.attach_node_address(node_addr).ok();
        let state = WireState::new(
            registry,
            Arc::new(TenantRegistry::new(kv.clone())),
            Arc::new(SessionRegistry::new(kv)),
        );
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = serve(listener, state).await;
        });
        (kv_dir, obj_dir)
    };
    let (_a_kv, _a_obj) = spawn(meta.clone(), "node-a".into(), port_a).await;
    let (_b_kv, _b_obj) = spawn(meta.clone(), "node-b".into(), port_b).await;

    // Both inserts go via node-a. session_a stays local;
    // session_b is forwarded by the existing wire-level
    // forwarder to node-b. After both writes, session_locators
    // has rows for both sessions on the right tablets.
    let cli = client(port_a).await;
    let coll = cli
        .database("t-fanout")
        .collection::<bson::Document>("events");
    coll.insert_one(doc! { "session_id": &session_a, "kind": "from-node-a" })
        .await
        .unwrap();
    coll.insert_one(doc! { "session_id": &session_b, "kind": "from-node-b" })
        .await
        .unwrap();

    // Sanity-check: the locator table sees both sessions on
    // the expected tablets. If this fails, the rest of the
    // assertions can't tell us anything useful.
    let loc_a = agentdb_meta::session_locators::get(meta.pool(), &session_a)
        .await
        .unwrap()
        .expect("session_a locator");
    let loc_b = agentdb_meta::session_locators::get(meta.pool(), &session_b)
        .await
        .unwrap()
        .expect("session_b locator");
    assert_eq!(loc_a.tablet_id, Some(0), "session_a should land on tablet 0");
    assert_eq!(loc_b.tablet_id, Some(1), "session_b should land on tablet 1");

    // Unpinned find on node-a: must return events from BOTH
    // sessions. The `from-node-b` event lives on node-b's KV
    // only — surfacing it requires the new cross-node fanout.
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
        kinds.contains("from-node-a"),
        "fanout missing local session events: {kinds:?}"
    );
    assert!(
        kinds.contains("from-node-b"),
        "fanout missing peer session events (cross-node fanout regression): {kinds:?}"
    );
}
