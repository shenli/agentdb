//! Cross-node Mongo wire forwarding — end-to-end.
//!
//! Spins up two wire servers (node-a and node-b) against a
//! shared meta store. Configures the tablets table so
//! session "s-on-b" routes to node-b. Issues a Mongo
//! insert + find from a client connected to node-a — the
//! local node must forward the frames to node-b and relay
//! the reply back.

use std::sync::Arc;

use agentdb_core::{
    BoxRegistry, LocationProxy, NodeAddressProxy, SessionRegistry, TenantRegistry, SHARD_COUNT,
};
use agentdb_meta::{
    nodes as meta_nodes, shards as meta_shards, tablets as meta_tablets, MetaStore,
};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use futures_util::StreamExt;
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
async fn insert_via_node_a_is_forwarded_to_owner_node_b() {
    // Shared meta store and shared object storage so both
    // nodes agree on tenant/session writes.
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta = Arc::new(
        MetaStore::open_sqlite(&meta_file.path().to_string_lossy())
            .await
            .unwrap(),
    );

    // Figure out which tablet "s-on-b" routes to using the
    // same shard-hash the wire handler uses, then route that
    // shard to tablet 1 (node-b).
    let session_id = "s-on-b";
    let shard = agentdb_core::shard_id(session_id) as i64;
    // Seed the shards table so LocationProxy has something
    // to read.
    meta_shards::seed_shards(meta.pool(), SHARD_COUNT as i64, 100)
        .await
        .unwrap();
    // Ensure tablet rows exist + assign owners.
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
    // Point the target shard at tablet 1 (owned by node-b).
    // Every other shard stays NULL → default hash routing.
    meta_shards::set_assignment(meta.pool(), shard, Some(1), 200)
        .await
        .unwrap();

    // Bind the two listeners first so we know their ports
    // before we register in the nodes table.
    let port_b_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_b = port_b_listener.local_addr().unwrap().port();
    let port_a_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_a = port_a_listener.local_addr().unwrap().port();
    drop(port_b_listener);
    drop(port_a_listener);

    let addr_b = format!("127.0.0.1:{port_b}");
    let addr_a = format!("127.0.0.1:{port_a}");
    meta_nodes::upsert(
        meta.pool(),
        "node-a",
        &addr_a,
        Some(addr_a.as_str()),
        "alive",
        100,
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
    )
    .await
    .unwrap();

    // Spawn both nodes bound to their specific ports.
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let spawn_fixed_port = |meta: Arc<MetaStore>, node_id: String, port: u16| async move {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        let node_addr_proxy = NodeAddressProxy::new(meta.clone(), node_id).await.unwrap();
        let location_proxy = LocationProxy::new(meta.clone()).await.unwrap();
        registry.attach_location(location_proxy).ok();
        registry.attach_node_address(node_addr_proxy).ok();
        let state = WireState::new(registry, tenants, sessions);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = serve(listener, state).await;
        });
        (kv_dir, obj_dir)
    };
    let (_a_kv, _a_obj) = spawn_fixed_port(meta.clone(), "node-a".into(), port_a).await;
    let (_b_kv, _b_obj) = spawn_fixed_port(meta.clone(), "node-b".into(), port_b).await;

    // Client targets node-a. The insert targets "s-on-b",
    // which routes to tablet 1 (node-b). Node-a's wire
    // handler should forward the frame to node-b.
    let cli = client(port_a).await;
    let events = cli
        .database("t-fwd")
        .collection::<bson::Document>("events");
    events
        .insert_one(doc! { "session_id": session_id, "kind": "hello-peer" })
        .await
        .unwrap();

    // Query via node-a again. Because the session lives on
    // node-b, this find is also forwarded. The event we just
    // inserted should show up.
    let cursor = events
        .find(doc! { "session_id": session_id })
        .await
        .unwrap();
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 1, "forward didn't round-trip: {docs:?}");
    let payload = docs[0].get_document("payload").unwrap();
    assert_eq!(payload.get_str("kind").unwrap(), "hello-peer");

    // Sanity-check: a session on a DIFFERENT shard should
    // stay local on node-a (no forwarding attempted).
    let local_session = (0u64..1024)
        .map(|i| format!("s-local-{i}"))
        .find(|s| (agentdb_core::shard_id(s) as i64) != shard)
        .expect("couldn't find a local session id in 1024 tries");
    events
        .insert_one(doc! { "session_id": &local_session, "kind": "stays-local" })
        .await
        .unwrap();
    let cursor = events
        .find(doc! { "session_id": &local_session })
        .await
        .unwrap();
    let docs: Vec<bson::Document> = cursor.filter_map(|r| async move { r.ok() }).collect().await;
    assert_eq!(docs.len(), 1);
}
