//! M20 multi-node: Mongo wire surface when a session's shard
//! is owned by a peer node.
//!
//! Mongo wire cross-node **forwarding** is out of scope for this
//! PR (binary stateful protocol). But silent failure is not
//! acceptable: when a driver connects to a node that doesn't own
//! a session's tablet and runs a find/aggregate pinning that
//! session, the node must return `code: 10107`
//! (NotWritablePrimary) with the owning node's address. Drivers
//! treat 10107 as "reconnect"; humans see the address in errmsg
//! and know where to point their client.

use std::sync::Arc;

use agentdb_core::{BoxRegistry, NodeAddressProxy, SessionRegistry, TenantRegistry};
use agentdb_meta::{nodes as meta_nodes, shards as meta_shards, tablets as meta_tablets, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireState};
use bson::doc;
use mongodb::options::ClientOptions;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

async fn spawn_node_a_with_peer_b() -> (
    tempfile::NamedTempFile,
    TempDir,
    TempDir,
    u16,
    String, // node B's advertised address
) {
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    meta_shards::seed_shards(meta.pool(), agentdb_core::SHARD_COUNT as i64, 0)
        .await
        .unwrap();

    // node-a: this process. node-b: a synthetic peer row so the
    // NodeAddressProxy has something to resolve to. node-b
    // isn't actually running — we're only testing the error
    // signal when node A receives a mis-routed Mongo query.
    let node_b_addr = "127.0.0.1:65530".to_string();
    meta_nodes::upsert(meta.pool(), "node-a", "127.0.0.1:0", "alive", 100)
        .await
        .unwrap();
    meta_nodes::upsert(meta.pool(), "node-b", &node_b_addr, "alive", 100)
        .await
        .unwrap();
    for t in 0..4 {
        meta_tablets::get_or_create(meta.pool(), t, 100)
            .await
            .unwrap();
    }
    meta_tablets::assume_ownership(meta.pool(), 0, 0, "node-a", i64::MAX)
        .await
        .unwrap();
    meta_tablets::assume_ownership(meta.pool(), 1, 0, "node-a", i64::MAX)
        .await
        .unwrap();
    meta_tablets::assume_ownership(meta.pool(), 2, 0, "node-b", i64::MAX)
        .await
        .unwrap();
    meta_tablets::assume_ownership(meta.pool(), 3, 0, "node-b", i64::MAX)
        .await
        .unwrap();

    let data = TempDir::new().unwrap();
    let obj = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(data.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let location = agentdb_core::LocationProxy::new(meta.clone()).await.unwrap();
    registry.attach_location(location).ok();
    let addr_proxy = NodeAddressProxy::new(meta.clone(), "node-a".to_string())
        .await
        .unwrap();
    registry.attach_node_address(addr_proxy).ok();

    let state = WireState::new(
        registry,
        Arc::new(TenantRegistry::new(kv.clone())),
        Arc::new(SessionRegistry::new(kv)),
    );

    // Pin a session that hashes to tablet 3 (owned by node-b).
    // We do this by setting an explicit shard override for
    // whatever shard the session happens to hash to.
    // Session id is determined at test time; callers refresh
    // the location proxy after setting the override.
    let session = "s-remote";
    let shard_id = agentdb_core::shard_id(session) as i64;
    meta_shards::set_assignment(meta.pool(), shard_id, Some(3), 0)
        .await
        .unwrap();
    state
        .registry
        .location()
        .unwrap()
        .refresh()
        .await
        .unwrap();
    state
        .registry
        .node_address_proxy()
        .unwrap()
        .refresh()
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = serve(listener, state).await;
    });
    (meta_file, data, obj, port, node_b_addr)
}

#[tokio::test]
async fn mongo_find_with_session_on_peer_returns_not_writable_primary() {
    let (_m, _d, _o, port, node_b_addr) = spawn_node_a_with_peer_b().await;
    let opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .await
        .unwrap();
    let client = mongodb::Client::with_options(opts).unwrap();
    let coll = client
        .database("t-1")
        .collection::<bson::Document>("events");
    let err = coll
        .find_one(doc! { "session_id": "s-remote" })
        .await
        .expect_err("find for a session owned elsewhere must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("10107") || msg.to_lowercase().contains("not writable"),
        "expected NotWritablePrimary (code 10107), got: {msg}"
    );
    assert!(
        msg.contains(&node_b_addr) || msg.contains("node-b"),
        "errmsg should name the owning node (addr={node_b_addr}), got: {msg}"
    );
}

#[tokio::test]
async fn mongo_aggregate_with_session_on_peer_returns_not_writable_primary() {
    let (_m, _d, _o, port, node_b_addr) = spawn_node_a_with_peer_b().await;
    let opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .await
        .unwrap();
    let client = mongodb::Client::with_options(opts).unwrap();
    let coll = client
        .database("t-1")
        .collection::<bson::Document>("events");
    let mut cursor = match coll
        .aggregate(vec![doc! {
            "$match": { "session_id": "s-remote" }
        }])
        .await
    {
        Ok(c) => c,
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("10107") || msg.to_lowercase().contains("not writable"),
                "expected 10107 in aggregate error, got: {msg}"
            );
            assert!(
                msg.contains(&node_b_addr) || msg.contains("node-b"),
                "expected node-b address in errmsg, got: {msg}"
            );
            return;
        }
    };
    // If aggregate returned ok-ish, the next cursor op should
    // carry the error (some driver versions only raise on the
    // first fetch).
    use futures_util::stream::StreamExt;
    let item = cursor.next().await;
    match item {
        Some(Err(e)) => {
            let msg = e.to_string();
            assert!(
                msg.contains("10107") || msg.to_lowercase().contains("not writable"),
                "expected 10107 from cursor, got: {msg}"
            );
        }
        other => panic!("expected 10107 error, got {other:?}"),
    }
}

#[tokio::test]
async fn mongo_find_without_session_filter_is_not_affected() {
    // Guard: the cross-node signal only fires when the filter
    // pins `session_id = S`. A general tenant-scoped find must
    // still work (returns empty — no data was ever written in
    // this test).
    let (_m, _d, _o, port, _) = spawn_node_a_with_peer_b().await;
    let opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .await
        .unwrap();
    let client = mongodb::Client::with_options(opts).unwrap();
    let coll = client
        .database("t-1")
        .collection::<bson::Document>("events");
    let result = coll.find_one(doc! { "status": "active" }).await;
    // Either Ok(None) or Ok(Some(_)) — crucially, not a
    // NotWritablePrimary error.
    if let Err(e) = result {
        let msg = e.to_string();
        assert!(
            !msg.contains("10107") && !msg.to_lowercase().contains("not writable"),
            "non-session filter must not trigger cross-node signal: {msg}"
        );
    }
}
