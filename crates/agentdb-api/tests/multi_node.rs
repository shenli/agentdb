//! M20 multi-node: cross-node write forwarding end-to-end.
//!
//! Spawns two full AppState instances sharing a MetaStore in
//! the same tokio test runtime. The test only cares about the
//! REST surface; Mongo-wire cross-node is explicitly out of
//! scope.
//!
//! Node A owns tablets 0,1; node B owns tablets 2,3. A shard
//! override routes session "s-to-b" to tablet 3. The test posts
//! to node A and asserts:
//!
//!   - the response is 200
//!   - node B's metrics for box 3 show `appends == 1`
//!   - node A's metrics show no local appends (all four boxes
//!     have 0)
//!   - node A's `forwarded_writes_total` is 1

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, NodeAddressProxy, SessionRegistry, TenantRegistry};
use agentdb_meta::{self, nodes as meta_nodes, shards as meta_shards, tablets as meta_tablets, MetaStore};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::json;
use tempfile::TempDir;

struct Node {
    registry: Arc<BoxRegistry>,
    addr: String,
    // keep the tempdirs alive for the duration of the test.
    _data: TempDir,
    _obj: TempDir,
}

/// Bring up two nodes sharing a MetaStore. The MetaStore is
/// backed by a single SQLite file so both nodes see the same
/// `tablets` + `shards` + `nodes` tables.
async fn spawn_two_nodes() -> (tempfile::NamedTempFile, Arc<MetaStore>, Node, Node) {
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    meta_shards::seed_shards(meta.pool(), agentdb_core::SHARD_COUNT as i64, 0)
        .await
        .unwrap();

    // Bind both listeners up front so we know each node's port
    // before we upsert its row into the `nodes` table (forwarding
    // needs the address to already be there by the time a write
    // arrives).
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    meta_nodes::upsert(meta.pool(), "node-a", &addr_a.to_string(), "alive", 100)
        .await
        .unwrap();
    meta_nodes::upsert(meta.pool(), "node-b", &addr_b.to_string(), "alive", 100)
        .await
        .unwrap();

    // Each node owns 2 tablets. Both nodes' BoxRegistrys have
    // num_boxes=4, but the route_session_for_write path uses the
    // NodeAddressProxy to decide who's the actual owner, not
    // box_id arithmetic.
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

    let node_a = build_node(meta.clone(), "node-a", listener_a, addr_a).await;
    let node_b = build_node(meta.clone(), "node-b", listener_b, addr_b).await;
    (meta_file, meta, node_a, node_b)
}

async fn build_node(
    meta: Arc<MetaStore>,
    node_id: &str,
    listener: tokio::net::TcpListener,
    addr: std::net::SocketAddr,
) -> Node {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let data = TempDir::new().unwrap();
    let obj = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(data.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());

    let location = agentdb_core::LocationProxy::new(meta.clone())
        .await
        .unwrap();
    registry.attach_location(location).ok();

    let addr_proxy = NodeAddressProxy::new(meta.clone(), node_id.to_string())
        .await
        .unwrap();
    registry.attach_node_address(addr_proxy).ok();

    let state = AppState {
        registry: registry.clone(),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
    };
    let app = router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Node {
        registry,
        addr: format!("http://{addr}"),
        _data: data,
        _obj: obj,
    }
}

fn sum_appends(reg: &BoxRegistry) -> u64 {
    (0..reg.num_boxes())
        .filter_map(|i| reg.box_by_id(i))
        .map(|b| b.metrics.snapshot().appends)
        .sum()
}

#[tokio::test]
async fn write_to_node_a_for_tablet_owned_by_b_lands_on_b() {
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    // Route session "s-to-b" to tablet 3 (node B).
    let session = "s-to-b";
    let shard_id = agentdb_core::shard_id(session) as i64;
    meta_shards::set_assignment(meta.pool(), shard_id, Some(3), 0)
        .await
        .unwrap();
    // Force a proxy refresh on both nodes so the override is
    // visible before we send traffic. (Both proxies do the same
    // scan — 16K rows, trivially fast.)
    let loc_a = node_a.registry.location().unwrap();
    loc_a.refresh().await.unwrap();
    let loc_b = node_b.registry.location().unwrap();
    loc_b.refresh().await.unwrap();
    let na_a = node_a.registry.node_address_proxy().unwrap();
    na_a.refresh().await.unwrap();
    let na_b = node_b.registry.node_address_proxy().unwrap();
    na_b.refresh().await.unwrap();

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/tenants/t-1/sessions/{session}/append_and_fetch",
        node_a.addr
    );
    let resp = client
        .post(&url)
        .json(&json!({"event": {"text": "hello-multi-node"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "POST must succeed: {}",
        resp.text().await.unwrap()
    );

    // Node B's box 3 got the append. Node A saw none.
    let b3 = node_b
        .registry
        .box_by_id(3)
        .unwrap()
        .metrics
        .snapshot()
        .appends;
    assert_eq!(b3, 1, "node B's box 3 must have processed the write");
    assert_eq!(
        sum_appends(&node_a.registry),
        0,
        "node A must not have processed any write locally"
    );
    // Node A recorded the forward on its box 0 metrics.
    let fa = node_a
        .registry
        .box_by_id(0)
        .unwrap()
        .metrics
        .snapshot()
        .forwarded_writes_total;
    assert_eq!(fa, 1, "node A must have counted one forwarded write");
}

#[tokio::test]
async fn loop_guard_rejects_already_forwarded_requests() {
    // If node B sends a request to node A with
    // X-AgentDB-Forwarded: 1, and node A's own routing says
    // "forward to node B", the node must not re-forward —
    // it returns 421 Misdirected Request so the cycle dies
    // loudly rather than ping-ponging silently.
    let (_meta_file, meta, node_a, _node_b) = spawn_two_nodes().await;
    let session = "s-to-b";
    let shard_id = agentdb_core::shard_id(session) as i64;
    meta_shards::set_assignment(meta.pool(), shard_id, Some(3), 0)
        .await
        .unwrap();
    node_a.registry.location().unwrap().refresh().await.unwrap();
    node_a
        .registry
        .node_address_proxy()
        .unwrap()
        .refresh()
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/tenants/t-1/sessions/{session}/append_and_fetch",
        node_a.addr
    );
    let resp = client
        .post(&url)
        .header("X-AgentDB-Forwarded", "1")
        .json(&json!({"event": {"text": "ping"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 421, "expected Misdirected Request");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("misdirected") && body.contains("routing drift"),
        "421 body should explain the drift: {body}"
    );
}

#[tokio::test]
async fn locally_owned_tablet_does_not_forward() {
    // Override points at tablet 0 (owned by node A). A write
    // sent to node A must not forward anywhere; it runs
    // locally.
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    let session = "s-local";
    let shard_id = agentdb_core::shard_id(session) as i64;
    meta_shards::set_assignment(meta.pool(), shard_id, Some(0), 0)
        .await
        .unwrap();
    node_a.registry.location().unwrap().refresh().await.unwrap();

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/tenants/t-1/sessions/{session}/append_and_fetch",
        node_a.addr
    );
    let resp = client
        .post(&url)
        .json(&json!({"event": {"text": "local"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    // Landed on A's box 0.
    let a0 = node_a
        .registry
        .box_by_id(0)
        .unwrap()
        .metrics
        .snapshot()
        .appends;
    assert_eq!(a0, 1);
    // No forwarding.
    assert_eq!(
        node_a
            .registry
            .box_by_id(0)
            .unwrap()
            .metrics
            .snapshot()
            .forwarded_writes_total,
        0
    );
    // Node B saw nothing.
    assert_eq!(sum_appends(&node_b.registry), 0);
}
