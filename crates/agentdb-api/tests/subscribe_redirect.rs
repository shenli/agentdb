//! Multi-node 421 redirect for subscribe (Phase 1 Task 8, spec §6.8).
//!
//! Phase 1 doesn't proxy SSE across nodes. When a subscribe lands
//! on a node that doesn't own the requested session(s), the server
//! returns 421 Misdirected Request with the owner's address(es) so
//! the SDK reconnects directly. The two-node fixture mirrors
//! `tests/multi_node.rs`'s `spawn_two_nodes`: shared `MetaStore`,
//! tablets 0/1 → node A, tablets 2/3 → node B, shard overrides
//! pin specific session ids onto the desired tablet.
//!
//! No SSE round-trip — the redirect happens before the stream
//! opens, so a one-shot HTTP GET is enough.

use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, NodeAddressProxy, SessionRegistry, TenantRegistry};
use agentdb_meta::{
    self, nodes as meta_nodes, shards as meta_shards, tablets as meta_tablets, MetaStore,
};
use agentdb_storage::{KvStore, RocksKvStore};
use tempfile::TempDir;

struct Node {
    registry: Arc<BoxRegistry>,
    addr: String,
    _data: TempDir,
    _obj: TempDir,
}

async fn spawn_two_nodes() -> (tempfile::NamedTempFile, Arc<MetaStore>, Node, Node) {
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    meta_shards::seed_shards(meta.pool(), agentdb_core::SHARD_COUNT as i64, 0)
        .await
        .unwrap();

    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();

    meta_nodes::upsert(
        meta.pool(),
        "node-a",
        &addr_a.to_string(),
        None,
        "alive",
        100,
        None,
    )
    .await
    .unwrap();
    meta_nodes::upsert(
        meta.pool(),
        "node-b",
        &addr_b.to_string(),
        None,
        "alive",
        100,
        None,
    )
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
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
        admin_auth: None,
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
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

/// Pin `session` to `tablet` and refresh both nodes' caches so
/// the override is visible. Mirrors `seed_session_on_node_b` in
/// `multi_node.rs` but without the seed write — the subscribe
/// redirect doesn't need state, only routing.
async fn pin_session_to_tablet(
    meta: &Arc<MetaStore>,
    nodes: &[&Node],
    session: &str,
    tablet: i64,
) {
    let shard_id = agentdb_core::shard_id(session) as i64;
    meta_shards::set_assignment(meta.pool(), shard_id, Some(tablet), 0)
        .await
        .unwrap();
    for n in nodes {
        n.registry.location().unwrap().refresh().await.unwrap();
        n.registry
            .node_address_proxy()
            .unwrap()
            .refresh()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn subscribe_to_session_owned_by_peer_returns_421_with_owner() {
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    let session = "s-on-b";
    pin_session_to_tablet(&meta, &[&node_a, &node_b], session, 3).await;

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/tenants/t-1/sessions/{session}/subscribe",
        node_a.addr
    );
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        421,
        "subscribe on non-owner must redirect"
    );
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .expect("Location header present")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        location.contains(&node_b.addr.replace("http://", ""))
            || location.contains(&node_b.addr),
        "Location must point at node B; got {location} (node B addr {})",
        node_b.addr,
    );
    assert!(
        location.contains(&format!("/api/tenants/t-1/sessions/{session}/subscribe")),
        "Location must preserve the subscribe path; got {location}",
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "not_owner");
    let owner_addr = body["owner_addr"].as_str().expect("owner_addr in body");
    assert!(
        node_b.addr.contains(owner_addr),
        "owner_addr {owner_addr} must match node B {}",
        node_b.addr,
    );
    assert_eq!(body["owner_node_id"], "node-b");
}

#[tokio::test]
async fn subscribe_to_locally_owned_session_does_not_redirect() {
    // Sanity: when the session is on this node, the subscribe
    // path proceeds (200 + SSE). We don't read any frames here —
    // verifying the status is enough to prove we didn't 421.
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    let session = "s-local";
    pin_session_to_tablet(&meta, &[&node_a, &node_b], session, 0).await;

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/tenants/t-1/sessions/{session}/subscribe",
        node_a.addr
    );
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "subscribe on owner must succeed"
    );
    drop(resp);
}

#[tokio::test]
async fn tenant_wide_subscribe_with_session_on_peer_returns_421_with_owner_addrs() {
    // Two-node cluster. Session "s-a" on node A (tablet 0),
    // "s-b" on node B (tablet 3). Subscribe to the tenant on
    // node A *with both sessions whitelisted* → expect 421
    // because s-b lives on B. Without a whitelist we'd serve
    // locally (and just miss s-b's events) — the spec calls
    // that out as the Phase 1 simplification.
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    pin_session_to_tablet(&meta, &[&node_a, &node_b], "s-a", 0).await;
    pin_session_to_tablet(&meta, &[&node_a, &node_b], "s-b", 3).await;

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/tenants/t-1/subscribe?session=s-a&session=s-b",
        node_a.addr
    );
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        421,
        "tenant subscribe with peer-owned session in whitelist must redirect"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "not_owner");
    let addrs = body["owner_addrs"]
        .as_array()
        .expect("owner_addrs array in body");
    assert_eq!(addrs.len(), 1, "only s-b is on a peer; one owner expected");
    let only = addrs[0].as_str().unwrap();
    assert!(
        node_b.addr.contains(only),
        "owner_addrs entry {only} must match node B {}",
        node_b.addr,
    );
}

#[tokio::test]
async fn tenant_wide_subscribe_with_from_seq_per_session_to_peer_redirects() {
    // The redirect guard also fires when the catch-up cursor
    // names a peer-owned session, even without a whitelist.
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    pin_session_to_tablet(&meta, &[&node_a, &node_b], "s-b", 3).await;

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/tenants/t-1/subscribe?from_seq_per_session=s-b:0",
        node_a.addr
    );
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 421);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "not_owner");
    let addrs = body["owner_addrs"].as_array().unwrap();
    assert_eq!(addrs.len(), 1);
}

#[tokio::test]
async fn tenant_wide_subscribe_without_whitelist_serves_locally() {
    // Phase 1 simplification: with no whitelist and no per-session
    // catch-up cursor, the server serves locally (200) and the
    // client just sees an empty stream for peer-owned sessions.
    // Enumerating every session in a tenant to discover peer owners
    // is out of scope for Phase 1.
    let (_meta_file, _meta, node_a, _node_b) = spawn_two_nodes().await;
    let client = reqwest::Client::new();
    let url = format!("{}/api/tenants/t-1/subscribe", node_a.addr);
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "tenant subscribe without specific sessions must serve locally"
    );
    drop(resp);
}
