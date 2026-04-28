//! Multi-node 421 redirect for v2 subscribe routes.
//!
//! When a subscribe lands on a node that doesn't own the requested
//! session, the server returns 421 Misdirected Request with the
//! owner's address so the SDK reconnects directly. The redirect
//! target path is now the v2 URL `/api/sessions/{s}/subscribe`.
//!
//! The two-node fixture mirrors `tests/multi_node.rs`'s
//! `spawn_two_nodes`: shared MetaStore, tablets 0/1 → node A,
//! tablets 2/3 → node B, shard overrides pin specific session ids
//! onto the desired tablet.

use std::sync::Arc;

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, NodeAddressProxy, SessionRegistry, TenantRegistry};
use agentdb_meta::{
    self, nodes as meta_nodes, shards as meta_shards, tablets as meta_tablets, MetaStore,
};
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
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
    // Use BoxRegistry::new (no lease management) so manual assume_ownership
    // calls in spawn_two_nodes work with expected_epoch=0.  The MetaStore is
    // still wired into AppState.auth for user-credential lookup; routing uses
    // the LocationProxy / NodeAddressProxy attached below.
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
        auth: Some(AuthConfig { meta: Some(meta.clone()) }),
        admin_auth: None,
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
        broadcast_bus: agentdb_api::BroadcastBus::default(),
    };
    let app = router(state).layer(Extension(meta.clone()));
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

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

async fn create_user(base: &str, user_id: &str, tenant: &str, password: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/admin/users"))
        .json(&serde_json::json!({
            "user_id": user_id,
            "tenant_external": tenant,
            "password": password,
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create_user failed: {}", resp.text().await.unwrap());
}

/// Pin `session` to `tablet` and refresh both nodes' caches so
/// the override is visible. Does NOT seed a write — the subscribe
/// redirect fires on routing alone.
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

/// Pin session to tablet 3 (node B) and create the auth user. Returns
/// the Basic auth header for the user. No write is issued — routing
/// decisions use the shard table only; `resolve_session_tenant` relies
/// on auth-derived tenant (registry has no MetaStore).
async fn pin_session_to_node_b(
    meta: &Arc<MetaStore>,
    node_a: &Node,
    node_b: &Node,
    session: &str,
    user_id: &str,
    tenant: &str,
) -> String {
    pin_session_to_tablet(meta, &[node_a, node_b], session, 3).await;
    // Idempotent user create.
    let _ = reqwest::Client::new()
        .post(format!("{}/api/admin/users", node_a.addr))
        .json(&serde_json::json!({
            "user_id": user_id,
            "tenant_external": tenant,
            "password": "pass",
        }))
        .send()
        .await
        .unwrap();
    basic(user_id, "pass")
}

#[tokio::test]
async fn subscribe_to_session_owned_by_peer_returns_421_with_owner() {
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    let session = "s-on-b";
    // Pin session to tablet 3 (node B) and create auth user.
    let auth = pin_session_to_node_b(&meta, &node_a, &node_b, session, "redir-user", "t-1").await;

    let client = reqwest::Client::new();
    let url = format!("{}/api/sessions/{session}/subscribe", node_a.addr);
    let resp = client
        .get(&url)
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
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
        location.contains(&format!("/api/sessions/{session}/subscribe")),
        "Location must use v2 subscribe path; got {location}",
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
    // Sanity: when the session is on this node, subscribe proceeds (200).
    // Pin the session to tablet 0 (node A owns tablets 0 and 1).
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    let session = "s-local";
    pin_session_to_tablet(&meta, &[&node_a, &node_b], session, 0).await;
    create_user(&node_a.addr, "local-user", "t-1", "pass").await;
    let auth = basic("local-user", "pass");

    let client = reqwest::Client::new();
    let url = format!("{}/api/sessions/{session}/subscribe", node_a.addr);
    let resp = client
        .get(&url)
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
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
    // node A with both sessions in the whitelist → expect 421
    // because s-b lives on B.
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    pin_session_to_tablet(&meta, &[&node_a, &node_b], "s-a", 0).await;
    pin_session_to_tablet(&meta, &[&node_a, &node_b], "s-b", 3).await;
    create_user(&node_a.addr, "wide-user", "t-1", "pass").await;
    let auth = basic("wide-user", "pass");

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/subscribe?session=s-a&session=s-b",
        node_a.addr
    );
    let resp = client
        .get(&url)
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
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
    // The redirect guard fires when the catch-up cursor names a
    // peer-owned session, even without a session whitelist.
    let (_meta_file, meta, node_a, node_b) = spawn_two_nodes().await;
    pin_session_to_tablet(&meta, &[&node_a, &node_b], "s-b2", 3).await;
    create_user(&node_a.addr, "seq-user", "t-1", "pass").await;
    let auth = basic("seq-user", "pass");

    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/subscribe?from_seq_per_session=s-b2:0",
        node_a.addr
    );
    let resp = client
        .get(&url)
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 421);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "not_owner");
    let addrs = body["owner_addrs"].as_array().unwrap();
    assert_eq!(addrs.len(), 1);
}

#[tokio::test]
async fn tenant_wide_subscribe_without_whitelist_serves_locally() {
    // Phase 1 simplification: with no whitelist and no per-session
    // catch-up cursor, the server serves locally (200).
    let (_meta_file, _meta, node_a, _node_b) = spawn_two_nodes().await;
    create_user(&node_a.addr, "local2-user", "t-1", "pass").await;
    let auth = basic("local2-user", "pass");

    let client = reqwest::Client::new();
    let url = format!("{}/api/subscribe", node_a.addr);
    let resp = client
        .get(&url)
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "tenant subscribe without specific sessions must serve locally"
    );
    drop(resp);
}
