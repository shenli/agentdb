//! HTTP API round-trip tests.
//!
//! Migrated to v2 session routes with Basic auth + meta-backed registry.

use std::sync::Arc;

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

struct Env {
    _kv_dir: TempDir,
    _obj_dir: TempDir,
    _meta_file: tempfile::NamedTempFile,
    base: String,
}

async fn spawn() -> Env {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let kv_dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            4,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "test-node".into(),
        )
        .await
        .unwrap(),
    );
    let state = AppState {
        registry,
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
    let app = router(state).layer(axum::Extension(meta));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Env {
        _kv_dir: kv_dir,
        _obj_dir: obj_dir,
        _meta_file: meta_file,
        base: format!("http://{addr}"),
    }
}

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

async fn create_user(base: &str, user_id: &str, tenant: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/admin/users"))
        .json(&json!({
            "user_id": user_id,
            "tenant_external": tenant,
            "password": "pw",
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create_user: {}", resp.status());
}

#[tokio::test]
#[serial]
async fn append_and_fetch_endpoint_round_trip() {
    let env = spawn().await;
    create_user(&env.base, "u1", "t-1").await;
    let auth = basic("u1", "pw");
    let client = reqwest::Client::new();

    let resp1: serde_json::Value = client
        .post(format!("{}/api/sessions/s-1/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({"event": {"kind": "hello"}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp1["seq"], 1);
    assert_eq!(resp1["recent"].as_array().unwrap().len(), 1);
    assert_eq!(resp1["recent"][0]["payload"]["kind"], "hello");

    let resp2: serde_json::Value = client
        .post(format!("{}/api/sessions/s-1/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({"event": {"kind": "followup"}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp2["seq"], 2);
    let recent = resp2["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0]["payload"]["kind"], "hello");
    assert_eq!(recent[1]["payload"]["kind"], "followup");
}

#[tokio::test]
#[serial]
async fn actor_id_roundtrips_through_append_and_fetch() {
    let env = spawn().await;
    create_user(&env.base, "agent", "t-agent").await;
    let auth = basic("agent", "pw");
    let client = reqwest::Client::new();

    // Append one event with actor_id set.
    let resp: serde_json::Value = client
        .post(format!("{}/api/sessions/s-agent/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({
            "event": {"tool": "search"},
            "actor_id": "agent-planner",
            "fetch_last_n": 1,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["seq"], 1);
    let recent = resp["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0]["actor_id"], "agent-planner");

    // A follow-up append without actor_id must not surface the field at all.
    let resp2: serde_json::Value = client
        .post(format!("{}/api/sessions/s-agent/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({
            "event": {"tool": "reflect"},
            "fetch_last_n": 1,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let recent2 = resp2["recent"].as_array().unwrap();
    assert!(
        recent2[0].get("actor_id").is_none(),
        "event without actor_id should not include the field in JSON: {}",
        recent2[0]
    );
}

#[tokio::test]
#[serial]
async fn fetch_last_n_respects_bound() {
    let env = spawn().await;
    create_user(&env.base, "ubound", "t-1").await;
    let auth = basic("ubound", "pw");
    let client = reqwest::Client::new();

    for i in 1..=4 {
        client
            .post(format!("{}/api/sessions/s-bound/append_and_fetch", env.base))
            .header("Authorization", &auth)
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let resp: serde_json::Value = client
        .post(format!("{}/api/sessions/s-bound/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({"event": {"i": 5}, "fetch_last_n": 2}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["seq"], 5);
    let recent = resp["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0]["payload"]["i"], 4);
    assert_eq!(recent[1]["payload"]["i"], 5);
}

#[tokio::test]
#[serial]
async fn admin_force_tier_cold_then_warm_round_trips() {
    let env = spawn().await;
    create_user(&env.base, "u1", "t-1").await;
    let auth = basic("u1", "pw");
    let client = reqwest::Client::new();

    // Seed an event so the session exists.
    client
        .post(format!("{}/api/sessions/s-1/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let cold: serde_json::Value = client
        .post(format!("{}/api/admin/tier/force", env.base))
        .json(&json!({
            "tenant_id": "t-1",
            "session_id": "s-1",
            "target": "Cold"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(cold["tier"], "cold");

    let warm: serde_json::Value = client
        .post(format!("{}/api/admin/tier/force", env.base))
        .json(&json!({
            "tenant_id": "t-1",
            "session_id": "s-1",
            "target": "Warm"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(warm["tier"], "warm");
}

#[tokio::test]
#[serial]
async fn fork_endpoint_creates_child_and_fetch_last_n_merges_parent_tail() {
    let env = spawn().await;
    create_user(&env.base, "u1", "t-1").await;
    let auth = basic("u1", "pw");
    let client = reqwest::Client::new();

    // Seed parent with 2 events.
    for i in 1..=2u64 {
        client
            .post(format!("{}/api/sessions/parent/append_and_fetch", env.base))
            .header("Authorization", &auth)
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // Fork via auth-bound route (no tenant_id in body needed; auth provides it).
    let fork: serde_json::Value = client
        .post(format!("{}/api/sessions/parent/fork", env.base))
        .header("Authorization", &auth)
        .json(&json!({
            "new_session_id": "child"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fork["new_session_id"], "child");
    assert_eq!(fork["parent_session_id"], "parent");
    assert_eq!(fork["parent_seq_at_fork"], 3);

    // Append to child, request fetch_last_n=5 — should include parent tail + child event.
    let resp: serde_json::Value = client
        .post(format!("{}/api/sessions/child/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({"event": {"c": 1}, "fetch_last_n": 5}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let recent = resp["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 3);
}

#[tokio::test]
#[serial]
async fn every_response_carries_x_agentdb_hlc_header() {
    // M14 Phase F: the HLC middleware must inject X-AgentDB-HLC on
    // every response and must absorb inbound headers on every request.
    let env = spawn().await;
    create_user(&env.base, "uhlc", "t-hlc").await;
    let auth = basic("uhlc", "pw");
    let client = reqwest::Client::new();

    let first = client
        .post(format!("{}/api/sessions/s-hlc/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .json(&json!({"event": {"k": 1}, "fetch_last_n": 1}))
        .send()
        .await
        .unwrap();
    let hdr1 = first
        .headers()
        .get("x-agentdb-hlc")
        .expect("response must carry X-AgentDB-HLC")
        .to_str()
        .unwrap()
        .to_string();

    // Header must parse as `physical_ns.logical`.
    let (phys, log) = hdr1.split_once('.').expect("header has `phys.log` form");
    let phys: u64 = phys.parse().expect("physical_ns is numeric");
    let _log: u32 = log.parse().expect("logical is numeric");
    assert!(phys > 0, "physical_ns should be non-zero after an append");

    // Second request — server HLC must not regress.
    let second = client
        .post(format!("{}/api/sessions/s-hlc/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .header("x-agentdb-hlc", hdr1.clone())
        .json(&json!({"event": {"k": 2}, "fetch_last_n": 1}))
        .send()
        .await
        .unwrap();
    let hdr2 = second
        .headers()
        .get("x-agentdb-hlc")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let parse = |s: &str| {
        let (p, l) = s.split_once('.').unwrap();
        (p.parse::<u64>().unwrap(), l.parse::<u32>().unwrap())
    };
    assert!(
        parse(&hdr2) >= parse(&hdr1),
        "hlc header regressed: {hdr1} -> {hdr2}"
    );
}

#[tokio::test]
#[serial]
async fn far_future_hlc_header_is_rejected_with_400() {
    let env = spawn().await;
    create_user(&env.base, "ubogus", "t-bogus").await;
    let auth = basic("ubogus", "pw");
    let client = reqwest::Client::new();

    // Inject an HLC 1 hour in the future — far beyond default 10s max_offset.
    let one_hour_ns: u64 = 3_600 * 1_000_000_000;
    let future_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        + one_hour_ns;
    let bogus = format!("{future_ns}.0");

    let resp = client
        .post(format!("{}/api/sessions/s/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .header("x-agentdb-hlc", bogus)
        .json(&json!({"event": {"k": 1}, "fetch_last_n": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        400,
        "expected 400 for HLC beyond max_offset"
    );
}

#[tokio::test]
#[serial]
async fn admin_hlc_endpoint_reports_running_clock_and_reject_count() {
    let env = spawn().await;
    create_user(&env.base, "ur", "t-r").await;
    let auth = basic("ur", "pw");
    let client = reqwest::Client::new();

    // Baseline — no rejects yet.
    let before: serde_json::Value = client
        .get(format!("{}/api/admin/hlc", env.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(before["rejects"], 0);
    assert_eq!(before["max_offset_secs"], 10);
    assert!(
        before["wall_clock_ns"].as_u64().unwrap() > 0,
        "wall clock should be populated"
    );

    // Trigger a rejection via a far-future header.
    let one_hour_ns: u64 = 3_600 * 1_000_000_000;
    let future_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        + one_hour_ns;
    let _ = client
        .post(format!("{}/api/sessions/s/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .header("x-agentdb-hlc", format!("{future_ns}.0"))
        .json(&json!({"event": {"k": 1}, "fetch_last_n": 1}))
        .send()
        .await
        .unwrap();

    let after: serde_json::Value = client
        .get(format!("{}/api/admin/hlc", env.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after["rejects"], 1, "reject counter must advance");
    assert!(after["physical_ns"].as_u64().unwrap() > 0);
}

#[tokio::test]
#[serial]
async fn malformed_hlc_header_is_ignored() {
    // Parsing is lenient: a malformed header is treated as "no HLC" and
    // the request proceeds normally.
    let env = spawn().await;
    create_user(&env.base, "ux", "t-x").await;
    let auth = basic("ux", "pw");
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/sessions/s/append_and_fetch", env.base))
        .header("Authorization", &auth)
        .header("x-agentdb-hlc", "not-a-timestamp")
        .json(&json!({"event": {"k": 1}, "fetch_last_n": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "malformed header must not 400");
}
