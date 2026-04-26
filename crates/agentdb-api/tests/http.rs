use std::sync::Arc;

use agentdb_api::{router, AppState, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

async fn spawn() -> (TempDir, TempDir, String) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let state = AppState {
        registry: Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap()),
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: None,
        admin_auth: None,
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, format!("http://{addr}"))
}

#[tokio::test]
#[serial]
async fn append_and_fetch_endpoint_round_trip() {
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    let resp1: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"
        ))
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
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"
        ))
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
    // M15 Part C: REST AppendAndFetchRequest accepts an optional
    // actor_id, which is lifted onto Event::actor_id and echoed back
    // in the `recent` response.
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Append one event with actor_id set.
    let resp: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-agent/sessions/s-agent/append_and_fetch"
        ))
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
        .post(format!(
            "{base}/api/tenants/t-agent/sessions/s-agent/append_and_fetch"
        ))
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
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    for i in 1..=4 {
        client
            .post(format!(
                "{base}/api/tenants/t-1/sessions/s-bound/append_and_fetch"
            ))
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let resp: serde_json::Value = client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-bound/append_and_fetch"
        ))
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
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Seed an event so the session exists.
    client
        .post(format!(
            "{base}/api/tenants/t-1/sessions/s-1/append_and_fetch"
        ))
        .json(&json!({"event": {"text": "hi"}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let cold: serde_json::Value = client
        .post(format!("{base}/api/admin/tier/force"))
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
        .post(format!("{base}/api/admin/tier/force"))
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
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Seed parent with 2 events.
    for i in 1..=2u64 {
        client
            .post(format!(
                "{base}/api/tenants/t-1/sessions/parent/append_and_fetch"
            ))
            .json(&json!({"event": {"i": i}, "fetch_last_n": 0}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // Fork.
    let fork: serde_json::Value = client
        .post(format!("{base}/api/sessions/parent/fork"))
        .json(&json!({
            "tenant_id": "t-1",
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
        .post(format!(
            "{base}/api/tenants/t-1/sessions/child/append_and_fetch"
        ))
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
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    let first = client
        .post(format!(
            "{base}/api/tenants/t-hlc/sessions/s-hlc/append_and_fetch"
        ))
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

    // Second request — server HLC must not regress. We send our own
    // header back (echo) so the server absorbs it. The response's
    // header must be >= what we sent.
    let second = client
        .post(format!(
            "{base}/api/tenants/t-hlc/sessions/s-hlc/append_and_fetch"
        ))
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
    // Ordering is lexicographic on (phys, log) — compare as tuples.
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
    let (_dir, _obj_dir, base) = spawn().await;
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
        .post(format!(
            "{base}/api/tenants/t-bogus/sessions/s/append_and_fetch"
        ))
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
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    // Baseline — no rejects yet.
    let before: serde_json::Value = client
        .get(format!("{base}/api/admin/hlc"))
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

    // Trigger a rejection via a far-future header (same as the Phase F
    // test). The /hlc endpoint's `rejects` field must reflect it.
    let one_hour_ns: u64 = 3_600 * 1_000_000_000;
    let future_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        + one_hour_ns;
    let _ = client
        .post(format!(
            "{base}/api/tenants/t-r/sessions/s/append_and_fetch"
        ))
        .header("x-agentdb-hlc", format!("{future_ns}.0"))
        .json(&json!({"event": {"k": 1}, "fetch_last_n": 1}))
        .send()
        .await
        .unwrap();

    let after: serde_json::Value = client
        .get(format!("{base}/api/admin/hlc"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after["rejects"], 1, "reject counter must advance");
    // Physical must be non-zero after all this activity.
    assert!(after["physical_ns"].as_u64().unwrap() > 0);
}

#[tokio::test]
#[serial]
async fn malformed_hlc_header_is_ignored() {
    // Parsing is lenient: a malformed header is treated as "no HLC" and
    // the request proceeds normally.
    let (_dir, _obj_dir, base) = spawn().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{base}/api/tenants/t-x/sessions/s/append_and_fetch"
        ))
        .header("x-agentdb-hlc", "not-a-timestamp")
        .json(&json!({"event": {"k": 1}, "fetch_last_n": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "malformed header must not 400");
}
