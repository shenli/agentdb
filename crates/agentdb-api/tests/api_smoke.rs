//! End-to-end smoke tests for the V2.0 HTTP routes.
//!
//! These tests drive the router over real HTTP via `reqwest`, exercising the
//! same path that an external client would: serde DTO → handler → runtime →
//! JSON response. The runtime is in-memory, so each test gets a clean state.

mod common;

use agentdb_api::{
    routing_http::{
        RouteRuntime, RouteSnapshot, HDR_OWNER_NODE, HDR_ROUTE_ACTION, HDR_ROUTE_REASON,
        HDR_TABLET_ID,
    },
    state::ProxyPolicy,
    AppState,
};
use agentdb_core::{runtime::FetchRequest, Id, IdKind, Runtime};
use agentdb_meta::{nodes::Node, tablets::Tablet};
use common::{spawn_app, spawn_app_with_state, TestApp};
use serde_json::json;

#[tokio::test]
async fn create_tenant_agent_session_append_fetch_round_trip() {
    let app = spawn_app().await;

    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "acme", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap().to_string();

    let a: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/agents", app.base))
        .json(&json!({"alias": "rep-1", "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = a["agent_id"].as_str().unwrap().to_string();

    let s: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = s["session_id"].as_str().unwrap().to_string();

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);

    // fetch_events returns Vec<EventEnvelope> directly (not wrapped).
    let events: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let events = events.as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["seq"].as_u64().unwrap(), 0);
    assert_eq!(events[0]["kind"], "x");
}

#[tokio::test]
async fn list_tenants_and_sessions_returns_counts_and_rows() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let tenants: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(tenants["count"].as_u64().unwrap() >= 1);
    assert!(
        tenants["tenants"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tenant| tenant["tenant_id"] == tid)
    );

    let agents: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/agents", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(agents["count"].as_u64().unwrap() >= 1);
    assert!(
        agents["agents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|agent| agent["agent_id"] == aid)
    );

    let sessions: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/sessions", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(sessions["count"].as_u64().unwrap() >= 1);
    assert!(
        sessions["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| session["session_id"] == sid && session["agent_id"] == aid)
    );
}

#[tokio::test]
async fn append_and_fetch_returns_recent() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/append-and-fetch",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": [],
            "last": 5,
            "max_bytes": 1_000_000
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);
    let recent = r["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 1);
}

#[tokio::test]
async fn lifecycle_transition_to_cold_preserves_fetch_and_reports_storage() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..3 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let before: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(before.as_array().unwrap().len(), 3);

    let storage: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "cold"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(storage["lifecycle"], "cold");
    assert_eq!(storage["sealed_segment_count"].as_u64().unwrap(), 1);
    assert_eq!(storage["warm_head_from_seq"].as_u64().unwrap(), 3);

    let after: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after.as_array().unwrap().len(), 3);

    let storage_get: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/storage",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(storage_get["sealed_segment_count"].as_u64().unwrap(), 1);
    assert_eq!(storage_get["next_seq"].as_u64().unwrap(), 3);
}

#[tokio::test]
async fn lifecycle_transition_to_archive_preserves_fetch_and_reports_manifest() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..3 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let storage: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "archive"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(storage["lifecycle"], "archive");
    assert_eq!(storage["sealed_segment_count"].as_u64().unwrap(), 0);
    assert!(storage["archive_manifest"].is_object());

    let events: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(events.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn restore_archived_session_materializes_a_cold_segment() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..3 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "archive"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let restored: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/restore",
            app.base
        ))
        .json(&json!({"target": "cold"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(restored["lifecycle"], "cold");
    assert_eq!(restored["sealed_segment_count"].as_u64().unwrap(), 1);
    assert!(restored["archive_manifest"].is_null());

    let events: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(events.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn storage_summary_reports_lifecycle_counts() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..2 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "archive"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let fleet: serde_json::Value = app
        .client
        .get(format!("{}/api/storage/summary", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(fleet["session_count"].as_u64().unwrap() >= 1);
    assert!(fleet["lifecycle_counts"]["archive"].as_u64().unwrap() >= 1);
    assert!(fleet["archive_manifest_count"].as_u64().unwrap() >= 1);

    let tenant: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/storage-summary", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tenant["tenant_id"], tid);
    assert_eq!(tenant["lifecycle_counts"]["archive"].as_u64().unwrap(), 1);
    assert_eq!(tenant["archive_manifest_count"].as_u64().unwrap(), 1);
}

#[tokio::test]
async fn session_handlers_add_local_route_headers_when_snapshot_owner_is_local() {
    let app = spawn_routed_app("node-a").await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    assert_eq!(r.headers()[HDR_ROUTE_ACTION].to_str().unwrap(), "local");
    assert_eq!(r.headers()[HDR_OWNER_NODE].to_str().unwrap(), "node-a");
    assert_eq!(r.headers()[HDR_TABLET_ID].to_str().unwrap(), "0");
}

#[tokio::test]
async fn admin_routing_status_reports_disabled_mode() {
    let app = spawn_app().await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/routing", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "disabled");
    assert_eq!(body["local_node_id"], "single-node");
    assert!(body.get("snapshot").is_none());
}

#[tokio::test]
async fn admin_routing_status_reports_snapshot_metadata() {
    let app = spawn_routed_app("node-a").await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/routing", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "snapshot");
    assert_eq!(body["local_node_id"], "node-a");
    assert_eq!(body["snapshot"]["shard_count"], 1);
    assert_eq!(body["snapshot"]["tablet_count"], 1);
    assert_eq!(body["snapshot"]["node_count"], 2);
    assert_eq!(body["snapshot"]["session_locator_count"], 0);
    assert_eq!(body["snapshot"]["nodes"][0]["region"], "us-west-2");
    assert_eq!(
        body["snapshot"]["nodes"][0]["availability_zone"],
        "us-west-2a"
    );
    assert_eq!(body["snapshot"]["nodes"][0]["route_location"], "us-west-2");
    assert_eq!(body["snapshot"]["tablets"][0]["owner_node_id"], "node-a");
    assert_eq!(body["snapshot"]["tablets"][0]["owner_region"], "us-west-2");
    assert_eq!(
        body["snapshot"]["tablets"][0]["owner_availability_zone"],
        "us-west-2a"
    );
    assert_eq!(
        body["snapshot"]["tablets"][0]["owner_route_location"],
        "us-west-2"
    );
    assert_eq!(body["snapshot"]["tablets"][0]["local_owner"], true);
}

#[tokio::test]
async fn admin_routing_decision_reports_disabled_local_dry_run() {
    let app = spawn_app().await;

    let body: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/admin/routing/decision?tenant_id=ten_1&session_id=ses_1",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "disabled");
    assert_eq!(body["tenant_id"], "ten_1");
    assert_eq!(body["session_id"], "ses_1");
    assert_eq!(body["action"], "local");
    assert_eq!(body["session_locator_found"], false);
    assert!(body["route_error_code"].is_null());
}

#[tokio::test]
async fn admin_routing_decision_reports_snapshot_forward_dry_run() {
    let app = spawn_routed_app("node-b").await;

    let body: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/admin/routing/decision?tenant_id=ten_1&session_id=ses_1",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "snapshot");
    assert_eq!(body["local_node_id"], "node-a");
    assert_eq!(body["action"], "forward");
    assert_eq!(body["shard_id"], 0);
    assert_eq!(body["tablet_id"], 0);
    assert_eq!(body["owner_node_id"], "node-b");
    assert_eq!(body["owner_location"], "us-west-2");
    assert_eq!(body["epoch"], 1);
    assert_eq!(body["session_locator_found"], false);
    assert!(body["reason"].is_null());
    assert!(body["route_error_code"].is_null());
}

#[tokio::test]
async fn session_handlers_proxy_remote_owner_append() {
    let (app, _owner) = spawn_proxy_pair().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?trace=1",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);

    let events = app
        .runtime
        .fetch_from_seq(FetchRequest {
            tenant_id: Id::parse_kind(&tid, IdKind::Tenant).unwrap(),
            session_id: Id::parse_kind(&sid, IdKind::Session).unwrap(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "x");
}

#[tokio::test]
async fn forwarded_mutation_with_stale_epoch_is_rejected_by_owner() {
    let (app, _owner) = spawn_stale_epoch_proxy_pair().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status().as_u16(), 409);
    assert_eq!(
        r.headers()[HDR_ROUTE_REASON].to_str().unwrap(),
        "ROUTE_STALE_TABLET_EPOCH"
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "ROUTE_STALE_TABLET_EPOCH");

    let events = app
        .runtime
        .fetch_from_seq(FetchRequest {
            tenant_id: Id::parse_kind(&tid, IdKind::Tenant).unwrap(),
            session_id: Id::parse_kind(&sid, IdKind::Session).unwrap(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
async fn proxy_rejects_oversized_owner_response() {
    let (app, owner) = spawn_proxy_pair_with_policy(128).await;
    let (tid, aid, sid) = bootstrap_session(&app).await;
    owner
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            owner.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "large",
            "payload": {"blob": "x".repeat(512)},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let r = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status().as_u16(), 502);
    assert_eq!(
        r.headers()[HDR_ROUTE_REASON].to_str().unwrap(),
        "ROUTE_FORWARD_RESPONSE_TOO_LARGE"
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "ROUTE_FORWARD_RESPONSE_TOO_LARGE");
}

#[tokio::test]
async fn session_handlers_return_route_rejection_for_unassigned_tablet() {
    let app = spawn_unassigned_routed_app().await;
    let (tid, _aid, sid) = bootstrap_session(&app).await;

    let r = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status().as_u16(), 503);
    assert_eq!(r.headers()[HDR_ROUTE_ACTION].to_str().unwrap(), "reject");
    assert_eq!(
        r.headers()[HDR_ROUTE_REASON].to_str().unwrap(),
        "ROUTE_UNASSIGNED"
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "ROUTE_UNASSIGNED");
}

#[tokio::test]
async fn idempotency_key_replays_result() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let body = json!({
        "agent_id": aid,
        "kind": "x",
        "payload": {"v": 1},
        "artifact_refs": [],
        "source_refs": []
    });

    let r1: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let r2: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(r1["event_id"], r2["event_id"]);
    assert_eq!(r1["seq"], r2["seq"]);
}

#[tokio::test]
async fn idempotency_key_conflict_on_payload_change() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r1 = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"v": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert!(r1.status().is_success());

    let r2 = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"v": 2},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status().as_u16(), 409);
    let body: serde_json::Value = r2.json().await.unwrap();
    assert_eq!(body["code"], "IDEMPOTENCY_CONFLICT");
}

#[tokio::test]
async fn unknown_tenant_returns_404() {
    let app = spawn_app().await;
    let bogus = "ten_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .get(format!("{}/api/tenants/{bogus}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "TENANT_NOT_FOUND");
}

#[tokio::test]
async fn create_agent_under_unknown_tenant_returns_404() {
    let app = spawn_app().await;
    let bogus = "ten_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .post(format!("{}/api/tenants/{bogus}/agents", app.base))
        .json(&json!({"alias": null, "config": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

#[tokio::test]
async fn append_to_unknown_session_returns_404() {
    let app = spawn_app().await;
    let (tid, _aid, _sid) = bootstrap_session(&app).await;
    let bogus = "ses_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{bogus}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": "agt_018f0000-0000-7000-8000-000000000000",
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

async fn bootstrap_session(app: &TestApp) -> (String, String, String) {
    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "test", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap().to_string();

    let a: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/agents", app.base))
        .json(&json!({"alias": null, "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = a["agent_id"].as_str().unwrap().to_string();

    let s: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = s["session_id"].as_str().unwrap().to_string();
    (tid, aid, sid)
}

async fn spawn_routed_app(owner_node_id: &str) -> TestApp {
    let runtime = Runtime::in_memory();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some(owner_node_id))])
        .with_nodes(vec![
            node("node-a", "http://127.0.0.1:18080"),
            node("node-b", "http://127.0.0.1:18081"),
        ]);
    let state = AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", snapshot));
    spawn_app_with_state(state).await
}

async fn spawn_proxy_pair() -> (TestApp, TestApp) {
    spawn_proxy_pair_with_policy(16 * 1024 * 1024).await
}

async fn spawn_proxy_pair_with_policy(max_response_bytes: u64) -> (TestApp, TestApp) {
    let runtime = Runtime::in_memory();
    let owner_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-b"))])
        .with_nodes(vec![node("node-b", "http://127.0.0.1:18081")]);
    let owner = spawn_app_with_state(
        AppState::new(runtime.clone())
            .with_routing(RouteRuntime::snapshot("node-b", owner_snapshot)),
    )
    .await;
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-b"))])
        .with_nodes(vec![
            node("node-a", "http://127.0.0.1:18080"),
            node("node-b", &owner.base),
        ]);
    let local = spawn_app_with_state(
        AppState::new(runtime)
            .with_routing(RouteRuntime::snapshot("node-a", snapshot))
            .with_proxy_policy(ProxyPolicy {
                timeout_ms: 5_000,
                max_response_bytes,
            }),
    )
    .await;
    (local, owner)
}

async fn spawn_stale_epoch_proxy_pair() -> (TestApp, TestApp) {
    let runtime = Runtime::in_memory();
    let owner_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet_with_epoch(0, Some("node-b"), 2)])
        .with_nodes(vec![node("node-b", "http://127.0.0.1:18081")]);
    let owner = spawn_app_with_state(
        AppState::new(runtime.clone())
            .with_routing(RouteRuntime::snapshot("node-b", owner_snapshot)),
    )
    .await;
    let local_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet_with_epoch(0, Some("node-b"), 1)])
        .with_nodes(vec![
            node("node-a", "http://127.0.0.1:18080"),
            node("node-b", &owner.base),
        ]);
    let local = spawn_app_with_state(
        AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", local_snapshot)),
    )
    .await;
    (local, owner)
}

async fn spawn_unassigned_routed_app() -> TestApp {
    let runtime = Runtime::in_memory();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, None)])
        .with_nodes(vec![node("node-a", "http://127.0.0.1:18080")]);
    let state = AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", snapshot));
    spawn_app_with_state(state).await
}

fn node(id: &str, address: &str) -> Node {
    Node {
        node_id: id.into(),
        address: address.into(),
        mongo_address: None,
        state: "alive".into(),
        last_heartbeat_ms: 100,
        joined_at_ms: 100,
        availability_zone: Some("us-west-2a".into()),
        region: Some("us-west-2".into()),
    }
}

fn tablet(id: i64, owner: Option<&str>) -> Tablet {
    tablet_with_epoch(id, owner, 1)
}

fn tablet_with_epoch(id: i64, owner: Option<&str>, epoch: i64) -> Tablet {
    Tablet {
        tablet_id: id,
        owner_node_id: owner.map(str::to_string),
        epoch,
        lease_expires_at_ms: i64::MAX,
        created_at_ms: 0,
        handoff_target_node_id: None,
    }
}
