mod common;

use common::spawn_app;
use serde_json::json;

#[tokio::test]
async fn record_write_emits_backpressure_header_after_soft_quota() {
    let app = spawn_app().await;
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "bp-records",
            "policy": {
                "quota": {"soft_max_records": 1, "max_records": 10}
            }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap();

    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["docs"],
            "text": "alpha"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    assert_eq!(
        resp.headers()
            .get("x-agentdb-backpressure")
            .and_then(|v| v.to_str().ok()),
        Some("records=100")
    );
}

#[tokio::test]
async fn artifact_write_emits_backpressure_and_hard_quota_rejects() {
    let app = spawn_app().await;
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "bp-artifacts",
            "policy": {
                "quota": {
                    "soft_max_artifact_storage_bytes": 4,
                    "max_artifact_storage_bytes": 6
                }
            }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap();

    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .body("1234")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("x-agentdb-backpressure")
            .and_then(|v| v.to_str().ok()),
        Some("artifact_storage=100")
    );

    let rejected = app
        .client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .body("123")
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status().as_u16(), 422);
    let body: serde_json::Value = rejected.json().await.unwrap();
    assert_eq!(body["code"], "POLICY_DENIED");
}

#[tokio::test]
async fn session_and_append_writes_emit_backpressure_headers() {
    let app = spawn_app().await;
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "bp-session-events",
            "policy": {
                "quota": {
                    "soft_max_active_sessions": 1,
                    "max_active_sessions": 2,
                    "soft_max_events_per_session": 1,
                    "max_events_per_session": 2
                }
            }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap();
    let agent: serde_json::Value = app
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
    let aid = agent["agent_id"].as_str().unwrap();

    let session_resp = app
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
        .unwrap();
    assert_eq!(
        session_resp
            .headers()
            .get("x-agentdb-backpressure")
            .and_then(|v| v.to_str().ok()),
        Some("active_sessions=100")
    );
    let session: serde_json::Value = session_resp.json().await.unwrap();
    let sid = session["session_id"].as_str().unwrap();

    let append_resp = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        append_resp
            .headers()
            .get("x-agentdb-backpressure")
            .and_then(|v| v.to_str().ok()),
        Some("active_sessions=100,session_events=100")
    );
}

#[tokio::test]
async fn policy_sweep_endpoints_run_lifecycle_and_gc_for_tenant() {
    let app = spawn_app().await;
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "sweeps",
            "policy": {
                "lifecycle": {"cold_after_ms": 0},
                "retention": {"retention_ms": 0}
            }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap();
    let agent: serde_json::Value = app
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
    let aid = agent["agent_id"].as_str().unwrap();
    app.client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let lifecycle: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/policy/lifecycle-sweep",
            app.base
        ))
        .json(&json!({"now_ms": u64::MAX}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(lifecycle["sessions_transitioned"].as_u64(), Some(1));

    let record: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({"namespace": ["docs"], "text": "purge me"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let rid = record["record_id"].as_str().unwrap();
    app.client
        .delete(format!("{}/api/tenants/{tid}/records/{rid}", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let gc: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/policy/gc-sweep", app.base))
        .json(&json!({"now_ms": u64::MAX}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(gc["records_purged"].as_u64(), Some(1));

    let missing = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/{rid}?include_deleted=true",
            app.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status().as_u16(), 404);
}

#[tokio::test]
async fn policy_worker_endpoint_schedules_by_priority() {
    let app = spawn_app().await;
    let low: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "worker-low",
            "policy": {"priority": "low", "lifecycle": {"cold_after_ms": 0}}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let high: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "worker-high",
            "policy": {"priority": "high", "lifecycle": {"cold_after_ms": 0}}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let low_tid = low["tenant_id"].as_str().unwrap();
    let high_tid = high["tenant_id"].as_str().unwrap();

    for tid in [low_tid, high_tid] {
        let agent: serde_json::Value = app
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
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/agents/{}/sessions",
                app.base,
                agent["agent_id"].as_str().unwrap()
            ))
            .json(&json!({"alias": null}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let report: serde_json::Value = app
        .client
        .post(format!("{}/api/policy/worker/run", app.base))
        .json(&json!({"now_ms": u64::MAX, "max_tenants": 1}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(report["tenants_considered"].as_u64(), Some(2));
    assert_eq!(report["tenants_scheduled"].as_u64(), Some(1));
    assert_eq!(report["tenants"][0]["tenant_id"], high_tid);
    assert_eq!(report["tenants"][0]["priority"], "high");
    assert_eq!(
        report["lifecycle"]["sessions_transitioned"].as_u64(),
        Some(1)
    );
}

#[tokio::test]
async fn delete_session_then_gc_sweep_purges_session_events() {
    let app = spawn_app().await;
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "session-delete",
            "policy": {"retention": {"retention_ms": 0}}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap();
    let agent: serde_json::Value = app
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
    let aid = agent["agent_id"].as_str().unwrap();
    let session: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": "delete-me"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = session["session_id"].as_str().unwrap();

    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let deleted = app
        .client
        .delete(format!("{}/api/tenants/{tid}/sessions/{sid}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status().as_u16(), 204);
    let fetched = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(fetched.status().as_u16(), 404);

    let gc: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/policy/gc-sweep", app.base))
        .json(&json!({"now_ms": u64::MAX}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(gc["sessions_purged"].as_u64(), Some(1));
    assert_eq!(gc["events_purged"].as_u64(), Some(1));
}

#[tokio::test]
async fn gc_sweep_reports_idempotency_purges() {
    let app = spawn_app().await;
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "idem-gc",
            "policy": {"retention": {"retention_ms": 0}}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap();
    let agent: serde_json::Value = app
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
    let aid = agent["agent_id"].as_str().unwrap();
    let session: serde_json::Value = app
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
    let sid = session["session_id"].as_str().unwrap();
    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "idem-gc")
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let gc: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/policy/gc-sweep", app.base))
        .json(&json!({"now_ms": u64::MAX}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(gc["idempotency_purged"].as_u64(), Some(1));
}
