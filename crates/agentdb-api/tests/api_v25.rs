mod common;

use agentdb_api::routing_http::{
    HDR_CELL_ID, HDR_PLACEMENT_SCOPE_ID, HDR_TABLET_EPOCH, HDR_TABLET_ID,
};
use common::spawn_app;
use serde_json::json;

async fn seed(app: &common::TestApp) -> (String, String, String) {
    seed_with_key(app, "api-v25").await
}

async fn seed_with_key(app: &common::TestApp, external_key: &str) -> (String, String, String) {
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": external_key, "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap().to_string();
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
    let aid = agent["agent_id"].as_str().unwrap().to_string();
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
    let sid = session["session_id"].as_str().unwrap().to_string();
    (tid, aid, sid)
}

async fn append(app: &common::TestApp, tid: &str, aid: &str, sid: &str, kind: &str, n: u64) {
    append_payload(app, tid, aid, sid, kind, json!({ "n": n })).await;
}

async fn append_payload(
    app: &common::TestApp,
    tid: &str,
    aid: &str,
    sid: &str,
    kind: &str,
    payload: serde_json::Value,
) {
    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": kind,
            "payload": payload,
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

#[tokio::test]
async fn fork_replay_and_subscribe_snapshot_work_over_http() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    append(&app, &tid, &aid, &sid, "parent", 0).await;
    append(&app, &tid, &aid, &sid, "parent", 1).await;
    append(&app, &tid, &aid, &sid, "parent", 2).await;

    let child: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/fork",
            app.base
        ))
        .json(&json!({"at_seq": 1, "alias": "child"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let child_sid = child["session_id"].as_str().unwrap();
    append(&app, &tid, &aid, child_sid, "child", 10).await;

    let replay: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{child_sid}/replay",
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
    let kinds: Vec<_> = replay["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, vec!["parent", "parent", "child"]);
    assert_eq!(replay["materialized_prefix"], false);

    let materialized: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{child_sid}/replay-prefix",
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
    assert_eq!(materialized["materialized_prefix"], true);
    assert_eq!(materialized["fork_depth"], 1);

    let replay_after_prefix: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{child_sid}/replay",
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
    assert_eq!(replay_after_prefix["materialized_prefix"], true);

    let sse = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/subscribe?from_seq=1",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(sse.contains("event: event"));
    assert!(sse.contains("\"seq\":1"));
    assert!(sse.contains("\"seq\":2"));
    assert!(!sse.contains("\"seq\":0"));
}

#[tokio::test]
async fn context_and_watch_work_over_http() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    append(&app, &tid, &aid, &sid, "message", 1).await;
    append(&app, &tid, &aid, &sid, "message", 2).await;
    app.client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["mem"],
            "text": "renewal note",
            "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let context: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/context",
            app.base
        ))
        .json(&json!({
            "max_events": 1,
            "max_bytes": 8000,
            "include": ["recent_events", "records", "policy"],
            "record_query": {"namespace": ["mem"], "text": "renewal"}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(context["events"].as_array().unwrap().len(), 1);
    assert_eq!(context["records"].as_array().unwrap().len(), 1);
    assert!(context["policy"].is_object());

    let items: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/watch?scope=tenant", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(items.as_array().unwrap().len(), 2);
    assert_eq!(items[0]["cell_id"], serde_json::Value::Null);
    let cursor = items[0]["cursor"].as_str().unwrap();
    let after: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/watch?scope=agent&agent_id={aid}&from_cursor={cursor}",
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
    assert_eq!(after.as_array().unwrap().len(), 1);

    let routed_items: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/watch?scope=tenant", app.base))
        .header(HDR_TABLET_ID, "7")
        .header(HDR_TABLET_EPOCH, "11")
        .header(HDR_CELL_ID, "cell-a")
        .header(HDR_PLACEMENT_SCOPE_ID, "tenant/api-v25")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(routed_items[0]["cell_id"], "cell-a");
    assert_eq!(routed_items[0]["placement_scope_id"], "tenant/api-v25");
    assert_eq!(routed_items[0]["tablet_id"], 7);
    assert_eq!(routed_items[0]["tablet_epoch"], 11);
    let routed_cursor = routed_items[0]["cursor"].as_str().unwrap();
    assert!(routed_cursor.contains("cell=cell-a"));
    assert!(routed_cursor.contains("scope=tenant/api-v25"));
    assert!(routed_cursor.contains("tablet=7"));
    assert!(routed_cursor.contains("epoch=11"));
}

#[tokio::test]
async fn export_watch_returns_event_envelopes_and_resume_cursor() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    append_payload(
        &app,
        &tid,
        &aid,
        &sid,
        "model_call",
        json!({
            "model": "gpt-test",
            "provider": "openai",
            "status": "ok",
            "duration_ms": 123,
            "input_tokens": 7,
            "output_tokens": 11,
            "total_tokens": 18,
            "cost_micros": 42,
            "metadata": {"trace": "abc"},
            "tags": ["golden-path"]
        }),
    )
    .await;
    append_payload(
        &app,
        &tid,
        &aid,
        &sid,
        "tool_result",
        json!({
            "tool_name": "shell",
            "tool_call_id": "call-1",
            "status": "ok",
            "duration_ms": 9
        }),
    )
    .await;

    let first: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/exports/watch?scope=tenant&limit=1",
            app.base
        ))
        .header(HDR_TABLET_ID, "7")
        .header(HDR_TABLET_EPOCH, "11")
        .header(HDR_CELL_ID, "cell-a")
        .header(HDR_PLACEMENT_SCOPE_ID, "tenant/api-v25")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let first_items = first["items"].as_array().unwrap();
    assert_eq!(first_items.len(), 1);
    assert_eq!(first["next_cursor"], first_items[0]["cursor"]);
    assert_eq!(first["high_watermark"], first_items[0]["cursor"]);
    assert_eq!(first_items[0]["export_version"], "2026-05-14");
    assert_eq!(first_items[0]["op"], "insert");
    assert_eq!(first_items[0]["object_type"], "event");
    assert_eq!(first_items[0]["tenant_id"], tid);
    assert_eq!(first_items[0]["agent_id"], aid);
    assert_eq!(first_items[0]["session_id"], sid);
    assert_eq!(first_items[0]["seq"], 0);
    assert_eq!(first_items[0]["kind"], "model_call");
    assert_eq!(first_items[0]["model"], "gpt-test");
    assert_eq!(first_items[0]["provider"], "openai");
    assert_eq!(first_items[0]["duration_ms"], 123);
    assert_eq!(first_items[0]["total_tokens"], 18);
    assert_eq!(first_items[0]["cost_micros"], 42);
    assert_eq!(first_items[0]["cell_id"], "cell-a");
    assert_eq!(first_items[0]["placement_scope_id"], "tenant/api-v25");
    assert_eq!(first_items[0]["tablet_id"], 7);
    assert_eq!(first_items[0]["tablet_epoch"], 11);
    assert_eq!(
        first_items[0]["data"]["event_id"],
        first_items[0]["event_id"]
    );
    assert_eq!(
        first_items[0]["data"]["payload"]["metadata"]["trace"],
        "abc"
    );

    let cursor = first["next_cursor"].as_str().unwrap();
    let after: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/exports/watch?scope=session&session_id={sid}&from_cursor={cursor}",
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
    let after_items = after["items"].as_array().unwrap();
    assert_eq!(after_items.len(), 1);
    assert_eq!(after_items[0]["kind"], "tool_result");
    assert_eq!(after_items[0]["tool_name"], "shell");
    assert_eq!(after_items[0]["tool_call_id"], "call-1");
}

#[tokio::test]
async fn export_status_reports_durable_export_capabilities() {
    let app = spawn_app().await;
    let (tid, _aid, _sid) = seed(&app).await;
    let status: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/exports/status", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["export_version"], "2026-05-14");
    assert_eq!(status["tenant_id"], tid);
    assert_eq!(status["mode"], "durable_ledger");
    assert_eq!(status["durable_ledger"], false);
    assert_eq!(status["managed_consumers"], true);
    assert_eq!(
        status["supported_object_types"],
        json!([
            "event",
            "tenant",
            "agent",
            "session",
            "artifact",
            "record",
            "skill",
            "skill_binding"
        ])
    );
}

#[tokio::test]
async fn export_watch_scope_never_leaks_other_tenants_or_agents() {
    let app = spawn_app().await;
    let (tenant_a, agent_a, session_a) = seed_with_key(&app, "api-v25-a").await;
    let (tenant_b, agent_b, session_b) = seed_with_key(&app, "api-v25-b").await;
    append(&app, &tenant_a, &agent_a, &session_a, "tenant-a", 1).await;
    append(&app, &tenant_b, &agent_b, &session_b, "tenant-b", 2).await;

    let tenant_items: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tenant_a}/exports/watch?scope=tenant",
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
    let items = tenant_items["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["tenant_id"], tenant_a);
    assert_eq!(items[0]["agent_id"], agent_a);
    assert_eq!(items[0]["session_id"], session_a);
    assert_eq!(items[0]["kind"], "tenant-a");

    let agent_items: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tenant_a}/exports/watch?scope=agent&agent_id={agent_a}",
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
    assert_eq!(agent_items["items"].as_array().unwrap().len(), 1);

    let wrong_agent = app
        .client
        .get(format!(
            "{}/api/tenants/{tenant_a}/exports/watch?scope=agent&agent_id={agent_b}",
            app.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_agent.status(), reqwest::StatusCode::NOT_FOUND);

    let wrong_session = app
        .client
        .get(format!(
            "{}/api/tenants/{tenant_a}/exports/watch?scope=session&session_id={session_b}",
            app.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_session.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn export_backfill_pages_event_snapshots_and_rejects_unknown_includes() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    append(&app, &tid, &aid, &sid, "first", 1).await;
    append(&app, &tid, &aid, &sid, "second", 2).await;

    let first_page: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/exports/backfill", app.base))
        .json(&json!({
            "scope": "session",
            "session_id": sid,
            "include": ["events"],
            "limit": 1
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let items = first_page["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["op"], "snapshot");
    assert_eq!(items[0]["kind"], "first");
    assert_eq!(first_page["complete"], false);
    assert_eq!(first_page["snapshot_high_watermark"], items[0]["cursor"]);

    let cursor = first_page["next_cursor"].as_str().unwrap();
    let second_page: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/exports/backfill", app.base))
        .json(&json!({
            "scope": "session",
            "session_id": sid,
            "include": ["events"],
            "from_cursor": cursor,
            "limit": 10
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let second_items = second_page["items"].as_array().unwrap();
    assert_eq!(second_items.len(), 1);
    assert_eq!(second_items[0]["kind"], "second");
    assert_eq!(second_page["complete"], true);

    app.client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["mem"],
            "text": "snapshot memory",
            "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let metadata_page: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/exports/backfill", app.base))
        .json(&json!({
            "scope": "tenant",
            "include": ["tenants", "agents", "sessions", "records"],
            "limit": 10
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let object_types = metadata_page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["object_type"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        object_types,
        ["agent", "record", "session", "tenant"]
            .into_iter()
            .collect()
    );
    assert!(metadata_page["next_cursor"]
        .as_str()
        .unwrap()
        .starts_with("snapshot:"));
    assert_eq!(metadata_page["complete"], true);

    let rejected = app
        .client
        .post(format!("{}/api/tenants/{tid}/exports/backfill", app.base))
        .json(&json!({
            "scope": "tenant",
            "include": ["widgets"],
            "limit": 10
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn export_consumers_claim_ack_and_rewind_cursors() {
    let app = spawn_app().await;
    let (tid, aid, sid) = seed(&app).await;
    append(&app, &tid, &aid, &sid, "before-consumer", 1).await;

    let created: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/exports/consumers", app.base))
        .json(&json!({
            "name": "clickhouse",
            "scope": "tenant",
            "include": ["events"]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["consumer_name"], "clickhouse");
    assert_eq!(created["scope"], "tenant");
    assert_eq!(created["include"], json!(["events"]));
    assert!(created["last_acked_cursor"].is_null());
    assert_eq!(created["pending_items"], serde_json::Value::Null);

    let claimed: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/clickhouse/claim",
            app.base
        ))
        .json(&json!({
            "lease_owner": "worker-a",
            "lease_ms": 60_000
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(claimed["lease_owner"], "worker-a");
    assert!(
        claimed["lease_expires_at_ms"].as_u64().unwrap()
            > claimed["updated_at_ms"].as_u64().unwrap()
    );

    let rejected_claim = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/clickhouse/claim",
            app.base
        ))
        .json(&json!({
            "lease_owner": "worker-b",
            "lease_ms": 60_000
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rejected_claim.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    let rejected_ack = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/clickhouse/ack",
            app.base
        ))
        .json(&json!({
            "cursor": "v2:0001",
            "lease_owner": "worker-b"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rejected_ack.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    let acked: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/clickhouse/ack",
            app.base
        ))
        .json(&json!({
            "cursor": "v2:0001",
            "snapshot_high_watermark": "v2:0001",
            "lease_owner": "worker-a"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(acked["last_acked_cursor"], "v2:0001");
    assert_eq!(acked["snapshot_high_watermark"], "v2:0001");
    append(&app, &tid, &aid, &sid, "after-ack", 2).await;

    let stale_ack = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/clickhouse/ack",
            app.base
        ))
        .json(&json!({
            "cursor": "v2:0000",
            "lease_owner": "worker-a"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        stale_ack.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    let rewound: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/clickhouse/rewind",
            app.base
        ))
        .json(&json!({
            "cursor": null,
            "lease_owner": "worker-a"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(rewound["last_acked_cursor"].is_null());

    let fetched: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/exports/consumers/clickhouse",
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
    assert_eq!(fetched["consumer_name"], "clickhouse");
    assert!(fetched["last_acked_cursor"].is_null());
    assert_eq!(fetched["pending_items"], 2);
    assert!(fetched["high_watermark"]
        .as_str()
        .unwrap()
        .starts_with("v2:"));
    assert!(fetched["pending_oldest_created_at_ms"].is_u64());
    assert!(fetched["lag_ms"].is_u64());
    assert_eq!(fetched["lease_active"], true);

    let admin: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/exports", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(admin["managed_consumers"], true);
    assert_eq!(admin["consumer_count"], 1);
    assert_eq!(admin["consumers"][0]["consumer_name"], "clickhouse");
    assert_eq!(admin["consumers"][0]["pending_items"], 2);
    assert_eq!(admin["metrics"]["consumer_count"], 1);
    assert_eq!(admin["metrics"]["leased_consumer_count"], 1);
    assert_eq!(admin["metrics"]["pending_items_total"], 2);
    assert!(admin["metrics"]["max_lag_ms"].is_u64());
    assert!(admin["metrics"]["oldest_pending_created_at_ms"].is_u64());
    assert_eq!(
        admin["metrics"]["oldest_retained_cursors"][0]["tenant_id"],
        tid
    );
}
