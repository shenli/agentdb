//! End-to-end HTTP tests for the V2.2b record routes and the `append`
//! path's `source_refs.record_id` validation.
//!
//! Drives the router over real HTTP (`reqwest`) so we cover:
//! - the create/get/delete round trip;
//! - the `GET /records/search` endpoint and its repeatable `namespace` /
//!   `tag` query params;
//! - cross-tenant isolation on record GET;
//! - soft-delete removing a record from search;
//! - the route-ordering choice (literal `/search` beats dynamic
//!   `/:record_id`);
//! - the validation wired into `Runtime::append` for `source_refs.record_id`.

mod common;

use common::{spawn_app, TestApp};
use serde_json::json;

#[tokio::test]
async fn record_round_trip() {
    let app = spawn_app().await;
    let (tid, _aid, _sid) = bootstrap_session(&app).await;

    // POST /api/tenants/{tid}/records
    let body = json!({
        "namespace": ["agent", "support"],
        "text": "customer prefers ROI-focused copy",
        "tags": ["preference"],
        "policy": {"mode": "metadata_only", "fields": [], "vector_allowed": false},
        "metadata": {"category": "preference"}
    });
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201, "got {}", resp.status());
    let r: serde_json::Value = resp.json().await.unwrap();
    let rid = r["record_id"].as_str().unwrap().to_string();
    assert!(rid.starts_with("rec_"), "rid was {rid}");
    assert_eq!(r["namespace"], json!(["agent", "support"]));
    assert_eq!(r["tags"], json!(["preference"]));

    // GET /api/tenants/{tid}/records/{rid}
    let got: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/records/{rid}", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["record_id"], rid);
    assert_eq!(got["text"], "customer prefers ROI-focused copy");
}

#[tokio::test]
async fn create_record_with_omitted_optional_fields() {
    // Minimal body: just a namespace. The server must apply default
    // policy + null-everything-else without erroring.
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({"namespace": ["minimal"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);
    let r: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(r["namespace"], json!(["minimal"]));
    // Default policy is metadata_only/no vector.
    assert_eq!(r["policy"]["mode"], "metadata_only");
    assert_eq!(r["policy"]["vector_allowed"], false);
}

#[tokio::test]
async fn search_records_by_namespace_and_text() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    // Create three records: two under namespace=test with shared term
    // "apple", one under namespace=other.
    for (ns, text, tag) in [
        ("test", "apple cider", "fruit"),
        ("test", "apple pie", "fruit"),
        ("other", "apple sauce", "fruit"),
    ] {
        let body = json!({
            "namespace": [ns],
            "text": text,
            "tags": [tag],
            "policy": {"mode": "selected_fields", "fields": ["text_field"], "vector_allowed": false},
            "metadata": {"text_field": text}
        });
        let resp = app
            .client
            .post(format!("{}/api/tenants/{tid}/records", app.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 201);
    }

    // Search with text=apple under namespace=test → expects 2 hits.
    let resp: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/search?text=apple&namespace=test",
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
    let recs = resp["records"].as_array().unwrap();
    assert_eq!(
        recs.len(),
        2,
        "expected 2 matches in namespace=test, got {recs:?}"
    );
    for r in recs {
        assert_eq!(r["namespace"], json!(["test"]));
    }
}

#[tokio::test]
async fn search_records_with_no_text_returns_namespace_listing() {
    // Search with no `text` param walks every record in (tenant,
    // namespace). Useful to inspect what's there.
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    for i in 0..3 {
        let body = json!({
            "namespace": ["bucket"],
            "text": format!("item {i}"),
            "policy": {"mode": "metadata_only", "fields": [], "vector_allowed": false}
        });
        app.client
            .post(format!("{}/api/tenants/{tid}/records", app.base))
            .json(&body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let resp: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/search?namespace=bucket",
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
    assert_eq!(resp["records"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn post_search_records_supports_vector_embedding() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    for (text, embedding) in [
        ("east", vec![1.0, 0.0, 0.0]),
        ("north", vec![0.0, 1.0, 0.0]),
        ("up", vec![0.0, 0.0, 1.0]),
    ] {
        app.client
            .post(format!("{}/api/tenants/{tid}/records", app.base))
            .json(&json!({
                "namespace": ["vectors"],
                "text": text,
                "embedding": embedding,
                "policy": {
                    "mode": "metadata_only",
                    "fields": [],
                    "vector_allowed": true
                }
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let resp: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/records/search", app.base))
        .json(&json!({
            "namespace": ["vectors"],
            "embedding": [0.0, 1.0, 0.0],
            "limit": 3
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let recs = resp["records"].as_array().unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(recs[0]["text"], "north");
}

#[tokio::test]
async fn post_vector_search_skips_records_when_policy_denies_vector() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    app.client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["vectors"],
            "text": "denied",
            "embedding": [1.0, 0.0, 0.0],
            "policy": {
                "mode": "metadata_only",
                "fields": [],
                "vector_allowed": false
            }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/records/search", app.base))
        .json(&json!({
            "namespace": ["vectors"],
            "embedding": [1.0, 0.0, 0.0],
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
    assert!(resp["records"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn search_route_beats_dynamic_record_id_route() {
    // Route-ordering regression: `/records/search` must be matched as the
    // static path even though `/records/:record_id` would otherwise eat
    // the `search` segment. We assert by hitting `/records/search` with
    // no query — must come back with an empty `records` array, not a
    // 404 RECORD_NOT_FOUND from the GET-by-id handler.
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;
    let resp = app
        .client
        .get(format!("{}/api/tenants/{tid}/records/search", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "got {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["records"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn record_not_found_under_wrong_tenant() {
    let app = spawn_app().await;
    let (tid1, _, _) = bootstrap_session_with_key(&app, "first").await;

    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid1}/records", app.base))
        .json(&json!({"namespace": ["x"]}))
        .send()
        .await
        .unwrap();
    let rid = resp.json::<serde_json::Value>().await.unwrap()["record_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (tid2, _, _) = bootstrap_session_with_key(&app, "second").await;
    let resp = app
        .client
        .get(format!("{}/api/tenants/{tid2}/records/{rid}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "RECORD_NOT_FOUND");
}

#[tokio::test]
async fn delete_record_removes_from_search() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["delns"],
            "text": "find me",
            "policy": {"mode": "selected_fields", "fields": ["text"], "vector_allowed": false},
            "metadata": {"text": "find me"}
        }))
        .send()
        .await
        .unwrap();
    let rid = resp.json::<serde_json::Value>().await.unwrap()["record_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Search returns it.
    let pre: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/search?text=find&namespace=delns",
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
    assert_eq!(pre["records"].as_array().unwrap().len(), 1);

    // DELETE.
    let resp = app
        .client
        .delete(format!("{}/api/tenants/{tid}/records/{rid}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);

    // Search returns empty.
    let post: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/search?text=find&namespace=delns",
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
    assert!(post["records"].as_array().unwrap().is_empty());

    // GET on the deleted id is now a 404.
    let resp = app
        .client
        .get(format!("{}/api/tenants/{tid}/records/{rid}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    // Admin/audit read can include the soft-deleted marker.
    let deleted: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/{rid}?include_deleted=true",
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
    assert!(deleted["deleted_at_ms"].as_u64().is_some());
}

#[tokio::test]
async fn delete_unknown_record_returns_404() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;
    let bogus = "rec_018f0000-0000-7000-8000-000000000000";
    let resp = app
        .client
        .delete(format!("{}/api/tenants/{tid}/records/{bogus}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn create_record_under_unknown_tenant_returns_404() {
    let app = spawn_app().await;
    let bogus = "ten_018f0000-0000-7000-8000-000000000000";
    let resp = app
        .client
        .post(format!("{}/api/tenants/{bogus}/records", app.base))
        .json(&json!({"namespace": ["x"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "TENANT_NOT_FOUND");
}

#[tokio::test]
async fn append_event_with_source_record_ref_succeeds() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    // Create a record under the same tenant, then reference it via a
    // source_ref on an event.
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({"namespace": ["ns"]}))
        .send()
        .await
        .unwrap();
    let rid = resp.json::<serde_json::Value>().await.unwrap()["record_id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "tool_result",
            "payload": {},
            "artifact_refs": [],
            "source_refs": [{"record_id": rid}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "got {}", resp.status());
    let r: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);
}

#[tokio::test]
async fn append_event_with_bogus_source_record_ref_returns_404() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let bogus = "rec_018f0000-0000-7000-8000-000000000000";
    let resp = app
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
            "source_refs": [{"record_id": bogus}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "RECORD_NOT_FOUND");
}

// ---- helpers ----

async fn bootstrap_session(app: &TestApp) -> (String, String, String) {
    bootstrap_session_with_key(app, "test").await
}

async fn bootstrap_session_with_key(app: &TestApp, external_key: &str) -> (String, String, String) {
    let t: serde_json::Value = app
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
