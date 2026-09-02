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
async fn record_create_returns_commit_seq_accepted_by_search() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    let body = json!({
        "namespace": ["visibility"],
        "metadata": {"title": "fresh token"},
        "policy": {"mode": "metadata_only", "fields": [], "vector_allowed": false}
    });
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);
    let commit_seq = resp
        .headers()
        .get("X-AgentDB-Record-Commit-Seq")
        .expect("record commit sequence header")
        .to_str()
        .unwrap()
        .to_string();
    let created: serde_json::Value = resp.json().await.unwrap();

    let searched: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/search?text=fresh&namespace=visibility&min_visible_record_commit_seq={commit_seq}",
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
    let matches = searched["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["record"]["record_id"], created["record_id"]);
}

#[tokio::test]
async fn admin_record_index_status_reports_watermarks() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    app.client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["visibility"],
            "metadata": {"title": "indexed"},
            "policy": {"mode": "metadata_only", "fields": [], "vector_allowed": false}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let status: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/indexes/records", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(status["local_index_watermark"].as_u64().unwrap() >= 1);
    assert!(status["global_index_watermark"].as_u64().unwrap() >= 1);
    assert_eq!(status["local_index_lag_tokens"], 0);
    assert_eq!(status["global_index_lag_tokens"], 0);
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
    let recs = resp["matches"].as_array().unwrap();
    assert_eq!(
        recs.len(),
        2,
        "expected 2 matches in namespace=test, got {recs:?}"
    );
    for r in recs {
        assert_eq!(r["record"]["namespace"], json!(["test"]));
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
    assert_eq!(resp["matches"].as_array().unwrap().len(), 3);
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

    let recs = resp["matches"].as_array().unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(recs[0]["record"]["text"], "north");
    assert!(recs[0]["score"].as_f64().unwrap() > recs[1]["score"].as_f64().unwrap());
    assert_eq!(recs[0]["components"]["vector"], recs[0]["score"]);
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
    assert!(resp["matches"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn post_search_records_filters_by_metadata() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    for (text, metadata) in [
        (
            "wire mongo test passed",
            json!({
                "repo": "agentdb",
                "component": "wire",
                "kind": "test_result",
                "status": "passed",
                "confidence": 0.95,
                "source": {"commit": "abc123"}
            }),
        ),
        (
            "api smoke test passed",
            json!({
                "repo": "agentdb",
                "component": "api",
                "kind": "test_result",
                "status": "passed",
                "confidence": 0.90,
                "source": {"commit": "def456"}
            }),
        ),
        (
            "wire mongo test failed",
            json!({
                "repo": "agentdb",
                "component": "wire",
                "kind": "test_result",
                "status": "failed",
                "confidence": 0.40
            }),
        ),
    ] {
        app.client
            .post(format!("{}/api/tenants/{tid}/records", app.base))
            .json(&json!({
                "namespace": ["db0", "memory"],
                "text": text,
                "metadata": metadata,
                "tags": ["test"],
                "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
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
            "namespace": ["db0", "memory"],
            "text": "wire test",
            "metadata_filter": {
                "all": [
                    {"path": "repo", "op": "eq", "value": "agentdb"},
                    {"path": "component", "op": "eq", "value": "wire"},
                    {"path": "confidence", "op": "gte", "value": 0.8},
                    {"path": "source.commit", "op": "exists", "value": true}
                ],
                "none": [
                    {"path": "status", "op": "eq", "value": "failed"}
                ]
            },
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

    let matches = resp["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1, "matches: {matches:?}");
    assert_eq!(matches[0]["record"]["metadata"]["component"], "wire");
    assert_eq!(matches[0]["record"]["metadata"]["status"], "passed");
}

#[tokio::test]
async fn post_search_records_returns_ranked_score_components() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    for (text, embedding) in [("alpha beta", vec![1.0, 0.0]), ("alpha", vec![0.0, 1.0])] {
        app.client
            .post(format!("{}/api/tenants/{tid}/records", app.base))
            .json(&json!({
                "namespace": ["scores"],
                "text": text,
                "embedding": embedding,
                "policy": {"mode": "summary_only", "fields": [], "vector_allowed": true}
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
            "namespace": ["scores"],
            "text": "alpha",
            "embedding": [1.0, 0.0],
            "limit": 2
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let matches = resp["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0]["rank"], 1);
    assert_eq!(matches[1]["rank"], 2);
    assert!(matches[0]["score"].as_f64().unwrap() >= matches[1]["score"].as_f64().unwrap());
    assert!(matches[0]["components"]["text"].as_f64().is_some());
    assert!(matches[0]["components"]["vector"].as_f64().is_some());
    assert_eq!(matches[0]["record"]["text"], "alpha beta");
}

#[tokio::test]
async fn record_external_id_create_is_idempotent_and_upsert_updates() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    let create_body = json!({
        "external_id": "mem-http-1",
        "namespace": ["memory"],
        "text": "original memory",
        "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
    });
    let first_resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&create_body)
        .send()
        .await
        .unwrap();
    assert_eq!(first_resp.status().as_u16(), 201);
    let first_commit = first_resp
        .headers()
        .get("X-AgentDB-Record-Commit-Seq")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let first: serde_json::Value = first_resp.json().await.unwrap();
    let rid = first["record_id"].as_str().unwrap().to_string();
    assert_eq!(first["external_id"], "mem-http-1");
    assert_eq!(first["version"], 1);

    let duplicate_resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "external_id": "mem-http-1",
            "namespace": ["memory"],
            "text": "duplicate should not replace",
            "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate_resp.status().as_u16(), 201);
    let duplicate_commit = duplicate_resp
        .headers()
        .get("X-AgentDB-Record-Commit-Seq")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let duplicate: serde_json::Value = duplicate_resp.json().await.unwrap();
    assert_eq!(duplicate["record_id"], rid);
    assert_eq!(duplicate["text"], "original memory");
    assert_eq!(duplicate_commit, first_commit);

    let upsert_resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/records/upsert", app.base))
        .json(&json!({
            "external_id": "mem-http-1",
            "namespace": ["memory"],
            "text": "updated memory",
            "metadata": {"source": "upsert"},
            "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upsert_resp.status().as_u16(), 200);
    let upserted: serde_json::Value = upsert_resp.json().await.unwrap();
    assert_eq!(upserted["record_id"], rid);
    assert_eq!(upserted["text"], "updated memory");
    assert_eq!(upserted["version"], 2);
    assert_eq!(upserted["metadata"]["source"], "upsert");
}

#[tokio::test]
async fn record_patch_version_status_search_filters_and_touch() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    let created: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/records", app.base))
        .json(&json!({
            "namespace": ["memory"],
            "text": "durable memory",
            "tags": ["memory"],
            "metadata": {"kind": "memory"},
            "policy": {"mode": "summary_only", "fields": [], "vector_allowed": false}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let rid = created["record_id"].as_str().unwrap();
    let version = created["version"].as_u64().unwrap();

    let touched: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/records/search", app.base))
        .json(&json!({
            "namespace": ["memory"],
            "text": "durable",
            "touch": true,
            "min_score": 0.0,
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
    let matches = touched["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["record"]["record_id"], rid);
    assert_eq!(matches[0]["record"]["access_count"], 1);
    assert_eq!(matches[0]["record"]["version"], version);

    let conflict = app
        .client
        .patch(format!("{}/api/tenants/{tid}/records/{rid}", app.base))
        .json(&json!({"expected_version": 99, "status": "superseded"}))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status().as_u16(), 409);
    let err: serde_json::Value = conflict.json().await.unwrap();
    assert_eq!(err["code"], "VERSION_CONFLICT");

    let superseded: serde_json::Value = app
        .client
        .patch(format!("{}/api/tenants/{tid}/records/{rid}", app.base))
        .json(&json!({"expected_version": version, "status": "superseded"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(superseded["status"], "superseded");
    assert_eq!(superseded["version"], version + 1);

    let default_search: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/search?namespace=memory",
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
    assert!(default_search["matches"].as_array().unwrap().is_empty());

    let history_search: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/records/search?namespace=memory&text=durable&include_superseded=true&min_score=0&created_after_ms=0&created_before_ms={}",
            app.base,
            superseded["updated_at_ms"].as_u64().unwrap() + 1
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let matches = history_search["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0]["record"]["status"], "superseded");
}

#[tokio::test]
async fn tenant_meta_routes_round_trip() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    let meta: serde_json::Value = app
        .client
        .put(format!("{}/api/tenants/{tid}/meta/db0.cursor", app.base))
        .json(&json!({"value": {"offset": 7}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(meta["value"], json!({"offset": 7}));

    let got_meta: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/meta/db0.cursor", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got_meta["value"], json!({"offset": 7}));
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
    assert!(body["matches"].as_array().unwrap().is_empty());
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
    assert_eq!(pre["matches"].as_array().unwrap().len(), 1);

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
    assert!(post["matches"].as_array().unwrap().is_empty());

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
