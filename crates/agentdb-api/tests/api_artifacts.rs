//! End-to-end HTTP tests for the V2.1b artifact routes and the `append` path's
//! artifact_refs / inline-payload-size enforcement.
//!
//! Drives the router over real HTTP (`reqwest`) so we cover:
//! - the raw-bytes upload contract (no multipart, no base64);
//! - the `Content-Type` and `X-Sha256` header plumbing;
//! - the metadata + blob round trip;
//! - cross-tenant isolation on artifact GET;
//! - the validation wired into `Runtime::append` for refs and payload size.

mod common;

use common::{spawn_app, TestApp};
use serde_json::json;

#[tokio::test]
async fn artifact_round_trip() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    // POST raw bytes, Content-Type from header.
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .header("Content-Type", "text/plain")
        .body("hello world")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201, "got {}", resp.status());
    let art_meta: serde_json::Value = resp.json().await.unwrap();
    let aid = art_meta["artifact_id"].as_str().unwrap().to_string();
    assert_eq!(art_meta["bytes"].as_u64().unwrap(), 11);
    assert_eq!(art_meta["content_type"], "text/plain");
    assert_eq!(art_meta["sha256"].as_str().unwrap().len(), 64);
    // sha256("hello world")
    assert_eq!(
        art_meta["sha256"],
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );

    // GET metadata.
    let meta: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/artifacts/{aid}", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(meta["artifact_id"], aid);

    // GET blob: Content-Type echoed back, body matches.
    let resp = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/artifacts/{aid}/blob",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/plain"
    );
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "11"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(&body[..], b"hello world");
}

#[tokio::test]
async fn artifact_without_content_type_defaults_to_octet_stream() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    // No Content-Type header on the POST. reqwest will *not* add one for raw
    // bodies, so the runtime sees None and the GET /blob falls back to
    // application/octet-stream.
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .body(b"\x00\x01\x02".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);
    let aid = resp.json::<serde_json::Value>().await.unwrap()["artifact_id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/artifacts/{aid}/blob",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "application/octet-stream"
    );
}

#[tokio::test]
async fn artifact_sha256_mismatch_returns_422() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .header("X-Sha256", "0".repeat(64))
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 422);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "HASH_MISMATCH");
}

#[tokio::test]
async fn artifact_too_large_returns_422() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;

    // 17 MB clears the 16 MB cap. We could push the in-process server harder
    // but this is enough to confirm the limit fires before the content
    // backend touches anything.
    let big = vec![0u8; 17 * 1024 * 1024];
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 422);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "ARTIFACT_TOO_LARGE");
}

#[tokio::test]
async fn artifact_get_under_wrong_tenant_returns_404() {
    let app = spawn_app().await;
    let (tid1, _, _) = bootstrap_session_with_key(&app, "first").await;

    // Create an artifact in tenant 1.
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid1}/artifacts", app.base))
        .body("data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);
    let aid = resp.json::<serde_json::Value>().await.unwrap()["artifact_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Tenant 2 must not see it.
    let (tid2, _, _) = bootstrap_session_with_key(&app, "second").await;
    let resp = app
        .client
        .get(format!("{}/api/tenants/{tid2}/artifacts/{aid}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "ARTIFACT_NOT_FOUND");
}

#[tokio::test]
async fn artifact_get_blob_under_wrong_tenant_returns_404() {
    let app = spawn_app().await;
    let (tid1, _, _) = bootstrap_session_with_key(&app, "first").await;
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid1}/artifacts", app.base))
        .body("data")
        .send()
        .await
        .unwrap();
    let aid = resp.json::<serde_json::Value>().await.unwrap()["artifact_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (tid2, _, _) = bootstrap_session_with_key(&app, "second").await;
    let resp = app
        .client
        .get(format!(
            "{}/api/tenants/{tid2}/artifacts/{aid}/blob",
            app.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn artifact_get_unknown_returns_404() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;
    let bogus = "art_018f0000-0000-7000-8000-000000000000";
    let resp = app
        .client
        .get(format!("{}/api/tenants/{tid}/artifacts/{bogus}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn append_event_with_artifact_ref_succeeds() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    // Create an artifact under the same tenant.
    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .body("payload")
        .send()
        .await
        .unwrap();
    let art: serde_json::Value = resp.json().await.unwrap();
    let art_id = art["artifact_id"].as_str().unwrap();

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "tool_result",
            "payload": {},
            "artifact_refs": [{"artifact_id": art_id, "bytes": 7}],
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
}

#[tokio::test]
async fn append_event_with_bogus_artifact_ref_returns_404() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let bogus = "art_018f0000-0000-7000-8000-000000000000";
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
            "artifact_refs": [{"artifact_id": bogus}],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "ARTIFACT_NOT_FOUND");
}

#[tokio::test]
async fn append_oversized_inline_payload_returns_422() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    // 70_000 bytes of "x" inside a JSON object easily clears the 64 KB cap
    // once serialized.
    let big = "x".repeat(70_000);
    let resp = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"big": big},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 422);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "EVENT_TOO_LARGE");
    assert!(
        err["suggestion"].is_string(),
        "EventTooLarge body should carry a 'suggestion' field, got: {err}"
    );
}

// ---- helpers ----

async fn bootstrap_session(app: &TestApp) -> (String, String, String) {
    bootstrap_session_with_key(app, "test").await
}

/// Caller-supplied tenant external_key so multi-tenant tests can spin up
/// fresh tenants without colliding on the unique-alias constraint.
async fn bootstrap_session_with_key(
    app: &TestApp,
    external_key: &str,
) -> (String, String, String) {
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
