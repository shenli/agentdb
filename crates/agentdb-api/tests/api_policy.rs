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
