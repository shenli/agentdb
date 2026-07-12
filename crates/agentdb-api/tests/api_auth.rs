mod common;

use agentdb_api::AppState;
use agentdb_core::Runtime;
use agentdb_meta::MetaStore;
use common::spawn_app_with_state;
use serde_json::json;

#[tokio::test]
async fn static_api_token_protects_external_api_but_not_health() {
    let state = AppState::new(Runtime::in_memory())
        .with_api_auth_token("current-api-token-123")
        .with_api_auth_previous_token("previous-api-token-1");
    let app = spawn_app_with_state(state).await;

    assert_eq!(
        app.client
            .get(format!("{}/api/tenants", app.base))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    for token in ["current-api-token-123", "previous-api-token-1"] {
        assert_eq!(
            app.client
                .get(format!("{}/api/tenants", app.base))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
    }
    assert_eq!(
        app.client
            .get(format!("{}/health/live", app.base))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
}

#[tokio::test]
async fn metadata_admin_tokens_bootstrap_enforce_scope_and_revoke() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let state = AppState::new(Runtime::in_memory()).with_auth_meta(meta);
    let app = spawn_app_with_state(state).await;

    let bootstrap: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tokens", app.base))
        .json(&json!({"description": "bootstrap", "scope": "admin"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let admin_token = bootstrap["token"].as_str().unwrap();

    assert_eq!(
        app.client
            .get(format!("{}/api/admin/scheduler", app.base))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    let readonly: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tokens", app.base))
        .bearer_auth(admin_token)
        .json(&json!({"description": "observer", "scope": "readonly"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let readonly_token = readonly["token"].as_str().unwrap();

    assert_eq!(
        app.client
            .get(format!("{}/api/admin/tokens", app.base))
            .bearer_auth(readonly_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        app.client
            .post(format!("{}/api/admin/drain", app.base))
            .bearer_auth(readonly_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let readonly_id = readonly["token_id"].as_str().unwrap();
    let revoked: serde_json::Value = app
        .client
        .delete(format!("{}/api/admin/tokens/{readonly_id}", app.base))
        .bearer_auth(admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(revoked["revoked"], true);
    assert_eq!(
        app.client
            .get(format!("{}/api/admin/tokens", app.base))
            .bearer_auth(readonly_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    let admin_id = bootstrap["token_id"].as_str().unwrap();
    app.client
        .delete(format!("{}/api/admin/tokens/{admin_id}", app.base))
        .bearer_auth(admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        app.client
            .post(format!("{}/api/admin/tokens", app.base))
            .json(&json!({"description": "must not reopen", "scope": "admin"}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn readiness_rejects_draining_nodes_while_liveness_stays_up() {
    let state = AppState::new(Runtime::in_memory());
    state.drain.activate(1);
    let app = spawn_app_with_state(state).await;

    assert_eq!(
        app.client
            .get(format!("{}/health/ready", app.base))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        app.client
            .get(format!("{}/health/live", app.base))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
}
