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

#[tokio::test]
async fn tenant_scoped_tokens_bind_the_data_plane_to_one_tenant() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let state = AppState::new(Runtime::in_memory()).with_auth_meta(meta);
    let app = spawn_app_with_state(state).await;

    // Open bootstrap: the data plane works and /api/me reports root.
    let me: serde_json::Value = app
        .client
        .get(format!("{}/api/me", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["kind"], "root");

    let tenant_a: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "tenant-a"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tenant_b: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "tenant-b"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid_a = tenant_a["tenant_id"].as_str().unwrap();
    let tid_b = tenant_b["tenant_id"].as_str().unwrap();

    let minted_admin: serde_json::Value = app
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
    let admin_token = minted_admin["token"].as_str().unwrap();

    // First mint closes the data plane too, not just /api/admin.
    assert_eq!(
        app.client
            .get(format!("{}/api/tenants/{tid_a}", app.base))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    // Mint-time validation of the tenant binding.
    for (body, expect) in [
        (
            json!({"description": "no tenant", "scope": "tenant"}),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            json!({"description": "admin with tenant", "scope": "admin", "tenant_id": tid_a}),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            json!({"description": "ghost", "scope": "tenant", "tenant_id": "t_does_not_exist"}),
            reqwest::StatusCode::NOT_FOUND,
        ),
    ] {
        assert_eq!(
            app.client
                .post(format!("{}/api/admin/tokens", app.base))
                .bearer_auth(admin_token)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            expect,
            "mint body: {body}"
        );
    }

    let minted_tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tokens", app.base))
        .bearer_auth(admin_token)
        .json(&json!({"description": "workspace a", "scope": "tenant", "tenant_id": tid_a}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tenant_token = minted_tenant["token"].as_str().unwrap();
    assert_eq!(minted_tenant["tenant_id"], tid_a);

    // Own tenant: reads and writes work.
    assert_eq!(
        app.client
            .get(format!("{}/api/tenants/{tid_a}", app.base))
            .bearer_auth(tenant_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    let agent_status = app
        .client
        .post(format!("{}/api/tenants/{tid_a}/agents", app.base))
        .bearer_auth(tenant_token)
        .json(&json!({"alias": "worker"}))
        .send()
        .await
        .unwrap()
        .status();
    assert!(agent_status.is_success(), "agent create: {agent_status}");

    // Everything outside the bound tenant is forbidden.
    for (method, path) in [
        ("GET", format!("/api/tenants/{tid_b}")),
        ("GET", "/api/tenants".to_string()),
        ("POST", "/api/tenants".to_string()),
        ("GET", "/api/admin/scheduler".to_string()),
        ("GET", "/api/storage/summary".to_string()),
    ] {
        let request = match method {
            "GET" => app.client.get(format!("{}{path}", app.base)),
            _ => app
                .client
                .post(format!("{}{path}", app.base))
                .json(&json!({"external_key": "nope"})),
        };
        assert_eq!(
            request
                .bearer_auth(tenant_token)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN,
            "{method} {path}"
        );
    }

    // /api/me lets an SDK discover its workspace from the credential alone.
    let me: serde_json::Value = app
        .client
        .get(format!("{}/api/me", app.base))
        .bearer_auth(tenant_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me["kind"], "tenant");
    assert_eq!(me["tenant_id"], tid_a);
    let me_admin: serde_json::Value = app
        .client
        .get(format!("{}/api/me", app.base))
        .bearer_auth(admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(me_admin["kind"], "admin");

    // Admin-scope tokens now reach the data plane as well.
    assert_eq!(
        app.client
            .get(format!("{}/api/tenants/{tid_b}", app.base))
            .bearer_auth(admin_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );

    // Revocation takes effect immediately despite the verify cache.
    let tenant_token_id = minted_tenant["token_id"].as_str().unwrap();
    app.client
        .delete(format!("{}/api/admin/tokens/{tenant_token_id}", app.base))
        .bearer_auth(admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        app.client
            .get(format!("{}/api/tenants/{tid_a}", app.base))
            .bearer_auth(tenant_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
}

/// Chaos: hammer the data plane with a tenant token while another task
/// revokes it and mints replacements. Falsifies two claims about the verify
/// cache: (1) after a revoke response is acknowledged, no further request
/// with the dead token ever succeeds (invalidate-on-revoke works under
/// concurrency), and (2) auth races never surface as 5xx.
#[tokio::test]
async fn revocation_races_with_live_traffic_without_grants_or_errors() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let state = AppState::new(Runtime::in_memory()).with_auth_meta(meta);
    let app = spawn_app_with_state(state).await;

    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "chaos"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap().to_string();

    let minted_admin: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tokens", app.base))
        .json(&json!({"description": "root", "scope": "admin"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let admin_token = minted_admin["token"].as_str().unwrap().to_string();

    for round in 0..8 {
        let minted: serde_json::Value = app
            .client
            .post(format!("{}/api/admin/tokens", app.base))
            .bearer_auth(&admin_token)
            .json(&json!({
                "description": format!("chaos-{round}"),
                "scope": "tenant",
                "tenant_id": tid,
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let token = minted["token"].as_str().unwrap().to_string();
        let token_id = minted["token_id"].as_str().unwrap().to_string();

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let revoked_at = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut workers = Vec::new();
        for _ in 0..4 {
            let client = app.client.clone();
            let base = app.base.clone();
            let (tid, token) = (tid.clone(), token.clone());
            let (stop, revoked_at) = (stop.clone(), revoked_at.clone());
            workers.push(tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // Order matters: read the revocation flag BEFORE sending,
                    // so a success with the flag already set is a true
                    // post-revocation grant, not a race in the test itself.
                    let already_revoked = revoked_at.load(std::sync::atomic::Ordering::SeqCst);
                    let status = client
                        .get(format!("{base}/api/tenants/{tid}"))
                        .bearer_auth(&token)
                        .send()
                        .await
                        .unwrap()
                        .status();
                    assert!(!status.is_server_error(), "auth race surfaced as {status}");
                    if already_revoked {
                        assert_eq!(
                            status,
                            reqwest::StatusCode::UNAUTHORIZED,
                            "revoked token was accepted after revoke ack"
                        );
                    }
                }
            }));
        }

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        app.client
            .delete(format!("{}/api/admin/tokens/{token_id}", app.base))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        revoked_at.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for worker in workers {
            worker.await.unwrap();
        }
    }
}
