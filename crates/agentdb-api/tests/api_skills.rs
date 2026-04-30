mod common;

use common::{spawn_app, TestApp};
use serde_json::json;

#[tokio::test]
async fn skill_register_bind_list_and_skill_used_round_trip() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let skill: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/skills", app.base))
        .header("Idempotency-Key", "register-skill")
        .json(&json!({
            "name": "summarize",
            "version": "1.0.0",
            "manifest": {"entrypoint": "run"},
            "artifact_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let skill_id = skill["skill_id"].as_str().unwrap().to_string();
    assert!(skill_id.starts_with("skl_"));

    let binding: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/skills/bind",
            app.base
        ))
        .header("Idempotency-Key", "bind-skill")
        .json(&json!({"skill_id": skill_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(binding["name"], "summarize");
    assert_eq!(binding["version"], "1.0.0");

    let listed: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/agents/{aid}/skills",
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
    assert_eq!(listed["skills"].as_array().unwrap().len(), 1);

    let event: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "skill_used",
            "payload": {"skill_id": skill_id, "version": "1.0.0"},
            "source_refs": [{"skill_id": skill_id}]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(event["seq"], 0);
}

#[tokio::test]
async fn duplicate_skill_version_returns_409() {
    let app = spawn_app().await;
    let (tid, _, _) = bootstrap_session(&app).await;
    let body = json!({"name": "tool", "version": "1.0.0", "manifest": {}});

    app.client
        .post(format!("{}/api/tenants/{tid}/skills", app.base))
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp = app
        .client
        .post(format!("{}/api/tenants/{tid}/skills", app.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "ALIAS_CONFLICT");
}

#[tokio::test]
async fn bind_unknown_skill_returns_404() {
    let app = spawn_app().await;
    let (tid, aid, _) = bootstrap_session(&app).await;
    let bogus = "skl_018f0000-0000-7000-8000-000000000000";
    let resp = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/skills/bind",
            app.base
        ))
        .json(&json!({"skill_id": bogus}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "SKILL_NOT_FOUND");
}

#[tokio::test]
async fn skill_used_wrong_version_returns_404() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;
    let skill: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/skills", app.base))
        .json(&json!({"name": "tool", "version": "1.0.0", "manifest": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let skill_id = skill["skill_id"].as_str().unwrap();

    let resp = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "skill_used",
            "payload": {"skill_id": skill_id, "version": "2.0.0"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["code"], "SKILL_NOT_FOUND");
}

#[tokio::test]
async fn delete_skill_hides_binding_and_gc_purges_skill() {
    let app = spawn_app().await;
    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "external_key": "skill-delete",
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
    let tid = t["tenant_id"].as_str().unwrap();
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
    let aid = a["agent_id"].as_str().unwrap();
    let skill: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/skills", app.base))
        .json(&json!({"name": "tool", "version": "1.0.0", "manifest": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let skill_id = skill["skill_id"].as_str().unwrap();
    app.client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/skills/bind",
            app.base
        ))
        .json(&json!({"skill_id": skill_id}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let deleted = app
        .client
        .delete(format!("{}/api/tenants/{tid}/skills/{skill_id}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status().as_u16(), 204);

    let listed: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/agents/{aid}/skills",
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
    assert!(listed["skills"].as_array().unwrap().is_empty());

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
    assert_eq!(gc["skills_purged"].as_u64(), Some(1));
    assert_eq!(gc["skill_bindings_purged"].as_u64(), Some(1));
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
