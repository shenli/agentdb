//! Integration tests for the CORS middleware on v2 subscribe routes.
//!
//! v2 subscribe paths (`/api/sessions/:sid/subscribe`) are tenant-agnostic
//! in the URL; the CORS middleware cannot consult the per-tenant allowlist
//! and instead falls back to *default-permissive* behavior: any `Origin`
//! header is echoed back unconditionally. This is the deliberate v2 policy
//! (see `cors.rs` doc comment).
//!
//! Covers the four observable behaviors:
//!   1. Any origin (including non-allowlisted) → echoed via
//!      Access-Control-Allow-Origin (v2 permissive mode).
//!   2. OPTIONS preflight → 204 with method/header advertisements.
//!   3. No Origin header → no CORS header on response.
//!   4. An allowlisted origin is also echoed (permissive catches all).

mod common;

use common::{seed_basic_user, TestServer};

#[tokio::test]
async fn any_origin_is_echoed_on_v2_path() {
    // v2 CORS is permissive: any origin is echoed, regardless of whether
    // it appears in the per-tenant CorsConfig allowlist.
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "cors-user", "t-1", "pass").await;
    // Seed a session so resolve_session_tenant succeeds.
    reqwest::Client::new()
        .post(format!("{}/api/sessions/s-cors/append_and_fetch", server.base))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"event": {"x": 1}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/s-cors/subscribe", server.base))
        .header("Authorization", &auth)
        .header("Origin", "https://evil.example.com")
        .send()
        .await
        .expect("send GET subscribe");
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .expect("CORS header present on v2 path")
            .to_str()
            .unwrap(),
        "https://evil.example.com",
        "v2 path echoes any origin (permissive)"
    );
    drop(resp);
}

#[tokio::test]
async fn allowlisted_origin_is_also_echoed() {
    // When the origin is explicitly allowlisted in CorsConfig, v2 still
    // echoes it (permissive catches all, not just non-listed ones).
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "cors-user2", "t-1", "pass").await;
    reqwest::Client::new()
        .post(format!("{}/api/sessions/s-cors2/append_and_fetch", server.base))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"event": {"x": 1}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/s-cors2/subscribe", server.base))
        .header("Authorization", &auth)
        .header("Origin", "https://app.example.com")
        .send()
        .await
        .expect("send GET subscribe");
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .expect("CORS header present")
            .to_str()
            .unwrap(),
        "https://app.example.com"
    );
    drop(resp);
}

#[tokio::test]
async fn options_preflight_returns_204() {
    // CORS middleware short-circuits OPTIONS before auth — no user needed.
    let server = TestServer::start_with_basic_auth().await;
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/api/sessions/s-preflight/subscribe", server.base),
        )
        .header("Origin", "https://app.example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .expect("send OPTIONS preflight");
    assert_eq!(resp.status(), 204);
    let methods = resp
        .headers()
        .get("access-control-allow-methods")
        .expect("Access-Control-Allow-Methods present")
        .to_str()
        .unwrap();
    assert!(methods.contains("GET"));
    let headers = resp
        .headers()
        .get("access-control-allow-headers")
        .expect("Access-Control-Allow-Headers present")
        .to_str()
        .unwrap();
    assert!(headers.to_lowercase().contains("authorization"));
}

#[tokio::test]
async fn no_origin_header_means_no_cors_header() {
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "cors-user3", "t-1", "pass").await;
    reqwest::Client::new()
        .post(format!("{}/api/sessions/s-cors3/append_and_fetch", server.base))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"event": {"x": 1}, "fetch_last_n": 0}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/s-cors3/subscribe", server.base))
        .header("Authorization", &auth)
        .send()
        .await
        .expect("send GET subscribe");
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("access-control-allow-origin").is_none(),
        "no Origin header → no CORS header"
    );
    drop(resp);
}
