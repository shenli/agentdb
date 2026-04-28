//! End-to-end JWT auth on the subscribe path.
//!
//! Migrated to v2 routes: `/api/sessions/{s}/subscribe` with Basic-auth
//! + JWT config. Sessions are seeded via Basic auth first so the locator
//! exists; JWT is then used on the v2 subscribe path.

mod common;

use common::{mint_hmac_token, mint_hmac_token_for_sessions, seed_basic_user, TestServer};

/// Seed one event for `session` using Basic auth so the locator exists
/// and v2 subscribe can resolve tenant.
async fn seed_session(server: &TestServer, auth: &str, session: &str) {
    let resp = server
        .post_json_basic(
            &format!("/api/sessions/{session}/append_and_fetch"),
            serde_json::json!({"event": {"seed": true}, "fetch_last_n": 0}),
            auth,
        )
        .await;
    assert!(resp.status().is_success(), "seed failed: {}", resp.status());
}

#[tokio::test]
async fn header_bearer_jwt_grants_subscribe_with_wildcard_sessions() {
    let server = TestServer::start_with_basic_auth_and_jwt_hmac("t-1", b"secret").await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;
    seed_session(&server, &auth, "s-1").await;

    let token = mint_hmac_token(b"secret", "t-1", "*", 60);
    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/s-1/subscribe", server.base))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status().as_u16(), 200, "wildcard JWT must grant");
    drop(resp);
}

#[tokio::test]
async fn query_param_access_token_grants_subscribe() {
    let server = TestServer::start_with_basic_auth_and_jwt_hmac("t-1", b"secret").await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;
    seed_session(&server, &auth, "s-1").await;

    let token = mint_hmac_token(b"secret", "t-1", "*", 60);
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/sessions/s-1/subscribe?access_token={token}",
            server.base
        ))
        .send()
        .await
        .expect("send");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "JWT in query param must grant"
    );
    drop(resp);
}

#[tokio::test]
async fn missing_jwt_returns_401_with_jwt_invalid_code() {
    let server = TestServer::start_with_basic_auth_and_jwt_hmac("t-1", b"secret").await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;
    seed_session(&server, &auth, "s-1").await;

    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/s-1/subscribe", server.base))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status().as_u16(), 401);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["code"], "jwt_invalid", "body: {body}");
}

#[tokio::test]
async fn jwt_with_session_not_in_claim_returns_403() {
    let server = TestServer::start_with_basic_auth_and_jwt_hmac("t-1", b"secret").await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;
    seed_session(&server, &auth, "s-1").await;

    // Token only allows `s-2` but we subscribe to `s-1`.
    let token = mint_hmac_token_for_sessions(b"secret", "t-1", &["s-2"], 60);
    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/s-1/subscribe", server.base))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status().as_u16(), 403);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["code"], "session_not_in_jwt", "body: {body}");
}

#[tokio::test]
async fn both_header_and_query_param_returns_400() {
    let server = TestServer::start_with_basic_auth_and_jwt_hmac("t-1", b"secret").await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;
    seed_session(&server, &auth, "s-1").await;

    let token = mint_hmac_token(b"secret", "t-1", "*", 60);
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/sessions/s-1/subscribe?access_token={token}",
            server.base
        ))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("send");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "supplying both must fail-loud rather than guess"
    );
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["code"], "bad_request_dual_auth", "body: {body}");
}

/// Task 9d: a JWT minted with a 2-second TTL must close the live
/// stream with a `jwt_expired` error frame after `exp` is reached.
#[tokio::test]
async fn jwt_expires_mid_stream_sends_error_and_closes() {
    let server = TestServer::start_with_basic_auth_and_jwt_hmac("t-1", b"secret").await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;
    seed_session(&server, &auth, "s-1").await;

    let token = mint_hmac_token(b"secret", "t-1", "*", 2);

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/api/sessions/s-1/subscribe", server.base))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("send sse open");
    assert_eq!(resp.status().as_u16(), 200);

    // Drive at least one event through the stream.
    client
        .post(format!(
            "{}/api/sessions/s-1/append_and_fetch",
            server.base
        ))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}))
        .send()
        .await
        .expect("first append");

    use futures_util::StreamExt;
    let mut buf = String::new();
    let mut bytes_stream = resp.bytes_stream();
    let mut got_first_event = false;
    let mut got_jwt_expired = false;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        let chunk = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            bytes_stream.next(),
        )
        .await;
        match chunk {
            Ok(Some(Ok(bytes))) => {
                buf.push_str(std::str::from_utf8(&bytes).expect("utf8"));
                while let Some(idx) = buf.find("\n\n") {
                    let block = buf[..idx].to_string();
                    buf.drain(..idx + 2);
                    if block.contains("event: subscribed") {
                        // Initial control frame; skip.
                    } else if block.contains("\"jwt_expired\"") {
                        got_jwt_expired = true;
                    } else if block.contains("\"seq\":") {
                        got_first_event = true;
                    }
                }
                if got_jwt_expired {
                    break;
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {
                // Keep poking so the per-yield expiry check fires.
                client
                    .post(format!(
                        "{}/api/sessions/s-1/append_and_fetch",
                        server.base
                    ))
                    .header("Authorization", &auth)
                    .json(&serde_json::json!({"event": {"kind": "msg"}, "fetch_last_n": 0}))
                    .send()
                    .await
                    .expect("pump append");
            }
        }
    }
    assert!(
        got_first_event,
        "expected at least one envelope before expiry"
    );
    assert!(
        got_jwt_expired,
        "expected jwt_expired error frame after exp",
    );
}

#[tokio::test]
async fn no_jwt_config_falls_back_without_jwt() {
    // A server with Basic auth but no JWT config: subscribe with no
    // JWT header (just Basic auth) must succeed.
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;

    let resp = reqwest::Client::new()
        .get(format!("{}/api/sessions/s-1/subscribe", server.base))
        .header("Authorization", &auth)
        .send()
        .await
        .expect("send");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "no JWT config + Basic auth = open"
    );
    drop(resp);
}
