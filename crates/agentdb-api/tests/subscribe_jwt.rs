//! End-to-end JWT auth on the subscribe path (Phase 1 Task 9c-3,
//! spec §6.13).
//!
//! Boots an in-process server with a per-tenant HMAC JWT config and
//! exercises the four documented paths plus the dual-auth and
//! no-config-fallback edge cases:
//!
//! 1. JWT in `Authorization: Bearer …` with `sessions=*` → 200.
//! 2. JWT in `?access_token=` → 200.
//! 3. Missing JWT → 401 + body code `jwt_invalid`.
//! 4. JWT whose `sessions` claim doesn't include the path session
//!    → 403 + body code `session_not_in_jwt`.
//! 5. JWT in BOTH header and query → 400 + `bad_request_dual_auth`.
//! 6. Tenant has no JWT config → falls through to whatever auth the
//!    route applies (the harness boots with `auth: None`, so 200).
//!
//! No SSE round-trip — the auth decision happens before the stream
//! opens, so a one-shot HTTP GET is enough. Tests for live tail
//! and replay live in their own files.

mod common;

use common::{mint_hmac_token, mint_hmac_token_for_sessions, TestServer};

#[tokio::test]
async fn header_bearer_jwt_grants_subscribe_with_wildcard_sessions() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token(b"secret", "t-1", "*", 60);

    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status().as_u16(), 200, "wildcard JWT must grant");
    drop(resp);
}

#[tokio::test]
async fn query_param_access_token_grants_subscribe() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token(b"secret", "t-1", "*", 60);

    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe?access_token={token}",
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
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status().as_u16(), 401);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["code"], "jwt_invalid", "body: {body}");
}

#[tokio::test]
async fn jwt_with_session_not_in_claim_returns_403() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    // Token only allows `s-2` but we subscribe to `s-1` — the claim
    // is valid by signature/expiry/issuer but doesn't grant the
    // requested resource. Expect 403, not 401, because this is an
    // authorization (not authentication) failure.
    let token = mint_hmac_token_for_sessions(b"secret", "t-1", &["s-2"], 60);

    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
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
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token(b"secret", "t-1", "*", 60);

    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe?access_token={token}",
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
/// Spec §6.13 step 5. We open SSE with a short token, append an
/// event so we know the live tail is healthy, then keep pumping
/// appends past expiry until the close frame arrives. The pump is
/// what triggers the per-yield expiry check; without it the stream
/// would stay parked on `recv()` and only the keep-alive comments
/// would flow.
#[tokio::test]
async fn jwt_expires_mid_stream_sends_error_and_closes() {
    let server = TestServer::start_with_jwt_hmac(b"secret").await;
    let token = mint_hmac_token(b"secret", "t-1", "*", 2);

    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("send sse open");
    assert_eq!(resp.status().as_u16(), 200);

    // Drive at least one event through the stream up front so we
    // know the live tail wired correctly before we assert on the
    // expiry path.
    client
        .post(format!(
            "{}/api/tenants/t-1/sessions/s-1/append_and_fetch",
            server.base
        ))
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
                // Timed out waiting for a chunk — the live tail is
                // parked on recv() and the per-yield expiry check
                // only fires on the next event. Keep poking the bus
                // so the close frame eventually arrives.
                client
                    .post(format!(
                        "{}/api/tenants/t-1/sessions/s-1/append_and_fetch",
                        server.base
                    ))
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
    // `TestServer::start()` boots with `jwt_config: default()` and
    // `auth: None`. The subscribe extractor returns `Ok(None)` for
    // any tenant absent from the JWT map; with Basic auth also off,
    // the request just succeeds. Documents the "JWT is opt-in per
    // tenant" promise: deployments that don't enable JWT for a
    // tenant must keep working unchanged.
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/api/tenants/t-1/sessions/s-1/subscribe",
            server.base
        ))
        .send()
        .await
        .expect("send");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "no JWT config + no Basic auth = open"
    );
    drop(resp);
}
