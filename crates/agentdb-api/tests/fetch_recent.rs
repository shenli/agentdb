//! Integration tests for the read-only `fetch_recent` endpoint.
//!
//! Migrated to v2 routes:
//! - Append via `/api/sessions/{s}/append_and_fetch` (with Basic auth)
//! - Fetch via `/api/sessions/{s}/fetch_recent` (with Basic auth)

mod common;

use common::{seed_basic_user, TestServer};
use serde_json::{json, Value};

/// Append one event to the session via the v2 auth-bound route.
async fn append(server: &TestServer, auth: &str, session: &str, payload: Value) {
    server
        .post_json_basic(
            &format!("/api/sessions/{session}/append_and_fetch"),
            json!({"event": payload, "fetch_last_n": 0}),
            auth,
        )
        .await
        .error_for_status()
        .unwrap();
}

/// Call the v2 session-keyed fetch_recent and return the parsed JSON body.
async fn fetch_recent(server: &TestServer, auth: &str, session: &str, n: usize) -> Value {
    let resp = server
        .post_json_basic(
            &format!("/api/sessions/{session}/fetch_recent"),
            json!({"n": n}),
            auth,
        )
        .await;
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(status, 200, "fetch_recent returned {status}: {body}");
    body
}

/// A session that has never had an event appended still returns
/// `{ "recent": [] }` rather than an error.
#[tokio::test]
async fn empty_session_returns_empty_recent() {
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "alice", "t-1", "pw").await;
    // Don't append anything; fetch should return empty.
    let body = fetch_recent(&server, &auth, "s-new", 10).await;
    assert_eq!(body["recent"], json!([]), "expected empty recent array");
}

/// After appending 5 events, `n: 3` returns the last 3 in ascending
/// sequence order.
#[tokio::test]
async fn fetch_last_3_of_5_events() {
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "alice", "t-2", "pw").await;
    for i in 1u64..=5 {
        append(&server, &auth, "s-5", json!({"i": i})).await;
    }
    let body = fetch_recent(&server, &auth, "s-5", 3).await;
    let recent = body["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 3, "expected 3 events, got: {recent:?}");
    let seqs: Vec<u64> = recent
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![3, 4, 5], "unexpected seqs: {seqs:?}");
}

/// `n: 0` must return an empty array even when the session has events.
#[tokio::test]
async fn n_zero_returns_empty() {
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "alice", "t-3", "pw").await;
    for i in 1u64..=3 {
        append(&server, &auth, "s-z", json!({"i": i})).await;
    }
    let body = fetch_recent(&server, &auth, "s-z", 0).await;
    assert_eq!(body["recent"], json!([]), "n=0 should return empty");
}

/// When `n` exceeds the number of stored events, all events are returned.
#[tokio::test]
async fn n_larger_than_session_returns_all() {
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "alice", "t-4", "pw").await;
    for i in 1u64..=2 {
        append(&server, &auth, "s-all", json!({"i": i})).await;
    }
    let body = fetch_recent(&server, &auth, "s-all", 100).await;
    let recent = body["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2, "expected all 2 events, got: {recent:?}");
    let seqs: Vec<u64> = recent
        .iter()
        .map(|e| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2]);
}

/// The session-keyed fetch_recent returns the same result as append-then-fetch.
#[tokio::test]
async fn session_keyed_route_works_with_auth() {
    let server = TestServer::start_with_basic_auth().await;
    let auth = seed_basic_user(&server, "alice", "t-5", "pw").await;
    append(&server, &auth, "s-keyed", json!({"seed": true})).await;
    append(&server, &auth, "s-keyed", json!({"second": true})).await;

    let body = fetch_recent(&server, &auth, "s-keyed", 10).await;
    let recent = body["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 2);
}
