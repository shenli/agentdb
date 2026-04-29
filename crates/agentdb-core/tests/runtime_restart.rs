//! Restart-safety tests for the KV-backed V2 Runtime (Task 11).
//!
//! Each test opens a fresh `RocksKvStore` against a TempDir, drives some
//! traffic through `Runtime::with_kv`, drops the runtime, reopens the same
//! KV directory, and asserts the next runtime sees the prior state.

use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{KvStore, RocksKvStore};
use tempfile::TempDir;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    Arc::new(RocksKvStore::open(path).expect("open rocks kv"))
}

#[tokio::test]
async fn events_and_next_seq_survive_restart() {
    let dir = TempDir::new().unwrap();

    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv);
    let t = rt
        .create_tenant(NewTenant {
            external_key: "acme".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 1})))
        .await
        .unwrap();
    rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 2})))
        .await
        .unwrap();
    drop(rt);

    // Reopen the same on-disk KV path. RocksDB requires the prior handle to be
    // dropped first (single-writer lock), which happened above when `rt` (and
    // its captured Arc<KvStore>) went out of scope.
    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2);

    let events = rt2
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].seq, 1);

    // next_seq should be 2 after restart — confirmed by the seq assigned to a
    // fresh append being exactly 2.
    let r3 = rt2
        .append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 3})))
        .await
        .unwrap();
    assert_eq!(r3.seq, 2);
}

#[tokio::test]
async fn idempotency_records_survive_restart() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());

    let rt = Runtime::with_kv(kv);
    let (t, a, s) = rt.bootstrap_test_session().await;
    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 1}));
    req.idempotency_key = Some("k1".into());
    let r1 = rt.append(req.clone()).await.unwrap();
    drop(rt);

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2);
    let r2 = rt2.append(req).await.unwrap();
    assert_eq!(r1.event_id, r2.event_id);
    assert_eq!(r1.seq, r2.seq);

    // Only one event was actually persisted in the session.
    let events = rt2
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1, "idempotent retry must not create a second event");
    assert_eq!(events[0].seq, 0);
}

#[tokio::test]
async fn tenants_agents_and_sessions_hydrate_on_open() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv);
    let (t, a, s) = rt.bootstrap_test_session().await;
    drop(rt);

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2);

    // Append should work — proves tenant + agent + session were rehydrated.
    let r = rt2
        .append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({})))
        .await
        .unwrap();
    assert_eq!(r.seq, 0);
}

#[tokio::test]
async fn append_and_fetch_uses_kv_recent() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv);
    let (t, a, s) = rt.bootstrap_test_session().await;
    for i in 0..3_u64 {
        rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": i})))
            .await
            .unwrap();
    }
    let r = rt
        .append_and_fetch(
            AppendRequest::simple(&t, &a, &s, "tail", serde_json::json!({})),
            /* last */ 5,
            /* max_bytes */ 1_000_000,
        )
        .await
        .unwrap();
    assert_eq!(r.seq, 3);
    assert_eq!(r.recent.len(), 4);
    assert_eq!(r.recent[0].seq, 0);
    assert_eq!(r.recent[3].seq, 3);
    assert_eq!(r.recent[3].kind, "tail");
}

#[tokio::test]
async fn event_by_id_index_is_written_and_survives_restart() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv);
    let (t, a, s) = rt.bootstrap_test_session().await;
    let r = rt
        .append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 1})))
        .await
        .unwrap();
    drop(rt);

    // Reopen — the event_by_id row must still resolve to (session_id, seq).
    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2);
    let lookup = rt2
        .lookup_event_by_id(&t.tenant_id, &r.event_id)
        .await
        .unwrap();
    assert!(lookup.is_some(), "event_by_id row missing after restart");
    let (sid, seq) = lookup.unwrap();
    assert_eq!(sid.as_str(), s.session_id.as_str());
    assert_eq!(seq, 0);
}

#[tokio::test]
async fn fetch_with_wrong_tenant_id_fails() {
    use agentdb_core::errors::ErrorCode;
    use agentdb_core::id::{Id, IdKind};

    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv);
    let (_t, _a, s) = rt.bootstrap_test_session().await;
    let bogus_tenant = Id::new(IdKind::Tenant);
    let err = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: bogus_tenant,
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionNotFound);
}
