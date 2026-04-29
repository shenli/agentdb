use agentdb_core::v2::agent::NewAgent;
use agentdb_core::v2::errors::ErrorCode;
use agentdb_core::v2::runtime::{AppendRequest, Runtime};
use agentdb_core::v2::session::NewSession;
use agentdb_core::v2::tenant::NewTenant;

#[tokio::test]
async fn append_assigns_contiguous_seqs() {
    let rt = Runtime::in_memory();
    let t = rt.create_tenant(NewTenant {
        external_key: "acme".into(),
        policy: serde_json::json!({}),
    }).await.unwrap();
    let a = rt.create_agent(NewAgent {
        tenant_id: t.tenant_id.clone(),
        alias: None,
        config: serde_json::json!({}),
    }).await.unwrap();
    let s = rt.create_session(NewSession {
        tenant_id: t.tenant_id.clone(),
        agent_id: a.agent_id.clone(),
        alias: None,
    }).await.unwrap();

    let r1 = rt.append(AppendRequest::simple(&t, &a, &s, "ping", serde_json::json!({"i": 1}))).await.unwrap();
    assert_eq!(r1.seq, 0);

    let r2 = rt.append(AppendRequest::simple(&t, &a, &s, "ping", serde_json::json!({"i": 2}))).await.unwrap();
    assert_eq!(r2.seq, 1);

    assert_ne!(r1.event_id, r2.event_id);
}

#[tokio::test]
async fn append_and_fetch_returns_recent() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}))).await.unwrap();
    rt.append(AppendRequest::simple(&t, &a, &s, "y", serde_json::json!({}))).await.unwrap();

    let r3 = rt.append_and_fetch(
        AppendRequest::simple(&t, &a, &s, "z", serde_json::json!({})),
        /* last */ 5,
        /* max_bytes */ 1_000_000,
    ).await.unwrap();
    assert_eq!(r3.seq, 2);
    assert_eq!(r3.recent.len(), 3);
    assert_eq!(r3.recent[0].seq, 0);
    assert_eq!(r3.recent[2].seq, 2);
    assert_eq!(r3.recent.last().unwrap().kind, "z");
}

#[tokio::test]
async fn idempotent_append_returns_original_result() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 1}));
    req.idempotency_key = Some("k1".into());

    let r1 = rt.append(req.clone()).await.unwrap();
    let r2 = rt.append(req).await.unwrap();
    assert_eq!(r1.seq, r2.seq);
    assert_eq!(r1.event_id, r2.event_id);

    // Only ONE event was actually persisted.
    let recent = rt.append_and_fetch(
        AppendRequest::simple(&t, &a, &s, "y", serde_json::json!({})),
        /* last */ 10, 1_000_000,
    ).await.unwrap().recent;
    assert_eq!(recent.len(), 2, "expected 1 idempotent event + 1 fresh, got: {recent:?}");
}

#[tokio::test]
async fn idempotent_conflict_on_payload_change() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let mut req1 = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 1}));
    req1.idempotency_key = Some("k1".into());
    rt.append(req1).await.unwrap();

    let mut req2 = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 2}));
    req2.idempotency_key = Some("k1".into());
    let err = rt.append(req2).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::IdempotencyConflict);
}

#[tokio::test]
async fn create_agent_rejects_unknown_tenant() {
    use agentdb_core::v2::id::{Id, IdKind};
    let rt = Runtime::in_memory();
    let err = rt.create_agent(NewAgent {
        tenant_id: Id::new(IdKind::Tenant),
        alias: None,
        config: serde_json::json!({}),
    }).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::TenantNotFound);
}

#[tokio::test]
async fn create_session_rejects_unknown_agent() {
    use agentdb_core::v2::id::{Id, IdKind};
    let rt = Runtime::in_memory();
    let t = rt.create_tenant(NewTenant {
        external_key: "x".into(),
        policy: serde_json::json!({}),
    }).await.unwrap();
    let err = rt.create_session(NewSession {
        tenant_id: t.tenant_id,
        agent_id: Id::new(IdKind::Agent),
        alias: None,
    }).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::AgentNotFound);
}

#[tokio::test]
async fn append_rejects_unknown_session() {
    use agentdb_core::v2::id::{Id, IdKind};
    let rt = Runtime::in_memory();
    let (t, a, _real_session) = rt.bootstrap_test_session().await;
    let bogus = Id::new(IdKind::Session);
    let req = AppendRequest {
        tenant_id: t.tenant_id.clone(),
        agent_id: a.agent_id.clone(),
        session_id: bogus,
        kind: "x".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
    };
    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionNotFound);
}
