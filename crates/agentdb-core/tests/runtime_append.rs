use agentdb_core::agent::NewAgent;
use agentdb_core::errors::ErrorCode;
use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;

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
    use agentdb_core::id::{Id, IdKind};
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
    use agentdb_core::id::{Id, IdKind};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_idempotent_appends_resolve_consistently() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    // 16 concurrent appends, all with idempotency key "k1" and identical payload
    // — every result should agree (same event_id+seq) and only one event should
    // actually be persisted in the session.
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let rt = rt.clone();
        let t = t.clone();
        let a = a.clone();
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = AppendRequest::simple(&t, &a, &s, "k", serde_json::json!({"v": 1}));
            req.idempotency_key = Some("k1".into());
            rt.append(req).await
        }));
    }
    let mut results = Vec::new();
    for h in tasks {
        results.push(h.await.unwrap().unwrap());
    }
    let first = &results[0];
    for r in &results {
        assert_eq!(r.event_id, first.event_id, "all idempotent appends must return the same event_id");
        assert_eq!(r.seq, first.seq, "all idempotent appends must return the same seq");
    }

    // Only one event was persisted in the session.
    let resp = rt.append_and_fetch(
        AppendRequest::simple(&t, &a, &s, "tail", serde_json::json!({})),
        /* last */ 100,
        /* max_bytes */ 1_000_000,
    ).await.unwrap();
    // 1 idempotent + 1 fresh "tail" = 2 events
    assert_eq!(
        resp.recent.len(),
        2,
        "expected exactly 1 idempotent commit + 1 tail append; got {:?}",
        resp.recent.iter().map(|e| (e.seq, &e.kind)).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn idempotent_conflict_on_kind_change() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    let mut r1 = AppendRequest::simple(&t, &a, &s, "kind_a", serde_json::json!({}));
    r1.idempotency_key = Some("k1".into());
    rt.append(r1).await.unwrap();

    let mut r2 = AppendRequest::simple(&t, &a, &s, "kind_b", serde_json::json!({}));
    r2.idempotency_key = Some("k1".into());
    let err = rt.append(r2).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::IdempotencyConflict);
}

#[tokio::test]
async fn idempotent_conflict_on_artifact_refs_change() {
    use agentdb_core::event::ArtifactRef;
    use agentdb_core::id::{Id, IdKind};

    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    let mut r1 = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}));
    r1.idempotency_key = Some("k1".into());
    rt.append(r1).await.unwrap();

    let mut r2 = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}));
    r2.artifact_refs = vec![ArtifactRef {
        artifact_id: Id::new(IdKind::Artifact),
        content_type: None,
        bytes: None,
        sha256: None,
    }];
    r2.idempotency_key = Some("k1".into());
    let err = rt.append(r2).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::IdempotencyConflict);
}

#[tokio::test]
async fn append_rejects_unknown_session() {
    use agentdb_core::id::{Id, IdKind};
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

#[tokio::test]
async fn lookup_event_by_id_works_in_memory() {
    // Symmetry with the KV-backed path: in-memory `event_by_id` must
    // resolve to (session_id, seq) after an append. Previously this
    // returned `Ok(None)` because the in-memory backend didn't track
    // the secondary index.
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    let r = rt
        .append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 1})))
        .await
        .unwrap();

    let lookup = rt
        .lookup_event_by_id(&t.tenant_id, &r.event_id)
        .await
        .unwrap();
    assert!(
        lookup.is_some(),
        "in-memory event_by_id lookup should resolve after append"
    );
    let (sid, seq) = lookup.unwrap();
    assert_eq!(sid.as_str(), s.session_id.as_str());
    assert_eq!(seq, 0);
}

#[tokio::test]
async fn lookup_event_by_id_rejects_cross_tenant_in_memory() {
    // Tenant scope: an event_id owned by tenant t1 must NOT resolve
    // when looked up under tenant t2. Returning `Some` would leak the
    // existence of cross-tenant events to a probing caller.
    let rt = Runtime::in_memory();
    let (t1, a, s) = rt.bootstrap_test_session().await;
    let r = rt
        .append(AppendRequest::simple(&t1, &a, &s, "x", serde_json::json!({})))
        .await
        .unwrap();
    let t2 = rt
        .create_tenant(NewTenant {
            external_key: "t2".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    let lookup = rt
        .lookup_event_by_id(&t2.tenant_id, &r.event_id)
        .await
        .unwrap();
    assert!(
        lookup.is_none(),
        "cross-tenant lookup must be None to avoid existence leak"
    );
}
