use agentdb_core::errors::ErrorCode;
use agentdb_core::id::{Id, IdKind};
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};

#[tokio::test]
async fn fetch_from_seq_returns_ordered_events() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    for i in 0..5_u64 {
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "k",
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }

    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 2,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].seq, 2);
    assert_eq!(events[1].seq, 3);
    assert_eq!(events[2].seq, 4);
}

#[tokio::test]
async fn fetch_from_seq_respects_limit() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    for _ in 0..10 {
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "k",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    }
    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 4,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[3].seq, 3);
}

#[tokio::test]
async fn fetch_recent_returns_latest_events_in_order() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    for i in 0..6_u64 {
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "k",
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }

    let events = rt
        .fetch_recent(t.tenant_id.clone(), s.session_id.clone(), 3)
        .await
        .unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].seq, 3);
    assert_eq!(events[1].seq, 4);
    assert_eq!(events[2].seq, 5);
}

#[tokio::test]
async fn fetch_from_seq_empty_when_past_end() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    rt.append(AppendRequest::simple(
        &t,
        &a,
        &s,
        "k",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 100,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
async fn fetch_from_seq_rejects_unknown_session() {
    let rt = Runtime::in_memory();
    let (t, _, _) = rt.bootstrap_test_session().await;
    let err = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: Id::new(IdKind::Session),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionNotFound);
}
