//! Bulk append (`Runtime::append_batch`): many ordered events to one session
//! committed in a single write batch — the session-log import path.

use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::{NewAgent, NewSession, NewTenant};
use agentdb_storage::{InMemoryContentBackend, InMemoryKvStore};
use serde_json::json;
use std::sync::Arc;

async fn seed() -> (
    Arc<Runtime>,
    agentdb_core::Tenant,
    agentdb_core::Agent,
    agentdb_core::Session,
) {
    let rt = Runtime::with_kv(
        Arc::new(InMemoryKvStore::new()),
        Arc::new(InMemoryContentBackend::default()),
    );
    let t = rt
        .create_tenant(NewTenant {
            external_key: "batch".into(),
            policy: json!({}),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: json!({}),
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
    (rt, t, a, s)
}

#[tokio::test]
async fn append_batch_assigns_contiguous_ordered_seqs() {
    let (rt, t, a, s) = seed().await;

    let reqs: Vec<AppendRequest> = (0..10)
        .map(|i| AppendRequest::simple(&t, &a, &s, "msg", json!({ "i": i })))
        .collect();
    let resp = rt.append_batch(reqs).await.unwrap();
    assert_eq!(resp.len(), 10);
    for (i, r) in resp.iter().enumerate() {
        assert_eq!(r.seq, i as u64, "seqs are contiguous from 0");
    }

    // Fetched back in order, payloads intact.
    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 10);
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e.seq, i as u64);
        assert_eq!(e.payload["i"], i);
    }

    // A second batch continues the same seq sequence.
    let more = rt
        .append_batch(vec![AppendRequest::simple(
            &t,
            &a,
            &s,
            "msg",
            json!({ "i": 10 }),
        )])
        .await
        .unwrap();
    assert_eq!(more[0].seq, 10);

    // An empty batch is a no-op.
    assert!(rt.append_batch(Vec::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn append_batch_rejects_mixed_session_and_refs() {
    let (rt, t, a, s) = seed().await;
    let s2 = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    // Two different sessions in one batch → rejected.
    let mixed = vec![
        AppendRequest::simple(&t, &a, &s, "m", json!({})),
        AppendRequest::simple(&t, &a, &s2, "m", json!({})),
    ];
    assert!(rt.append_batch(mixed).await.is_err());

    // A per-event idempotency key is not supported on the batch path.
    let mut with_idem = AppendRequest::simple(&t, &a, &s, "m", json!({}));
    with_idem.idempotency_key = Some("k".into());
    assert!(rt.append_batch(vec![with_idem]).await.is_err());
}
