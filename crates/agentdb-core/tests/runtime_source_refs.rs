//! Validation tests for `Runtime::append`'s `source_refs.record_id` contract
//! introduced in V2.2b.
//!
//! - `source_refs[i].record_id` that don't exist (or live under a different
//!   tenant) make the append fail with `RecordNotFound`, with no event
//!   committed.
//! - `source_refs[i].record_id` that resolves to a live record under the same
//!   tenant accepts normally.
//! - Other `SourceRef` variants (artifact_id / session_id+seq / event_id /
//!   skill_id) are intentionally NOT validated in V2.2 — they're provenance
//!   hints; see the comment in `runtime.rs` for the full rationale.

use agentdb_core::errors::ErrorCode;
use agentdb_core::event::SourceRef;
use agentdb_core::id::{Id, IdKind};
use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::tenant::NewTenant;

fn empty_source_ref() -> SourceRef {
    SourceRef {
        session_id: None,
        seq: None,
        event_id: None,
        artifact_id: None,
        record_id: None,
    }
}

fn default_new_record(tenant_id: Id) -> NewRecord {
    NewRecord {
        tenant_id,
        external_id: None,
        namespace: vec!["test".into()],
        scope: serde_json::json!({}),
        text: Some("hello".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn append_accepts_source_ref_pointing_to_real_record() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let rec = rt
        .write_record(default_new_record(t.tenant_id.clone()))
        .await
        .unwrap();

    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}));
    req.source_refs = vec![SourceRef {
        record_id: Some(rec.record_id.clone()),
        ..empty_source_ref()
    }];

    let resp = rt.append(req).await.unwrap();
    assert_eq!(resp.seq, 0);
}

#[tokio::test]
async fn append_rejects_source_ref_to_missing_record() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let bogus = Id::new(IdKind::Record);
    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}));
    req.source_refs = vec![SourceRef {
        record_id: Some(bogus),
        ..empty_source_ref()
    }];

    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);

    // No event was committed: session next_seq is still 0.
    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
async fn append_rejects_source_ref_to_cross_tenant_record() {
    let rt = Runtime::in_memory();
    let (t1, a1, s1) = rt.bootstrap_test_session().await;

    // bootstrap_test_session uses external_key="test"; pick a different one
    // for the second tenant so the alias-uniqueness check doesn't fire.
    let t2 = rt
        .create_tenant(NewTenant {
            external_key: "t2".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let rec_in_t2 = rt
        .write_record(default_new_record(t2.tenant_id.clone()))
        .await
        .unwrap();

    let mut req = AppendRequest::simple(&t1, &a1, &s1, "x", serde_json::json!({}));
    req.source_refs = vec![SourceRef {
        record_id: Some(rec_in_t2.record_id),
        ..empty_source_ref()
    }];

    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);
}

#[tokio::test]
async fn append_ignores_source_ref_with_other_variants_set() {
    // V2.2 only validates source_refs.record_id. Filling in only
    // session_id+seq (or any other variant) must NOT fail the append even
    // when those refs point at nonsense — they're provenance hints.
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let bogus_session = Id::new(IdKind::Session);
    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}));
    req.source_refs = vec![SourceRef {
        session_id: Some(bogus_session),
        seq: Some(99),
        ..empty_source_ref()
    }];

    let resp = rt.append(req).await.unwrap();
    assert_eq!(resp.seq, 0);
}
