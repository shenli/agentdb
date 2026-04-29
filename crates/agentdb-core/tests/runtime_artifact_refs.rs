//! Validation tests for `Runtime::append`'s artifact_refs / payload-size
//! contract introduced in V2.1b.
//!
//! - artifact_refs that don't exist (or live under a different tenant) make
//!   the append fail with `ArtifactNotFound`, with no event committed.
//! - artifact_refs that resolve under the same tenant accept normally.
//! - inline payloads larger than `MAX_INLINE_EVENT_BYTES` (64 KB) reject with
//!   `EventTooLarge` and a `suggestion` pointing at artifacts.

use agentdb_core::artifact::NewArtifact;
use agentdb_core::errors::ErrorCode;
use agentdb_core::event::ArtifactRef;
use agentdb_core::id::{Id, IdKind};
use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::tenant::NewTenant;

#[tokio::test]
async fn append_rejects_unknown_artifact_ref() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let bogus = ArtifactRef {
        artifact_id: Id::new(IdKind::Artifact),
        content_type: None,
        bytes: None,
        sha256: None,
    };
    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}));
    req.artifact_refs = vec![bogus];

    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);

    // No event was committed: session next_seq is still 0.
    let events = rt
        .fetch_from_seq(agentdb_core::runtime::FetchRequest {
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
async fn append_accepts_valid_artifact_ref() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: None,
            },
            b"hi".to_vec(),
        )
        .await
        .unwrap();

    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({}));
    req.artifact_refs = vec![ArtifactRef {
        artifact_id: art.artifact_id.clone(),
        content_type: art.content_type.clone(),
        bytes: Some(art.bytes),
        sha256: Some(art.sha256.clone()),
    }];

    let resp = rt.append(req).await.unwrap();
    assert_eq!(resp.seq, 0);
}

#[tokio::test]
async fn append_rejects_cross_tenant_artifact_ref() {
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
    let art_in_t2 = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t2.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            b"hi".to_vec(),
        )
        .await
        .unwrap();

    let mut req = AppendRequest::simple(&t1, &a1, &s1, "x", serde_json::json!({}));
    req.artifact_refs = vec![ArtifactRef {
        artifact_id: art_in_t2.artifact_id,
        content_type: None,
        bytes: None,
        sha256: None,
    }];

    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);
}

#[tokio::test]
async fn append_rejects_oversized_inline_payload() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;

    // 64 KB is the V2.1 cap; 70 KB clears it with margin and stays small
    // enough not to inflate test memory.
    let big = "x".repeat(70_000);
    let req = AppendRequest::simple(
        &t,
        &a,
        &s,
        "x",
        serde_json::json!({ "big": big }),
    );
    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::EventTooLarge);
    assert!(
        err.suggestion.is_some(),
        "EventTooLarge should carry a 'use artifact_refs' suggestion"
    );
}
