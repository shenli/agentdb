//! Runtime-level tests for the V2.1 artifact API.
//!
//! Covers create/get/get_blob, sha256 verification, oversize rejection,
//! cross-tenant isolation, and not-found error mapping. Restart safety lives
//! in `runtime_restart.rs` so this file stays focused on the in-memory
//! Runtime path.

use agentdb_core::artifact::NewArtifact;
use agentdb_core::errors::ErrorCode;
use agentdb_core::id::{Id, IdKind};
use agentdb_core::runtime::Runtime;
use agentdb_core::tenant::NewTenant;

async fn fresh_tenant(rt: &Runtime, key: &str) -> agentdb_core::tenant::Tenant {
    rt.create_tenant(NewTenant {
        external_key: key.into(),
        policy: serde_json::json!({}),
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn create_artifact_round_trip() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let bytes = b"hello world".to_vec();
    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: None,
            },
            bytes.clone(),
        )
        .await
        .unwrap();

    assert!(art.artifact_id.as_str().starts_with("art_"));
    assert_eq!(art.bytes, 11);
    assert_eq!(art.tenant_id.as_str(), t.tenant_id.as_str());
    assert_eq!(art.content_type.as_deref(), Some("text/plain"));
    assert!(art.deleted_at_ms.is_none());
    assert!(art.raw_purged_at_ms.is_none());
    // sha256("hello world")
    assert_eq!(
        art.sha256,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );

    let got = rt
        .get_artifact(&t.tenant_id, &art.artifact_id)
        .await
        .unwrap();
    assert_eq!(got.artifact_id.as_str(), art.artifact_id.as_str());

    let blob = rt
        .get_artifact_blob(&t.tenant_id, &art.artifact_id)
        .await
        .unwrap();
    assert_eq!(blob, bytes);

    let report = rt
        .verify_artifact_lifecycle_for_tenant(&t.tenant_id)
        .await
        .unwrap();
    assert!(report.ok());
    assert_eq!(report.artifacts_checked, 1);
    assert_eq!(report.raw_present, 1);
    assert_eq!(report.raw_purged, 0);
    assert_eq!(report.issues.len(), 0);
}

#[tokio::test]
async fn artifact_lifecycle_verification_reports_derivative_only_purge() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "purge".into(),
            policy: serde_json::json!({
                "lifecycle": {"derivative_only_after_ms": 0}
            }),
        })
        .await
        .unwrap();
    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: None,
            },
            b"raw bytes".to_vec(),
        )
        .await
        .unwrap();

    let sweep = rt
        .sweep_lifecycle_for_tenant_at(&t.tenant_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(sweep.artifacts_raw_purged, 1);

    let blob_err = rt
        .get_artifact_blob(&t.tenant_id, &art.artifact_id)
        .await
        .unwrap_err();
    assert_eq!(blob_err.code, ErrorCode::ArtifactNotFound);

    let report = rt
        .verify_artifact_lifecycle_for_tenant(&t.tenant_id)
        .await
        .unwrap();
    assert!(report.ok());
    assert_eq!(report.artifacts_checked, 1);
    assert_eq!(report.raw_present, 0);
    assert_eq!(report.raw_purged, 1);
    assert_eq!(report.issues.len(), 0);
}

#[tokio::test]
async fn create_artifact_rejects_oversized() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let bytes = vec![0u8; 17 * 1024 * 1024]; // > 16 MB cap
    let err = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            bytes,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactTooLarge);
}

#[tokio::test]
async fn create_artifact_accepts_matching_sha256() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let bytes = b"hello".to_vec();
    // sha256("hello")
    let expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: None,
                expected_sha256: Some(expected.into()),
            },
            bytes,
        )
        .await
        .unwrap();
    assert_eq!(art.sha256, expected);
}

#[tokio::test]
async fn create_artifact_rejects_mismatched_sha256() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let bytes = b"hello".to_vec();
    let err = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: None,
                expected_sha256: Some(
                    "0000000000000000000000000000000000000000000000000000000000000000".into(),
                ),
            },
            bytes,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::HashMismatch);
}

#[tokio::test]
async fn create_artifact_rejects_unknown_tenant() {
    let rt = Runtime::in_memory();
    let bogus = Id::new(IdKind::Tenant);
    let err = rt
        .create_artifact(
            NewArtifact {
                tenant_id: bogus,
                content_type: None,
                expected_sha256: None,
            },
            b"x".to_vec(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::TenantNotFound);
}

#[tokio::test]
async fn get_artifact_rejects_cross_tenant() {
    let rt = Runtime::in_memory();
    let t1 = fresh_tenant(&rt, "t1").await;
    let t2 = fresh_tenant(&rt, "t2").await;
    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t1.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            b"x".to_vec(),
        )
        .await
        .unwrap();
    let err = rt
        .get_artifact(&t2.tenant_id, &art.artifact_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);

    // Cross-tenant blob access fails the same way — proves the tenant
    // check happens before the content backend is touched.
    let err = rt
        .get_artifact_blob(&t2.tenant_id, &art.artifact_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);
}

#[tokio::test]
async fn get_artifact_returns_not_found_for_missing() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let err = rt
        .get_artifact(&t.tenant_id, &Id::new(IdKind::Artifact))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);
}

#[tokio::test]
async fn get_artifact_blob_returns_not_found_for_missing() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let err = rt
        .get_artifact_blob(&t.tenant_id, &Id::new(IdKind::Artifact))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);
}
