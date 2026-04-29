//! Pure-data tests for the V2.1 `Artifact` runtime type.
//!
//! Runtime-level tests (create/get/get_blob, persistence, cross-tenant
//! isolation) live in `artifact_runtime.rs`. This file pins the shape of
//! the value object itself: id prefix, default soft-delete fields, and
//! serde round-trip behavior.

use agentdb_core::artifact::{Artifact, NewArtifact};
use agentdb_core::id::{Id, IdKind};
use agentdb_storage::content::PutResult;

fn sample_new(tenant_id: Id, ct: Option<&str>) -> NewArtifact {
    NewArtifact {
        tenant_id,
        content_type: ct.map(str::to_string),
        expected_sha256: None,
    }
}

#[test]
fn from_put_populates_expected_fields() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Artifact::from_put(
        sample_new(tenant.clone(), Some("text/plain")),
        PutResult {
            bytes: 11,
            sha256_hex: "deadbeef".into(),
        },
    );
    assert_eq!(a.tenant_id.as_str(), tenant.as_str());
    assert!(a.artifact_id.as_str().starts_with("art_"));
    assert_eq!(a.bytes, 11);
    assert_eq!(a.sha256, "deadbeef");
    assert_eq!(a.content_type.as_deref(), Some("text/plain"));
    assert!(a.deleted_at_ms.is_none());
    assert!(a.raw_purged_at_ms.is_none());
    assert!(a.created_at_ms > 0);
}

#[test]
fn serde_round_trip_preserves_all_fields() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Artifact::from_put(
        sample_new(tenant, Some("application/json")),
        PutResult {
            bytes: 5,
            sha256_hex: "abc123".into(),
        },
    );
    let s = serde_json::to_string(&a).unwrap();
    let b: Artifact = serde_json::from_str(&s).unwrap();
    assert_eq!(b.artifact_id.as_str(), a.artifact_id.as_str());
    assert_eq!(b.tenant_id.as_str(), a.tenant_id.as_str());
    assert_eq!(b.content_type, a.content_type);
    assert_eq!(b.bytes, a.bytes);
    assert_eq!(b.sha256, a.sha256);
    assert_eq!(b.created_at_ms, a.created_at_ms);
    assert_eq!(b.deleted_at_ms, a.deleted_at_ms);
    assert_eq!(b.raw_purged_at_ms, a.raw_purged_at_ms);
}

#[test]
fn serde_omits_none_content_type() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Artifact::from_put(
        sample_new(tenant, None),
        PutResult {
            bytes: 0,
            sha256_hex: "".into(),
        },
    );
    let s = serde_json::to_string(&a).unwrap();
    assert!(
        !s.contains("content_type"),
        "content_type=None should be omitted; got: {s}"
    );
}
