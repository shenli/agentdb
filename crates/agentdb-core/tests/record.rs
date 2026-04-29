//! Unit tests for the V2.2 SearchableRecord runtime type.
//!
//! Runtime-level (write/get/search/delete) tests live in
//! `record_runtime.rs`; this file stays focused on shape, defaults, and
//! serde round-tripping.

use agentdb_core::event::SourceRef;
use agentdb_core::id::{Id, IdKind};
use agentdb_core::record::{IndexingMode, IndexingPolicy, NewRecord, SearchableRecord};

fn fresh_new_record() -> NewRecord {
    NewRecord {
        tenant_id: Id::new(IdKind::Tenant),
        namespace: vec!["agent".into(), "support".into()],
        scope: serde_json::json!({}),
        text: Some("hello world".into()),
        metadata: serde_json::json!({"title": "alpha"}),
        tags: vec!["greeting".into()],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    }
}

#[test]
fn create_assigns_typed_id_and_default_shape() {
    let r = SearchableRecord::create(fresh_new_record());

    assert!(r.record_id.as_str().starts_with("rec_"));
    assert_eq!(r.record_id.kind(), IdKind::Record);
    assert!(r.deleted_at_ms.is_none());
    assert!(r.expires_at_ms.is_none());
    assert!(r.created_at_ms > 0);
    assert_eq!(
        r.namespace,
        vec!["agent".to_string(), "support".to_string()]
    );
    assert_eq!(r.text.as_deref(), Some("hello world"));
}

#[test]
fn namespace_path_joins_with_slash() {
    let r = SearchableRecord::create(fresh_new_record());
    assert_eq!(r.namespace_path(), "agent/support");

    let mut req = fresh_new_record();
    req.namespace = vec![];
    let r2 = SearchableRecord::create(req);
    assert_eq!(r2.namespace_path(), "");

    let mut req = fresh_new_record();
    req.namespace = vec!["only".into()];
    let r3 = SearchableRecord::create(req);
    assert_eq!(r3.namespace_path(), "only");
}

#[test]
fn indexing_policy_default_is_metadata_only_no_vector() {
    let p = IndexingPolicy::default();
    assert_eq!(p.mode, IndexingMode::MetadataOnly);
    assert!(!p.vector_allowed);
    assert!(p.fields.is_empty());
}

#[test]
fn indexing_policy_selected_fields_round_trips() {
    let p = IndexingPolicy {
        mode: IndexingMode::SelectedFields,
        fields: vec!["title".into(), "body".into()],
        vector_allowed: true,
    };
    let s = serde_json::to_string(&p).unwrap();
    let back: IndexingPolicy = serde_json::from_str(&s).unwrap();
    assert_eq!(back, p);
    // Confirm the serde rename_all turned the mode into snake_case.
    assert!(s.contains("\"selected_fields\""));
}

#[test]
fn record_round_trips_through_serde_with_all_fields() {
    let mut req = fresh_new_record();
    req.embedding = Some(vec![0.1, 0.2, 0.3]);
    req.source = Some(SourceRef {
        session_id: None,
        seq: None,
        event_id: None,
        artifact_id: None,
        record_id: None,
        skill_id: None,
    });
    req.expires_at_ms = Some(9_999_999_999_999);
    req.policy = IndexingPolicy {
        mode: IndexingMode::SummaryOnly,
        fields: vec![],
        vector_allowed: true,
    };
    let r = SearchableRecord::create(req);

    let s = serde_json::to_string(&r).unwrap();
    let back: SearchableRecord = serde_json::from_str(&s).unwrap();
    assert_eq!(back, r);
}

#[test]
fn is_expired_reflects_expires_at_ms() {
    let mut req = fresh_new_record();
    req.expires_at_ms = Some(0);
    let r = SearchableRecord::create(req);
    assert!(r.is_expired(), "expires_at_ms = 0 must be in the past");

    let mut req = fresh_new_record();
    req.expires_at_ms = Some(u64::MAX);
    let r = SearchableRecord::create(req);
    assert!(!r.is_expired(), "expires_at_ms = u64::MAX is in the future");

    let r = SearchableRecord::create(fresh_new_record());
    assert!(!r.is_expired(), "no expiration => not expired");
}
