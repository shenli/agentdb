use agentdb_core::event::{ArtifactRef, EventEnvelope, NewEvent, SourceRef};
use agentdb_core::id::{Id, IdKind};

#[test]
fn envelope_carries_required_fields() {
    let tenant = Id::new(IdKind::Tenant);
    let agent = Id::new(IdKind::Agent);
    let session = Id::new(IdKind::Session);
    let e = EventEnvelope::create(NewEvent {
        tenant_id: tenant.clone(),
        agent_id: agent.clone(),
        session_id: session.clone(),
        seq: 0,
        kind: "tool_result".into(),
        payload: serde_json::json!({"summary": "ok"}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: Some("client-1".into()),
        policy_version: 1,
    });
    assert!(e.event_id.as_str().starts_with("evt_"));
    assert_eq!(e.tenant_id, tenant);
    assert_eq!(e.agent_id, agent);
    assert_eq!(e.session_id, session);
    assert_eq!(e.seq, 0);
    assert_eq!(e.kind, "tool_result");
    assert_eq!(e.schema_version, agentdb_core::VERSION);
    assert_eq!(e.idempotency_key.as_deref(), Some("client-1"));
    assert_eq!(e.policy_version, 1);
    assert!(e.deleted_at_ms.is_none());
    assert!(e.created_at_ms > 0);
    assert!(e.artifact_refs.is_empty());
    assert!(e.source_refs.is_empty());
    assert_eq!(e.payload, serde_json::json!({"summary": "ok"}));
}

#[test]
fn envelope_round_trips_through_json() {
    let e = EventEnvelope::create(NewEvent {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        session_id: Id::new(IdKind::Session),
        seq: 7,
        kind: "skill_used".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
        policy_version: 1,
    });
    let s = serde_json::to_string(&e).unwrap();
    let back: EventEnvelope = serde_json::from_str(&s).unwrap();
    assert_eq!(back.event_id, e.event_id);
    assert_eq!(back.seq, 7);
    assert_eq!(back.kind, "skill_used");
    assert_eq!(back.idempotency_key, None);
    assert_eq!(back.schema_version, agentdb_core::VERSION);
}

#[test]
fn envelope_carries_artifact_and_source_refs() {
    let art = Id::new(IdKind::Artifact);
    let parent_session = Id::new(IdKind::Session);
    let e = EventEnvelope::create(NewEvent {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        session_id: Id::new(IdKind::Session),
        seq: 3,
        kind: "tool_result".into(),
        payload: serde_json::json!({"summary": "found leads"}),
        artifact_refs: vec![ArtifactRef {
            artifact_id: art.clone(),
            content_type: Some("application/jsonl".into()),
            bytes: Some(3_481_230),
            sha256: Some("deadbeef".into()),
        }],
        source_refs: vec![SourceRef {
            session_id: Some(parent_session.clone()),
            seq: Some(42),
            event_id: None,
            artifact_id: None,
            record_id: None,
            skill_id: None,
        }],
        idempotency_key: None,
        policy_version: 1,
    });
    assert_eq!(e.artifact_refs.len(), 1);
    assert_eq!(e.artifact_refs[0].artifact_id, art);
    assert_eq!(e.source_refs.len(), 1);
    assert_eq!(e.source_refs[0].session_id, Some(parent_session));
    assert_eq!(e.source_refs[0].seq, Some(42));
}

#[test]
fn idempotency_key_is_omitted_from_json_when_none() {
    let e = EventEnvelope::create(NewEvent {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        session_id: Id::new(IdKind::Session),
        seq: 0,
        kind: "x".into(),
        payload: serde_json::json!(null),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
        policy_version: 1,
    });
    let v = serde_json::to_value(&e).unwrap();
    assert!(v.get("idempotency_key").is_none(), "idempotency_key should be omitted when None; got {v}");
}
