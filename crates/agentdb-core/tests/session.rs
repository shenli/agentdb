use agentdb_core::id::{Id, IdKind};
use agentdb_core::session::{Lifecycle, NewSession, Session, Status};

#[test]
fn session_starts_active_warm_with_seq_zero() {
    let tenant = Id::new(IdKind::Tenant);
    let agent = Id::new(IdKind::Agent);
    let s = Session::create(NewSession {
        tenant_id: tenant.clone(),
        agent_id: agent.clone(),
        alias: Some("lead-followup-123".into()),
    });
    assert!(s.session_id.as_str().starts_with("ses_"));
    assert_eq!(s.tenant_id, tenant);
    assert_eq!(s.agent_id, agent);
    assert_eq!(s.next_seq, 0);
    assert_eq!(s.status, Status::Active);
    assert_eq!(s.lifecycle, Lifecycle::Warm);
    assert!(s.forked_from.is_none());
    assert_eq!(s.policy_version, 1);
    assert!(s.deleted_at_ms.is_none());
    assert!(s.created_at_ms > 0);
    assert_eq!(s.last_active_at_ms, s.created_at_ms);
}

#[test]
fn session_without_alias_is_allowed() {
    let s = Session::create(NewSession {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        alias: None,
    });
    assert!(s.alias.is_none());
}

#[test]
fn session_round_trips_through_serde() {
    let s = Session::create(NewSession {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        alias: Some("x".into()),
    });
    let j = serde_json::to_string(&s).unwrap();
    let parsed: Session = serde_json::from_str(&j).unwrap();
    assert_eq!(parsed.session_id, s.session_id);
    assert_eq!(parsed.tenant_id, s.tenant_id);
    assert_eq!(parsed.agent_id, s.agent_id);
    assert_eq!(parsed.next_seq, 0);
    assert_eq!(parsed.status, Status::Active);
    assert_eq!(parsed.lifecycle, Lifecycle::Warm);
}

#[test]
fn status_and_lifecycle_serialize_lowercase() {
    let s = Session::create(NewSession {
        tenant_id: Id::new(IdKind::Tenant),
        agent_id: Id::new(IdKind::Agent),
        alias: None,
    });
    let v = serde_json::to_value(&s).unwrap();
    assert_eq!(v["status"], "active");
    assert_eq!(v["lifecycle"], "warm");
}
