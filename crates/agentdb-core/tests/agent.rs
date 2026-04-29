use agentdb_core::agent::{Agent, NewAgent};
use agentdb_core::id::{Id, IdKind};

#[test]
fn agent_belongs_to_tenant_and_has_alias() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Agent::create(NewAgent {
        tenant_id: tenant.clone(),
        alias: Some("sales-rep".into()),
        config: serde_json::json!({"model": "claude-sonnet-4.5"}),
    });
    assert!(a.agent_id.as_str().starts_with("agt_"));
    assert_eq!(a.tenant_id, tenant);
    assert_eq!(a.alias.as_deref(), Some("sales-rep"));
    assert!(a.deleted_at_ms.is_none());
    assert!(a.created_at_ms > 0);
}

#[test]
fn agent_without_alias_is_allowed() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Agent::create(NewAgent {
        tenant_id: tenant,
        alias: None,
        config: serde_json::json!({}),
    });
    assert!(a.alias.is_none());
}

#[test]
fn agent_round_trips_through_serde() {
    let tenant = Id::new(IdKind::Tenant);
    let a = Agent::create(NewAgent {
        tenant_id: tenant,
        alias: Some("rep-1".into()),
        config: serde_json::json!({"k": 1}),
    });
    let s = serde_json::to_string(&a).unwrap();
    let parsed: Agent = serde_json::from_str(&s).unwrap();
    assert_eq!(parsed.agent_id, a.agent_id);
    assert_eq!(parsed.tenant_id, a.tenant_id);
    assert_eq!(parsed.alias, a.alias);
}
