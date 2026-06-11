use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::Runtime;
use agentdb_core::session::Lifecycle;
use agentdb_core::{NewAgent, NewSession, NewTenant, Tenant};

async fn tenant(rt: &Runtime, key: &str, priority: &str) -> Tenant {
    rt.create_tenant(NewTenant {
        external_key: key.into(),
        policy: serde_json::json!({
            "priority": priority,
            "lifecycle": {"cold_after_ms": 0},
            "retention": {"retention_ms": 0}
        }),
    })
    .await
    .unwrap()
}

async fn session(rt: &Runtime, tenant: &Tenant) -> agentdb_core::Session {
    let agent = rt
        .create_agent(NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    rt.create_session(NewSession {
        tenant_id: tenant.tenant_id.clone(),
        agent_id: agent.agent_id,
        alias: None,
    })
    .await
    .unwrap()
}

fn record(tenant: &Tenant) -> NewRecord {
    NewRecord {
        tenant_id: tenant.tenant_id.clone(),
        external_id: None,
        namespace: vec!["worker".into()],
        scope: serde_json::json!({}),
        text: Some("purge".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn policy_worker_schedules_highest_priority_tenants_first() {
    let rt = Runtime::in_memory();
    let low = tenant(&rt, "low", "low").await;
    let critical = tenant(&rt, "critical", "critical").await;
    let low_session = session(&rt, &low).await;
    let critical_session = session(&rt, &critical).await;

    let report = rt.run_policy_worker_once_at(u64::MAX, 1).await.unwrap();
    assert_eq!(report.tenants_considered, 2);
    assert_eq!(report.tenants_scheduled, 1);
    assert_eq!(report.tenants[0].tenant_id, critical.tenant_id);
    assert_eq!(report.lifecycle.sessions_transitioned, 1);

    assert_eq!(
        rt.get_session(&critical.tenant_id, &critical_session.session_id)
            .await
            .unwrap()
            .lifecycle,
        Lifecycle::Cold
    );
    assert_eq!(
        rt.get_session(&low.tenant_id, &low_session.session_id)
            .await
            .unwrap()
            .lifecycle,
        Lifecycle::Warm
    );
}

#[tokio::test]
async fn policy_worker_runs_lifecycle_and_gc_for_scheduled_tenants() {
    let rt = Runtime::in_memory();
    let high = tenant(&rt, "high", "high").await;
    session(&rt, &high).await;
    let rec = rt.write_record(record(&high)).await.unwrap();
    rt.delete_record(&high.tenant_id, &rec.record_id)
        .await
        .unwrap();

    let report = rt.run_policy_worker_once_at(u64::MAX, 10).await.unwrap();
    assert_eq!(report.tenants_considered, 1);
    assert_eq!(report.tenants_scheduled, 1);
    assert_eq!(report.lifecycle.sessions_transitioned, 1);
    assert_eq!(report.gc.records_purged, 1);
    assert_eq!(report.tenants[0].lifecycle.sessions_transitioned, 1);
    assert_eq!(report.tenants[0].gc.records_purged, 1);

    assert!(rt
        .get_record_with_options(&high.tenant_id, &rec.record_id, true)
        .await
        .is_err());
}
