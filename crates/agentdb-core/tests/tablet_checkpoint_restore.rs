use std::{collections::BTreeMap, sync::Arc};

use agentdb_core::{
    runtime::{AppendRequest, FetchRequest, Runtime},
    NewAgent, NewSession, NewTenant, RUNTIME_PRIMARY_TABLET_ID,
};
use agentdb_storage::{
    GroupedRocksDbTabletStore, InMemoryContentBackend, InMemoryKvStore, ReplicatedLog,
    SingleNodeReplicatedLog, TabletStoreLifecycle,
};

#[tokio::test]
async fn restored_tablet_replays_log_tail_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let log: Arc<dyn ReplicatedLog + Send + Sync> = Arc::new(SingleNodeReplicatedLog::new(
        Arc::new(InMemoryKvStore::new()),
    ));
    let source_lifecycle = Arc::new(GroupedRocksDbTabletStore::new(
        dir.path().join("source"),
        1,
        8,
    ));
    let source = Runtime::with_tablet_store_lifecycle_and_replicated_logs(
        source_lifecycle,
        Arc::new(InMemoryContentBackend::default()),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log.clone())]),
    );

    let (tenant, agent, session) = source
        .with_tablet_context(RUNTIME_PRIMARY_TABLET_ID, async {
            let tenant = source
                .create_tenant(NewTenant {
                    external_key: "restore-tail".into(),
                    policy: serde_json::json!({}),
                })
                .await
                .unwrap();
            let agent = source
                .create_agent(NewAgent {
                    tenant_id: tenant.tenant_id.clone(),
                    alias: Some("agent".into()),
                    config: serde_json::json!({}),
                })
                .await
                .unwrap();
            let session = source
                .create_session(NewSession {
                    tenant_id: tenant.tenant_id.clone(),
                    agent_id: agent.agent_id.clone(),
                    alias: Some("session".into()),
                })
                .await
                .unwrap();
            source
                .append(AppendRequest::simple(
                    &tenant,
                    &agent,
                    &session,
                    "before-checkpoint",
                    serde_json::json!({"i": 1}),
                ))
                .await
                .unwrap();
            (tenant, agent, session)
        })
        .await;

    let checkpoint_dir = dir.path().join("checkpoint");
    let manifest = source
        .checkpoint_tablet_store(RUNTIME_PRIMARY_TABLET_ID, &checkpoint_dir)
        .unwrap();
    assert!(manifest.latest_applied_index > 0);

    source
        .with_tablet_context(RUNTIME_PRIMARY_TABLET_ID, async {
            source
                .append(AppendRequest::simple(
                    &tenant,
                    &agent,
                    &session,
                    "after-checkpoint",
                    serde_json::json!({"i": 2}),
                ))
                .await
        })
        .await
        .unwrap();

    let target_lifecycle = Arc::new(GroupedRocksDbTabletStore::new(
        dir.path().join("target"),
        1,
        8,
    ));
    target_lifecycle
        .restore_tablet_checkpoint(RUNTIME_PRIMARY_TABLET_ID, &checkpoint_dir)
        .unwrap();
    let restored = Runtime::with_tablet_store_lifecycle_and_replicated_logs(
        target_lifecycle,
        Arc::new(InMemoryContentBackend::default()),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]),
    );

    let events = restored
        .with_tablet_context(RUNTIME_PRIMARY_TABLET_ID, async {
            restored
                .fetch_from_seq(FetchRequest {
                    tenant_id: tenant.tenant_id.clone(),
                    session_id: session.session_id.clone(),
                    from_seq: 0,
                    limit: 10,
                })
                .await
        })
        .await
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, "before-checkpoint");
    assert_eq!(events[1].kind, "after-checkpoint");
}
