use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{
    AppendRequest, ContextInclude, ContextRequest, ForkSessionRequest, Runtime,
    RuntimeRouteContext, SearchRequest, WatchRequest, WatchScope,
};
use agentdb_core::{NewAgent, NewSession, NewTenant, RUNTIME_PRIMARY_TABLET_ID};
use agentdb_storage::{
    InMemoryContentBackend, InMemoryKvStore, PrefixScopedTabletStore, ReplicatedLog,
    SingleNodeReplicatedLog,
};
use std::{collections::BTreeMap, sync::Arc};

async fn seed() -> (
    Arc<Runtime>,
    agentdb_core::Tenant,
    agentdb_core::Agent,
    agentdb_core::Session,
) {
    let rt = Runtime::in_memory();
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "v25".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let agent = rt
        .create_agent(NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let session = rt
        .create_session(NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: agent.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();
    (rt, tenant, agent, session)
}

#[tokio::test]
async fn forked_session_replay_merges_parent_cutoff_then_child_events() {
    let (rt, tenant, agent, parent) = seed().await;
    for n in 0..3 {
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &parent,
            "parent",
            serde_json::json!({"n": n}),
        ))
        .await
        .unwrap();
    }

    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 1,
            alias: Some("child".into()),
        })
        .await
        .unwrap();
    assert_eq!(child.next_seq, 0);
    assert_eq!(child.forked_from.as_ref().unwrap().at_seq, 1);

    rt.append(AppendRequest {
        tenant_id: tenant.tenant_id.clone(),
        agent_id: agent.agent_id.clone(),
        session_id: child.session_id.clone(),
        kind: "child".into(),
        payload: serde_json::json!({"n": 10}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
    })
    .await
    .unwrap();

    let replay = rt
        .replay_session(&tenant.tenant_id, &child.session_id)
        .await
        .unwrap();
    let seqs: Vec<_> = replay
        .events
        .iter()
        .map(|event| (event.kind.as_str(), event.seq))
        .collect();
    assert_eq!(seqs, vec![("parent", 0), ("parent", 1), ("child", 0)]);
}

#[tokio::test]
async fn context_builder_respects_includes_and_byte_budget() {
    let (rt, tenant, agent, session) = seed().await;
    for n in 0..3 {
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"text": format!("event {n}")}),
        ))
        .await
        .unwrap();
    }
    rt.write_record(NewRecord {
        tenant_id: tenant.tenant_id.clone(),
        namespace: vec!["mem".into()],
        scope: serde_json::json!({}),
        text: Some("renewal memory".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy {
            mode: agentdb_core::record::IndexingMode::SummaryOnly,
            fields: vec![],
            vector_allowed: false,
        },
        expires_at_ms: None,
    })
    .await
    .unwrap();

    let context = rt
        .build_context(ContextRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            max_events: 3,
            max_bytes: 8_000,
            include: vec![
                ContextInclude::RecentEvents,
                ContextInclude::Records,
                ContextInclude::Policy,
            ],
            record_query: Some(SearchRequest::text_only(
                tenant.tenant_id.clone(),
                vec!["mem".into()],
                "renewal",
                10,
            )),
        })
        .await
        .unwrap();
    assert_eq!(context.events.len(), 3);
    assert_eq!(context.records.len(), 1);
    assert!(context.policy.is_some());
    assert!(!context.truncated);

    let tiny = rt
        .build_context(ContextRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            max_events: 10,
            max_bytes: 1,
            include: vec![ContextInclude::RecentEvents],
            record_query: None,
        })
        .await
        .unwrap();
    assert!(tiny.events.is_empty());
    assert!(tiny.truncated);
}

#[tokio::test]
async fn tenant_and_agent_watch_return_ordered_cursors_without_duplicates() {
    let (rt, tenant, agent, first) = seed().await;
    let second = rt
        .create_session(NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: agent.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();
    rt.append(AppendRequest::simple(
        &tenant,
        &agent,
        &first,
        "first",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    rt.append(AppendRequest::simple(
        &tenant,
        &agent,
        &second,
        "second",
        serde_json::json!({}),
    ))
    .await
    .unwrap();

    let tenant_items = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            from_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(tenant_items.len(), 2);

    let after_first = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Agent,
            session_id: None,
            agent_id: Some(agent.agent_id.clone()),
            from_cursor: Some(tenant_items[0].cursor.clone()),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(after_first.len(), 1);
    assert_eq!(after_first[0].event.kind, "second");
}

#[tokio::test]
async fn watch_cursor_includes_tablet_context_and_applied_index() {
    let kv = Arc::new(InMemoryKvStore::new());
    let lifecycle = Arc::new(PrefixScopedTabletStore::new(kv.clone()));
    let log: Arc<dyn ReplicatedLog + Send + Sync> = Arc::new(SingleNodeReplicatedLog::new(
        Arc::new(InMemoryKvStore::new()),
    ));
    let rt = Runtime::with_tablet_store_lifecycle_and_replicated_logs(
        lifecycle,
        Arc::new(InMemoryContentBackend::default()),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]),
    );

    let (tenant, agent, session) = rt
        .with_tablet_context(RUNTIME_PRIMARY_TABLET_ID, async {
            let tenant = rt
                .create_tenant(NewTenant {
                    external_key: "tablet-watch".into(),
                    policy: serde_json::json!({}),
                })
                .await
                .unwrap();
            let agent = rt
                .create_agent(NewAgent {
                    tenant_id: tenant.tenant_id.clone(),
                    alias: Some("watcher".into()),
                    config: serde_json::json!({}),
                })
                .await
                .unwrap();
            let session = rt
                .create_session(NewSession {
                    tenant_id: tenant.tenant_id.clone(),
                    agent_id: agent.agent_id.clone(),
                    alias: Some("watch-session".into()),
                })
                .await
                .unwrap();
            rt.append(AppendRequest::simple(
                &tenant,
                &agent,
                &session,
                "observed",
                serde_json::json!({}),
            ))
            .await
            .unwrap();
            (tenant, agent, session)
        })
        .await;

    let route_context = RuntimeRouteContext {
        tablet_id: Some(RUNTIME_PRIMARY_TABLET_ID),
        cell_id: Some("cell-a".into()),
        placement_scope_id: Some("tenant/tablet-watch".into()),
        tablet_epoch: Some(9),
    };
    let items = rt
        .with_route_context(route_context, async {
            rt.watch(WatchRequest {
                tenant_id: tenant.tenant_id.clone(),
                scope: WatchScope::Session,
                session_id: Some(session.session_id.clone()),
                agent_id: Some(agent.agent_id.clone()),
                from_cursor: None,
                limit: 10,
            })
            .await
        })
        .await
        .unwrap();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].cell_id.as_deref(), Some("cell-a"));
    assert_eq!(
        items[0].placement_scope_id.as_deref(),
        Some("tenant/tablet-watch")
    );
    assert_eq!(items[0].tablet_id, Some(RUNTIME_PRIMARY_TABLET_ID));
    assert_eq!(items[0].tablet_epoch, Some(9));
    assert!(items[0].applied_index.unwrap_or_default() > 0);
    assert!(items[0].cursor.starts_with("v2:"));
    assert!(items[0].cursor.contains("cell=cell-a"));
    assert!(items[0].cursor.contains("scope=tenant/tablet-watch"));
    assert!(items[0].cursor.contains("tablet=1"));
    assert!(items[0].cursor.contains("epoch=9"));
    assert!(items[0].cursor.contains("applied="));
}
