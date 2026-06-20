use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{
    AckExportConsumerRequest, AppendRequest, ClaimExportConsumerRequest, ContextInclude,
    ContextRequest, CreateExportConsumerRequest, ExportWatchRequest, ForkSessionRequest, Runtime,
    RuntimeRouteContext, SearchRequest, WatchRequest, WatchScope,
};
use agentdb_core::{NewAgent, NewSession, NewTenant, RUNTIME_PRIMARY_TABLET_ID};
use agentdb_storage::{
    InMemoryContentBackend, InMemoryKvStore, KvStore, PrefixScopedTabletStore, ReplicatedLog,
    SingleNodeReplicatedLog,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

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
        external_id: None,
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
    assert_eq!(
        after_first[0]
            .event
            .as_ref()
            .expect("watch item carries event")
            .kind,
        "second"
    );
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

    let resumed_without_route_context = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(session.session_id.clone()),
            agent_id: Some(agent.agent_id.clone()),
            from_cursor: Some(items[0].cursor.clone()),
            limit: 10,
        })
        .await
        .unwrap();
    assert!(resumed_without_route_context.is_empty());
}

#[tokio::test]
async fn export_consumers_survive_kv_rehydrate() {
    let kv = Arc::new(InMemoryKvStore::new());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv.clone(), content.clone());
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "export-consumer".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let consumer = rt
        .create_export_consumer(CreateExportConsumerRequest {
            tenant_id: tenant.tenant_id.clone(),
            consumer_name: "clickhouse".into(),
            scope: WatchScope::Tenant,
            agent_id: None,
            session_id: None,
            include: vec!["events".into()],
        })
        .await
        .unwrap();
    assert_eq!(consumer.consumer_name, "clickhouse");

    rt.claim_export_consumer(ClaimExportConsumerRequest {
        tenant_id: tenant.tenant_id.clone(),
        consumer_name: "clickhouse".into(),
        lease_owner: "worker-a".into(),
        lease_ms: 60_000,
    })
    .await
    .unwrap();
    rt.ack_export_consumer(AckExportConsumerRequest {
        tenant_id: tenant.tenant_id.clone(),
        consumer_name: "clickhouse".into(),
        cursor: "v2:0001".into(),
        snapshot_high_watermark: Some("v2:0001".into()),
        lease_owner: Some("worker-a".into()),
    })
    .await
    .unwrap();

    let hydrated = Runtime::with_kv(kv, content);
    let loaded = hydrated
        .get_export_consumer(&tenant.tenant_id, "clickhouse")
        .await
        .unwrap();
    assert_eq!(loaded.scope, WatchScope::Tenant);
    assert_eq!(loaded.last_acked_cursor.as_deref(), Some("v2:0001"));
    assert_eq!(loaded.snapshot_high_watermark.as_deref(), Some("v2:0001"));
    assert_eq!(loaded.lease_owner.as_deref(), Some("worker-a"));
}

#[tokio::test]
async fn export_watch_uses_durable_ledger_cursors_with_persistent_kv() {
    let kv = Arc::new(InMemoryKvStore::new());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv.clone(), content.clone());
    assert!(rt.durable_export_ledger_enabled());

    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "export-ledger".into(),
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

    rt.append(AppendRequest::simple(
        &tenant,
        &agent,
        &session,
        "first",
        serde_json::json!({"n": 1}),
    ))
    .await
    .unwrap();
    let other_agent = rt
        .create_agent(NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let other_session = rt
        .create_session(NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: other_agent.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();
    rt.append(AppendRequest::simple(
        &tenant,
        &other_agent,
        &other_session,
        "other-agent",
        serde_json::json!({"n": 99}),
    ))
    .await
    .unwrap();
    rt.append(AppendRequest::simple(
        &tenant,
        &agent,
        &session,
        "second",
        serde_json::json!({"n": 2}),
    ))
    .await
    .unwrap();

    let first_page = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: None,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].item.data["kind"], "first");
    assert!(first_page[0].cursor.starts_with("ledger:"));

    let hydrated = Runtime::with_kv(kv.clone(), content);
    let second_page = hydrated
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(session.session_id.clone()),
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: Some(first_page[0].cursor.clone()),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].item.data["kind"], "second");
    assert!(second_page[0].cursor.starts_with("ledger:"));

    let agent_page = hydrated
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Agent,
            session_id: None,
            agent_id: Some(agent.agent_id.clone()),
            include: vec!["events".into()],
            from_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    let agent_kinds = agent_page
        .iter()
        .map(|item| item.item.data["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(agent_kinds, vec!["first", "second"]);

    assert_eq!(
        hydrated
            .export_high_watermark(&tenant.tenant_id)
            .await
            .unwrap()
            .as_deref(),
        Some(second_page[0].cursor.as_str())
    );
    let oldest = hydrated
        .export_oldest_available_cursor(&tenant.tenant_id)
        .await
        .unwrap()
        .expect("oldest export cursor exists");
    assert!(oldest.starts_with("ledger:"));

    let malformed = hydrated
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: Some("v2:00000000000000000000".into()),
            limit: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(malformed.code, agentdb_core::ErrorCode::InvalidCursor);

    let wrong_session = hydrated
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(other_session.session_id.clone()),
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: Some(first_page[0].cursor.clone()),
            limit: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(wrong_session.code, agentdb_core::ErrorCode::InvalidCursor);

    let first_ledger_key = format!(
        "export_ledger/{}",
        first_page[0].cursor.trim_start_matches("ledger:")
    );
    let tenant_ledger_prefix = format!("export_ledger/{}/", tenant.tenant_id.as_str());
    let primary_tablet_prefix = format!("tablet_state/{RUNTIME_PRIMARY_TABLET_ID:020}/");
    let first_ledger_key = format!("{primary_tablet_prefix}{first_ledger_key}");
    let tenant_ledger_prefix = format!("{primary_tablet_prefix}{tenant_ledger_prefix}");
    let pruned_keys = kv
        .scan_prefix(tenant_ledger_prefix.as_bytes())
        .unwrap()
        .map(|item| item.unwrap().0)
        .filter(|key| key.as_slice() <= first_ledger_key.as_bytes())
        .collect::<Vec<_>>();
    assert!(!pruned_keys.is_empty());
    for key in pruned_keys {
        kv.delete(&key).unwrap();
    }

    let expired_cursor = hydrated
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: Some(first_page[0].cursor.clone()),
            limit: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(expired_cursor.code, agentdb_core::ErrorCode::InvalidCursor);
    assert!(expired_cursor.message.contains("oldest_available_cursor="));
}

#[tokio::test]
async fn export_watch_paged_resume_survives_rehydrate_without_gaps_or_duplicates() {
    let kv = Arc::new(InMemoryKvStore::new());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv.clone(), content.clone());

    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "export-ledger-stability".into(),
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

    for n in 0..64_u64 {
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "paged",
            serde_json::json!({ "n": n }),
        ))
        .await
        .unwrap();
    }

    let mut active = Runtime::with_kv(kv.clone(), content.clone());
    let mut cursor = None;
    let mut cursors = BTreeSet::new();
    let mut values = Vec::new();
    let mut pages = 0_u64;
    loop {
        let page = active
            .export_watch(ExportWatchRequest {
                tenant_id: tenant.tenant_id.clone(),
                scope: WatchScope::Tenant,
                session_id: None,
                agent_id: None,
                include: vec!["events".into()],
                from_cursor: cursor.clone(),
                limit: 7,
            })
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        pages += 1;
        for item in &page {
            assert!(
                cursors.insert(item.cursor.clone()),
                "duplicate export cursor {}",
                item.cursor
            );
            values.push(item.item.data["payload"]["n"].as_u64().unwrap());
        }
        cursor = page.last().map(|item| item.cursor.clone());
        if pages % 3 == 0 {
            active = Runtime::with_kv(kv.clone(), content.clone());
        }
    }

    assert!(pages > 8, "expected multiple small export pages");
    assert_eq!(values, (0..64_u64).collect::<Vec<_>>());
    assert_eq!(cursors.len(), 64);
    assert_eq!(
        active
            .export_high_watermark(&tenant.tenant_id)
            .await
            .unwrap(),
        cursor
    );
}

#[tokio::test]
async fn export_watch_emits_metadata_updates_and_tombstones_with_persistent_kv() {
    let kv = Arc::new(InMemoryKvStore::new());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv, content);

    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "export-metadata".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let agent = rt
        .create_agent(NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: Some("metadata-agent".into()),
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let session = rt
        .create_session(NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: agent.agent_id.clone(),
            alias: Some("metadata-session".into()),
        })
        .await
        .unwrap();
    let record = rt
        .write_record(NewRecord {
            tenant_id: tenant.tenant_id.clone(),
            external_id: None,
            namespace: vec!["mem".into()],
            scope: serde_json::json!({}),
            text: Some("exported memory".into()),
            metadata: serde_json::json!({"k": "v"}),
            tags: vec!["export".into()],
            source: None,
            embedding: None,
            policy: IndexingPolicy::default(),
            expires_at_ms: None,
        })
        .await
        .unwrap();
    rt.delete_record(&tenant.tenant_id, &record.record_id)
        .await
        .unwrap();

    let tenant_items = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec![
                "tenants".into(),
                "agents".into(),
                "sessions".into(),
                "records".into(),
            ],
            from_cursor: None,
            limit: 20,
        })
        .await
        .unwrap();
    let object_types = tenant_items
        .iter()
        .map(|item| item.item.object_type.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        object_types,
        ["agent", "record", "session", "tenant"]
            .into_iter()
            .collect()
    );
    assert!(tenant_items
        .iter()
        .any(|item| item.item.object_id == record.record_id.as_str()
            && item.item.op == "delete"
            && item.item.tombstone.is_some()));

    let session_items = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(session.session_id.clone()),
            agent_id: None,
            include: vec!["sessions".into()],
            from_cursor: None,
            limit: 20,
        })
        .await
        .unwrap();
    assert_eq!(session_items.len(), 1);
    assert_eq!(session_items[0].item.object_id, session.session_id.as_str());
}

#[tokio::test]
async fn export_watch_emits_retention_gc_tombstones_with_persistent_kv() {
    let kv = Arc::new(InMemoryKvStore::new());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv, content);

    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "export-retention".into(),
            policy: serde_json::json!({"retention": {"retention_ms": 0}}),
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
    rt.append(AppendRequest::simple(
        &tenant,
        &agent,
        &session,
        "will-be-purged",
        serde_json::json!({"n": 1}),
    ))
    .await
    .unwrap();
    let record = rt
        .write_record(NewRecord {
            tenant_id: tenant.tenant_id.clone(),
            external_id: None,
            namespace: vec!["mem".into()],
            scope: serde_json::json!({}),
            text: Some("retention memory".into()),
            metadata: serde_json::json!({}),
            tags: vec![],
            source: None,
            embedding: None,
            policy: IndexingPolicy::default(),
            expires_at_ms: None,
        })
        .await
        .unwrap();

    rt.delete_record(&tenant.tenant_id, &record.record_id)
        .await
        .unwrap();
    rt.delete_session(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    rt.gc_sweep_for_tenant_at(&tenant.tenant_id, now_ms + 1)
        .await
        .unwrap();

    let items = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec!["events".into(), "records".into(), "sessions".into()],
            from_cursor: None,
            limit: 100,
        })
        .await
        .unwrap();
    assert!(items.iter().any(|item| {
        item.item.object_type == "record"
            && item.item.object_id == record.record_id.as_str()
            && item.item.op == "delete"
            && item.item.tombstone.as_ref().and_then(|t| t.get("reason"))
                == Some(&serde_json::json!("retention_gc"))
    }));
    assert!(items.iter().any(|item| {
        item.item.object_type == "event"
            && item.item.op == "delete"
            && item.item.tombstone.as_ref().and_then(|t| t.get("reason"))
                == Some(&serde_json::json!("retention_gc"))
    }));
    assert!(items.iter().any(|item| {
        item.item.object_type == "session"
            && item.item.object_id == session.session_id.as_str()
            && item.item.op == "delete"
            && item.item.tombstone.as_ref().and_then(|t| t.get("reason"))
                == Some(&serde_json::json!("retention_gc"))
    }));

    let purged_tenant = rt
        .create_tenant(NewTenant {
            external_key: "export-tenant-purge".into(),
            policy: serde_json::json!({"retention": {"retention_ms": 0}}),
        })
        .await
        .unwrap();
    rt.delete_tenant(&purged_tenant.tenant_id).await.unwrap();
    rt.gc_sweep_at(now_ms + 2).await.unwrap();
    let tenant_items = rt
        .export_watch(ExportWatchRequest {
            tenant_id: purged_tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec!["tenants".into()],
            from_cursor: None,
            limit: 20,
        })
        .await
        .unwrap();
    assert!(tenant_items.iter().any(|item| {
        item.item.object_type == "tenant"
            && item.item.object_id == purged_tenant.tenant_id.as_str()
            && item.item.op == "delete"
            && item.item.tombstone.as_ref().and_then(|t| t.get("reason"))
                == Some(&serde_json::json!("tenant_purge"))
    }));
}
