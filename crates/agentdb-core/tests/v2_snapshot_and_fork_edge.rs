//! Tests for the sandbox snapshot-ref protocol (#2) and the fork-aware
//! changefeed (#4). See `docs/design/design.md` and
//! `docs/reference/sandbox-snapshot-contract.md`.

use agentdb_core::runtime::{
    AppendRequest, ExportWatchRequest, ForkSessionRequest, NewSnapshotRef, Runtime, WatchRequest,
    WatchScope,
};
use agentdb_core::session::{SnapshotLayer, SnapshotRef};
use agentdb_core::{NewAgent, NewSession, NewTenant};
use std::sync::Arc;

async fn seed() -> (
    Arc<Runtime>,
    agentdb_core::Tenant,
    agentdb_core::Agent,
    agentdb_core::Session,
) {
    let rt = Runtime::in_memory();
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "snap".into(),
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

fn layers() -> Vec<SnapshotLayer> {
    vec![
        SnapshotLayer {
            name: "deps".into(),
            ref_key: "obj://deps/abc".into(),
            kind: Some("directory".into()),
        },
        SnapshotLayer {
            name: "repo".into(),
            ref_key: "obj://repo/def".into(),
            kind: Some("directory".into()),
        },
    ]
}

async fn append_n(
    rt: &Runtime,
    tenant: &agentdb_core::Tenant,
    agent: &agentdb_core::Agent,
    session: &agentdb_core::Session,
    n: u64,
) {
    for i in 0..n {
        rt.append(AppendRequest::simple(
            tenant,
            agent,
            session,
            "msg",
            serde_json::json!({ "i": i }),
        ))
        .await
        .unwrap();
    }
}

// ---- #2 snapshot-ref ----

#[tokio::test]
async fn record_and_resolve_snapshot_on_leaf_session() {
    let (rt, tenant, agent, session) = seed().await;
    append_n(&rt, &tenant, &agent, &session, 3).await;

    let snap = rt
        .record_session_snapshot(NewSnapshotRef {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            at_seq: 2,
            layers: layers(),
            expires_at_ms: None,
        })
        .await
        .unwrap();
    assert_eq!(snap.at_seq, 2);
    assert_eq!(snap.layers.len(), 2);
    assert!(snap.snapshot_id.as_str().starts_with("snp_"));

    let resolved = rt
        .resolve_resume_snapshot(&tenant.tenant_id, &session.session_id, 10_000)
        .await
        .unwrap();
    let chosen: SnapshotRef = resolved.snapshot.expect("snapshot resolved");
    assert_eq!(chosen.snapshot_id.as_str(), snap.snapshot_id.as_str());
    assert_eq!(resolved.replay_from_seq, 2);
    assert_eq!(
        resolved.owning_session_id.as_ref().map(|id| id.as_str()),
        Some(session.session_id.as_str())
    );
    assert!(!resolved.dangling_skipped);
}

#[tokio::test]
async fn resolve_picks_newest_and_skips_expired_dangling_snapshot() {
    let (rt, tenant, agent, session) = seed().await;
    append_n(&rt, &tenant, &agent, &session, 5).await;

    // Older, still-valid snapshot.
    rt.record_session_snapshot(NewSnapshotRef {
        tenant_id: tenant.tenant_id.clone(),
        session_id: session.session_id.clone(),
        at_seq: 1,
        layers: layers(),
        expires_at_ms: Some(1_000_000),
    })
    .await
    .unwrap();
    // Newer snapshot that will be expired at resolve time -> dangling.
    rt.record_session_snapshot(NewSnapshotRef {
        tenant_id: tenant.tenant_id.clone(),
        session_id: session.session_id.clone(),
        at_seq: 4,
        layers: layers(),
        expires_at_ms: Some(5_000),
    })
    .await
    .unwrap();

    let resolved = rt
        .resolve_resume_snapshot(&tenant.tenant_id, &session.session_id, 10_000)
        .await
        .unwrap();
    let chosen = resolved.snapshot.expect("falls back to older snapshot");
    assert_eq!(
        chosen.at_seq, 1,
        "newer snapshot expired, fall back to older"
    );
    assert_eq!(resolved.replay_from_seq, 1);
    assert!(resolved.dangling_skipped);
}

#[tokio::test]
async fn forked_child_inherits_parent_snapshot_at_fork_point() {
    let (rt, tenant, agent, parent) = seed().await;
    append_n(&rt, &tenant, &agent, &parent, 3).await;
    rt.record_session_snapshot(NewSnapshotRef {
        tenant_id: tenant.tenant_id.clone(),
        session_id: parent.session_id.clone(),
        at_seq: 1,
        layers: layers(),
        expires_at_ms: None,
    })
    .await
    .unwrap();

    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 1,
            alias: None,
        })
        .await
        .unwrap();

    let resolved = rt
        .resolve_resume_snapshot(&tenant.tenant_id, &child.session_id, 10_000)
        .await
        .unwrap();
    let chosen = resolved.snapshot.expect("child inherits parent snapshot");
    assert_eq!(chosen.at_seq, 1);
    assert_eq!(
        resolved.owning_session_id.as_ref().map(|id| id.as_str()),
        Some(parent.session_id.as_str()),
        "inherited snapshot is owned by the parent"
    );
    assert_eq!(
        resolved.replay_from_seq, 0,
        "inherited base: replay the whole child branch on top"
    );
}

#[tokio::test]
async fn resolve_returns_none_when_no_snapshot() {
    let (rt, tenant, _agent, session) = seed().await;
    let resolved = rt
        .resolve_resume_snapshot(&tenant.tenant_id, &session.session_id, 10_000)
        .await
        .unwrap();
    assert!(resolved.snapshot.is_none());
    assert_eq!(resolved.replay_from_seq, 0);
}

#[tokio::test]
async fn snapshot_rejects_empty_layers_and_out_of_range_seq() {
    let (rt, tenant, agent, session) = seed().await;
    append_n(&rt, &tenant, &agent, &session, 2).await;

    let no_layers = rt
        .record_session_snapshot(NewSnapshotRef {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            at_seq: 1,
            layers: vec![],
            expires_at_ms: None,
        })
        .await;
    assert!(no_layers.is_err());

    let beyond = rt
        .record_session_snapshot(NewSnapshotRef {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            at_seq: 99,
            layers: layers(),
            expires_at_ms: None,
        })
        .await;
    assert!(beyond.is_err());
}

#[tokio::test]
async fn snapshot_survives_kv_rehydrate() {
    use agentdb_storage::{InMemoryContentBackend, InMemoryKvStore};
    let kv = Arc::new(InMemoryKvStore::new());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv.clone(), content.clone());
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "snap-rehydrate".into(),
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
    append_n(&rt, &tenant, &agent, &session, 2).await;
    let snap = rt
        .record_session_snapshot(NewSnapshotRef {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            at_seq: 1,
            layers: layers(),
            expires_at_ms: None,
        })
        .await
        .unwrap();

    let hydrated = Runtime::with_kv(kv, content);
    let resolved = hydrated
        .resolve_resume_snapshot(&tenant.tenant_id, &session.session_id, 10_000)
        .await
        .unwrap();
    assert_eq!(
        resolved.snapshot.unwrap().snapshot_id.as_str(),
        snap.snapshot_id.as_str()
    );
}

// ---- #4 fork-aware changefeed ----

#[tokio::test]
async fn tenant_watch_emits_fork_edge_and_does_not_reemit_parent_events() {
    let (rt, tenant, agent, parent) = seed().await;
    append_n(&rt, &tenant, &agent, &parent, 2).await;
    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 1,
            alias: None,
        })
        .await
        .unwrap();
    rt.append(AppendRequest {
        tenant_id: tenant.tenant_id.clone(),
        agent_id: agent.agent_id.clone(),
        session_id: child.session_id.clone(),
        kind: "child".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        credential_refs: vec![],
        idempotency_key: None,
    })
    .await
    .unwrap();

    let items = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            from_cursor: None,
            limit: 100,
        })
        .await
        .unwrap();

    // 2 parent events + 1 child event + 1 fork-edge.
    let fork_edges: Vec<_> = items.iter().filter_map(|i| i.fork_edge.as_ref()).collect();
    assert_eq!(fork_edges.len(), 1, "exactly one fork-edge");
    assert_eq!(
        fork_edges[0].child_session_id.as_str(),
        child.session_id.as_str()
    );
    assert_eq!(
        fork_edges[0].parent_session_id.as_str(),
        parent.session_id.as_str()
    );
    assert_eq!(fork_edges[0].fork_point_seq, 1);

    // Each physical event appears exactly once; no parent re-emission on child.
    let child_events: Vec<_> = items
        .iter()
        .filter_map(|i| i.event.as_ref())
        .filter(|e| e.session_id.as_str() == child.session_id.as_str())
        .collect();
    assert_eq!(child_events.len(), 1, "child emits only its own event");
    assert_eq!(child_events[0].seq, 0);
    let parent_events: Vec<_> = items
        .iter()
        .filter_map(|i| i.event.as_ref())
        .filter(|e| e.session_id.as_str() == parent.session_id.as_str())
        .collect();
    assert_eq!(parent_events.len(), 2);
}

#[tokio::test]
async fn session_watch_on_parent_discovers_child_via_fork_edge() {
    let (rt, tenant, agent, parent) = seed().await;
    append_n(&rt, &tenant, &agent, &parent, 2).await;
    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 1,
            alias: None,
        })
        .await
        .unwrap();

    let items = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(parent.session_id.clone()),
            agent_id: None,
            from_cursor: None,
            limit: 100,
        })
        .await
        .unwrap();

    assert!(
        items.iter().any(|i| i
            .fork_edge
            .as_ref()
            .is_some_and(|e| e.child_session_id.as_str() == child.session_id.as_str())),
        "parent session-scope feed announces the child via a fork-edge"
    );
}

#[tokio::test]
async fn fork_edge_is_gap_free_across_resume_and_pagination() {
    let (rt, tenant, agent, parent) = seed().await;
    append_n(&rt, &tenant, &agent, &parent, 2).await;

    // Cursor captured BEFORE the fork exists.
    let pre_fork = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            from_cursor: None,
            limit: 100,
        })
        .await
        .unwrap();
    let pre_fork_cursor = pre_fork.last().unwrap().cursor.clone();

    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 1,
            alias: None,
        })
        .await
        .unwrap();
    rt.append(AppendRequest {
        tenant_id: tenant.tenant_id.clone(),
        agent_id: agent.agent_id.clone(),
        session_id: child.session_id.clone(),
        kind: "child".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        credential_refs: vec![],
        idempotency_key: None,
    })
    .await
    .unwrap();

    // An old cursor from before the fork still discovers the child + fork-edge.
    let after = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            from_cursor: Some(pre_fork_cursor),
            limit: 100,
        })
        .await
        .unwrap();
    assert!(
        after.iter().any(|i| i.fork_edge.is_some()),
        "fork-edge discovered after resume"
    );
    assert!(
        after.iter().any(|i| i
            .event
            .as_ref()
            .is_some_and(|e| e.session_id.as_str() == child.session_id.as_str())),
        "child event discovered after resume"
    );

    // Page through from scratch with limit 1; every cursor unique, fork-edge once.
    let mut cursor = None;
    let mut cursors = std::collections::BTreeSet::new();
    let mut fork_edge_count = 0;
    loop {
        let page = rt
            .watch(WatchRequest {
                tenant_id: tenant.tenant_id.clone(),
                scope: WatchScope::Tenant,
                session_id: None,
                agent_id: None,
                from_cursor: cursor.clone(),
                limit: 1,
            })
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        for item in &page {
            assert!(cursors.insert(item.cursor.clone()), "duplicate cursor");
            if item.fork_edge.is_some() {
                fork_edge_count += 1;
            }
        }
        cursor = page.last().map(|i| i.cursor.clone());
    }
    assert_eq!(
        fork_edge_count, 1,
        "fork-edge delivered exactly once across pagination"
    );
    // 2 parent events + 1 child event + 1 fork-edge = 4 distinct items.
    assert_eq!(cursors.len(), 4);
}

#[tokio::test]
async fn export_watch_in_memory_skips_fork_edges_without_stalling_small_pages() {
    let (rt, tenant, agent, parent) = seed().await;
    append_n(&rt, &tenant, &agent, &parent, 2).await;

    let pre_fork = rt
        .watch(WatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            from_cursor: None,
            limit: 100,
        })
        .await
        .unwrap();
    let pre_fork_cursor = pre_fork.last().unwrap().cursor.clone();

    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 1,
            alias: None,
        })
        .await
        .unwrap();
    rt.append(AppendRequest {
        tenant_id: tenant.tenant_id.clone(),
        agent_id: agent.agent_id.clone(),
        session_id: child.session_id.clone(),
        kind: "child".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        credential_refs: vec![],
        idempotency_key: None,
    })
    .await
    .unwrap();

    let exported = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: Some(pre_fork_cursor),
            limit: 1,
        })
        .await
        .unwrap();

    assert_eq!(exported.len(), 1);
    assert_eq!(exported[0].item.object_type, "event");
    assert_eq!(
        exported[0].item.session_id.as_ref().map(|id| id.as_str()),
        Some(child.session_id.as_str())
    );
}

#[tokio::test]
async fn export_watch_persistent_ledger_emits_fork_edge_control_item() {
    use agentdb_storage::{InMemoryContentBackend, InMemoryKvStore};

    let kv = Arc::new(InMemoryKvStore::new());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv, content);
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "fork-edge-export".into(),
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
    let parent = rt
        .create_session(NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: agent.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();
    append_n(&rt, &tenant, &agent, &parent, 2).await;

    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: parent.session_id.clone(),
            at_seq: 1,
            alias: None,
        })
        .await
        .unwrap();

    let tenant_edges = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Tenant,
            session_id: None,
            agent_id: None,
            include: vec!["fork_edges".into()],
            from_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(tenant_edges.len(), 1);
    assert_eq!(tenant_edges[0].item.object_type, "fork_edge");
    assert_eq!(
        tenant_edges[0]
            .item
            .data
            .get("parent_session_id")
            .and_then(|value| value.as_str()),
        Some(parent.session_id.as_str())
    );
    assert_eq!(
        tenant_edges[0]
            .item
            .data
            .get("child_session_id")
            .and_then(|value| value.as_str()),
        Some(child.session_id.as_str())
    );
    assert_eq!(
        tenant_edges[0]
            .item
            .data
            .get("fork_point_seq")
            .and_then(|value| value.as_u64()),
        Some(1)
    );

    let parent_edges = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(parent.session_id.clone()),
            agent_id: None,
            include: vec!["fork_edges".into()],
            from_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(parent_edges.len(), 1);
    assert_eq!(
        parent_edges[0]
            .item
            .session_id
            .as_ref()
            .map(|id| id.as_str()),
        Some(parent.session_id.as_str())
    );
}
