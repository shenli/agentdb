//! Stage 1 of KV-authoritative state: the in-memory metadata maps are a
//! cache over durable KV rows, not the authority. Every test here evicts the
//! caches (`debug_evict_metadata_caches`) and asserts the public surface
//! still behaves identically via the KV fallback — the keystone property
//! that lets Stage 2 stop hydrating everything at startup.

use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::artifact::NewArtifact;
use agentdb_core::errors::ErrorCode;
use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{
    AppendRequest, FetchRequest, ForkSessionRequest, Runtime, WatchRequest, WatchScope,
};
use agentdb_core::session::{Lifecycle, NewSession};
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{ContentBackend, InMemoryContentBackend, InMemoryKvStore, KvStore};

fn persistent_runtime() -> Arc<Runtime> {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());
    Runtime::with_kv(kv, content)
}

async fn bootstrap(
    rt: &Runtime,
) -> (
    agentdb_core::Tenant,
    agentdb_core::Agent,
    agentdb_core::Session,
) {
    let t = rt
        .create_tenant(NewTenant {
            external_key: "kv-auth".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: Some("agent-alias".into()),
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: Some("session-alias".into()),
        })
        .await
        .unwrap();
    (t, a, s)
}

#[tokio::test]
async fn every_funnel_survives_full_cache_eviction() {
    let rt = persistent_runtime();
    let (t, a, s) = bootstrap(&rt).await;

    for n in 0..3u64 {
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "message",
            serde_json::json!({ "n": n }),
        ))
        .await
        .unwrap();
    }
    let artifact = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: None,
            },
            b"blob bytes".to_vec(),
        )
        .await
        .unwrap();

    // Wipe every metadata cache: all subsequent operations must transparently
    // reload from KV.
    rt.debug_evict_metadata_caches().await;

    // Reads.
    assert_eq!(
        rt.get_tenant(&t.tenant_id).await.unwrap().external_key,
        "kv-auth"
    );
    rt.debug_evict_metadata_caches().await;
    assert_eq!(
        rt.get_agent(&t.tenant_id, &a.agent_id)
            .await
            .unwrap()
            .agent_id,
        a.agent_id
    );
    rt.debug_evict_metadata_caches().await;
    let session = rt.get_session(&t.tenant_id, &s.session_id).await.unwrap();
    assert_eq!(session.next_seq, 3);
    rt.debug_evict_metadata_caches().await;
    assert_eq!(
        rt.get_artifact(&t.tenant_id, &artifact.artifact_id)
            .await
            .unwrap()
            .sha256,
        artifact.sha256
    );
    rt.debug_evict_metadata_caches().await;
    assert_eq!(
        rt.get_artifact_blob(&t.tenant_id, &artifact.artifact_id)
            .await
            .unwrap(),
        b"blob bytes"
    );

    // Event reads.
    rt.debug_evict_metadata_caches().await;
    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: usize::MAX,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    rt.debug_evict_metadata_caches().await;
    let recent = rt
        .fetch_recent(t.tenant_id.clone(), s.session_id.clone(), 2)
        .await
        .unwrap();
    assert_eq!(recent.len(), 2);

    // Writes: append lands with a contiguous seq on a cold cache.
    rt.debug_evict_metadata_caches().await;
    let resp = rt
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "message",
            serde_json::json!({ "n": 3 }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.seq, 3, "contiguous seq across eviction");

    // Fork with replay visibility, on a cold cache.
    rt.debug_evict_metadata_caches().await;
    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            at_seq: 1,
            alias: Some("fork-alias".into()),
        })
        .await
        .unwrap();
    rt.debug_evict_metadata_caches().await;
    let replay = rt
        .replay_session(&t.tenant_id, &child.session_id)
        .await
        .unwrap();
    assert_eq!(replay.events.len(), 2);

    // Lifecycle transitions on a cold cache.
    rt.debug_evict_metadata_caches().await;
    rt.transition_session_lifecycle(&t.tenant_id, &s.session_id, Lifecycle::Cold)
        .await
        .unwrap();
    rt.debug_evict_metadata_caches().await;
    rt.transition_session_lifecycle(&t.tenant_id, &s.session_id, Lifecycle::Archive)
        .await
        .unwrap();
    rt.debug_evict_metadata_caches().await;
    let after_tiering = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: usize::MAX,
        })
        .await
        .unwrap();
    assert_eq!(
        after_tiering.len(),
        4,
        "tiered history readable after eviction"
    );

    // Session-scoped watch on a cold cache.
    rt.debug_evict_metadata_caches().await;
    let items = rt
        .watch(WatchRequest {
            tenant_id: t.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(s.session_id.clone()),
            agent_id: None,
            from_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(!items.is_empty());

    // Records on a cold cache.
    rt.debug_evict_metadata_caches().await;
    rt.upsert_record_with_commit(NewRecord {
        tenant_id: t.tenant_id.clone(),
        external_id: None,
        namespace: vec!["notes".into()],
        scope: serde_json::json!({}),
        text: Some("hello".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    })
    .await
    .unwrap();

    // Backpressure report on a cold cache.
    rt.debug_evict_metadata_caches().await;
    rt.backpressure_for_tenant(&t.tenant_id).await.unwrap();
}

#[tokio::test]
async fn soft_deleted_rows_stay_invisible_after_eviction() {
    let rt = persistent_runtime();
    let (t, _a, s) = bootstrap(&rt).await;

    rt.delete_session(&t.tenant_id, &s.session_id)
        .await
        .unwrap();
    rt.debug_evict_metadata_caches().await;

    // The fallback loads the tombstone; liveness filters must still apply.
    let err = rt
        .get_session(&t.tenant_id, &s.session_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionNotFound);
}

#[tokio::test]
async fn alias_conflicts_survive_eviction() {
    let rt = persistent_runtime();
    let (t, a, _s) = bootstrap(&rt).await;

    // The session alias map was wiped; creating another session with the
    // same alias must still conflict via the durable alias row — without
    // the fallback this would silently create a duplicate alias.
    rt.debug_evict_metadata_caches().await;
    let err = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: Some("session-alias".into()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);

    rt.debug_evict_metadata_caches().await;
    let err = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: Some("agent-alias".into()),
            config: serde_json::json!({}),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);

    rt.debug_evict_metadata_caches().await;
    let err = rt
        .create_tenant(NewTenant {
            external_key: "kv-auth".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);
}

/// Cache-miss chaos: concurrent appenders and readers across many sessions
/// while an evictor repeatedly wipes the metadata caches. Invariants: no
/// operation fails, per-session seqs stay contiguous, and every payload
/// reads back in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_traffic_survives_repeated_cache_eviction() {
    let rt = persistent_runtime();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "kv-chaos".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let mut sessions = Vec::new();
    for _ in 0..6 {
        sessions.push(
            rt.create_session(NewSession {
                tenant_id: t.tenant_id.clone(),
                agent_id: a.agent_id.clone(),
                alias: None,
            })
            .await
            .unwrap(),
        );
    }

    let mut tasks = Vec::new();
    for s in &sessions {
        let rt = rt.clone();
        let (t, a, s) = (t.clone(), a.clone(), s.clone());
        tasks.push(tokio::spawn(async move {
            for n in 0..40u64 {
                let resp = rt
                    .append(AppendRequest::simple(
                        &t,
                        &a,
                        &s,
                        "message",
                        serde_json::json!({ "n": n }),
                    ))
                    .await
                    .unwrap();
                assert_eq!(resp.seq, n, "contiguous seq under eviction chaos");
                if n % 7 == 0 {
                    let recent = rt
                        .fetch_recent(t.tenant_id.clone(), s.session_id.clone(), 3)
                        .await
                        .unwrap();
                    assert!(!recent.is_empty());
                }
            }
        }));
    }
    {
        let rt = rt.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..200 {
                rt.debug_evict_metadata_caches().await;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    rt.debug_evict_metadata_caches().await;
    for s in &sessions {
        let events = rt
            .fetch_from_seq(FetchRequest {
                tenant_id: t.tenant_id.clone(),
                session_id: s.session_id.clone(),
                from_seq: 0,
                limit: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 40);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.seq, i as u64);
            assert_eq!(event.payload["n"], i as u64);
        }
    }
}

/// Stage 2: a lazy runtime starts with empty metadata caches, serves
/// everything through the KV fallback, and the eviction sweep keeps the
/// cache bounded without breaking traffic.
#[tokio::test]
async fn lazy_runtime_serves_cold_and_eviction_sweep_bounds_the_cache() {
    use agentdb_core::runtime::HydrationMode;

    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());

    // Seed through a full runtime, then reopen lazily over the same KV.
    let (tenant_id, agent_id, session_ids) = {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "lazy-mode".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let a = rt
            .create_agent(NewAgent {
                tenant_id: t.tenant_id.clone(),
                alias: None,
                config: serde_json::json!({}),
            })
            .await
            .unwrap();
        let mut ids = Vec::new();
        for _ in 0..8 {
            let s = rt
                .create_session(NewSession {
                    tenant_id: t.tenant_id.clone(),
                    agent_id: a.agent_id.clone(),
                    alias: None,
                })
                .await
                .unwrap();
            rt.append(AppendRequest::simple(
                &t,
                &a,
                &s,
                "message",
                serde_json::json!({"seed": true}),
            ))
            .await
            .unwrap();
            ids.push(s.session_id);
        }
        (t.tenant_id, a.agent_id, ids)
    };

    let rt = Runtime::with_kv_and_hydration_mode(kv, content, HydrationMode::Lazy);

    // Cold start: enumeration sees the full population despite empty caches.
    let listed = rt.list_sessions(&tenant_id).await.unwrap();
    assert_eq!(listed.len(), 8, "lazy enumeration reads KV, not the cache");

    // Traffic works cold, and appends stay contiguous.
    for session_id in &session_ids {
        let events = rt
            .fetch_from_seq(FetchRequest {
                tenant_id: tenant_id.clone(),
                session_id: session_id.clone(),
                from_seq: 0,
                limit: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
    }

    // The cache is now warm with 8 sessions; a sweep with a cap of 4 clears
    // it, and traffic keeps flowing through the fallback afterwards.
    let evicted = rt
        .sweep_metadata_cache_with_caps(10_000, 10_000, 4, 10_000)
        .await;
    assert!(
        evicted >= 8,
        "sweep cleared the oversized session cache: {evicted}"
    );

    let t = rt.get_tenant(&tenant_id).await.unwrap();
    let a = rt.get_agent(&tenant_id, &agent_id).await.unwrap();
    for (i, session_id) in session_ids.iter().enumerate() {
        let s = rt.get_session(&tenant_id, session_id).await.unwrap();
        let resp = rt
            .append(AppendRequest::simple(
                &t,
                &a,
                &s,
                "message",
                serde_json::json!({"post_evict": i}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.seq, 1, "contiguous after eviction");
    }

    // Chaos: concurrent traffic racing an aggressive sweeper.
    let mut tasks = Vec::new();
    #[allow(clippy::unnecessary_to_owned)]
    for session_id in session_ids.iter().cloned() {
        let rt = rt.clone();
        let (t, a) = (t.clone(), a.clone());
        let tid = tenant_id.clone();
        tasks.push(tokio::spawn(async move {
            let s = rt.get_session(&tid, &session_id).await.unwrap();
            for n in 0..25u64 {
                let resp = rt
                    .append(AppendRequest::simple(
                        &t,
                        &a,
                        &s,
                        "message",
                        serde_json::json!({"n": n}),
                    ))
                    .await
                    .unwrap();
                assert_eq!(resp.seq, 2 + n);
            }
        }));
    }
    {
        let rt = rt.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..100 {
                rt.sweep_metadata_cache_with_caps(0, 0, 0, 0).await;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    for session_id in &session_ids {
        let events = rt
            .fetch_from_seq(FetchRequest {
                tenant_id: tenant_id.clone(),
                session_id: session_id.clone(),
                from_seq: 0,
                limit: usize::MAX,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 27, "seed + post-evict + 25 chaos appends");
    }
}

/// Records are KV-authoritative too: in lazy mode the search indexes are
/// built at startup but record ROWS are not resident; search materializes
/// hits through the fallback, external-id dedupe works cold, and eviction
/// of the records cache never changes results.
#[tokio::test]
async fn lazy_records_search_and_dedupe_work_with_no_resident_rows() {
    use agentdb_core::runtime::{HydrationMode, SearchRequest};

    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());

    let tenant_id = {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "lazy-records".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        for i in 0..5 {
            rt.upsert_record_with_commit(NewRecord {
                tenant_id: t.tenant_id.clone(),
                external_id: Some(format!("ext-{i}")),
                namespace: vec!["notes".into()],
                scope: serde_json::json!({}),
                text: Some(format!("searchable note number {i}")),
                metadata: serde_json::json!({"i": i}),
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
        }
        t.tenant_id
    };

    let rt = Runtime::with_kv_and_hydration_mode(kv, content, HydrationMode::Lazy);

    // Text search materializes rows through the fallback (none resident).
    let matches = rt
        .search_records(SearchRequest {
            tenant_id: tenant_id.clone(),
            namespace: vec!["notes".into()],
            sharding_key: None,
            text: Some("searchable note".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 50,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(matches.len(), 5, "index hits materialize via KV fallback");

    // Enumeration and brute-force (no text) search read the authority.
    assert_eq!(rt.list_records(&tenant_id).await.unwrap().len(), 5);
    let brute = rt
        .search_records(SearchRequest {
            tenant_id: tenant_id.clone(),
            namespace: vec!["notes".into()],
            sharding_key: None,
            text: None,
            embedding: None,
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 50,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(brute.len(), 5);

    // External-id dedupe on a cold cache: upserting ext-2 must UPDATE the
    // existing record, not create a duplicate.
    rt.debug_evict_metadata_caches().await;
    rt.upsert_record_with_commit(NewRecord {
        tenant_id: tenant_id.clone(),
        external_id: Some("ext-2".into()),
        namespace: vec!["notes".into()],
        scope: serde_json::json!({}),
        text: Some("updated text for two".into()),
        metadata: serde_json::json!({"updated": true}),
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
    assert_eq!(
        rt.list_records(&tenant_id).await.unwrap().len(),
        5,
        "cold upsert deduped by external id"
    );

    // Evict the records cache and search again: identical results. The
    // global index is eventually consistent (outbox-fed), so drain the
    // outbox first — same as the background worker would.
    rt.apply_record_index_outbox_once().await.unwrap();
    rt.debug_evict_metadata_caches().await;
    let matches = rt
        .search_records(SearchRequest {
            tenant_id: tenant_id.clone(),
            namespace: vec!["notes".into()],
            sharding_key: None,
            text: Some("updated text".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 50,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].metadata["updated"], true);
}
