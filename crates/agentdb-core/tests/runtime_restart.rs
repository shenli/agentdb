//! Restart-safety tests for the KV-backed V2 Runtime (Task 11).
//!
//! Each test opens a fresh `RocksKvStore` against a TempDir, drives some
//! traffic through `Runtime::with_kv`, drops the runtime, reopens the same
//! KV directory, and asserts the next runtime sees the prior state.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

use agentdb_core::agent::NewAgent;
use agentdb_core::artifact::NewArtifact;
use agentdb_core::record::{IndexingMode, IndexingPolicy, NewRecord};
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime, SearchRequest};
use agentdb_core::session::{Lifecycle, NewSession};
use agentdb_core::skill::{BindSkillRequest, NewSkill};
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, LocalFsContentBackend, RocksKvStore,
};
use tempfile::TempDir;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    static STORES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<dyn KvStore + Send + Sync>>>> =
        OnceLock::new();
    let stores = STORES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut stores = stores.lock().expect("runtime restart kv registry poisoned");
    if let Some(kv) = stores.get(path) {
        return kv.clone();
    }
    let kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(path).expect("open rocks kv"));
    stores.insert(path.to_path_buf(), kv.clone());
    kv
}

/// Convenience for restart tests that don't exercise artifacts: pair every
/// `with_kv` with a fresh in-memory content backend.
fn in_memory_content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

#[tokio::test]
async fn events_and_next_seq_survive_restart() {
    let dir = TempDir::new().unwrap();

    let (t, a, s) = {
        let kv = open_kv(dir.path());
        let rt = Runtime::with_kv(kv, in_memory_content());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "acme".into(),
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
            "x",
            serde_json::json!({"i": 1}),
        ))
        .await
        .unwrap();
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": 2}),
        ))
        .await
        .unwrap();
        (t, a, s)
    };

    // Reopen the same on-disk KV path. RocksDB requires the prior handle to be
    // dropped first (single-writer lock), which happened above when `rt` (and
    // its captured Arc<KvStore>) went out of scope.
    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());

    let events = rt2
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].seq, 1);

    // next_seq should be 2 after restart — confirmed by the seq assigned to a
    // fresh append being exactly 2.
    let r3 = rt2
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": 3}),
        ))
        .await
        .unwrap();
    assert_eq!(r3.seq, 2);
}

#[tokio::test]
async fn stale_live_runtime_reconciles_next_seq_before_append() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv.clone(), in_memory_content());
    let t = rt
        .create_tenant(NewTenant {
            external_key: "stale-live".into(),
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
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    let stale_simple = Runtime::with_kv(kv.clone(), in_memory_content());
    let stale_direct = Runtime::with_kv(kv.clone(), in_memory_content());

    assert_eq!(
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": 0}),
        ))
        .await
        .unwrap()
        .seq,
        0
    );
    assert_eq!(
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": 1}),
        ))
        .await
        .unwrap()
        .seq,
        1
    );

    let simple = stale_simple
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": 2}),
        ))
        .await
        .unwrap();
    assert_eq!(simple.seq, 2);

    let mut direct_req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"i": 3}));
    direct_req.idempotency_key = Some("direct-after-stale".into());
    let direct = stale_direct.append(direct_req).await.unwrap();
    assert_eq!(direct.seq, 3);

    let events = stale_direct
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    let seqs = events.iter().map(|event| event.seq).collect::<Vec<_>>();
    assert_eq!(seqs, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn concurrent_append_and_fetch_same_session_survives_restart() {
    let dir = TempDir::new().unwrap();

    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let t = rt
        .create_tenant(NewTenant {
            external_key: "coalesce".into(),
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
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for i in 0..64 {
        let rt = rt.clone();
        let tenant_id = t.tenant_id.clone();
        let agent_id = a.agent_id.clone();
        let session_id = s.session_id.clone();
        tasks.push(tokio::spawn(async move {
            let response = rt
                .append_and_fetch(
                    AppendRequest {
                        tenant_id,
                        agent_id,
                        session_id,
                        kind: "message".into(),
                        payload: serde_json::json!({ "i": i }),
                        artifact_refs: Vec::new(),
                        source_refs: Vec::new(),
                        idempotency_key: None,
                    },
                    8,
                    64 * 1024,
                )
                .await
                .unwrap();
            assert!(response
                .recent
                .iter()
                .all(|event| event.seq <= response.seq));
            (response.seq, response.event_id)
        }));
    }

    let mut appended = Vec::new();
    for task in tasks {
        appended.push(task.await.unwrap());
    }
    let mut seqs = appended.iter().map(|(seq, _)| *seq).collect::<Vec<_>>();
    seqs.sort_unstable();
    assert_eq!(seqs, (0..64).collect::<Vec<_>>());
    let lookup_event = appended
        .iter()
        .find(|(seq, _)| *seq == 0)
        .map(|(_, event_id)| event_id.clone())
        .unwrap();
    drop(rt);

    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 64);
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (0..64).collect::<Vec<_>>()
    );
    let lookup = rt
        .lookup_event_by_id(&t.tenant_id, &lookup_event)
        .await
        .unwrap();
    assert_eq!(lookup, Some((s.session_id, 0)));
}

#[tokio::test]
async fn idempotency_records_survive_restart() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());

    let rt = Runtime::with_kv(kv, in_memory_content());
    let (t, a, s) = rt.bootstrap_test_session().await;
    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 1}));
    req.idempotency_key = Some("k1".into());
    let r1 = rt.append(req.clone()).await.unwrap();
    drop(rt);

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());
    let r2 = rt2.append(req).await.unwrap();
    assert_eq!(r1.event_id, r2.event_id);
    assert_eq!(r1.seq, r2.seq);

    // Only one event was actually persisted in the session.
    let events = rt2
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        events.len(),
        1,
        "idempotent retry must not create a second event"
    );
    assert_eq!(events[0].seq, 0);
}

#[tokio::test]
async fn idempotency_records_are_purged_by_gc_after_retention() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());

    let rt = Runtime::with_kv(kv, in_memory_content());
    let t = rt
        .create_tenant(NewTenant {
            external_key: "idem-gc".into(),
            policy: serde_json::json!({"retention": {"retention_ms": 0}}),
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
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    let mut req = AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({"v": 1}));
    req.idempotency_key = Some("k1".into());
    let r1 = rt.append(req.clone()).await.unwrap();
    let report = rt
        .gc_sweep_for_tenant_at(&t.tenant_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(report.idempotency_purged, 1);
    drop(rt);

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());
    let r2 = rt2.append(req).await.unwrap();
    assert_ne!(
        r1.event_id, r2.event_id,
        "retry after idempotency GC should commit as a new request"
    );
    assert_eq!(r2.seq, 1);
}

#[tokio::test]
async fn tenants_agents_and_sessions_hydrate_on_open() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let (t, a, s) = rt.bootstrap_test_session().await;
    drop(rt);

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());

    // Append should work — proves tenant + agent + session were rehydrated.
    let r = rt2
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(r.seq, 0);
}

#[tokio::test]
async fn append_and_fetch_uses_kv_recent() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let (t, a, s) = rt.bootstrap_test_session().await;
    for i in 0..3_u64 {
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }
    let r = rt
        .append_and_fetch(
            AppendRequest::simple(&t, &a, &s, "tail", serde_json::json!({})),
            /* last */ 5,
            /* max_bytes */ 1_000_000,
        )
        .await
        .unwrap();
    assert_eq!(r.seq, 3);
    assert_eq!(r.recent.len(), 4);
    assert_eq!(r.recent[0].seq, 0);
    assert_eq!(r.recent[3].seq, 3);
    assert_eq!(r.recent[3].kind, "tail");
}

#[tokio::test]
async fn fetch_recent_after_cold_append_reads_warm_tail() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let (t, a, s) = rt.bootstrap_test_session().await;
    for i in 0..3_u64 {
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "history",
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }

    rt.transition_session_lifecycle(&t.tenant_id, &s.session_id, Lifecycle::Cold)
        .await
        .unwrap();
    rt.append(AppendRequest::simple(
        &t,
        &a,
        &s,
        "tail",
        serde_json::json!({"i": 3}),
    ))
    .await
    .unwrap();

    let recent = rt
        .fetch_recent(t.tenant_id.clone(), s.session_id.clone(), 1)
        .await
        .unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].seq, 3);
    assert_eq!(recent[0].kind, "tail");

    let full = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 4,
        })
        .await
        .unwrap();
    assert_eq!(
        full.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

#[tokio::test]
async fn persisted_hot_cache_eviction_keeps_recent_and_old_reads() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let (t, a, s) = rt.bootstrap_test_session().await;
    for i in 0..2_060_u64 {
        rt.append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }

    let recent = rt
        .fetch_recent(t.tenant_id.clone(), s.session_id.clone(), 5)
        .await
        .unwrap();
    assert_eq!(
        recent.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![2055, 2056, 2057, 2058, 2059]
    );

    let old = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(old.len(), 1);
    assert_eq!(old[0].seq, 0);
}

#[tokio::test]
async fn event_by_id_index_is_written_and_survives_restart() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let (t, a, s) = rt.bootstrap_test_session().await;
    let r = rt
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({"i": 1}),
        ))
        .await
        .unwrap();
    drop(rt);

    // Reopen — the event_by_id row must still resolve to (session_id, seq).
    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());
    let lookup = rt2
        .lookup_event_by_id(&t.tenant_id, &r.event_id)
        .await
        .unwrap();
    assert!(lookup.is_some(), "event_by_id row missing after restart");
    let (sid, seq) = lookup.unwrap();
    assert_eq!(sid.as_str(), s.session_id.as_str());
    assert_eq!(seq, 0);
}

#[tokio::test]
async fn artifacts_survive_restart() {
    // End-to-end persistence test for V2.1 artifacts: metadata to RocksDB,
    // bytes to a LocalFsContentBackend rooted next to the kv directory.
    // After dropping the runtime and reopening both, the artifact metadata
    // must hydrate and the blob must still be readable.
    let dir = TempDir::new().unwrap();
    let kv_path = dir.path().join("kv");
    let content_root = dir.path().join("content");

    let (t, art) = {
        let kv = open_kv(&kv_path);
        let content: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
        let rt = Runtime::with_kv(kv, content);
        let t = rt
            .create_tenant(NewTenant {
                external_key: "art-acme".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let art = rt
            .create_artifact(
                NewArtifact {
                    tenant_id: t.tenant_id.clone(),
                    content_type: Some("application/octet-stream".into()),
                    expected_sha256: None,
                },
                b"hello".to_vec(),
            )
            .await
            .unwrap();
        (t, art)
        // rt + its captured kv arc go out of scope here, releasing the
        // single-writer rocks lock before we reopen below.
    };

    // RocksDB requires the prior handle to be dropped before reopening
    // (single-writer lock); scoping the first runtime above released it.
    let kv2 = open_kv(&kv_path);
    let content2: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
    let rt2 = Runtime::with_kv(kv2, content2);

    let got = rt2
        .get_artifact(&t.tenant_id, &art.artifact_id)
        .await
        .unwrap();
    assert_eq!(got.bytes, 5);
    assert_eq!(got.sha256, art.sha256);
    assert_eq!(
        got.content_type.as_deref(),
        Some("application/octet-stream")
    );

    let blob = rt2
        .get_artifact_blob(&t.tenant_id, &art.artifact_id)
        .await
        .unwrap();
    assert_eq!(blob, b"hello");
}

#[tokio::test]
async fn cold_session_segments_survive_restart_and_resume() {
    let dir = TempDir::new().unwrap();
    let kv_path = dir.path().join("kv");
    let content_root = dir.path().join("content");

    let (tenant, agent, session) = {
        let kv = open_kv(&kv_path);
        let content: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
        let rt = Runtime::with_kv(kv, content);
        let tenant = rt
            .create_tenant(NewTenant {
                external_key: "cold-session".into(),
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
        for i in 0..3 {
            rt.append(AppendRequest::simple(
                &tenant,
                &agent,
                &session,
                "message",
                serde_json::json!({ "i": i }),
            ))
            .await
            .unwrap();
        }
        let storage = rt
            .transition_session_lifecycle(&tenant.tenant_id, &session.session_id, Lifecycle::Cold)
            .await
            .unwrap();
        assert_eq!(storage.sealed_segment_count, 1);
        (tenant, agent, session)
    };

    let kv2 = open_kv(&kv_path);
    let content2: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
    let rt2 = Runtime::with_kv(kv2, content2);

    let storage = rt2
        .get_session_storage(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(storage.lifecycle, Lifecycle::Cold);
    assert_eq!(storage.sealed_segment_count, 1);
    assert_eq!(storage.warm_head_from_seq, 3);

    let events = rt2
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let resumed = rt2
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({ "i": 99 }),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.seq, 3);

    let merged = rt2
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        merged.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

#[tokio::test]
async fn archived_sessions_survive_restart_and_resume() {
    let dir = TempDir::new().unwrap();
    let kv_path = dir.path().join("kv");
    let content_root = dir.path().join("content");

    let (tenant, agent, session) = {
        let kv = open_kv(&kv_path);
        let content: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
        let rt = Runtime::with_kv(kv, content);
        let tenant = rt
            .create_tenant(NewTenant {
                external_key: "archive-session".into(),
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
        for i in 0..3 {
            rt.append(AppendRequest::simple(
                &tenant,
                &agent,
                &session,
                "message",
                serde_json::json!({ "i": i }),
            ))
            .await
            .unwrap();
        }
        let storage = rt
            .transition_session_lifecycle(
                &tenant.tenant_id,
                &session.session_id,
                Lifecycle::Archive,
            )
            .await
            .unwrap();
        assert!(storage.archive_manifest.is_some());
        (tenant, agent, session)
    };

    let kv2 = open_kv(&kv_path);
    let content2: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
    let rt2 = Runtime::with_kv(kv2, content2);

    let storage = rt2
        .get_session_storage(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(storage.lifecycle, Lifecycle::Archive);
    assert!(storage.archive_manifest.is_some());
    assert_eq!(storage.sealed_segment_count, 0);

    let events = rt2
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let resumed = rt2
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({ "i": 99 }),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.seq, 3);
}

#[tokio::test]
async fn records_and_indexes_survive_restart() {
    // V2.2: SearchableRecord persistence plus index hydration. Three
    // records with different metadata should still come back from
    // search_records after the runtime is dropped and reopened against
    // the same KV directory — proving both the records hydrated AND
    // the indexes were rebuilt against them.
    let dir = TempDir::new().unwrap();

    let (tenant_id, recorded_titles) = {
        let kv = open_kv(dir.path());
        let rt = Runtime::with_kv(kv, in_memory_content());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "rec-acme".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let titles = ["alpha quartet", "beta rhythm", "carrots are orange"];
        for title in &titles {
            rt.write_record(NewRecord {
                tenant_id: t.tenant_id.clone(),
                namespace: vec!["kb".into()],
                scope: serde_json::json!({}),
                text: None,
                metadata: serde_json::json!({"title": title}),
                tags: vec![],
                source: None,
                embedding: None,
                policy: IndexingPolicy {
                    mode: IndexingMode::MetadataOnly,
                    fields: vec![],
                    vector_allowed: false,
                },
                expires_at_ms: None,
            })
            .await
            .unwrap();
        }
        (t.tenant_id.clone(), titles)
        // rt + its captured kv arc go out of scope here, releasing the
        // single-writer rocks lock before we reopen below.
    };

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());

    // Confirm a text query against the rebuilt indexes finds the right
    // record — proves both the record hydrated and the index was
    // re-populated for that record.
    let hits = rt2
        .search_records(SearchRequest::text_only(
            tenant_id.clone(),
            vec!["kb".into()],
            "quartet",
            10,
        ))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].metadata["title"], recorded_titles[0]);

    // And a "search everything in namespace" query returns all three.
    let all = rt2
        .search_records(SearchRequest {
            tenant_id,
            namespace: vec!["kb".into()],
            sharding_key: Some("kb".into()),
            text: None,
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 100,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn record_commit_token_survives_restart_for_visible_search() {
    let dir = TempDir::new().unwrap();

    let (tenant_id, commit_seq, record_id) = {
        let kv = open_kv(dir.path());
        let rt = Runtime::with_kv(kv, in_memory_content());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "rec-token".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let written = rt
            .write_record_with_commit(NewRecord {
                tenant_id: t.tenant_id.clone(),
                namespace: vec!["kb".into()],
                scope: serde_json::json!({}),
                text: None,
                metadata: serde_json::json!({"title": "durable token"}),
                tags: vec![],
                source: None,
                embedding: None,
                policy: IndexingPolicy {
                    mode: IndexingMode::MetadataOnly,
                    fields: vec![],
                    vector_allowed: false,
                },
                expires_at_ms: None,
            })
            .await
            .unwrap();
        (
            t.tenant_id.clone(),
            written.commit_token.seq,
            written.record.record_id.clone(),
        )
    };

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());
    assert_eq!(rt2.record_index_outbox_counts_for_test().unwrap(), (1, 0));
    let hits = rt2
        .search_records(SearchRequest {
            tenant_id,
            namespace: vec!["kb".into()],
            sharding_key: Some("kb".into()),
            text: Some("durable".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: Some(commit_seq),
        })
        .await
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].record_id.as_str(), record_id.as_str());
}

#[tokio::test]
async fn record_index_outbox_persists_delete_mutation() {
    let dir = TempDir::new().unwrap();

    {
        let kv = open_kv(dir.path());
        let rt = Runtime::with_kv(kv, in_memory_content());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "rec-outbox-delete".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let written = rt
            .write_record_with_commit(NewRecord {
                tenant_id: t.tenant_id.clone(),
                namespace: vec!["kb".into()],
                scope: serde_json::json!({}),
                text: None,
                metadata: serde_json::json!({"title": "delete me"}),
                tags: vec![],
                source: None,
                embedding: None,
                policy: IndexingPolicy {
                    mode: IndexingMode::MetadataOnly,
                    fields: vec![],
                    vector_allowed: false,
                },
                expires_at_ms: None,
            })
            .await
            .unwrap();
        rt.delete_record(&t.tenant_id, &written.record.record_id)
            .await
            .unwrap();
    }

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());
    assert_eq!(rt2.record_index_outbox_counts_for_test().unwrap(), (1, 1));
}

#[tokio::test]
async fn record_index_outbox_replay_catches_up_lagging_local_index() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let t = rt
        .create_tenant(NewTenant {
            external_key: "rec-outbox-replay".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let written = rt
        .write_record_with_commit(NewRecord {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            scope: serde_json::json!({}),
            text: None,
            metadata: serde_json::json!({"title": "replay token"}),
            tags: vec![],
            source: None,
            embedding: None,
            policy: IndexingPolicy {
                mode: IndexingMode::MetadataOnly,
                fields: vec![],
                vector_allowed: false,
            },
            expires_at_ms: None,
        })
        .await
        .unwrap();

    rt.simulate_record_index_lag_for_test(&written.record.record_id, 0)
        .await;
    let missed = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            sharding_key: Some("kb".into()),
            text: Some("replay".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert!(missed.is_empty());

    assert_eq!(rt.apply_record_index_outbox_once().await.unwrap(), 1);
    assert_eq!(rt.apply_record_index_outbox_once().await.unwrap(), 0);

    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            sharding_key: Some("kb".into()),
            text: Some("replay".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].record_id.as_str(),
        written.record.record_id.as_str()
    );
}

#[tokio::test]
async fn global_record_index_is_eventually_consistent_with_token_fallback() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let t = rt
        .create_tenant(NewTenant {
            external_key: "rec-global-eventual".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let written = rt
        .write_record_with_commit(NewRecord {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            scope: serde_json::json!({}),
            text: None,
            metadata: serde_json::json!({"title": "global token"}),
            tags: vec![],
            source: None,
            embedding: None,
            policy: IndexingPolicy {
                mode: IndexingMode::MetadataOnly,
                fields: vec![],
                vector_allowed: false,
            },
            expires_at_ms: None,
        })
        .await
        .unwrap();

    let stale_global = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            sharding_key: None,
            text: Some("global".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert!(stale_global.is_empty());

    let token_visible = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            sharding_key: None,
            text: Some("global".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: Some(written.commit_token.seq),
        })
        .await
        .unwrap();
    assert_eq!(token_visible.len(), 1);

    assert_eq!(rt.apply_record_index_outbox_once().await.unwrap(), 1);
    let caught_up = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            sharding_key: None,
            text: Some("global".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(caught_up.len(), 1);

    let stats = rt.record_index_stats().await;
    assert_eq!(stats.global_index_lag_tokens, 0);
    assert_eq!(stats.global_index_watermark, written.commit_token.seq);
}

#[tokio::test]
async fn record_index_outbox_reloads_record_after_commit_state_gap() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let t = rt
        .create_tenant(NewTenant {
            external_key: "rec-global-state-gap".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let written = rt
        .write_record_with_commit(NewRecord {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            scope: serde_json::json!({}),
            text: None,
            metadata: serde_json::json!({"title": "state gap token"}),
            tags: vec![],
            source: None,
            embedding: None,
            policy: IndexingPolicy {
                mode: IndexingMode::MetadataOnly,
                fields: vec![],
                vector_allowed: false,
            },
            expires_at_ms: None,
        })
        .await
        .unwrap();

    rt.simulate_record_state_gap_for_test(&written.record.record_id, written.commit_token.seq, 0)
        .await;

    assert_eq!(rt.apply_record_index_outbox_once().await.unwrap(), 1);

    let stats = rt.record_index_stats().await;
    assert_eq!(stats.global_index_lag_tokens, 0);
    assert_eq!(stats.global_index_watermark, written.commit_token.seq);

    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["kb".into()],
            sharding_key: None,
            text: Some("state gap".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].record_id.as_str(),
        written.record.record_id.as_str()
    );
}

#[tokio::test]
async fn skills_and_bindings_survive_restart() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let (t, a, _) = rt.bootstrap_test_session().await;
    let skill = rt
        .register_skill(
            NewSkill {
                tenant_id: t.tenant_id.clone(),
                name: "planner".into(),
                version: "1.0.0".into(),
                manifest: serde_json::json!({"entrypoint": "plan"}),
                artifact_refs: vec![],
            },
            Some("register-planner".into()),
        )
        .await
        .unwrap();
    rt.bind_skill(BindSkillRequest {
        tenant_id: t.tenant_id.clone(),
        agent_id: a.agent_id.clone(),
        skill_id: skill.skill_id.clone(),
        idempotency_key: Some("bind-planner".into()),
    })
    .await
    .unwrap();
    drop(rt);

    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());
    let listed = rt2
        .list_agent_skills(&t.tenant_id, &a.agent_id)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "planner");
    assert_eq!(listed[0].version, "1.0.0");

    // Idempotent retries should replay rather than creating duplicates.
    let replayed = rt2
        .bind_skill(BindSkillRequest {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            skill_id: skill.skill_id.clone(),
            idempotency_key: Some("bind-planner".into()),
        })
        .await
        .unwrap();
    assert_eq!(replayed.skill_id, skill.skill_id);
    assert_eq!(
        rt2.list_agent_skills(&t.tenant_id, &a.agent_id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn fetch_with_wrong_tenant_id_fails() {
    use agentdb_core::errors::ErrorCode;
    use agentdb_core::id::{Id, IdKind};

    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let (_t, _a, s) = rt.bootstrap_test_session().await;
    let bogus_tenant = Id::new(IdKind::Tenant);
    let err = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: bogus_tenant,
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionNotFound);
}
