//! Restart-safety tests for the KV-backed V2 Runtime (Task 11).
//!
//! Each test opens a fresh `RocksKvStore` against a TempDir, drives some
//! traffic through `Runtime::with_kv`, drops the runtime, reopens the same
//! KV directory, and asserts the next runtime sees the prior state.

use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::artifact::NewArtifact;
use agentdb_core::record::{IndexingMode, IndexingPolicy, NewRecord};
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime, SearchRequest};
use agentdb_core::session::NewSession;
use agentdb_core::skill::{BindSkillRequest, NewSkill};
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, LocalFsContentBackend, RocksKvStore,
};
use tempfile::TempDir;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    Arc::new(RocksKvStore::open(path).expect("open rocks kv"))
}

/// Convenience for restart tests that don't exercise artifacts: pair every
/// `with_kv` with a fresh in-memory content backend.
fn in_memory_content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

#[tokio::test]
async fn events_and_next_seq_survive_restart() {
    let dir = TempDir::new().unwrap();

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
    drop(rt);

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
            text: None,
            embedding: None,
            tags: vec![],
            source_filter: None,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 3);
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
