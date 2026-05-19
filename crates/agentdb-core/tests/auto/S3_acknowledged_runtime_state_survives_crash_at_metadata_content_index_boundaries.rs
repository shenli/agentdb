// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S3 acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries
// Falsifies: C4, C8, C10, C12
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

use agentdb_core::{
    artifact::NewArtifact,
    record::{IndexingPolicy, NewRecord},
    runtime::{AppendRequest, FetchRequest, Runtime, SearchRequest},
};
use agentdb_storage::{
    content::sha256_hex, ContentBackend, KvStore, LocalFsContentBackend, RocksKvStore,
};
use tempfile::TempDir;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    static STORES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<dyn KvStore + Send + Sync>>>> =
        OnceLock::new();
    let stores = STORES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut stores = stores.lock().expect("kv registry poisoned");
    if let Some(kv) = stores.get(path) {
        return kv.clone();
    }
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(path).unwrap());
    stores.insert(path.to_path_buf(), kv.clone());
    kv
}

#[tokio::test]
async fn acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries() {
    let dir = TempDir::new().unwrap();
    let content_dir = TempDir::new().unwrap();
    let content: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(content_dir.path()));
    let rt = Runtime::with_kv(open_kv(dir.path()), content.clone());
    let (tenant, agent, session) = rt.bootstrap_test_session().await;

    let artifact = rt
        .create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: Some(sha256_hex(b"artifact bytes")),
            },
            b"artifact bytes".to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(
        rt.create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: Some("0000".into()),
            },
            b"wrong".to_vec(),
        )
        .await
        .unwrap_err()
        .code,
        agentdb_core::ErrorCode::HashMismatch
    );

    let record = rt
        .write_record_with_commit(NewRecord {
            tenant_id: tenant.tenant_id.clone(),
            namespace: vec!["atomicity".into()],
            scope: serde_json::json!({}),
            text: Some("boundary needle".into()),
            metadata: serde_json::json!({"title": "boundary needle"}),
            tags: vec!["probe".into()],
            source: None,
            embedding: None,
            policy: IndexingPolicy::default(),
            expires_at_ms: None,
        })
        .await
        .unwrap();
    rt.simulate_record_index_lag_for_test(&record.record.record_id, 0)
        .await;

    let append = rt
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "artifact_ref",
            serde_json::json!({ "artifact_id": artifact.artifact_id.as_str() }),
        ))
        .await
        .unwrap();
    drop(rt);

    let reopened = Runtime::with_kv(open_kv(dir.path()), content);
    assert_eq!(
        reopened
            .get_artifact_blob(&tenant.tenant_id, &artifact.artifact_id)
            .await
            .unwrap(),
        b"artifact bytes"
    );
    let hits = reopened
        .search_records(SearchRequest {
            tenant_id: tenant.tenant_id.clone(),
            namespace: vec!["atomicity".into()],
            sharding_key: Some("atomicity".into()),
            text: Some("needle".into()),
            embedding: None,
            tags: vec!["probe".into()],
            source_filter: None,
            limit: 10,
            min_visible_record_commit_seq: Some(record.commit_token.seq),
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].record_id, record.record.record_id);
    let events = reopened
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id,
            session_id: session.session_id,
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(events[0].event_id, append.event_id);
}
