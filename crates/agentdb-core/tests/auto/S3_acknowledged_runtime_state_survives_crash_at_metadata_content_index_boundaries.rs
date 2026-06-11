// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S3 acknowledged_runtime_state_survives_crash_at_metadata_content_index_boundaries
// Falsifies: C4, C8, C10, C12
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

use std::{
    collections::BTreeMap,
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
};

use agentdb_core::{
    artifact::NewArtifact,
    id::{Id, IdKind},
    record::{IndexingPolicy, NewRecord},
    runtime::{
        AppendRequest, ExportWatchRequest, FetchRequest, Runtime, SearchRequest, WatchScope,
    },
};
use agentdb_storage::{
    content::{sha256_hex, PutResult},
    ContentBackend, KvStore, LocalFsContentBackend, RocksKvStore,
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

#[derive(Default)]
struct PartialThenFailContentBackend {
    writes: Mutex<Vec<(String, String, Vec<u8>)>>,
}

impl PartialThenFailContentBackend {
    fn last_artifact_id(&self) -> Id {
        let writes = self.writes.lock().expect("content writes lock poisoned");
        Id::parse_kind(
            &writes.last().expect("content put attempted").1,
            IdKind::Artifact,
        )
        .expect("artifact id parses")
    }
}

impl ContentBackend for PartialThenFailContentBackend {
    fn put<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        tenant_id: &'life1 str,
        artifact_id: &'life2 str,
        bytes: &'life3 [u8],
    ) -> Pin<Box<dyn Future<Output = io::Result<PutResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let partial_len = bytes.len().min(4);
            self.writes
                .lock()
                .expect("content writes lock poisoned")
                .push((
                    tenant_id.to_string(),
                    artifact_id.to_string(),
                    bytes[..partial_len].to_vec(),
                ));
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "simulated partial content write failure",
            ))
        })
    }

    fn get<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        tenant_id: &'life1 str,
        artifact_id: &'life2 str,
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.writes
                .lock()
                .expect("content writes lock poisoned")
                .iter()
                .rev()
                .find(|(tenant, artifact, _)| tenant == tenant_id && artifact == artifact_id)
                .map(|(_, _, bytes)| bytes.clone())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing blob"))
        })
    }

    fn head<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        tenant_id: &'life1 str,
        artifact_id: &'life2 str,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            Ok(self
                .writes
                .lock()
                .expect("content writes lock poisoned")
                .iter()
                .any(|(tenant, artifact, _)| tenant == tenant_id && artifact == artifact_id))
        })
    }

    fn delete<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        tenant_id: &'life1 str,
        artifact_id: &'life2 str,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.writes
                .lock()
                .expect("content writes lock poisoned")
                .retain(|(tenant, artifact, _)| tenant != tenant_id || artifact != artifact_id);
            Ok(())
        })
    }
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
    let first_export = rt
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(session.session_id.clone()),
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(first_export.len(), 1);
    let first_export_cursor = first_export[0].cursor.clone();
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
            metadata_filter: None,
            limit: 10,
            min_visible_record_commit_seq: Some(record.commit_token.seq),
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].record_id, record.record.record_id);
    let events = reopened
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(events[0].event_id, append.event_id);

    let after_reopen_empty = reopened
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(session.session_id.clone()),
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: Some(first_export_cursor.clone()),
            limit: 10,
        })
        .await
        .unwrap();
    assert!(after_reopen_empty.is_empty());
    let second = reopened
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "post_restart",
            serde_json::json!({ "after": "reopen" }),
        ))
        .await
        .unwrap();
    let after_second_export = reopened
        .export_watch(ExportWatchRequest {
            tenant_id: tenant.tenant_id.clone(),
            scope: WatchScope::Session,
            session_id: Some(session.session_id.clone()),
            agent_id: None,
            include: vec!["events".into()],
            from_cursor: Some(first_export_cursor),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(after_second_export.len(), 1);
    assert_eq!(
        after_second_export[0].item.object_id,
        second.event_id.as_str()
    );
}

#[tokio::test]
async fn failed_partial_content_write_does_not_publish_artifact_metadata() {
    let dir = TempDir::new().unwrap();
    let content_dir = TempDir::new().unwrap();
    let failing_content = Arc::new(PartialThenFailContentBackend::default());
    let rt = Runtime::with_kv(open_kv(dir.path()), failing_content.clone());
    let (tenant, _, _) = rt.bootstrap_test_session().await;

    let err = rt
        .create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: Some(sha256_hex(b"partial bytes")),
            },
            b"partial bytes".to_vec(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, agentdb_core::ErrorCode::Internal);

    let failed_artifact_id = failing_content.last_artifact_id();
    assert_eq!(
        rt.get_artifact(&tenant.tenant_id, &failed_artifact_id)
            .await
            .unwrap_err()
            .code,
        agentdb_core::ErrorCode::ArtifactNotFound
    );
    drop(rt);

    let reopened = Runtime::with_kv(
        open_kv(dir.path()),
        Arc::new(LocalFsContentBackend::new(content_dir.path())),
    );
    assert_eq!(
        reopened
            .get_artifact(&tenant.tenant_id, &failed_artifact_id)
            .await
            .unwrap_err()
            .code,
        agentdb_core::ErrorCode::ArtifactNotFound
    );

    let recovered = reopened
        .create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: Some(sha256_hex(b"recovered bytes")),
            },
            b"recovered bytes".to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get_artifact_blob(&tenant.tenant_id, &recovered.artifact_id)
            .await
            .unwrap(),
        b"recovered bytes"
    );
}
