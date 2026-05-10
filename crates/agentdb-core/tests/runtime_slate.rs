use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::session::{Lifecycle, NewSession};
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{ContentBackend, KvStore, LocalFsContentBackend, SlateKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

async fn open_slate_kv(root: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
    Arc::new(SlateKvStore::open(store, "agentdb/slate").await.unwrap())
}

#[tokio::test]
async fn archived_session_storage_works_with_slate_kv() {
    let kv_root = TempDir::new().unwrap();
    let content_root = TempDir::new().unwrap();

    let kv = open_slate_kv(kv_root.path()).await;
    let content: Arc<dyn ContentBackend> =
        Arc::new(LocalFsContentBackend::new(content_root.path()));
    let rt = Runtime::with_kv(kv, content);

    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "slate-archive".into(),
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
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }

    let archived = rt
        .transition_session_lifecycle(&tenant.tenant_id, &session.session_id, Lifecycle::Archive)
        .await
        .unwrap();
    assert_eq!(archived.lifecycle, Lifecycle::Archive);
    assert!(archived.archive_manifest.is_some());

    let restored = rt
        .restore_session_from_archive(&tenant.tenant_id, &session.session_id, Lifecycle::Cold)
        .await
        .unwrap();
    assert_eq!(restored.lifecycle, Lifecycle::Cold);
    assert_eq!(restored.sealed_segment_count, 1);
    assert!(restored.archive_manifest.is_none());

    let events = rt
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
}

#[tokio::test]
async fn archived_session_storage_survives_slate_restart_and_resume() {
    let kv_root = TempDir::new().unwrap();
    let content_root = TempDir::new().unwrap();

    let (tenant, agent, session) = {
        let kv = open_slate_kv(kv_root.path()).await;
        let content: Arc<dyn ContentBackend> =
            Arc::new(LocalFsContentBackend::new(content_root.path()));
        let rt = Runtime::with_kv(kv, content);

        let tenant = rt
            .create_tenant(NewTenant {
                external_key: "slate-restart".into(),
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
                serde_json::json!({"i": i}),
            ))
            .await
            .unwrap();
        }

        let archived = rt
            .transition_session_lifecycle(
                &tenant.tenant_id,
                &session.session_id,
                Lifecycle::Archive,
            )
            .await
            .unwrap();
        assert_eq!(archived.lifecycle, Lifecycle::Archive);
        assert!(archived.archive_manifest.is_some());
        (tenant, agent, session)
    };

    let kv = open_slate_kv(kv_root.path()).await;
    let content: Arc<dyn ContentBackend> =
        Arc::new(LocalFsContentBackend::new(content_root.path()));
    let rt = Runtime::with_kv(kv, content);

    let archived = rt
        .get_session_storage(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(archived.lifecycle, Lifecycle::Archive);
    assert!(archived.archive_manifest.is_some());

    let restored = rt
        .restore_session_from_archive(&tenant.tenant_id, &session.session_id, Lifecycle::Cold)
        .await
        .unwrap();
    assert_eq!(restored.lifecycle, Lifecycle::Cold);
    assert_eq!(restored.sealed_segment_count, 1);
    assert!(restored.archive_manifest.is_none());

    let resumed = rt
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"i": 99}),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.seq, 3);
}
