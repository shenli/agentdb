use std::collections::BTreeMap;
use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::id::{Id, IdKind};
use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, ReplicatedLog, RocksKvStore,
    SingleNodeReplicatedLog,
};
use tempfile::TempDir;

fn content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

fn local_tablet_runtime(tablet_count: i64) -> (TempDir, Arc<Runtime>) {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let logs: BTreeMap<i64, Arc<dyn ReplicatedLog + Send + Sync>> = (1..=tablet_count)
        .map(|tablet_id| {
            (
                tablet_id,
                Arc::new(SingleNodeReplicatedLog::new(kv.clone()))
                    as Arc<dyn ReplicatedLog + Send + Sync>,
            )
        })
        .collect();
    (
        dir,
        Runtime::with_kv_and_replicated_logs(kv, content(), logs),
    )
}

#[tokio::test]
async fn local_tablet_key_routes_session_writes() {
    let (_dir, rt) = local_tablet_runtime(4);
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "tenant".into(),
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

    let session_id = (0..128)
        .map(|_| Id::new(IdKind::Session))
        .find(|id| {
            rt.local_tablet_for_key(id.as_str())
                .is_some_and(|tablet| tablet != 1)
        })
        .expect("expected a generated session id to map to a non-primary tablet");
    let target_tablet = rt.local_tablet_for_key(session_id.as_str()).unwrap();

    let session = rt
        .with_local_tablet_for_key(
            session_id.as_str(),
            rt.create_session_with_id(
                NewSession {
                    tenant_id: tenant.tenant_id.clone(),
                    agent_id: agent.agent_id.clone(),
                    alias: None,
                },
                session_id.clone(),
            ),
        )
        .await
        .unwrap();
    rt.with_local_tablet_for_key(
        session.session_id.as_str(),
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"i": 1}),
        )),
    )
    .await
    .unwrap();

    let stats = rt.local_tablet_storage_stats().unwrap();
    let target = stats
        .iter()
        .find(|stats| stats.tablet_id == target_tablet)
        .expect("target tablet stats");
    assert!(
        target.key_count > 0,
        "expected local session writes on tablet {target_tablet}, stats: {stats:?}"
    );
    assert_eq!(
        rt.fetch_recent(tenant.tenant_id, session.session_id, 1)
            .await
            .unwrap()
            .len(),
        1
    );
}
