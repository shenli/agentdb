use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use agentdb_core::runtime::Runtime;
use agentdb_core::tenant::NewTenant;
use agentdb_core::RUNTIME_PRIMARY_TABLET_ID;
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, RaftAppendEntriesRequest,
    RaftAppendEntriesResponse, RaftLogTransport, RaftQuorumReplicatedLog, RaftRsNodeConfig,
    RaftRsNodeRole, RaftRsSharedGroup, ReplicatedLog, RocksKvStore, SingleNodeReplicatedLog,
};
use tempfile::TempDir;

#[derive(Default)]
struct InMemoryRaftTransport {
    nodes: Mutex<HashMap<String, Arc<RaftQuorumReplicatedLog>>>,
    blocked: Mutex<HashSet<String>>,
}

impl InMemoryRaftTransport {
    fn insert(&self, node_id: &str, log: Arc<RaftQuorumReplicatedLog>) {
        self.nodes.lock().unwrap().insert(node_id.into(), log);
    }

    fn block(&self, node_id: &str) {
        self.blocked.lock().unwrap().insert(node_id.into());
    }
}

impl RaftLogTransport for InMemoryRaftTransport {
    fn append_entries(
        &self,
        target_node_id: &str,
        request: RaftAppendEntriesRequest,
    ) -> agentdb_storage::Result<RaftAppendEntriesResponse> {
        if self.blocked.lock().unwrap().contains(target_node_id) {
            return Err(agentdb_storage::Error::Other(format!(
                "node {target_node_id} unreachable"
            )));
        }
        let node = self
            .nodes
            .lock()
            .unwrap()
            .get(target_node_id)
            .cloned()
            .ok_or_else(|| agentdb_storage::Error::Other("missing raft node".into()))?;
        node.receive_append_entries(request)
    }
}

struct QuorumNode {
    _dir: TempDir,
    kv: Arc<dyn KvStore + Send + Sync>,
    log: Arc<RaftQuorumReplicatedLog>,
}

fn quorum_node(
    node_id: &str,
    voters: Vec<String>,
    transport: Arc<InMemoryRaftTransport>,
) -> QuorumNode {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let log =
        Arc::new(RaftQuorumReplicatedLog::new(kv.clone(), node_id, voters, 1, transport).unwrap());
    QuorumNode { _dir: dir, kv, log }
}

fn content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

#[tokio::test]
async fn runtime_persisted_writes_replicate_to_raft_quorum_followers() {
    let transport = Arc::new(InMemoryRaftTransport::default());
    let voters = vec!["n1".into(), "n2".into(), "n3".into()];
    let leader = quorum_node("n1", voters.clone(), transport.clone());
    let follower_a = quorum_node("n2", voters.clone(), transport.clone());
    let follower_b = quorum_node("n3", voters, transport.clone());
    transport.insert("n1", leader.log.clone());
    transport.insert("n2", follower_a.log.clone());
    transport.insert("n3", follower_b.log.clone());

    let leader_runtime =
        Runtime::with_kv_and_replicated_log(leader.kv.clone(), content(), leader.log.clone());
    let tenant = leader_runtime
        .create_tenant(NewTenant {
            external_key: "quorum-tenant".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    let follower_runtime =
        Runtime::with_kv_and_replicated_log(follower_a.kv.clone(), content(), follower_a.log);

    let hydrated = follower_runtime
        .get_tenant(&tenant.tenant_id)
        .await
        .unwrap();
    assert_eq!(hydrated.external_key, "quorum-tenant");
}

#[tokio::test]
async fn runtime_persisted_writes_replicate_with_raft_rs_quorum() {
    let dir = TempDir::new().unwrap();
    let raft_configs: Vec<_> = (1..=3)
        .map(|node_id| RaftRsNodeConfig {
            node_id,
            role: RaftRsNodeRole::Voter,
            raft_engine_dir: dir.path().join(format!("raft-n{node_id}")),
        })
        .collect();
    let (group, logs) = RaftRsSharedGroup::open(1, raft_configs).unwrap();
    group.elect(1).unwrap();

    let leader_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n1")).unwrap());
    let follower_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n2")).unwrap());

    let leader_runtime = Runtime::with_kv_and_replicated_log(leader_kv, content(), logs[0].clone());
    let tenant = leader_runtime
        .create_tenant(NewTenant {
            external_key: "raft-rs-tenant".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    let follower_runtime =
        Runtime::with_kv_and_replicated_log(follower_kv, content(), logs[1].clone());
    let hydrated = follower_runtime
        .get_tenant(&tenant.tenant_id)
        .await
        .unwrap();
    assert_eq!(hydrated.external_key, "raft-rs-tenant");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_persisted_writes_replicate_with_networked_raft_rs_quorum() {
    let dir = TempDir::new().unwrap();
    let raft_configs: Vec<_> = (1..=3)
        .map(|node_id| RaftRsNodeConfig {
            node_id,
            role: RaftRsNodeRole::Voter,
            raft_engine_dir: dir.path().join(format!("raft-n{node_id}")),
        })
        .collect();
    let (group_1, log_1) =
        RaftRsSharedGroup::open_local(RUNTIME_PRIMARY_TABLET_ID, raft_configs.clone(), 1).unwrap();
    let (group_2, log_2) =
        RaftRsSharedGroup::open_local(RUNTIME_PRIMARY_TABLET_ID, raft_configs.clone(), 2).unwrap();
    let (group_3, _log_3) =
        RaftRsSharedGroup::open_local(RUNTIME_PRIMARY_TABLET_ID, raft_configs, 3).unwrap();
    let groups = BTreeMap::from([(1, group_1), (2, group_2), (3, group_3)]);

    let leader_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n1")).unwrap());
    let follower_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n2")).unwrap());
    let leader_runtime = Runtime::with_kv_and_replicated_log(leader_kv, content(), log_1);

    let create = tokio::spawn(async move {
        leader_runtime
            .create_tenant(NewTenant {
                external_key: "raft-rs-network-tenant".into(),
                policy: serde_json::json!({}),
            })
            .await
    });
    for _ in 0..1_000 {
        let mut messages = Vec::new();
        for group in groups.values() {
            messages.extend(group.take_wire_messages());
        }
        for message in messages {
            groups
                .get(&message.to)
                .expect("target raft group")
                .step_wire_message(message)
                .unwrap();
        }
        if create.is_finished() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let tenant = create.await.unwrap().unwrap();

    let follower_runtime = Runtime::with_kv_and_replicated_log(follower_kv, content(), log_2);
    let hydrated = follower_runtime
        .get_tenant(&tenant.tenant_id)
        .await
        .unwrap();
    assert_eq!(hydrated.external_key, "raft-rs-network-tenant");
}

#[tokio::test]
async fn runtime_persisted_write_fails_without_raft_quorum() {
    let transport = Arc::new(InMemoryRaftTransport::default());
    let voters = vec!["n1".into(), "n2".into(), "n3".into()];
    let leader = quorum_node("n1", voters, transport.clone());
    transport.insert("n1", leader.log.clone());
    transport.block("n2");
    transport.block("n3");

    let leader_runtime =
        Runtime::with_kv_and_replicated_log(leader.kv.clone(), content(), leader.log);
    let err = leader_runtime
        .create_tenant(NewTenant {
            external_key: "quorum-failure".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap_err();

    assert!(err.message.contains("raft quorum not reached"));
}

#[tokio::test]
async fn runtime_tablet_context_routes_writes_and_snapshots_by_tablet() {
    let source_dir = TempDir::new().unwrap();
    let source_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(source_dir.path()).unwrap());
    let source_primary_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(source_kv.clone()));
    let source_tablet_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(source_kv.clone()));
    let source_runtime = Runtime::with_kv_and_replicated_logs(
        source_kv,
        content(),
        BTreeMap::from([
            (RUNTIME_PRIMARY_TABLET_ID, source_primary_log),
            (7, source_tablet_log),
        ]),
    );

    let create_runtime = source_runtime.clone();
    let tenant = source_runtime
        .with_tablet_context(7, async move {
            create_runtime
                .create_tenant(NewTenant {
                    external_key: "tablet-seven-tenant".into(),
                    policy: serde_json::json!({}),
                })
                .await
        })
        .await
        .unwrap();
    let payload = source_runtime.export_tablet_snapshot_payload(7).unwrap();

    let target_dir = TempDir::new().unwrap();
    let target_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
    let target_primary_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_tablet_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_logs = BTreeMap::from([
        (RUNTIME_PRIMARY_TABLET_ID, target_primary_log),
        (7, target_tablet_log),
    ]);
    let target_runtime =
        Runtime::with_kv_and_replicated_logs(target_kv.clone(), content(), target_logs.clone());
    assert!(target_runtime
        .install_tablet_snapshot_payload_and_rehydrate(7, &payload)
        .await
        .unwrap());

    let live_read_runtime = target_runtime.clone();
    let live_tenant_id = tenant.tenant_id.clone();
    let live_hydrated = target_runtime
        .with_tablet_context(7, async move {
            live_read_runtime.get_tenant(&live_tenant_id).await
        })
        .await
        .unwrap();
    assert_eq!(live_hydrated.external_key, "tablet-seven-tenant");

    let restarted_runtime = Runtime::with_kv_and_replicated_logs(target_kv, content(), target_logs);
    let read_runtime = restarted_runtime.clone();
    let hydrated = restarted_runtime
        .with_tablet_context(7, async move {
            read_runtime.get_tenant(&tenant.tenant_id).await
        })
        .await
        .unwrap();
    assert_eq!(hydrated.external_key, "tablet-seven-tenant");
}

#[tokio::test]
async fn runtime_can_register_new_replicated_tablets_after_startup() {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let primary_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
    let runtime = Runtime::with_kv_and_replicated_logs(
        kv.clone(),
        content(),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, primary_log)]),
    );
    assert!(!runtime.registered_tablet_ids().contains(&7));

    let tablet_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv));
    assert!(runtime.register_replicated_tablet(7, tablet_log).unwrap());
    assert!(runtime.registered_tablet_ids().contains(&7));

    let create_runtime = runtime.clone();
    let tenant = runtime
        .with_tablet_context(7, async move {
            create_runtime
                .create_tenant(NewTenant {
                    external_key: "late-tablet".into(),
                    policy: serde_json::json!({}),
                })
                .await
        })
        .await
        .unwrap();
    let read_runtime = runtime.clone();
    let hydrated = runtime
        .with_tablet_context(7, async move {
            read_runtime.get_tenant(&tenant.tenant_id).await
        })
        .await
        .unwrap();
    assert_eq!(hydrated.external_key, "late-tablet");

    assert!(runtime.unregister_replicated_tablet(7).unwrap());
    assert!(!runtime.registered_tablet_ids().contains(&7));
}
