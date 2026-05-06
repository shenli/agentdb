use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use agentdb_core::runtime::Runtime;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, RaftAppendEntriesRequest,
    RaftAppendEntriesResponse, RaftLogTransport, RaftQuorumReplicatedLog, RaftRsNodeConfig,
    RaftRsSharedGroup, RocksKvStore,
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
