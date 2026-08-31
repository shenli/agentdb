use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use agentdb_core::record::{IndexingMode, IndexingPolicy, NewRecord, RecordStatus};
use agentdb_core::runtime::{
    AckExportConsumerRequest, AppendRequest, CreateExportConsumerRequest, ExportWatchRequest,
    NewRecordEdge, RecordEdgeType, RecordUpdate, Runtime, SearchRequest, WatchScope,
};
use agentdb_core::tenant::NewTenant;
use agentdb_core::{NewAgent, NewSession, RUNTIME_PRIMARY_TABLET_ID};
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, RaftRsNodeConfig, RaftRsNodeRole,
    RaftRsReplicatedLog, RaftRsSharedGroup, ReplicatedLog, RocksKvStore, SingleNodeReplicatedLog,
};
use tempfile::TempDir;

fn content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

struct NetworkedRaftRsRuntimeCluster {
    _dir: TempDir,
    groups: BTreeMap<u64, RaftRsSharedGroup>,
    logs: BTreeMap<u64, Arc<RaftRsReplicatedLog>>,
    kv_dirs: BTreeMap<u64, PathBuf>,
}

impl NetworkedRaftRsRuntimeCluster {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let raft_configs: Vec<_> = (1..=3)
            .map(|node_id| RaftRsNodeConfig {
                node_id,
                role: RaftRsNodeRole::Voter,
                raft_engine_dir: dir.path().join(format!("raft-n{node_id}")),
            })
            .collect();
        let mut groups = BTreeMap::new();
        let mut logs = BTreeMap::new();
        let mut kv_dirs = BTreeMap::new();
        for node_id in 1..=3 {
            let (group, log) = RaftRsSharedGroup::open_local(
                RUNTIME_PRIMARY_TABLET_ID,
                raft_configs.clone(),
                node_id,
            )
            .unwrap();
            groups.insert(node_id, group);
            logs.insert(node_id, log);
            kv_dirs.insert(node_id, dir.path().join(format!("kv-n{node_id}")));
        }
        Self {
            _dir: dir,
            groups,
            logs,
            kv_dirs,
        }
    }

    fn runtime(&self, node_id: u64) -> Arc<Runtime> {
        let kv: Arc<dyn KvStore + Send + Sync> =
            Arc::new(RocksKvStore::open(&self.kv_dirs[&node_id]).unwrap());
        let log: Arc<dyn ReplicatedLog + Send + Sync> = self.logs[&node_id].clone();
        Runtime::with_kv_and_replicated_log(kv, content(), log)
    }

    async fn pump_until<F>(&self, isolated_nodes: &HashSet<u64>, mut done: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..2_000 {
            self.pump_once(isolated_nodes);
            if done() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!("raft-rs network did not reach expected condition");
    }

    fn pump_once(&self, isolated_nodes: &HashSet<u64>) -> usize {
        for (node_id, group) in &self.groups {
            if !isolated_nodes.contains(node_id) {
                group.tick(*node_id).unwrap();
            }
        }
        let mut messages = Vec::new();
        for group in self.groups.values() {
            messages.extend(group.take_wire_messages());
        }
        let delivered = messages.len();
        for message in messages {
            if isolated_nodes.contains(&message.from) || isolated_nodes.contains(&message.to) {
                continue;
            }
            self.groups
                .get(&message.to)
                .expect("target raft group")
                .step_wire_message(message)
                .unwrap();
        }
        delivered
    }

    fn drain(&self) {
        let isolated = HashSet::new();
        for _ in 0..50 {
            if self.pump_once(&isolated) == 0 {
                break;
            }
        }
    }

    async fn elect_leader(&self, node_id: u64) {
        self.groups[&node_id].campaign(node_id).unwrap();
        self.pump_until(&HashSet::new(), || {
            self.groups[&node_id]
                .status()
                .iter()
                .any(|status| status.node_id == node_id && status.role == "Leader")
        })
        .await;
    }
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

#[tokio::test]
async fn runtime_raft_rs_replicates_record_memory_primitives() {
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
            external_key: "raft-rs-record-memory".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let policy = IndexingPolicy {
        mode: IndexingMode::SummaryOnly,
        fields: vec![],
        vector_allowed: false,
    };
    let old = leader_runtime
        .write_record_with_commit(NewRecord {
            tenant_id: tenant.tenant_id.clone(),
            external_id: Some("memory-old".into()),
            namespace: vec!["memory".into()],
            scope: serde_json::json!({"agent_id": "agent-a"}),
            text: Some("replicated old memory".into()),
            metadata: serde_json::json!({"kind": "memory"}),
            tags: vec!["memory".into()],
            source: None,
            embedding: None,
            policy: policy.clone(),
            expires_at_ms: None,
        })
        .await
        .unwrap();
    let new = leader_runtime
        .write_record(NewRecord {
            tenant_id: tenant.tenant_id.clone(),
            external_id: Some("memory-new".into()),
            namespace: vec!["memory".into()],
            scope: serde_json::json!({"agent_id": "agent-a"}),
            text: Some("replicated new memory".into()),
            metadata: serde_json::json!({"kind": "memory"}),
            tags: vec!["memory".into()],
            source: None,
            embedding: None,
            policy,
            expires_at_ms: None,
        })
        .await
        .unwrap();
    leader_runtime
        .update_record_with_commit(RecordUpdate {
            tenant_id: tenant.tenant_id.clone(),
            record_id: old.record.record_id.clone(),
            expected_version: Some(old.record.version),
            namespace: None,
            scope: None,
            text: None,
            metadata: None,
            tags: None,
            source: None,
            embedding: None,
            policy: None,
            status: Some(RecordStatus::Superseded),
            expires_at_ms: None,
        })
        .await
        .unwrap();
    let edge = leader_runtime
        .create_record_edge(NewRecordEdge {
            tenant_id: tenant.tenant_id.clone(),
            source_record_id: new.record_id.clone(),
            target_record_id: old.record.record_id.clone(),
            edge_type: RecordEdgeType::Supersedes,
            metadata: serde_json::json!({"reason": "raft-test"}),
        })
        .await
        .unwrap();
    leader_runtime
        .set_tenant_meta(
            &tenant.tenant_id,
            "db0.cursor",
            serde_json::json!({"offset": 9}),
        )
        .await
        .unwrap();

    let follower_runtime =
        Runtime::with_kv_and_replicated_log(follower_kv, content(), logs[1].clone());
    let replicated_old = follower_runtime
        .get_record(&tenant.tenant_id, &old.record.record_id)
        .await
        .unwrap();
    assert_eq!(replicated_old.external_id.as_deref(), Some("memory-old"));
    assert_eq!(replicated_old.status, RecordStatus::Superseded);

    let history = follower_runtime
        .search_record_matches(SearchRequest {
            tenant_id: tenant.tenant_id.clone(),
            namespace: vec!["memory".into()],
            sharding_key: Some("memory".into()),
            text: Some("old".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: true,
            min_score: Some(0.0),
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].record.record_id, old.record.record_id);

    let edges = follower_runtime
        .record_edges(&tenant.tenant_id, &old.record.record_id)
        .await
        .unwrap();
    assert_eq!(edges, vec![edge]);
    let meta = follower_runtime
        .get_tenant_meta(&tenant.tenant_id, "db0.cursor")
        .await
        .unwrap();
    assert_eq!(meta.value, serde_json::json!({"offset": 9}));
}

#[tokio::test]
async fn runtime_raft_rs_log_compaction_reduces_tablet_compaction_debt() {
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
    let leader_runtime = Runtime::with_kv_and_replicated_log(leader_kv, content(), logs[0].clone());
    leader_runtime
        .create_tenant(NewTenant {
            external_key: "raft-rs-debt-tenant".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    let before = leader_runtime.local_tablet_storage_stats().unwrap();
    assert_eq!(before.len(), 1);
    assert!(before[0].applied_index > 0);
    assert!(before[0].compaction_debt_ops > 0);

    let compacted = leader_runtime
        .compact_tablet_log_through(RUNTIME_PRIMARY_TABLET_ID, before[0].applied_index)
        .unwrap();
    assert!(compacted > 0);

    let after = leader_runtime.local_tablet_storage_stats().unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].compaction_debt_ops, 0);
}

#[tokio::test]
async fn runtime_raft_rs_restarts_after_log_compaction_and_continues_writes() {
    let dir = TempDir::new().unwrap();
    let raft_configs: Vec<_> = (1..=3)
        .map(|node_id| RaftRsNodeConfig {
            node_id,
            role: RaftRsNodeRole::Voter,
            raft_engine_dir: dir.path().join(format!("raft-n{node_id}")),
        })
        .collect();
    let (group, logs) =
        RaftRsSharedGroup::open(RUNTIME_PRIMARY_TABLET_ID, raft_configs.clone()).unwrap();
    group.elect(1).unwrap();

    let kv_path = dir.path().join("kv-n1");
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(&kv_path).unwrap());
    let runtime = Runtime::with_kv_and_replicated_log(kv, content(), logs[0].clone());
    let before_restart = runtime
        .create_tenant(NewTenant {
            external_key: "before-compact-restart".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let applied_index = runtime.local_tablet_storage_stats().unwrap()[0].applied_index;
    assert!(
        runtime
            .compact_tablet_log_through(RUNTIME_PRIMARY_TABLET_ID, applied_index)
            .unwrap()
            > 0
    );
    drop(runtime);
    drop(logs);
    drop(group);

    let (reopened_group, reopened_logs) =
        RaftRsSharedGroup::open(RUNTIME_PRIMARY_TABLET_ID, raft_configs).unwrap();
    reopened_group.elect(1).unwrap();
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(&kv_path).unwrap());
    let reopened = Runtime::with_kv_and_replicated_log(kv, content(), reopened_logs[0].clone());
    let after_restart = reopened
        .create_tenant(NewTenant {
            external_key: "after-compact-restart".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    assert_eq!(
        reopened
            .get_tenant(&before_restart.tenant_id)
            .await
            .unwrap()
            .external_key,
        "before-compact-restart"
    );
    assert_eq!(
        reopened
            .get_tenant(&after_restart.tenant_id)
            .await
            .unwrap()
            .external_key,
        "after-compact-restart"
    );
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

    groups[&1].campaign(1).unwrap();
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
        if groups[&1]
            .status()
            .iter()
            .any(|status| status.node_id == 1 && status.role == "Leader")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_rs_failure_injection_commits_with_follower_loss_and_catches_up_after_rejoin() {
    let cluster = NetworkedRaftRsRuntimeCluster::new();
    cluster.elect_leader(1).await;
    let leader_runtime = cluster.runtime(1);
    let create_a_runtime = leader_runtime.clone();
    let create_a = tokio::spawn(async move {
        create_a_runtime
            .create_tenant(NewTenant {
                external_key: "partition-a".into(),
                policy: serde_json::json!({}),
            })
            .await
    });

    let isolated = HashSet::from([3]);
    cluster
        .pump_until(&isolated, || create_a.is_finished())
        .await;
    let tenant_a = create_a.await.unwrap().unwrap();

    let follower_two = cluster.runtime(2);
    assert_eq!(
        follower_two
            .get_tenant(&tenant_a.tenant_id)
            .await
            .unwrap()
            .external_key,
        "partition-a"
    );
    assert!(cluster
        .runtime(3)
        .get_tenant(&tenant_a.tenant_id)
        .await
        .is_err());

    let create_b_runtime = leader_runtime.clone();
    let create_b = tokio::spawn(async move {
        create_b_runtime
            .create_tenant(NewTenant {
                external_key: "partition-b".into(),
                policy: serde_json::json!({}),
            })
            .await
    });
    cluster
        .pump_until(&HashSet::new(), || create_b.is_finished())
        .await;
    let tenant_b = create_b.await.unwrap().unwrap();
    cluster.drain();

    let rejoined = cluster.runtime(3);
    assert_eq!(
        rejoined
            .get_tenant(&tenant_a.tenant_id)
            .await
            .unwrap()
            .external_key,
        "partition-a"
    );
    assert_eq!(
        rejoined
            .get_tenant(&tenant_b.tenant_id)
            .await
            .unwrap()
            .external_key,
        "partition-b"
    );
}

#[tokio::test]
async fn raft_rs_failure_injection_recovers_old_leader_with_snapshot_after_leader_loss() {
    let dir = TempDir::new().unwrap();
    let raft_configs: Vec<_> = (1..=3)
        .map(|node_id| RaftRsNodeConfig {
            node_id,
            role: RaftRsNodeRole::Voter,
            raft_engine_dir: dir.path().join(format!("raft-n{node_id}")),
        })
        .collect();
    let (group, logs) = RaftRsSharedGroup::open(RUNTIME_PRIMARY_TABLET_ID, raft_configs).unwrap();
    group.elect(1).unwrap();

    let kv1: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n1")).unwrap());
    let kv2: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n2")).unwrap());
    let old_leader = Runtime::with_kv_and_replicated_log(kv1, content(), logs[0].clone());
    let new_leader = Runtime::with_kv_and_replicated_log(kv2, content(), logs[1].clone());

    let before_loss = old_leader
        .create_tenant(NewTenant {
            external_key: "before-leader-loss".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    group.mark_unavailable(1).unwrap();
    for _ in 0..50 {
        if group.elect(2).is_ok() {
            break;
        }
        group.tick(2).unwrap();
        group.tick(3).unwrap();
    }
    let after_loss = new_leader
        .create_tenant(NewTenant {
            external_key: "after-leader-loss".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    let kv3: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n3")).unwrap());
    let surviving_follower = Runtime::with_kv_and_replicated_log(kv3, content(), logs[2].clone());
    assert_eq!(
        surviving_follower
            .get_tenant(&before_loss.tenant_id)
            .await
            .unwrap()
            .external_key,
        "before-leader-loss"
    );
    assert_eq!(
        surviving_follower
            .get_tenant(&after_loss.tenant_id)
            .await
            .unwrap()
            .external_key,
        "after-leader-loss"
    );

    group.mark_available(1).unwrap();
    let after_rejoin = new_leader
        .create_tenant(NewTenant {
            external_key: "after-old-leader-rejoin".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let application_snapshot = new_leader
        .export_tablet_snapshot_payload(RUNTIME_PRIMARY_TABLET_ID)
        .unwrap();
    let transfer = group
        .build_snapshot_transfer(2, 1, application_snapshot)
        .unwrap();
    let application_snapshot = group.install_snapshot_transfer(1, transfer).unwrap();
    drop(old_leader);
    let kv1: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n1")).unwrap());
    let rejoined_old_leader = Runtime::with_kv_and_replicated_log(kv1, content(), logs[0].clone());
    assert!(rejoined_old_leader
        .install_tablet_snapshot_payload_and_rehydrate(
            RUNTIME_PRIMARY_TABLET_ID,
            &application_snapshot
        )
        .await
        .unwrap());
    assert_eq!(
        rejoined_old_leader
            .get_tenant(&after_loss.tenant_id)
            .await
            .unwrap()
            .external_key,
        "after-leader-loss"
    );
    assert_eq!(
        rejoined_old_leader
            .get_tenant(&after_rejoin.tenant_id)
            .await
            .unwrap()
            .external_key,
        "after-old-leader-rejoin"
    );
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
async fn runtime_tablet_snapshot_preserves_export_ledger_high_watermark_and_consumers() {
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

    let seed_runtime = source_runtime.clone();
    let (tenant, event_cursor, source_high_watermark) = source_runtime
        .with_tablet_context(7, async move {
            let tenant = seed_runtime
                .create_tenant(NewTenant {
                    external_key: "export-snapshot-tenant".into(),
                    policy: serde_json::json!({}),
                })
                .await
                .unwrap();
            let agent = seed_runtime
                .create_agent(NewAgent {
                    tenant_id: tenant.tenant_id.clone(),
                    alias: None,
                    config: serde_json::json!({}),
                })
                .await
                .unwrap();
            let session = seed_runtime
                .create_session(NewSession {
                    tenant_id: tenant.tenant_id.clone(),
                    agent_id: agent.agent_id.clone(),
                    alias: None,
                })
                .await
                .unwrap();
            seed_runtime
                .append(AppendRequest::simple(
                    &tenant,
                    &agent,
                    &session,
                    "snapshot-event",
                    serde_json::json!({"value": 1}),
                ))
                .await
                .unwrap();

            let feed = seed_runtime
                .export_watch(ExportWatchRequest {
                    tenant_id: tenant.tenant_id.clone(),
                    scope: WatchScope::Tenant,
                    session_id: None,
                    agent_id: None,
                    include: vec!["events".into()],
                    from_cursor: None,
                    limit: 10,
                })
                .await
                .unwrap();
            assert_eq!(feed.len(), 1);
            assert_eq!(feed[0].item.data["kind"], "snapshot-event");

            seed_runtime
                .create_export_consumer(CreateExportConsumerRequest {
                    tenant_id: tenant.tenant_id.clone(),
                    consumer_name: "clickhouse".into(),
                    scope: WatchScope::Tenant,
                    agent_id: None,
                    session_id: None,
                    include: vec!["events".into()],
                })
                .await
                .unwrap();
            seed_runtime
                .ack_export_consumer(AckExportConsumerRequest {
                    tenant_id: tenant.tenant_id.clone(),
                    consumer_name: "clickhouse".into(),
                    cursor: Some(feed[0].cursor.clone()),
                    snapshot_high_watermark: Some(feed[0].cursor.clone()),
                    lease_owner: None,
                })
                .await
                .unwrap();
            let status = seed_runtime
                .get_export_consumer_status(&tenant.tenant_id, "clickhouse")
                .await
                .unwrap();
            assert_eq!(status.pending_items, 0);
            assert_eq!(
                status.high_watermark.as_deref(),
                Some(feed[0].cursor.as_str())
            );

            let high_watermark = seed_runtime
                .export_high_watermark(&tenant.tenant_id)
                .await
                .unwrap()
                .expect("source export high watermark");
            (tenant, feed[0].cursor.clone(), high_watermark)
        })
        .await;
    assert_eq!(source_high_watermark, event_cursor);

    let payload = source_runtime.export_tablet_snapshot_payload(7).unwrap();

    let target_dir = TempDir::new().unwrap();
    let target_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
    let target_primary_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_tablet_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_runtime = Runtime::with_kv_and_replicated_logs(
        target_kv,
        content(),
        BTreeMap::from([
            (RUNTIME_PRIMARY_TABLET_ID, target_primary_log),
            (7, target_tablet_log),
        ]),
    );
    assert!(target_runtime
        .install_tablet_snapshot_payload_and_rehydrate(7, &payload)
        .await
        .unwrap());

    let restored_runtime = target_runtime.clone();
    target_runtime
        .with_tablet_context(7, async move {
            let restored_high_watermark = restored_runtime
                .export_high_watermark(&tenant.tenant_id)
                .await
                .unwrap();
            assert_eq!(
                restored_high_watermark.as_deref(),
                Some(event_cursor.as_str())
            );

            let restored_feed = restored_runtime
                .export_watch(ExportWatchRequest {
                    tenant_id: tenant.tenant_id.clone(),
                    scope: WatchScope::Tenant,
                    session_id: None,
                    agent_id: None,
                    include: vec!["events".into()],
                    from_cursor: None,
                    limit: 10,
                })
                .await
                .unwrap();
            assert_eq!(restored_feed.len(), 1);
            assert_eq!(restored_feed[0].cursor, event_cursor);
            assert_eq!(restored_feed[0].item.data["kind"], "snapshot-event");

            let restored_consumer = restored_runtime
                .get_export_consumer(&tenant.tenant_id, "clickhouse")
                .await
                .unwrap();
            assert_eq!(
                restored_consumer.last_acked_cursor.as_deref(),
                Some(event_cursor.as_str())
            );
            assert_eq!(
                restored_consumer.snapshot_high_watermark.as_deref(),
                Some(event_cursor.as_str())
            );

            let restored_status = restored_runtime
                .get_export_consumer_status(&tenant.tenant_id, "clickhouse")
                .await
                .unwrap();
            assert_eq!(restored_status.pending_items, 0);
            assert_eq!(
                restored_status.high_watermark.as_deref(),
                Some(event_cursor.as_str())
            );
        })
        .await;
}

#[tokio::test]
async fn runtime_exports_retargeted_shard_snapshot_for_split_install() {
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
                    external_key: "retargeted-tablet-tenant".into(),
                    policy: serde_json::json!({}),
                })
                .await
        })
        .await
        .unwrap();
    let payload = source_runtime
        .export_tablet_snapshot_payload_for_shard_range(7, 8, 0, 63, 64)
        .unwrap();

    let target_dir = TempDir::new().unwrap();
    let target_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
    let target_primary_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_tablet_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_runtime = Runtime::with_kv_and_replicated_logs(
        target_kv,
        content(),
        BTreeMap::from([
            (RUNTIME_PRIMARY_TABLET_ID, target_primary_log),
            (8, target_tablet_log),
        ]),
    );
    assert!(target_runtime
        .install_tablet_snapshot_payload_and_rehydrate(8, &payload)
        .await
        .unwrap());

    let read_runtime = target_runtime.clone();
    let hydrated = target_runtime
        .with_tablet_context(8, async move {
            read_runtime.get_tenant(&tenant.tenant_id).await
        })
        .await
        .unwrap();
    assert_eq!(hydrated.external_key, "retargeted-tablet-tenant");
}

#[tokio::test]
async fn split_destination_snapshot_install_does_not_regress_source_session_sequence() {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let source_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
    let destination_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
    let runtime = Runtime::with_kv_and_replicated_logs(
        kv,
        content(),
        BTreeMap::from([
            (RUNTIME_PRIMARY_TABLET_ID, source_log),
            (8, destination_log),
        ]),
    );

    let seed_runtime = runtime.clone();
    let (tenant, agent, session) = runtime
        .with_tablet_context(RUNTIME_PRIMARY_TABLET_ID, async move {
            let tenant = seed_runtime
                .create_tenant(NewTenant {
                    external_key: "split-source-seq-tenant".into(),
                    policy: serde_json::json!({}),
                })
                .await
                .unwrap();
            let agent = seed_runtime
                .create_agent(NewAgent {
                    tenant_id: tenant.tenant_id.clone(),
                    alias: None,
                    config: serde_json::json!({}),
                })
                .await
                .unwrap();
            let session = seed_runtime
                .create_session(NewSession {
                    tenant_id: tenant.tenant_id.clone(),
                    agent_id: agent.agent_id.clone(),
                    alias: None,
                })
                .await
                .unwrap();
            let first = seed_runtime
                .append(AppendRequest::simple(
                    &tenant,
                    &agent,
                    &session,
                    "before-snapshot",
                    serde_json::json!({"seq": 0}),
                ))
                .await
                .unwrap();
            assert_eq!(first.seq, 0);
            (tenant, agent, session)
        })
        .await;

    let stale_destination_payload = runtime
        .export_tablet_snapshot_payload_for_shard_range(RUNTIME_PRIMARY_TABLET_ID, 8, 0, 63, 64)
        .unwrap();

    let source_runtime = runtime.clone();
    let second_tenant = tenant.clone();
    let second_agent = agent.clone();
    let second_session = session.clone();
    runtime
        .with_tablet_context(RUNTIME_PRIMARY_TABLET_ID, async move {
            let second = source_runtime
                .append(AppendRequest::simple(
                    &second_tenant,
                    &second_agent,
                    &second_session,
                    "after-snapshot-export",
                    serde_json::json!({"seq": 1}),
                ))
                .await
                .unwrap();
            assert_eq!(second.seq, 1);
        })
        .await;

    assert!(runtime
        .install_tablet_snapshot_payload(8, &stale_destination_payload)
        .unwrap());

    let append_runtime = runtime.clone();
    runtime
        .with_tablet_context(RUNTIME_PRIMARY_TABLET_ID, async move {
            let third = append_runtime
                .append(AppendRequest::simple(
                    &tenant,
                    &agent,
                    &session,
                    "after-destination-install",
                    serde_json::json!({"seq": 2}),
                ))
                .await
                .unwrap();
            assert_eq!(third.seq, 2);
        })
        .await;
}

#[tokio::test]
async fn runtime_merge_installs_snapshot_without_deleting_existing_target_rows() {
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
    let source_tenant = source_runtime
        .with_tablet_context(7, async move {
            create_runtime
                .create_tenant(NewTenant {
                    external_key: "merge-source-tenant".into(),
                    policy: serde_json::json!({}),
                })
                .await
        })
        .await
        .unwrap();
    let payload = source_runtime
        .export_tablet_snapshot_payload_for_shard_range(7, 9, 0, 63, 64)
        .unwrap();

    let target_dir = TempDir::new().unwrap();
    let target_kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
    let target_primary_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_tablet_log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(target_kv.clone()));
    let target_runtime = Runtime::with_kv_and_replicated_logs(
        target_kv,
        content(),
        BTreeMap::from([
            (RUNTIME_PRIMARY_TABLET_ID, target_primary_log),
            (9, target_tablet_log),
        ]),
    );
    let create_target_runtime = target_runtime.clone();
    let target_tenant = target_runtime
        .with_tablet_context(9, async move {
            create_target_runtime
                .create_tenant(NewTenant {
                    external_key: "merge-target-tenant".into(),
                    policy: serde_json::json!({}),
                })
                .await
        })
        .await
        .unwrap();

    assert!(target_runtime
        .install_tablet_snapshot_payload_merge_and_rehydrate(9, &payload)
        .await
        .unwrap());

    let source_read_runtime = target_runtime.clone();
    let source_hydrated = target_runtime
        .with_tablet_context(9, async move {
            source_read_runtime
                .get_tenant(&source_tenant.tenant_id)
                .await
        })
        .await
        .unwrap();
    let target_read_runtime = target_runtime.clone();
    let target_hydrated = target_runtime
        .with_tablet_context(9, async move {
            target_read_runtime
                .get_tenant(&target_tenant.tenant_id)
                .await
        })
        .await
        .unwrap();
    assert_eq!(source_hydrated.external_key, "merge-source-tenant");
    assert_eq!(target_hydrated.external_key, "merge-target-tenant");
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

/// Stage 3 regression: after a failover, the new leader's record commit-seq
/// counter must be past every sequence the old leader replicated (learned via
/// the incremental outbox drain, not a full rehydrate), and external-id
/// dedupe must hold across the failover.
#[tokio::test]
async fn failover_leader_continues_commit_seqs_and_honors_replicated_external_ids() {
    let dir = TempDir::new().unwrap();
    let raft_configs: Vec<_> = (1..=3)
        .map(|node_id| RaftRsNodeConfig {
            node_id,
            role: RaftRsNodeRole::Voter,
            raft_engine_dir: dir.path().join(format!("raft-n{node_id}")),
        })
        .collect();
    let (group, logs) = RaftRsSharedGroup::open(RUNTIME_PRIMARY_TABLET_ID, raft_configs).unwrap();
    group.elect(1).unwrap();

    let kv1: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n1")).unwrap());
    let kv2: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n2")).unwrap());
    let old_leader = Runtime::with_kv_and_replicated_log(kv1, content(), logs[0].clone());
    let new_leader = Runtime::with_kv_and_replicated_log(kv2, content(), logs[1].clone());

    let tenant = old_leader
        .create_tenant(NewTenant {
            external_key: "stage3-failover".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let policy = IndexingPolicy {
        mode: IndexingMode::SummaryOnly,
        fields: vec![],
        vector_allowed: false,
    };
    let mut max_seq = 0;
    for i in 0..5 {
        let written = old_leader
            .write_record_with_commit(NewRecord {
                tenant_id: tenant.tenant_id.clone(),
                external_id: Some(format!("stage3-{i}")),
                namespace: vec!["stage3".into()],
                scope: serde_json::json!({}),
                text: Some(format!("payload before failover {i}")),
                metadata: serde_json::json!({"i": i}),
                tags: vec![],
                source: None,
                embedding: None,
                policy: policy.clone(),
                expires_at_ms: None,
            })
            .await
            .unwrap();
        max_seq = max_seq.max(written.commit_token.seq);
    }

    group.mark_unavailable(1).unwrap();
    for _ in 0..50 {
        if group.elect(2).is_ok() {
            break;
        }
        group.tick(2).unwrap();
        group.tick(3).unwrap();
    }

    // New leader mints a commit seq strictly past everything replicated.
    let written = new_leader
        .write_record_with_commit(NewRecord {
            tenant_id: tenant.tenant_id.clone(),
            external_id: Some("stage3-after".into()),
            namespace: vec!["stage3".into()],
            scope: serde_json::json!({}),
            text: Some("payload after failover".into()),
            metadata: serde_json::json!({}),
            tags: vec![],
            source: None,
            embedding: None,
            policy: policy.clone(),
            expires_at_ms: None,
        })
        .await
        .unwrap();
    assert!(
        written.commit_token.seq > max_seq,
        "post-failover commit seq {} must exceed replicated max {}",
        written.commit_token.seq,
        max_seq
    );

    // External ids claimed before the failover dedupe on the new leader:
    // upserting stage3-2 must update the replicated record, not duplicate it.
    let upserted = new_leader
        .upsert_record_with_commit(NewRecord {
            tenant_id: tenant.tenant_id.clone(),
            external_id: Some("stage3-2".into()),
            namespace: vec!["stage3".into()],
            scope: serde_json::json!({}),
            text: Some("rewritten across failover".into()),
            metadata: serde_json::json!({"rewritten": true}),
            tags: vec![],
            source: None,
            embedding: None,
            policy,
            expires_at_ms: None,
        })
        .await
        .unwrap();
    assert!(
        upserted.record.version > 1,
        "upsert must have updated the replicated record (version {}), not created a duplicate",
        upserted.record.version
    );
    assert_eq!(
        new_leader
            .list_records(&tenant.tenant_id)
            .await
            .unwrap()
            .len(),
        6,
        "5 pre-failover records + 1 post-failover, no external-id duplicates"
    );
}
