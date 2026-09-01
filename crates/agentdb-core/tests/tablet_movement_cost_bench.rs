//! Baseline measurement for the manifest-based tablet-attachment evaluation
//! (hardening §7). Today a placement/replacement copies the tablet's FULL
//! runtime state as one snapshot payload (`export_tablet_snapshot_payload`).
//! This benchmark quantifies how that movement cost grows with the tablet's
//! session state, on the session-native workload (append → commit → apply).
//! The evidence it produces is what the go/no-go decision needs: if the full
//! snapshot dwarfs the "recent tail" a lazy-block attach would transfer, the
//! feature is worth prototyping; if not, it is not.
//!
//! This is a deliberate benchmark, not a CI test — it is `#[ignore]`d. Run:
//!   cargo test -p agentdb-core --test tablet_movement_cost_bench -- \
//!     --ignored --nocapture
//! and read the printed table. See
//! docs/design/tablet-attachment-evaluation.md for how to interpret it.

use std::sync::Arc;
use std::time::Instant;

use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::tenant::NewTenant;
use agentdb_core::{NewAgent, NewSession};
use agentdb_storage::{
    ContentBackend, InMemoryContentBackend, KvStore, RaftRsNodeConfig, RaftRsNodeRole,
    RaftRsSharedGroup, ReplicatedLog, RocksKvStore,
};
use tempfile::TempDir;

fn content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

#[tokio::test]
#[ignore = "deliberate benchmark; run with --ignored --nocapture"]
async fn movement_snapshot_cost_grows_with_session_state() {
    let dir = TempDir::new().unwrap();
    let raft_config = vec![RaftRsNodeConfig {
        node_id: 1,
        role: RaftRsNodeRole::Voter,
        raft_engine_dir: dir.path().join("raft-n1"),
    }];
    let (group, log) = RaftRsSharedGroup::open_local(1, raft_config, 1).unwrap();
    group.elect(1).unwrap();

    let kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(dir.path().join("kv-n1")).unwrap());
    let log: Arc<dyn ReplicatedLog + Send + Sync> = log;
    let rt = Runtime::with_kv_and_replicated_log(kv, content(), log);

    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "movement-bench".into(),
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

    // A representative event payload (~200 bytes of JSON, the kind of
    // message an agent session accumulates).
    let payload = serde_json::json!({
        "role": "assistant",
        "text": "x".repeat(180),
    });

    let checkpoints = [100usize, 500, 2_000, 8_000];
    let mut appended = 0usize;
    let mut first_bytes = 0usize;

    println!();
    println!("§7 baseline — full-state movement snapshot vs session size");
    println!(
        "{:>8}  {:>14}  {:>12}  {:>14}",
        "events", "snapshot_bytes", "export_ms", "bytes/event"
    );
    for &target in &checkpoints {
        while appended < target {
            rt.append(AppendRequest::simple(
                &tenant,
                &agent,
                &session,
                "message",
                payload.clone(),
            ))
            .await
            .unwrap();
            appended += 1;
        }
        let started = Instant::now();
        let snapshot = rt
            .export_tablet_snapshot_payload(agentdb_core::RUNTIME_PRIMARY_TABLET_ID)
            .unwrap();
        let export_ms = started.elapsed().as_secs_f64() * 1000.0;
        let bytes = snapshot.len();
        if first_bytes == 0 {
            first_bytes = bytes;
        }
        println!(
            "{:>8}  {:>14}  {:>12.1}  {:>14.1}",
            appended,
            bytes,
            export_ms,
            bytes as f64 / appended as f64
        );
    }

    let last_bytes = rt
        .export_tablet_snapshot_payload(agentdb_core::RUNTIME_PRIMARY_TABLET_ID)
        .unwrap()
        .len();
    println!();
    println!(
        "Full-state movement cost grew {:.1}x from {} to {} events \
         (a lazy-block attach would transfer ~the recent tail instead, roughly constant). \
         See docs/design/tablet-attachment-evaluation.md for the decision gate.",
        last_bytes as f64 / first_bytes.max(1) as f64,
        checkpoints[0],
        checkpoints[checkpoints.len() - 1],
    );

    // Sanity: the snapshot genuinely grows with state (this is the whole
    // point — movement cost is O(total state), not O(tail)).
    assert!(
        last_bytes > first_bytes * 4,
        "expected the full-state snapshot to grow substantially with session size"
    );
}
