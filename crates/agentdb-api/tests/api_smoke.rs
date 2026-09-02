//! End-to-end smoke tests for the V2.0 HTTP routes.
//!
//! These tests drive the router over real HTTP via `reqwest`, exercising the
//! same path that an external client would: serde DTO → handler → runtime →
//! JSON response. The runtime is in-memory, so each test gets a clean state.

mod common;

/// Operational reports are staleness-filtered against the real clock by the
/// admin endpoints, so tests seed with fresh timestamps.
fn recent_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use agentdb_api::{
    internal_raft_http::{RaftRsHttpClient, RaftRsHttpOutbox},
    raft_ingress::RaftRsGroupRegistry,
    routing_http::{
        RouteRuntime, RouteSnapshot, HDR_OWNER_NODE, HDR_ROUTE_ACTION, HDR_ROUTE_REASON,
        HDR_SHARD_ID, HDR_TABLET_ID,
    },
    state::{ProxyPolicy, RaftRsIngress, TabletStoreStatus},
    AppState,
};
use agentdb_core::{
    runtime::FetchRequest, Id, IdKind, NewAgent, NewSession, NewTenant, Runtime,
    RUNTIME_PRIMARY_TABLET_ID,
};
use agentdb_meta::{
    cells, node_operational, nodes::Node, placement_scopes, tablet_snapshots, tablets,
    tablets::Tablet, MetaStore,
};
use agentdb_storage::{
    AsyncMirrorContentBackend, ContentBackend, GroupedRocksDbTabletStore, InMemoryContentBackend,
    InMemoryKvStore, KvStore, RaftRsNodeConfig, RaftRsNodeRole, RaftRsReplicatedLog,
    RaftRsSharedGroup, RaftRsSnapshotTransfer, RaftRsWireMessage, ReplicatedLog, RocksKvStore,
    SingleNodeReplicatedLog,
};
use base64::Engine as _;
use common::{spawn_app, spawn_app_with_state, TestApp};
use serde_json::json;
use tempfile::TempDir;

#[derive(Default)]
struct TestRaftIngress {
    messages: Mutex<Vec<RaftRsWireMessage>>,
    snapshots: Mutex<Vec<RaftRsSnapshotTransfer>>,
}

impl RaftRsIngress for TestRaftIngress {
    fn step_wire_message(&self, message: RaftRsWireMessage) -> agentdb_storage::Result<()> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }

    fn install_snapshot_transfer(
        &self,
        _to: u64,
        transfer: RaftRsSnapshotTransfer,
    ) -> agentdb_storage::Result<Vec<u8>> {
        let payload = transfer.payload.clone();
        self.snapshots.lock().unwrap().push(transfer);
        Ok(payload)
    }
}

fn test_raft_group() -> (TempDir, RaftRsSharedGroup, Vec<Arc<RaftRsReplicatedLog>>) {
    let dir = TempDir::new().unwrap();
    let configs: Vec<_> = (1..=3)
        .map(|node_id| RaftRsNodeConfig {
            node_id,
            role: RaftRsNodeRole::Voter,
            raft_engine_dir: dir.path().join(format!("raft-n{node_id}")),
        })
        .collect();
    let (group, logs) = RaftRsSharedGroup::open(7, configs).unwrap();
    (dir, group, logs)
}

#[tokio::test]
async fn create_tenant_agent_session_append_fetch_round_trip() {
    let app = spawn_app().await;

    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "acme", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap().to_string();

    let a: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/agents", app.base))
        .json(&json!({"alias": "rep-1", "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = a["agent_id"].as_str().unwrap().to_string();

    let s: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = s["session_id"].as_str().unwrap().to_string();

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);

    // fetch_events returns Vec<EventEnvelope> directly (not wrapped).
    let events: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let events = events.as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["seq"].as_u64().unwrap(), 0);
    assert_eq!(events[0]["kind"], "x");
}

#[tokio::test]
async fn artifact_lifecycle_verification_reports_healthy_artifact() {
    let app = spawn_app().await;

    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "artifact-lifecycle", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap();

    app.client
        .post(format!("{}/api/tenants/{tid}/artifacts", app.base))
        .header("content-type", "text/plain")
        .body("hello artifact")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let report: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/artifacts/verify-lifecycle",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(report["tenant_id"], tid);
    assert_eq!(report["artifacts_checked"], 1);
    assert_eq!(report["raw_present"], 1);
    assert_eq!(report["raw_purged"], 0);
    assert_eq!(report["issues"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_tenants_and_sessions_returns_counts_and_rows() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let tenants: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(tenants["count"].as_u64().unwrap() >= 1);
    assert!(tenants["tenants"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tenant| tenant["tenant_id"] == tid));

    let agents: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/agents", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(agents["count"].as_u64().unwrap() >= 1);
    assert!(agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|agent| agent["agent_id"] == aid));

    let sessions: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/sessions", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(sessions["count"].as_u64().unwrap() >= 1);
    assert!(sessions["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|session| session["session_id"] == sid && session["agent_id"] == aid));
}

#[tokio::test]
async fn append_and_fetch_returns_recent() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/append-and-fetch",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": [],
            "last": 5,
            "max_bytes": 1_000_000
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);
    let recent = r["recent"].as_array().unwrap();
    assert_eq!(recent.len(), 1);
}

#[tokio::test]
async fn lifecycle_transition_to_cold_preserves_fetch_and_reports_storage() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..3 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let before: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(before.as_array().unwrap().len(), 3);

    let storage: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "cold"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(storage["lifecycle"], "cold");
    assert_eq!(storage["sealed_segment_count"].as_u64().unwrap(), 1);
    assert_eq!(storage["warm_head_from_seq"].as_u64().unwrap(), 3);

    let after: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after.as_array().unwrap().len(), 3);

    let storage_get: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/storage",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(storage_get["sealed_segment_count"].as_u64().unwrap(), 1);
    assert_eq!(storage_get["next_seq"].as_u64().unwrap(), 3);
}

#[tokio::test]
async fn lifecycle_transition_to_archive_preserves_fetch_and_reports_manifest() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..3 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    let storage: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "archive"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(storage["lifecycle"], "archive");
    assert_eq!(storage["sealed_segment_count"].as_u64().unwrap(), 0);
    assert!(storage["archive_manifest"].is_object());

    let events: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(events.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn restore_archived_session_materializes_a_cold_segment() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..3 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "archive"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let restored: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/restore",
            app.base
        ))
        .json(&json!({"target": "cold"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(restored["lifecycle"], "cold");
    assert_eq!(restored["sealed_segment_count"].as_u64().unwrap(), 1);
    assert!(restored["archive_manifest"].is_null());

    let events: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=100",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(events.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn storage_summary_reports_lifecycle_counts() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    for i in 0..2 {
        app.client
            .post(format!(
                "{}/api/tenants/{tid}/sessions/{sid}/events",
                app.base
            ))
            .json(&json!({
                "agent_id": aid,
                "kind": "message",
                "payload": {"i": i},
                "artifact_refs": [],
                "source_refs": []
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/lifecycle",
            app.base
        ))
        .json(&json!({"target": "archive"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let fleet: serde_json::Value = app
        .client
        .get(format!("{}/api/storage/summary", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(fleet["session_count"].as_u64().unwrap() >= 1);
    assert!(fleet["lifecycle_counts"]["archive"].as_u64().unwrap() >= 1);
    assert!(fleet["archive_manifest_count"].as_u64().unwrap() >= 1);

    let tenant: serde_json::Value = app
        .client
        .get(format!("{}/api/tenants/{tid}/storage-summary", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tenant["tenant_id"], tid);
    assert_eq!(tenant["lifecycle_counts"]["archive"].as_u64().unwrap(), 1);
    assert_eq!(tenant["archive_manifest_count"].as_u64().unwrap(), 1);
}

#[tokio::test]
async fn session_handlers_add_local_route_headers_when_snapshot_owner_is_local() {
    let app = spawn_routed_app("node-a").await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    assert_eq!(r.headers()[HDR_ROUTE_ACTION].to_str().unwrap(), "local");
    assert_eq!(r.headers()[HDR_OWNER_NODE].to_str().unwrap(), "node-a");
    assert_eq!(r.headers()[HDR_TABLET_ID].to_str().unwrap(), "0");
}

#[tokio::test]
async fn create_session_routes_to_owner_and_records_session_locator() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let owner_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-b"))])
        .with_nodes(vec![node("node-b", "http://127.0.0.1:18081")]);
    let owner = spawn_app_with_state(
        AppState::new(runtime.clone())
            .with_meta(meta.clone())
            .with_routing(RouteRuntime::snapshot("node-b", owner_snapshot)),
    )
    .await;
    let local_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-b"))])
        .with_nodes(vec![
            node("node-a", "http://127.0.0.1:18080"),
            node("node-b", &owner.base),
        ]);
    let local = spawn_app_with_state(
        AppState::new(runtime)
            .with_meta(meta.clone())
            .with_routing(RouteRuntime::snapshot("node-a", local_snapshot)),
    )
    .await;

    let t: serde_json::Value = local
        .client
        .post(format!("{}/api/tenants", local.base))
        .json(&json!({"external_key": "routed-create", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap().to_string();
    let a: serde_json::Value = local
        .client
        .post(format!("{}/api/tenants/{tid}/agents", local.base))
        .json(&json!({"alias": "agent", "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = a["agent_id"].as_str().unwrap().to_string();

    let response = local
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            local.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.headers().get(HDR_ROUTE_ACTION).unwrap(), "local");
    assert_eq!(response.headers().get(HDR_TABLET_ID).unwrap(), "0");
    assert_eq!(response.headers().get(HDR_SHARD_ID).unwrap(), "0");
    let session: serde_json::Value = response.json().await.unwrap();
    let sid = session["session_id"].as_str().unwrap();

    let locator = agentdb_api::sysmeta::get_locator(&local.runtime, sid)
        .await
        .unwrap()
        .expect("session locator");
    assert_eq!(locator.tenant_external_id.as_deref(), Some(tid.as_str()));
    assert_eq!(locator.tablet_id, Some(0));
    assert_eq!(locator.shard_id, 0);
}

#[tokio::test]
async fn session_locator_lifecycle_tracks_fork_delete_and_tenant_delete() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-a"))])
        .with_nodes(vec![node("node-a", "http://127.0.0.1:18080")]);
    let app = spawn_app_with_state(
        AppState::new(runtime)
            .with_meta(meta.clone())
            .with_routing(RouteRuntime::snapshot("node-a", snapshot)),
    )
    .await;

    let t: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": "locator-lifecycle", "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = t["tenant_id"].as_str().unwrap().to_string();
    let a: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/agents", app.base))
        .json(&json!({"alias": "agent", "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = a["agent_id"].as_str().unwrap().to_string();
    let s: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let sid = s["session_id"].as_str().unwrap().to_string();
    assert!(agentdb_api::sysmeta::get_locator(&app.runtime, &sid)
        .await
        .unwrap()
        .is_some());

    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let child: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/fork",
            app.base
        ))
        .json(&json!({"at_seq": 0, "alias": null}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let child_sid = child["session_id"].as_str().unwrap().to_string();
    let child_locator = agentdb_api::sysmeta::get_locator(&app.runtime, &child_sid)
        .await
        .unwrap()
        .expect("forked session locator");
    assert_eq!(child_locator.tablet_id, Some(0));
    assert_eq!(
        child_locator.tenant_external_id.as_deref(),
        Some(tid.as_str())
    );

    app.client
        .delete(format!("{}/api/tenants/{tid}/sessions/{sid}", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(agentdb_api::sysmeta::get_locator(&app.runtime, &sid)
        .await
        .unwrap()
        .is_none());
    assert!(agentdb_api::sysmeta::get_locator(&app.runtime, &child_sid)
        .await
        .unwrap()
        .is_some());

    app.client
        .delete(format!("{}/api/tenants/{tid}", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(agentdb_api::sysmeta::get_locator(&app.runtime, &child_sid)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn admin_routing_status_reports_disabled_mode() {
    let app = spawn_app_with_state(AppState::new(Runtime::in_memory())).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/routing", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "disabled");
    assert_eq!(body["local_node_id"], "single-node");
    assert!(body.get("snapshot").is_none());
}

#[tokio::test]
async fn admin_recovery_reports_disabled_without_backup_worker() {
    let app = spawn_app().await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/recovery", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], false);
    assert!(body["manifest_backup"].is_null());
    assert!(body["recovery_lag_ms"].is_null());
}

#[tokio::test]
async fn admin_recovery_reports_backup_status_and_lag() {
    let status = Arc::new(agentdb_api::state::RecoveryBackupStatus::new(
        "agentdb/recovery/manifest-latest.jsonl",
        60_000,
    ));
    status.record_success(0, 42, 4096);
    let state = agentdb_api::AppState::new(agentdb_core::Runtime::in_memory())
        .with_recovery_backup(status.clone());
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/recovery", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], true);
    assert_eq!(body["manifest_backup"]["last_entry_count"], 42);
    assert_eq!(
        body["manifest_backup"]["destination"],
        "agentdb/recovery/manifest-latest.jsonl"
    );
    assert!(body["recovery_lag_ms"].as_i64().unwrap() >= 0);
}

#[tokio::test]
async fn admin_content_mirror_reports_disabled_mode() {
    let app = spawn_app().await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/content-mirror", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], false);
    assert_eq!(body["backlog"], 0);
}

#[tokio::test]
async fn admin_content_mirror_reports_async_backlog_stats() {
    let primary = Arc::new(InMemoryContentBackend::default());
    let mirror = Arc::new(InMemoryContentBackend::default());
    let content_mirror = Arc::new(AsyncMirrorContentBackend::new(primary, mirror, 8));
    content_mirror
        .put("ten_a", "art_x", b"bytes")
        .await
        .unwrap();
    let state = AppState::new(Runtime::in_memory()).with_content_mirror(content_mirror.clone());
    let app = spawn_app_with_state(state).await;

    for _ in 0..100 {
        if content_mirror.stats().completed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/content-mirror", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], true);
    assert_eq!(body["queued"], 1);
    assert_eq!(body["completed"], 1);
    assert_eq!(body["failed"], 0);
    assert_eq!(body["backlog"], 0);
}

#[tokio::test]
async fn admin_raft_status_reports_disabled_mode() {
    let app = spawn_app_with_state(AppState::new(Runtime::in_memory())).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], false);
    assert_eq!(body["tablet_count"], 0);
    assert_eq!(body["summary"]["leaderless_tablet_count"], 0);
    assert_eq!(body["summary"]["catch_up_required_replica_count"], 0);
}

#[tokio::test]
async fn admin_raft_status_reports_registered_tablets() {
    let (_dir, group, _logs) = test_raft_group();
    group.elect(1).unwrap();
    let registry = RaftRsGroupRegistry::shared();
    registry.register(7, group).unwrap();
    let state = AppState::new(Runtime::in_memory()).with_raft_registry(registry);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], true);
    assert_eq!(body["tablet_count"], 1);
    assert_eq!(body["tablets"][0]["tablet_id"], 7);
    assert_eq!(body["tablets"][0]["replicas"].as_array().unwrap().len(), 3);
    assert_eq!(body["summary"]["leaderless_tablet_count"], 0);
    assert_eq!(body["summary"]["commit_index_drift_tablet_count"], 0);
}

#[tokio::test]
async fn admin_raft_status_joins_control_plane_replica_metadata() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    node_operational::seed_replica(meta.pool(), 7, "1", "voter", "healthy", 11, 12, recent_ms())
        .await
        .unwrap();
    node_operational::seed_replica(meta.pool(), 7, "2", "learner", "lagging", 3, 4, recent_ms())
        .await
        .unwrap();
    let (_dir, group, _logs) = test_raft_group();
    let registry = RaftRsGroupRegistry::shared();
    registry.register(7, group).unwrap();
    let state = AppState::new(Runtime::in_memory())
        .with_meta(meta)
        .with_raft_registry(registry);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], true);
    assert_eq!(body["control_plane_enabled"], true);
    assert_eq!(body["configured_replica_count"], 2);
    assert_eq!(body["unhealthy_replica_count"], 1);
    assert_eq!(body["summary"]["catch_up_required_replica_count"], 1);
    assert_eq!(body["summary"]["lagging_replica_count"], 1);
    let configured = body["tablets"][0]["configured_replicas"]
        .as_array()
        .unwrap();
    assert_eq!(configured.len(), 2);
    assert_eq!(configured[0]["node_id"], "1");
    assert_eq!(configured[0]["raft_node_id"], 1);
    assert_eq!(configured[0]["health"], "healthy");
    assert_eq!(configured[0]["local_runtime"], true);
    assert_eq!(configured[0]["runtime_commit_index"], 0);
    assert_eq!(configured[1]["node_id"], "2");
    assert_eq!(configured[1]["role"], "learner");
    assert_eq!(configured[1]["health"], "lagging");
}

#[tokio::test]
async fn admin_raft_status_reports_configured_only_replicas() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    node_operational::seed_replica(
        meta.pool(),
        17,
        "node-b",
        "learner",
        "lagging",
        3,
        4,
        recent_ms(),
    )
    .await
    .unwrap();
    let state = AppState::new(Runtime::in_memory()).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["enabled"], false);
    assert_eq!(body["control_plane_enabled"], true);
    assert_eq!(body["tablet_count"], 1);
    assert_eq!(body["configured_replica_count"], 1);
    assert_eq!(body["tablets"][0]["tablet_id"], 17);
    assert_eq!(body["tablets"][0]["local_group_open"], false);
    assert_eq!(
        body["tablets"][0]["configured_replicas"][0]["node_id"],
        "node-b"
    );
    assert_eq!(
        body["tablets"][0]["configured_replicas"][0]["catch_up_required"],
        true
    );
    assert_eq!(body["summary"]["catch_up_required_replica_count"], 1);
    assert_eq!(body["summary"]["snapshot_absent_for_catch_up_count"], 1);
}

#[tokio::test]
async fn admin_raft_catch_up_dry_run_reports_snapshot_candidates() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    node_operational::seed_replica(meta.pool(), 7, "1", "voter", "healthy", 2, 2, recent_ms())
        .await
        .unwrap();
    node_operational::seed_replica(meta.pool(), 7, "2", "learner", "lagging", 0, 0, recent_ms())
        .await
        .unwrap();
    let (_dir, group, logs) = test_raft_group();
    group.elect(1).unwrap();
    logs[0].append(7, b"catch-up-source".to_vec()).unwrap();
    let registry = RaftRsGroupRegistry::shared();
    registry.register(7, group).unwrap();
    let state = AppState::new(Runtime::in_memory())
        .with_meta(meta)
        .with_raft_registry(registry);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/raft/catch-up", app.base))
        .json(&json!({
            "tablet_id": 7,
            "target_node_id": "2",
            "dry_run": true,
            "min_lag": 1
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["dry_run"], true);
    assert_eq!(body["tablet_count"], 1);
    assert_eq!(body["attempted"], 1);
    assert_eq!(body["sent"], 0);
    assert_eq!(body["actions"][0]["tablet_id"], 7);
    assert_eq!(body["actions"][0]["node_id"], "2");
    assert_eq!(body["actions"][0]["to_raft_node_id"], 2);
    assert_eq!(body["actions"][0]["status"], "would_send");
}

#[tokio::test]
async fn admin_raft_status_reports_latest_snapshot_metadata() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    node_operational::seed_replica(meta.pool(), 7, "1", "voter", "healthy", 5, 5, recent_ms())
        .await
        .unwrap();
    node_operational::seed_replica(meta.pool(), 7, "2", "learner", "healthy", 4, 4, recent_ms())
        .await
        .unwrap();
    tablet_snapshots::record(
        meta.pool(),
        "snap-7-2",
        7,
        "1",
        1,
        "2",
        2,
        4,
        1,
        256,
        "abcd",
        None,
        "available",
        1_000,
        Some(i64::MAX),
    )
    .await
    .unwrap();
    let (_dir, group, _logs) = test_raft_group();
    let registry = RaftRsGroupRegistry::shared();
    registry.register(7, group).unwrap();
    let state = AppState::new(Runtime::in_memory())
        .with_meta(meta)
        .with_raft_registry(registry);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let configured = body["tablets"][0]["configured_replicas"]
        .as_array()
        .unwrap();
    let replica = configured
        .iter()
        .find(|replica| replica["node_id"] == "2")
        .unwrap();
    assert_eq!(replica["last_snapshot_index"], 4);
    assert_eq!(replica["last_snapshot_status"], "available");
}

#[tokio::test]
async fn admin_raft_snapshots_list_and_prune_expired_metadata() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    tablet_snapshots::record(
        meta.pool(),
        "snap-expired",
        7,
        "1",
        1,
        "2",
        2,
        4,
        1,
        256,
        "abcd",
        None,
        "available",
        1_000,
        Some(1),
    )
    .await
    .unwrap();
    let state = AppState::new(Runtime::in_memory()).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft/snapshots?tablet_id=7", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["snapshots"].as_array().unwrap().len(), 1);
    assert_eq!(body["snapshots"][0]["snapshot_id"], "snap-expired");
    assert_eq!(body["snapshots"][0]["expired"], true);

    let prune: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/raft/snapshots/prune", app.base))
        .json(&json!({"dry_run": false}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(prune["deleted_snapshot_count"], 1);

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft/snapshots?tablet_id=7", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(body["snapshots"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn internal_raft_message_requires_token() {
    let ingress = Arc::new(TestRaftIngress::default());
    let state = AppState::new(Runtime::in_memory())
        .with_raft_ingress(ingress)
        .with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;

    let response = app
        .client
        .post(format!("{}/api/internal/raft/messages", app.base))
        .json(&json!({
            "tablet_id": 7,
            "from": 1,
            "to": 2,
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"msg")
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn internal_raft_message_dispatches_to_ingress() {
    let ingress = Arc::new(TestRaftIngress::default());
    let state = AppState::new(Runtime::in_memory())
        .with_raft_ingress(ingress.clone())
        .with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .post(format!("{}/api/internal/raft/messages", app.base))
        .header("X-AgentDB-Internal-Token", "secret")
        .json(&json!({
            "tablet_id": 7,
            "from": 1,
            "to": 2,
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"msg")
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["status"], "accepted");
    let messages = ingress.messages.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].tablet_id, 7);
    assert_eq!(messages[0].from, 1);
    assert_eq!(messages[0].to, 2);
    assert_eq!(messages[0].payload, b"msg");
}

#[tokio::test]
async fn internal_raft_message_accepts_previous_token_during_rotation() {
    let ingress = Arc::new(TestRaftIngress::default());
    let state = AppState::new(Runtime::in_memory())
        .with_raft_ingress(ingress.clone())
        .with_internal_auth_token("current-secret")
        .with_internal_auth_previous_token("previous-secret");
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .post(format!("{}/api/internal/raft/messages", app.base))
        .header("X-AgentDB-Internal-Token", "previous-secret")
        .json(&json!({
            "tablet_id": 7,
            "from": 1,
            "to": 2,
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"msg")
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["status"], "accepted");
    assert_eq!(ingress.messages.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn internal_raft_message_rejects_unconfigured_peer_when_meta_is_enabled() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    node_operational::seed_replica(meta.pool(), 7, "1", "voter", "healthy", 1, 1, recent_ms())
        .await
        .unwrap();
    let ingress = Arc::new(TestRaftIngress::default());
    let state = AppState::new(Runtime::in_memory())
        .with_meta(meta)
        .with_raft_ingress(ingress)
        .with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;

    let response = app
        .client
        .post(format!("{}/api/internal/raft/messages", app.base))
        .header("X-AgentDB-Internal-Token", "secret")
        .json(&json!({
            "tablet_id": 7,
            "from": 1,
            "to": 2,
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"msg")
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn internal_raft_snapshot_dispatches_to_ingress() {
    let ingress = Arc::new(TestRaftIngress::default());
    let state = AppState::new(Runtime::in_memory())
        .with_raft_ingress(ingress.clone())
        .with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .post(format!("{}/api/internal/raft/snapshots", app.base))
        .header("X-AgentDB-Internal-Token", "secret")
        .json(&json!({
            "tablet_id": 7,
            "from": 1,
            "to": 2,
            "index": 42,
            "term": 3,
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"snapshot")
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["status"], "accepted");
    let snapshots = ingress.snapshots.lock().unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].tablet_id, 7);
    assert_eq!(snapshots[0].from, 1);
    assert_eq!(snapshots[0].to, 2);
    assert_eq!(snapshots[0].index, 42);
    assert_eq!(snapshots[0].term, 3);
    assert_eq!(snapshots[0].payload, b"snapshot");
}

#[tokio::test]
async fn internal_raft_snapshot_updates_replica_progress() {
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    node_operational::seed_replica(meta.pool(), 7, "1", "voter", "healthy", 42, 42, recent_ms())
        .await
        .unwrap();
    node_operational::seed_replica(meta.pool(), 7, "2", "learner", "lagging", 1, 1, recent_ms())
        .await
        .unwrap();
    let ingress = Arc::new(TestRaftIngress::default());
    let state = AppState::new(Runtime::in_memory())
        .with_meta(meta.clone())
        .with_raft_ingress(ingress)
        .with_internal_auth_token("secret");
    let operational = state.operational.clone();
    let app = spawn_app_with_state(state).await;

    app.client
        .post(format!("{}/api/internal/raft/snapshots", app.base))
        .header("X-AgentDB-Internal-Token", "secret")
        .json(&json!({
            "tablet_id": 7,
            "from": 1,
            "to": 2,
            "index": 42,
            "term": 3,
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(b"snapshot")
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Progress lands in the receiving node's in-process registry under its
    // own identity; the heartbeat serializes it into the node report.
    let (_, node_id, observed) = operational
        .snapshot(0)
        .into_iter()
        .find(|(tablet_id, _, _)| *tablet_id == 7)
        .expect("local progress observation");
    assert_eq!(node_id, "single-node");
    assert_eq!(observed.applied_index, 42);
    assert_eq!(observed.match_index, 42);
    assert_eq!(observed.health, "healthy");
}

#[tokio::test]
async fn internal_raft_http_client_sends_message_and_snapshot() {
    let ingress = Arc::new(TestRaftIngress::default());
    let state = AppState::new(Runtime::in_memory())
        .with_raft_ingress(ingress.clone())
        .with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;
    let client = RaftRsHttpClient::new("secret");

    client
        .send_message(
            &app.base,
            &RaftRsWireMessage {
                tablet_id: 7,
                from: 1,
                to: 2,
                payload: b"msg".to_vec(),
            },
        )
        .await
        .unwrap();
    client
        .send_snapshot(
            &app.base,
            &RaftRsSnapshotTransfer {
                tablet_id: 7,
                from: 1,
                to: 2,
                index: 42,
                term: 3,
                payload: b"snapshot".to_vec(),
            },
        )
        .await
        .unwrap();

    assert_eq!(ingress.messages.lock().unwrap()[0].payload, b"msg");
    assert_eq!(ingress.snapshots.lock().unwrap()[0].payload, b"snapshot");
}

#[tokio::test]
async fn internal_tablet_movement_snapshot_export_returns_retargeted_payload() {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let runtime = Runtime::with_kv_and_replicated_logs(
        kv.clone(),
        Arc::new(InMemoryContentBackend::default()),
        BTreeMap::from([
            (
                RUNTIME_PRIMARY_TABLET_ID,
                Arc::new(SingleNodeReplicatedLog::new(kv.clone()))
                    as Arc<dyn ReplicatedLog + Send + Sync>,
            ),
            (
                7,
                Arc::new(SingleNodeReplicatedLog::new(kv)) as Arc<dyn ReplicatedLog + Send + Sync>,
            ),
        ]),
    );
    let create_runtime = runtime.clone();
    let tenant = runtime
        .with_tablet_context(7, async move {
            create_runtime
                .create_tenant(NewTenant {
                    external_key: "movement-export".into(),
                    policy: json!({}),
                })
                .await
        })
        .await
        .unwrap();
    let state = AppState::new(runtime).with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;
    let client = RaftRsHttpClient::new("secret");

    let response = client
        .export_tablet_movement_snapshot(
            &app.base,
            &agentdb_api::internal_raft_http::TabletMovementSnapshotExportRequest {
                source_tablet_id: 7,
                target_tablet_id: 8,
                start_shard: 0,
                end_shard: 16_383,
                shard_count: 16_384,
            },
        )
        .await
        .unwrap();
    let payload = response.payload().unwrap();
    let snapshot: agentdb_core::tablet_state::TabletStateSnapshot =
        serde_json::from_slice(&payload).unwrap();

    assert_eq!(response.source_tablet_id, 7);
    assert_eq!(response.target_tablet_id, 8);
    assert_eq!(snapshot.tablet_id, 8);
    assert!(snapshot
        .entries
        .iter()
        .any(|entry| entry.key == format!("record/tenant/{}", tenant.tenant_id).into_bytes()));
}

#[tokio::test]
async fn internal_raft_registry_dispatches_message_to_group() {
    let (_dir, group, logs) = test_raft_group();
    group.elect(1).unwrap();
    group.mark_unavailable(3).unwrap();
    logs[0].append(7, b"registry-wire".to_vec()).unwrap();
    group.mark_available(3).unwrap();

    let registry = RaftRsGroupRegistry::shared();
    registry.register(7, group.clone()).unwrap();
    let state = AppState::new(Runtime::in_memory())
        .with_raft_ingress(registry)
        .with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;
    let outbox = RaftRsHttpOutbox::new("secret");
    outbox.set_peer_url(3, app.base.clone());

    let sent = outbox.flush_group_once(&group).await.unwrap();

    assert!(sent > 0);
    assert!(logs[2]
        .committed_entries(7, 0, 10)
        .unwrap()
        .iter()
        .any(|entry| entry.command == b"registry-wire"));
}

#[tokio::test]
async fn internal_raft_registry_rejects_unregistered_tablet() {
    let registry = RaftRsGroupRegistry::shared();
    let state = AppState::new(Runtime::in_memory())
        .with_raft_ingress(registry)
        .with_internal_auth_token("secret");
    let app = spawn_app_with_state(state).await;
    let client = RaftRsHttpClient::new("secret");

    let err = client
        .send_message(
            &app.base,
            &RaftRsWireMessage {
                tablet_id: 404,
                from: 1,
                to: 2,
                payload: b"missing".to_vec(),
            },
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("not registered"));
}

#[tokio::test]
async fn internal_raft_http_outbox_restores_messages_when_peer_missing() {
    let (_dir, group, logs) = test_raft_group();
    group.elect(1).unwrap();
    group.mark_unavailable(3).unwrap();
    logs[0].append(7, b"registry-wire".to_vec()).unwrap();
    let outbox = RaftRsHttpOutbox::new("secret");

    let err = outbox.flush_group_once(&group).await.unwrap_err();

    assert!(err.to_string().contains("peer URL is not configured"));
    assert!(!group.take_wire_messages().is_empty());
}

#[tokio::test]
async fn admin_routing_status_reports_snapshot_metadata() {
    let app = spawn_routed_app("node-a").await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/routing", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "snapshot");
    assert_eq!(body["local_node_id"], "node-a");
    assert_eq!(body["snapshot"]["shard_count"], 1);
    assert_eq!(body["snapshot"]["tablet_count"], 1);
    assert_eq!(body["snapshot"]["node_count"], 2);
    assert_eq!(body["snapshot"]["session_locator_count"], 0);
    assert_eq!(body["snapshot"]["nodes"][0]["region"], "us-west-2");
    assert_eq!(
        body["snapshot"]["nodes"][0]["availability_zone"],
        "us-west-2a"
    );
    assert_eq!(body["snapshot"]["nodes"][0]["route_location"], "us-west-2");
    assert_eq!(body["snapshot"]["tablets"][0]["owner_node_id"], "node-a");
    assert_eq!(body["snapshot"]["tablets"][0]["owner_region"], "us-west-2");
    assert_eq!(
        body["snapshot"]["tablets"][0]["owner_availability_zone"],
        "us-west-2a"
    );
    assert_eq!(
        body["snapshot"]["tablets"][0]["owner_route_location"],
        "us-west-2"
    );
    assert_eq!(body["snapshot"]["tablets"][0]["local_owner"], true);
}

#[tokio::test]
async fn admin_routing_decision_reports_disabled_local_dry_run() {
    let app = spawn_app_with_state(AppState::new(Runtime::in_memory())).await;

    let body: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/admin/routing/decision?tenant_id=ten_1&session_id=ses_1",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "disabled");
    assert_eq!(body["tenant_id"], "ten_1");
    assert_eq!(body["session_id"], "ses_1");
    assert_eq!(body["action"], "local");
    assert_eq!(body["session_locator_found"], false);
    assert!(body["route_error_code"].is_null());
}

#[tokio::test]
async fn admin_routing_decision_reports_snapshot_forward_dry_run() {
    let app = spawn_routed_app("node-b").await;

    let body: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/admin/routing/decision?tenant_id=ten_1&session_id=ses_1",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "snapshot");
    assert_eq!(body["local_node_id"], "node-a");
    assert_eq!(body["action"], "forward");
    assert_eq!(body["shard_id"], 0);
    assert_eq!(body["tablet_id"], 0);
    assert_eq!(body["owner_node_id"], "node-b");
    assert_eq!(body["owner_location"], "us-west-2");
    assert_eq!(body["epoch"], 1);
    assert_eq!(body["session_locator_found"], false);
    assert!(body["reason"].is_null());
    assert!(body["route_error_code"].is_null());
}

#[tokio::test]
async fn admin_cluster_health_reports_single_node_ok() {
    // Cluster control-plane routes are mounted only in cluster mode; the handler
    // still reports the disabled/single-node state when routing is unconfigured.
    let app = spawn_app_with_state(AppState::new(Runtime::in_memory())).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/cluster/health", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "disabled");
    assert_eq!(body["status"], "ok");
    assert_eq!(body["single_node"], true);
    assert_eq!(body["summary"]["node_count"], 1);
    assert_eq!(body["issues"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn admin_cluster_health_reports_snapshot_health() {
    let app = spawn_routed_app("node-a").await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/cluster/health", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "snapshot");
    assert_eq!(body["status"], "ok");
    assert_eq!(body["single_node"], false);
    assert_eq!(body["local_node_found"], true);
    assert_eq!(body["local_node_stale"], false);
    assert_eq!(body["summary"]["node_count"], 2);
    assert_eq!(body["summary"]["tablet_count"], 1);
    assert_eq!(body["summary"]["unowned_tablet_count"], 0);
}

#[tokio::test]
async fn admin_cluster_health_reports_degraded_unowned_tablet() {
    let app = spawn_unassigned_routed_app().await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/cluster/health", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["mode"], "snapshot");
    assert_eq!(body["status"], "degraded");
    assert_eq!(body["summary"]["unowned_tablet_count"], 1);
    assert!(body["issues"]
        .as_array()
        .unwrap()
        .iter()
        .any(|issue| issue.as_str().unwrap().contains("unowned")));
}

#[tokio::test]
async fn clustered_routing_rejects_export_until_cross_tablet_frontiers_exist() {
    let runtime = Runtime::in_memory();
    let tenant = runtime
        .create_tenant(agentdb_core::NewTenant {
            external_key: "cluster-export-gate".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-a"))])
        .with_nodes(vec![node("node-a", "http://127.0.0.1:18080")]);
    let state = AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", snapshot));
    let app = spawn_app_with_state(state).await;

    let response = app
        .client
        .get(format!(
            "{}/api/tenants/{}/exports/status",
            app.base, tenant.tenant_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["message"]
        .as_str()
        .unwrap_or_default()
        .contains("cross-tablet export frontiers"));
}

#[tokio::test]
async fn admin_cluster_readiness_rejects_stale_local_node() {
    let runtime = Runtime::in_memory();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-a"))])
        .with_nodes(vec![
            node_with_state_and_heartbeat("node-a", "http://127.0.0.1:18080", "alive", 0),
            node_with_state_and_heartbeat("node-b", "http://127.0.0.1:18081", "alive", 0),
        ]);
    let state = AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", snapshot));
    let app = spawn_app_with_state(state).await;

    let response = app
        .client
        .get(format!(
            "{}/api/admin/cluster/readiness?stale_threshold_ms=1",
            app.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ready"], false);
    assert!(body["reasons"]
        .as_array()
        .unwrap()
        .iter()
        .any(|reason| reason.as_str().unwrap().contains("stale")));
}

#[tokio::test]
async fn admin_nodes_returns_rows_with_staleness() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        i64::MAX,
        Some("us-west-2a"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-b",
        "http://127.0.0.1:18081",
        None,
        "draining",
        0,
        Some("us-west-2b"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/nodes?stale_threshold_ms=1", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let rows = body.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .any(|row| row["node_id"] == "node-a" && row["is_stale"] == false));
    assert!(rows.iter().any(|row| row["node_id"] == "node-b"
        && row["state"] == "draining"
        && row["is_stale"] == true));
}

#[tokio::test]
async fn admin_tablets_returns_rows() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::tablets::get_or_create(meta.pool(), 7, 100)
        .await
        .unwrap();
    agentdb_meta::tablets::assume_ownership(meta.pool(), 7, 0, "node-a", 500)
        .await
        .unwrap();
    agentdb_meta::tablets::mark_for_handoff(meta.pool(), 7, "node-b")
        .await
        .unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/tablets", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let rows = body.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["tablet_id"], 7);
    assert_eq!(rows[0]["owner_node_id"], "node-a");
    assert_eq!(rows[0]["handoff_target_node_id"], "node-b");
    assert_eq!(rows[0]["placement_scope_id"], "default");
    assert_eq!(rows[0]["state"], "serving");
    assert_eq!(rows[0]["encryption_policy_version"], 0);
}

#[tokio::test]
async fn admin_tablet_stats_returns_rows_and_aggregates() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::node_operational::seed_tablet_stats(
        meta.pool(),
        7,
        "node-a",
        10,
        100,
        1_000,
        1_100,
        12,
        1_000,
        6,
        6_000,
        500,
        recent_ms(),
    )
    .await
    .unwrap();
    agentdb_meta::node_operational::seed_tablet_stats(
        meta.pool(),
        7,
        "node-b",
        8,
        80,
        900,
        980,
        11,
        0,
        0,
        0,
        0,
        recent_ms(),
    )
    .await
    .unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/tablet-stats?tablet_id=7", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["stats"].as_array().unwrap().len(), 2);
    assert_eq!(body["aggregates"].as_array().unwrap().len(), 1);
    assert_eq!(body["aggregates"][0]["tablet_id"], 7);
    assert_eq!(body["aggregates"][0]["replica_count"], 2);
    assert_eq!(body["aggregates"][0]["storage_bytes"], 1_100);
    assert_eq!(body["aggregates"][0]["applied_index"], 12);
    assert_eq!(body["aggregates"][0]["sample_interval_ms"], 1_000);
    assert_eq!(body["aggregates"][0]["write_ops_delta"], 6);
    assert_eq!(body["aggregates"][0]["write_ops_per_sec_milli"], 6_000);
    assert_eq!(body["aggregates"][0]["compaction_debt_ops"], 500);
}

#[tokio::test]
async fn admin_tablet_ownership_mutations_round_trip() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        i64::MAX,
        Some("us-west-2a"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-b",
        "http://127.0.0.1:18081",
        None,
        "alive",
        i64::MAX,
        Some("us-west-2b"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let created: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/create", app.base))
        .json(&json!({"tablet_id": 9}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["action"], "create");
    assert_eq!(created["changed"], true);
    assert_eq!(created["tablet"]["tablet_id"], 9);
    assert_eq!(created["tablet"]["placement_scope_id"], "default");

    let assigned: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/9/assign", app.base))
        .json(&json!({"node_id": "node-a"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(assigned["changed"], true);
    assert_eq!(assigned["tablet"]["owner_node_id"], "node-a");
    assert_eq!(assigned["tablet"]["epoch"], 0);

    let handoff: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/9/handoff", app.base))
        .json(&json!({"target_node_id": "node-b"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(handoff["changed"], true);
    assert_eq!(handoff["tablet"]["owner_node_id"], "node-a");
    assert_eq!(handoff["tablet"]["handoff_target_node_id"], "node-b");

    let completed: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/9/handoff/complete", app.base))
        .json(&json!({"expected_source_node_id": "node-a"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(completed["changed"], true);
    assert_eq!(completed["new_owner_node_id"], "node-b");
    assert_eq!(completed["tablet"]["owner_node_id"], "node-b");
    assert_eq!(
        completed["tablet"]["handoff_target_node_id"],
        serde_json::Value::Null
    );
    assert_eq!(completed["tablet"]["epoch"], 1);

    let relinquished: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/9/relinquish", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(relinquished["changed"], true);
    assert_eq!(
        relinquished["tablet"]["owner_node_id"],
        serde_json::Value::Null
    );
    assert_eq!(relinquished["tablet"]["epoch"], 2);
}

#[tokio::test]
async fn admin_tablet_replacement_requires_caught_up_target() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        i64::MAX,
        Some("us-west-2a"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-b",
        "http://127.0.0.1:18081",
        None,
        "alive",
        i64::MAX,
        Some("us-west-2b"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    agentdb_meta::tablets::get_or_create(meta.pool(), 9, 100)
        .await
        .unwrap();
    agentdb_meta::tablets::assume_ownership(meta.pool(), 9, 0, "node-a", 500)
        .await
        .unwrap();
    node_operational::seed_replica(
        meta.pool(),
        9,
        "node-a",
        "voter",
        "healthy",
        42,
        42,
        recent_ms(),
    )
    .await
    .unwrap();
    let state = AppState::new(runtime).with_meta(meta.clone());
    let app = spawn_app_with_state(state).await;

    let prepared: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/admin/tablets/9/replacement/prepare",
            app.base
        ))
        .json(&json!({"target_node_id": "node-b"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(prepared["changed"], true);
    assert_eq!(prepared["source_node_id"], "node-a");
    assert_eq!(prepared["target_replica"]["role"], "learner");
    // Prepare writes placement INTENT; observed health arrives via node
    // reports, so a fresh target reads "unknown" (treated as not caught up).
    assert_eq!(prepared["target_replica"]["health"], "unknown");
    assert_eq!(prepared["tablet"]["handoff_target_node_id"], "node-b");

    let rejected = app
        .client
        .post(format!(
            "{}/api/admin/tablets/9/replacement/complete",
            app.base
        ))
        .json(&json!({"target_node_id": "node-b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    node_operational::seed_replica(
        meta.pool(),
        9,
        "node-b",
        "learner",
        "healthy",
        42,
        42,
        recent_ms(),
    )
    .await
    .unwrap();

    let completed: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/admin/tablets/9/replacement/complete",
            app.base
        ))
        .json(&json!({
            "target_node_id": "node-b",
            "min_applied_index": 42,
            "retire_source": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(completed["changed"], true);
    assert_eq!(completed["new_owner_node_id"], "node-b");
    assert_eq!(completed["required_applied_index"], 42);
    assert_eq!(completed["retired_source"], true);
    assert_eq!(completed["tablet"]["owner_node_id"], "node-b");
    assert_eq!(
        completed["tablet"]["handoff_target_node_id"],
        serde_json::Value::Null
    );
    assert_eq!(completed["target_replica"]["role"], "voter");
    assert_eq!(completed["source_replica"], serde_json::Value::Null);

    let replayed: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/admin/tablets/9/replacement/complete",
            app.base
        ))
        .json(&json!({
            "target_node_id": "node-b",
            "source_node_id": "node-a",
            "min_applied_index": 42,
            "retire_source": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replayed["changed"], false);
    assert_eq!(replayed["new_owner_node_id"], "node-b");
    assert_eq!(replayed["tablet"]["owner_node_id"], "node-b");
    assert_eq!(replayed["target_replica"]["role"], "voter");
}

#[tokio::test]
async fn admin_tablet_create_accepts_placement_metadata() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let created: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/create", app.base))
        .json(&json!({
            "tablet_id": 11,
            "placement_scope_id": "tenant/acme",
            "state": "creating",
            "raft_group_id": "raft/11",
            "encryption_policy_version": 3
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(created["changed"], true);
    assert_eq!(created["tablet"]["tablet_id"], 11);
    assert_eq!(created["tablet"]["placement_scope_id"], "tenant/acme");
    assert_eq!(created["tablet"]["state"], "creating");
    assert_eq!(created["tablet"]["raft_group_id"], "raft/11");
    assert_eq!(created["tablet"]["encryption_policy_version"], 3);
}

#[tokio::test]
async fn admin_tenant_policies_round_trip() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let policy: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tenant-policies", app.base))
        .json(&json!({
            "tenant_id": "ten_acme",
            "placement_class": "dedicated",
            "placement_scope_id": "tenant/acme",
            "encryption_class": "tenant",
            "key_policy_id": "key/acme",
            "encryption_policy_version": 2,
            "policy_version": 5
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(policy["tenant_id"], "ten_acme");
    assert_eq!(policy["placement_scope_id"], "tenant/acme");
    assert_eq!(policy["key_policy_id"], "key/acme");
    assert_eq!(policy["policy_version"], 5);

    let rows: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/tenant-policies", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);

    let deleted: serde_json::Value = app
        .client
        .delete(format!("{}/api/admin/tenant-policies/ten_acme", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(deleted["removed"], true);
}

#[tokio::test]
async fn create_tenant_can_bootstrap_placement_policy() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;
    let tenant_id = "ten_018f0000-0000-7000-8000-000000000123";

    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({
            "tenant_id": tenant_id,
            "external_key": "bootstrap-placement",
            "policy": {},
            "placement": {
                "placement_scope_id": "tenant/bootstrap",
                "cell_id": "cell-b",
                "placement_class": "dedicated",
                "tier": "paid",
                "priority": 20,
                "encryption_class": "tenant",
                "key_policy_id": "key/bootstrap",
                "encryption_policy_version": 3,
                "policy_version": 7
            }
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tenant["tenant_id"], tenant_id);

    let policies: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/tenant-policies", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let policy = policies
        .as_array()
        .unwrap()
        .iter()
        .find(|policy| policy["tenant_id"] == tenant_id)
        .unwrap();
    assert_eq!(policy["cell_id"], "cell-b");
    assert_eq!(policy["placement_scope_id"], "tenant/bootstrap");
    assert_eq!(policy["tier"], "paid");
    assert_eq!(policy["priority"], 20);
    assert_eq!(policy["key_policy_id"], "key/bootstrap");
    assert_eq!(policy["policy_version"], 7);
}

#[tokio::test]
async fn admin_tablet_ranges_replace_round_trip() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::tablet_ranges::upsert(meta.pool(), "default", 0, 99, 7, 1, 100)
        .await
        .unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let replaced: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablet-ranges/replace", app.base))
        .json(&json!({
            "placement_scope_id": "default",
            "old_ranges": [{"start_shard": 0, "end_shard": 99}],
            "new_ranges": [
                {"start_shard": 0, "end_shard": 49, "tablet_id": 8},
                {"start_shard": 50, "end_shard": 99, "tablet_id": 9}
            ],
            "map_epoch": 2
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replaced["changed"], true);
    assert_eq!(replaced["ranges"].as_array().unwrap().len(), 2);

    let rows: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/tablet-ranges", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row["tablet_id"] == 8));
    assert!(rows.iter().any(|row| row["tablet_id"] == 9));
}

#[tokio::test]
async fn admin_tablet_split_merge_prepare_rejects_conflicting_lifecycle_state() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::tablets::create_with_placement(
        meta.pool(),
        7,
        "default",
        "serving",
        Some("raft/7"),
        0,
        100,
    )
    .await
    .unwrap();
    agentdb_meta::tablets::create_with_placement(
        meta.pool(),
        8,
        "default",
        "serving",
        Some("raft/8"),
        0,
        100,
    )
    .await
    .unwrap();
    agentdb_meta::tablet_ranges::upsert(meta.pool(), "default", 0, 99, 7, 1, 100)
        .await
        .unwrap();
    agentdb_meta::tablet_ranges::upsert(meta.pool(), "default", 100, 199, 8, 1, 100)
        .await
        .unwrap();
    let app = spawn_app_with_state(AppState::new(runtime).with_meta(meta.clone())).await;

    let rejected_destination = app
        .client
        .post(format!("{}/api/admin/tablets/7/split/prepare", app.base))
        .json(&json!({
            "child_ranges": [
                {"start_shard": 0, "end_shard": 49, "tablet_id": 8},
                {"start_shard": 50, "end_shard": 99, "tablet_id": 9}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rejected_destination.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    agentdb_meta::tablets::set_state(meta.pool(), 7, "splitting")
        .await
        .unwrap();
    let rejected_source = app
        .client
        .post(format!("{}/api/admin/tablets/7/split/prepare", app.base))
        .json(&json!({
            "child_ranges": [
                {"start_shard": 0, "end_shard": 49, "tablet_id": 10},
                {"start_shard": 50, "end_shard": 99, "tablet_id": 11}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rejected_source.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    agentdb_meta::tablets::create_with_placement(
        meta.pool(),
        30,
        "merge-scope",
        "serving",
        Some("raft/30"),
        0,
        100,
    )
    .await
    .unwrap();
    agentdb_meta::tablets::create_with_placement(
        meta.pool(),
        31,
        "merge-scope",
        "merging",
        Some("raft/31"),
        0,
        100,
    )
    .await
    .unwrap();
    agentdb_meta::tablet_ranges::upsert(meta.pool(), "merge-scope", 0, 49, 30, 1, 100)
        .await
        .unwrap();
    agentdb_meta::tablet_ranges::upsert(meta.pool(), "merge-scope", 50, 99, 31, 1, 100)
        .await
        .unwrap();
    let rejected_merge_source = app
        .client
        .post(format!("{}/api/admin/tablets/merge/prepare", app.base))
        .json(&json!({
            "placement_scope_id": "merge-scope",
            "source_tablet_ids": [30, 31],
            "destination_tablet_id": 32
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rejected_merge_source.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn admin_tablet_split_merge_lifecycle_round_trip() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::tablets::create_with_placement(
        meta.pool(),
        7,
        "default",
        "serving",
        Some("raft/7"),
        0,
        100,
    )
    .await
    .unwrap();
    node_operational::seed_replica(
        meta.pool(),
        7,
        "node-a",
        "voter",
        "healthy",
        12,
        12,
        recent_ms(),
    )
    .await
    .unwrap();
    agentdb_meta::tablet_ranges::upsert(meta.pool(), "default", 0, 99, 7, 1, 100)
        .await
        .unwrap();
    let state = AppState::new(runtime).with_meta(meta.clone());
    let app = spawn_app_with_state(state).await;

    let response = app
        .client
        .post(format!("{}/api/admin/tablets/7/split/prepare", app.base))
        .json(&json!({
            "child_ranges": [
                {"start_shard": 0, "end_shard": 49, "tablet_id": 8},
                {"start_shard": 50, "end_shard": 99, "tablet_id": 9}
            ]
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let split_prepare: serde_json::Value = response.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{split_prepare}");
    assert_eq!(split_prepare["action"], "prepare_split");
    assert!(split_prepare["tablets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tablet| tablet["tablet_id"] == 7 && tablet["state"] == "splitting"));
    assert!(split_prepare["tablets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tablet| tablet["tablet_id"] == 8 && tablet["state"] == "creating"));
    assert_eq!(split_prepare["required_applied_index"], 12);
    assert_eq!(split_prepare["destinations_ready"], false);
    assert!(split_prepare["replicas"]
        .as_array()
        .unwrap()
        .iter()
        .any(|replica| replica["tablet_id"] == 8 && replica["role"] == "voter"));
    assert!(split_prepare["movements"]
        .as_array()
        .unwrap()
        .iter()
        .any(|movement| {
            movement["movement_id"] == "split:7:8"
                && movement["status"] == "pending"
                && movement["required_applied_index"] == 12
        }));

    let rejected = app
        .client
        .post(format!("{}/api/admin/tablets/7/split/complete", app.base))
        .json(&json!({
            "child_ranges": [
                {"start_shard": 0, "end_shard": 49, "tablet_id": 8},
                {"start_shard": 50, "end_shard": 99, "tablet_id": 9}
            ],
            "map_epoch": 2
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    for tablet_id in [8, 9] {
        node_operational::seed_replica(
            meta.pool(),
            tablet_id,
            "node-a",
            "voter",
            "healthy",
            12,
            12,
            recent_ms(),
        )
        .await
        .unwrap();
    }

    let split_complete: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/7/split/complete", app.base))
        .json(&json!({
            "child_ranges": [
                {"start_shard": 0, "end_shard": 49, "tablet_id": 8},
                {"start_shard": 50, "end_shard": 99, "tablet_id": 9}
            ],
            "map_epoch": 2
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(split_complete["action"], "complete_split");
    assert_eq!(split_complete["destinations_ready"], true);
    assert!(split_complete["movements"]
        .as_array()
        .unwrap()
        .iter()
        .all(|movement| movement["status"] == "completed"));
    assert!(split_complete["tablets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tablet| tablet["tablet_id"] == 7 && tablet["state"] == "tombstoned"));
    assert_eq!(split_complete["ranges"].as_array().unwrap().len(), 2);

    let response = app
        .client
        .post(format!("{}/api/admin/tablets/merge/prepare", app.base))
        .json(&json!({
            "source_tablet_ids": [8, 9],
            "destination_tablet_id": 10
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let merge_prepare: serde_json::Value = response.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{merge_prepare}");
    assert_eq!(merge_prepare["action"], "prepare_merge");
    assert_eq!(merge_prepare["required_applied_index"], 12);
    assert_eq!(merge_prepare["destinations_ready"], false);
    assert!(
        merge_prepare["movements"]
            .as_array()
            .unwrap()
            .iter()
            .any(|movement| movement["movement_id"] == "merge:8:10"
                && movement["status"] == "pending")
    );
    assert!(merge_prepare["tablets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tablet| tablet["tablet_id"] == 10 && tablet["state"] == "creating"));
    node_operational::seed_replica(
        meta.pool(),
        10,
        "node-a",
        "voter",
        "healthy",
        12,
        12,
        recent_ms(),
    )
    .await
    .unwrap();

    let merge_complete: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/merge/complete", app.base))
        .json(&json!({
            "source_tablet_ids": [8, 9],
            "destination_tablet_id": 10,
            "map_epoch": 3
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(merge_complete["action"], "complete_merge");
    assert_eq!(merge_complete["destinations_ready"], true);
    assert!(merge_complete["movements"]
        .as_array()
        .unwrap()
        .iter()
        .all(|movement| movement["status"] == "completed"));
    assert_eq!(merge_complete["ranges"].as_array().unwrap().len(), 1);
    assert_eq!(merge_complete["ranges"][0]["tablet_id"], 10);
    assert!(merge_complete["tablets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tablet| tablet["tablet_id"] == 10 && tablet["state"] == "serving"));
    assert!(merge_complete["tablets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tablet| tablet["tablet_id"] == 8 && tablet["state"] == "tombstoned"));

    let movements: serde_json::Value = app
        .client
        .get(format!(
            "{}/api/admin/tablet-movements?status=completed",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(movements
        .as_array()
        .unwrap()
        .iter()
        .any(|movement| movement["movement_id"] == "split:7:8"));
    assert!(movements
        .as_array()
        .unwrap()
        .iter()
        .any(|movement| movement["movement_id"] == "merge:8:10"));
}

#[tokio::test]
async fn admin_tablet_rebalance_assigns_unowned_and_moves_load() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        i64::MAX,
        Some("us-west-2a"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-b",
        "http://127.0.0.1:18081",
        None,
        "alive",
        i64::MAX,
        Some("us-west-2b"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-c",
        "http://127.0.0.1:18082",
        None,
        "draining",
        i64::MAX,
        Some("us-west-2c"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    for id in 1..=5 {
        agentdb_meta::tablets::get_or_create(meta.pool(), id, 100)
            .await
            .unwrap();
    }
    agentdb_meta::tablets::assign_to(meta.pool(), 1, "node-a")
        .await
        .unwrap();
    agentdb_meta::tablets::assign_to(meta.pool(), 2, "node-a")
        .await
        .unwrap();
    agentdb_meta::tablets::assign_to(meta.pool(), 3, "node-a")
        .await
        .unwrap();
    agentdb_meta::tablets::assign_to(meta.pool(), 4, "node-c")
        .await
        .unwrap();

    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let dry_run: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/rebalance", app.base))
        .json(&json!({"dry_run": true}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["eligible_node_count"], 2);
    assert!(dry_run["actions"].as_array().unwrap().iter().any(|action| {
        action["action"] == "assign" && action["tablet_id"] == 5 && action["reason"] == "unowned"
    }));
    assert!(dry_run["actions"].as_array().unwrap().iter().any(|action| {
        action["action"] == "prepare_replacement"
            && action["source_node_id"] == "node-c"
            && action["reason"] == "source_draining"
    }));

    let applied: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/rebalance", app.base))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(applied["dry_run"], false);
    assert!(applied["actions"].as_array().unwrap().len() >= 2);

    let tablets: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/tablets", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = tablets.as_array().unwrap();
    assert!(rows.iter().any(|row| row["tablet_id"] == 5
        && (row["owner_node_id"] == "node-a" || row["owner_node_id"] == "node-b")));
    assert!(rows.iter().any(|row| {
        row["handoff_target_node_id"] == "node-a" || row["handoff_target_node_id"] == "node-b"
    }));
    let raft: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/raft", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(raft["configured_replica_count"].as_u64().unwrap() >= 2);
    assert!(raft["tablets"].as_array().unwrap().iter().any(|tablet| {
        tablet["local_group_open"] == false
            && tablet["configured_replicas"]
                .as_array()
                .unwrap()
                .iter()
                .any(|replica| replica["role"] == "learner" && replica["catch_up_required"] == true)
    }));
}

#[tokio::test]
async fn admin_tablet_rebalance_promotes_target_and_retires_source_in_raft() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        i64::MAX,
        None,
        None,
    )
    .await
    .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-b",
        "http://127.0.0.1:18081",
        None,
        "alive",
        i64::MAX,
        None,
        None,
    )
    .await
    .unwrap();
    agentdb_meta::tablets::get_or_create(meta.pool(), 7, 100)
        .await
        .unwrap();
    agentdb_meta::tablets::assume_ownership(meta.pool(), 7, 0, "node-b", 20_000)
        .await
        .unwrap();
    agentdb_meta::tablets::mark_for_handoff(meta.pool(), 7, "node-a")
        .await
        .unwrap();
    node_operational::seed_replica(
        meta.pool(),
        7,
        "node-b",
        "voter",
        "healthy",
        10,
        10,
        recent_ms(),
    )
    .await
    .unwrap();
    node_operational::seed_replica(
        meta.pool(),
        7,
        "node-a",
        "learner",
        "healthy",
        10,
        10,
        recent_ms(),
    )
    .await
    .unwrap();

    let dir = TempDir::new().unwrap();
    let configs = vec![
        RaftRsNodeConfig::from_control_plane_replica(7, "node-b", "voter", dir.path()).unwrap(),
        RaftRsNodeConfig::from_control_plane_replica(7, "node-a", "learner", dir.path()).unwrap(),
    ];
    let leader_id = agentdb_storage::stable_raft_node_id("node-b").unwrap();
    let (group, _log) = RaftRsSharedGroup::open_local(7, configs, leader_id).unwrap();
    group.elect(leader_id).unwrap();
    let registry = RaftRsGroupRegistry::shared();
    registry.register(7, group).unwrap();

    let state = AppState::new(runtime)
        .with_meta(meta.clone())
        .with_raft_registry(registry);
    let app = spawn_app_with_state(state).await;
    let applied: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/rebalance", app.base))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(applied["actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| { action["action"] == "complete_replacement" && action["tablet_id"] == 7 }));
    let tablet = agentdb_meta::tablets::get(meta.pool(), 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tablet.owner_node_id.as_deref(), Some("node-a"));
    // Retirement removes placement INTENT; stale observations age out with
    // their reports.
    let intent = agentdb_meta::tablet_replicas::list_intent_by_tablet(meta.pool(), 7)
        .await
        .unwrap();
    assert!(!intent.iter().any(|row| row.node_id == "node-b"));
    assert!(intent
        .iter()
        .any(|row| row.node_id == "node-a" && row.role == "voter"));
}

#[tokio::test]
async fn admin_tablet_rebalance_skips_raft_retirement_when_group_is_unregistered() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        i64::MAX,
        None,
        None,
    )
    .await
    .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-b",
        "http://127.0.0.1:18081",
        None,
        "alive",
        i64::MAX,
        None,
        None,
    )
    .await
    .unwrap();
    agentdb_meta::tablets::get_or_create(meta.pool(), 7, 100)
        .await
        .unwrap();
    agentdb_meta::tablets::assume_ownership(meta.pool(), 7, 0, "node-b", 20_000)
        .await
        .unwrap();
    agentdb_meta::tablets::mark_for_handoff(meta.pool(), 7, "node-a")
        .await
        .unwrap();
    node_operational::seed_replica(
        meta.pool(),
        7,
        "node-b",
        "voter",
        "healthy",
        10,
        10,
        recent_ms(),
    )
    .await
    .unwrap();
    node_operational::seed_replica(
        meta.pool(),
        7,
        "node-a",
        "learner",
        "healthy",
        10,
        10,
        recent_ms(),
    )
    .await
    .unwrap();

    let registry = RaftRsGroupRegistry::shared();
    let state = AppState::new(runtime)
        .with_meta(meta.clone())
        .with_raft_registry(registry.clone());
    let app = spawn_app_with_state(state).await;
    let applied: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablets/rebalance", app.base))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(applied["actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| { action["action"] == "complete_replacement" && action["tablet_id"] == 7 }));
    let tablet = agentdb_meta::tablets::get(meta.pool(), 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tablet.owner_node_id.as_deref(), Some("node-a"));
    let replicas = node_operational::merged_replica_view_by_tablet(meta.pool(), 7, 0)
        .await
        .unwrap();
    assert!(replicas
        .iter()
        .any(|replica| replica.node_id == "node-b" && replica.health == "draining"));
    assert!(replicas
        .iter()
        .any(|replica| replica.node_id == "node-a" && replica.role == "voter"));

    let dir = TempDir::new().unwrap();
    let configs = vec![
        RaftRsNodeConfig::from_control_plane_replica(7, "node-a", "voter", dir.path()).unwrap(),
        RaftRsNodeConfig::from_control_plane_replica(7, "node-b", "learner", dir.path()).unwrap(),
    ];
    let leader_id = agentdb_storage::stable_raft_node_id("node-a").unwrap();
    let (group, _log) = RaftRsSharedGroup::open_local(7, configs, leader_id).unwrap();
    group.elect(leader_id).unwrap();
    registry.register(7, group).unwrap();

    app.client
        .post(format!("{}/api/admin/tablets/rebalance", app.base))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let intent = agentdb_meta::tablet_replicas::list_intent_by_tablet(meta.pool(), 7)
        .await
        .unwrap();
    assert!(!intent.iter().any(|row| row.node_id == "node-b"));
}

#[tokio::test]
async fn admin_nodes_returns_503_when_meta_is_missing() {
    let app = spawn_app_with_state(AppState::new(Runtime::in_memory())).await;

    let response = app
        .client
        .get(format!("{}/api/admin/nodes", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "KEY_UNAVAILABLE");
}

#[tokio::test]
async fn admin_node_register_and_delete_round_trip() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;

    let registered: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/nodes", app.base))
        .json(&json!({
            "node_id": "node-z",
            "address": "http://127.0.0.1:19999",
            "state": "alive",
            "availability_zone": "us-west-2a",
            "region": "us-west-2"
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(registered["node_id"], "node-z");
    assert_eq!(registered["address"], "http://127.0.0.1:19999");

    let deleted: serde_json::Value = app
        .client
        .delete(format!("{}/api/admin/nodes/node-z", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(deleted["node_id"], "node-z");
    assert_eq!(deleted["removed"], true);
}

#[tokio::test]
async fn admin_drain_marks_node_and_rejects_new_mutations() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        now_ms_i64(),
        Some("us-west-2a"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-a"))])
        .with_nodes(vec![node("node-a", "http://127.0.0.1:18080")]);
    let state = AppState::new(runtime)
        .with_meta(meta)
        .with_routing(RouteRuntime::snapshot("node-a", snapshot));
    let app = spawn_app_with_state(state).await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let drained: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/drain", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(drained["node_id"], "node-a");
    assert_eq!(drained["is_draining"], true);
    assert!(drained["drained_at_ms"].as_i64().unwrap() > 0);

    let drain_status: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/drain", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(drain_status["is_draining"], true);

    let readiness = app
        .client
        .get(format!("{}/api/admin/cluster/readiness", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(readiness.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let readiness_body: serde_json::Value = readiness.json().await.unwrap();
    assert!(readiness_body["reasons"]
        .as_array()
        .unwrap()
        .iter()
        .any(|reason| reason.as_str().unwrap().contains("draining")));

    let nodes: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/nodes", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(nodes
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["node_id"] == "node-a" && row["state"] == "draining"));

    let append = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/append-and-fetch",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": [],
            "last": 5,
            "max_bytes": 1_000_000
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(append.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let append_body: serde_json::Value = append.json().await.unwrap();
    assert_eq!(append_body["code"], "NODE_DRAINING");
}

#[tokio::test]
async fn admin_node_maintenance_can_drain_and_undrain_local_node() {
    let runtime = Runtime::in_memory();
    let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        now_ms_i64(),
        Some("us-west-2a"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-a"))])
        .with_nodes(vec![node("node-a", "http://127.0.0.1:18080")]);
    let state = AppState::new(runtime)
        .with_meta(meta)
        .with_routing(RouteRuntime::snapshot("node-a", snapshot));
    let app = spawn_app_with_state(state).await;

    let draining: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/nodes/node-a/maintenance", app.base))
        .json(&json!({"state": "draining"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(draining["state"], "draining");
    let drain_status: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/drain", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(drain_status["is_draining"], true);

    let alive: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/nodes/node-a/maintenance", app.base))
        .json(&json!({"state": "alive"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(alive["state"], "alive");
    let drain_status: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/drain", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(drain_status["is_draining"], false);
    assert_eq!(drain_status["drained_at_ms"], serde_json::Value::Null);
}

#[tokio::test]
async fn admin_backup_and_restore_validation_round_trip() {
    let runtime = Runtime::in_memory();
    let dir = std::env::temp_dir().join(format!("agentdb-api-backup-test-{}", now_ms_i64()));
    std::fs::create_dir_all(&dir).unwrap();
    let meta_path = dir.join("source-meta.sqlite");
    let meta = MetaStore::open_sqlite(&meta_path.to_string_lossy())
        .await
        .unwrap();
    agentdb_meta::nodes::upsert(
        meta.pool(),
        "node-a",
        "http://127.0.0.1:18080",
        None,
        "alive",
        now_ms_i64(),
        Some("us-west-2a"),
        Some("us-west-2"),
    )
    .await
    .unwrap();
    agentdb_meta::tablets::get_or_create(meta.pool(), 1, 100)
        .await
        .unwrap();
    let state = AppState::new(runtime).with_meta(meta);
    let app = spawn_app_with_state(state).await;
    let backup_root = dir.join("backups");

    let backup: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/backups", app.base))
        .json(&json!({
            "backup_id": "test-backup",
            "directory": backup_root.to_string_lossy()
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(backup["status"], "completed");
    let manifest_path = backup["manifest_path"].as_str().unwrap().to_string();
    assert!(std::path::Path::new(&manifest_path).exists());
    assert!(std::path::Path::new(backup["meta_backup_path"].as_str().unwrap()).exists());

    let validation: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/restores", app.base))
        .json(&json!({
            "manifest_path": manifest_path,
            "dry_run": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(validation["status"], "validated");
    assert!(validation["checks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|check| check == "meta backup exists"));

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn admin_backup_can_include_single_tablet_store_checkpoint() {
    let dir = std::env::temp_dir().join(format!("agentdb-api-tablet-backup-test-{}", now_ms_i64()));
    std::fs::create_dir_all(&dir).unwrap();
    let lifecycle = Arc::new(GroupedRocksDbTabletStore::new(
        dir.join("tablet-stores"),
        1,
        8,
    ));
    let log: Arc<dyn ReplicatedLog + Send + Sync> = Arc::new(SingleNodeReplicatedLog::new(
        Arc::new(InMemoryKvStore::new()),
    ));
    let runtime = Runtime::with_tablet_store_lifecycle_and_replicated_logs(
        lifecycle,
        Arc::new(InMemoryContentBackend::default()),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]),
    );
    runtime
        .create_tenant(NewTenant {
            external_key: "tablet-backup".into(),
            policy: json!({}),
        })
        .await
        .unwrap();
    let meta_path = dir.join("meta.sqlite");
    let meta = MetaStore::open_sqlite(&meta_path.to_string_lossy())
        .await
        .unwrap();
    cells::upsert(
        meta.pool(),
        "cell-a",
        Some("us-west-2"),
        "standard",
        "multi_az_standard",
        "stable",
        "shared_logical",
        "serving",
        Some("cell-a"),
        Some("cell-a"),
        1,
        now_ms_i64(),
    )
    .await
    .unwrap();
    placement_scopes::upsert_with_cell(
        meta.pool(),
        "tenant/tablet-backup",
        "dedicated",
        128,
        Some("seed"),
        "active",
        "cell-a",
        now_ms_i64(),
    )
    .await
    .unwrap();
    let tablet = tablets::create_with_placement(
        meta.pool(),
        RUNTIME_PRIMARY_TABLET_ID,
        "tenant/tablet-backup",
        "serving",
        Some("rg-1"),
        0,
        now_ms_i64(),
    )
    .await
    .unwrap();
    tablet_snapshots::record(
        meta.pool(),
        "snap-tablet-backup",
        RUNTIME_PRIMARY_TABLET_ID,
        "node-a",
        1,
        "node-b",
        2,
        12,
        3,
        42,
        "abc123",
        Some("s3://snapshots/snap-tablet-backup"),
        "available",
        now_ms_i64(),
        None,
    )
    .await
    .unwrap();
    let app = spawn_app_with_state(AppState::new(runtime).with_meta(meta)).await;
    let backup_root = dir.join("backups");

    let backup: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/backups", app.base))
        .json(&json!({
            "backup_id": "tablet-backup",
            "directory": backup_root.to_string_lossy(),
            "include_meta": false,
            "include_tablet_checkpoints": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let manifest_path = backup["manifest_path"].as_str().unwrap().to_string();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let checkpoints = manifest["tablet_store_checkpoints"].as_array().unwrap();
    assert_eq!(checkpoints.len(), 1);
    assert!(std::path::Path::new(checkpoints[0]["path"].as_str().unwrap()).exists());
    assert_eq!(
        checkpoints[0]["manifest"]["tablet_id"].as_i64().unwrap(),
        RUNTIME_PRIMARY_TABLET_ID
    );
    assert_eq!(
        checkpoints[0]["manifest"]["cell_id"].as_str().unwrap(),
        "cell-a"
    );
    assert_eq!(
        checkpoints[0]["manifest"]["placement_scope_id"]
            .as_str()
            .unwrap(),
        "tenant/tablet-backup"
    );
    assert_eq!(
        checkpoints[0]["manifest"]["tablet_epoch"].as_i64().unwrap(),
        tablet.epoch
    );
    assert_eq!(
        checkpoints[0]["manifest"]["latest_snapshot_index"]
            .as_u64()
            .unwrap(),
        12
    );
    assert!(
        checkpoints[0]["manifest"]["latest_applied_index"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        checkpoints[0]["manifest"]["checksum_sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );

    let validation: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/restores", app.base))
        .json(&json!({
            "manifest_path": manifest_path,
            "dry_run": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(validation["checks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|check| check == "tablet store checkpoints present (1)"));

    let mut tampered = manifest.clone();
    tampered["tablet_store_checkpoints"][0]["manifest"]["checksum_sha256"] =
        serde_json::Value::String("0".repeat(64));
    let tampered_path = backup_root
        .join("tablet-backup")
        .join("manifest-tampered.json");
    std::fs::write(
        &tampered_path,
        serde_json::to_vec_pretty(&tampered).unwrap(),
    )
    .unwrap();
    let bad_validation = app
        .client
        .post(format!("{}/api/admin/restores", app.base))
        .json(&json!({
            "manifest_path": tampered_path.to_string_lossy(),
            "dry_run": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        bad_validation.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn admin_scheduler_reports_status_snapshot() {
    let runtime = Runtime::in_memory();
    let scheduler = agentdb_api::state::SchedulerStatus::new();
    scheduler.update_leader("node-a", 7, 42_000, true);
    scheduler.add_dead_nodes_marked(2);
    scheduler.add_tablet_reassignments(3);
    scheduler.add_local_tablet_log_compactions(4, 40_500);
    scheduler.add_local_tablet_log_compaction_failure();
    scheduler.set_last_tick_at_ms(41_000);
    let state = AppState::new(runtime).with_scheduler(scheduler);
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/scheduler", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["leader_node_id"], "node-a");
    assert_eq!(body["leader_epoch"], 7);
    assert_eq!(body["leader_expires_at_ms"], 42_000);
    assert_eq!(body["is_leader"], true);
    assert_eq!(body["dead_nodes_marked_total"], 2);
    assert_eq!(body["tablet_reassignments_total"], 3);
    assert_eq!(body["local_tablet_log_compactions_total"], 4);
    assert_eq!(body["local_tablet_log_compaction_failures_total"], 1);
    assert_eq!(body["local_tablet_log_compaction_last_tick_at_ms"], 40_500);
    assert_eq!(body["last_tick_at_ms"], 41_000);
}

#[tokio::test]
async fn admin_scheduler_reports_tablet_store_domain_metrics() {
    let dir = TempDir::new().unwrap();
    let lifecycle = Arc::new(GroupedRocksDbTabletStore::new(
        dir.path().join("tablet-stores"),
        1,
        8,
    ));
    let log: Arc<dyn ReplicatedLog + Send + Sync> = Arc::new(SingleNodeReplicatedLog::new(
        Arc::new(InMemoryKvStore::new()),
    ));
    let runtime = Runtime::with_tablet_store_lifecycle_and_replicated_logs(
        lifecycle,
        Arc::new(InMemoryContentBackend::default()),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]),
    );
    runtime
        .create_tenant(NewTenant {
            external_key: "tablet-store-status".into(),
            policy: json!({}),
        })
        .await
        .unwrap();
    let state = AppState::new(runtime).with_tablet_store_status(TabletStoreStatus {
        layout: "grouped_rocksdb".into(),
        root_dir: Some(dir.path().join("tablet-stores").display().to_string()),
        tablets_per_domain: 1,
        max_open_domains: 8,
        domain_write_buffer_size_mib: 16,
        domain_max_write_buffers: 2,
        domain_parallelism: 1,
        domain_max_background_jobs: 1,
        domain_max_open_files: 128,
        domains: Vec::new(),
    });
    let app = spawn_app_with_state(state).await;

    let body: serde_json::Value = app
        .client
        .get(format!("{}/api/admin/scheduler", app.base))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    let domains = body["tablet_store"]["domains"].as_array().unwrap();
    assert_eq!(domains.len(), 1);
    assert_eq!(domains[0]["domain_id"], RUNTIME_PRIMARY_TABLET_ID);
    assert_eq!(domains[0]["open"], true);
    assert!(domains[0]["metrics"]["all_memtables_bytes"].is_u64());

    let compact: serde_json::Value = app
        .client
        .post(format!("{}/api/admin/tablet-store/compact", app.base))
        .json(&json!({
            "tablet_id": RUNTIME_PRIMARY_TABLET_ID
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(compact["status"], "compacted");
    assert_eq!(compact["tablet_id"], RUNTIME_PRIMARY_TABLET_ID);
    assert_eq!(compact["domains"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn session_handlers_proxy_remote_owner_append() {
    let (app, _owner) = spawn_proxy_pair().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?trace=1",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["seq"].as_u64().unwrap(), 0);

    let events = app
        .runtime
        .fetch_from_seq(FetchRequest {
            tenant_id: Id::parse_kind(&tid, IdKind::Tenant).unwrap(),
            session_id: Id::parse_kind(&sid, IdKind::Session).unwrap(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "x");
}

#[tokio::test]
async fn forwarded_mutation_with_stale_epoch_is_rejected_by_owner() {
    let (app, _owner) = spawn_stale_epoch_proxy_pair().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"i": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status().as_u16(), 409);
    assert_eq!(
        r.headers()[HDR_ROUTE_REASON].to_str().unwrap(),
        "ROUTE_STALE_TABLET_EPOCH"
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "ROUTE_STALE_TABLET_EPOCH");

    let events = app
        .runtime
        .fetch_from_seq(FetchRequest {
            tenant_id: Id::parse_kind(&tid, IdKind::Tenant).unwrap(),
            session_id: Id::parse_kind(&sid, IdKind::Session).unwrap(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
async fn proxy_rejects_oversized_owner_response() {
    let (app, owner) = spawn_proxy_pair_with_policy(128).await;
    let (tid, aid, sid) = bootstrap_session(&app).await;
    owner
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            owner.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "large",
            "payload": {"blob": "x".repeat(512)},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let r = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status().as_u16(), 502);
    assert_eq!(
        r.headers()[HDR_ROUTE_REASON].to_str().unwrap(),
        "ROUTE_FORWARD_RESPONSE_TOO_LARGE"
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "ROUTE_FORWARD_RESPONSE_TOO_LARGE");
}

#[tokio::test]
async fn session_handlers_return_route_rejection_for_unassigned_tablet() {
    let app = spawn_unassigned_routed_app().await;
    let (tid, _aid, sid) = bootstrap_session(&app).await;

    let r = app
        .client
        .get(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events?from_seq=0&limit=10",
            app.base
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(r.status().as_u16(), 503);
    assert_eq!(r.headers()[HDR_ROUTE_ACTION].to_str().unwrap(), "reject");
    assert_eq!(
        r.headers()[HDR_ROUTE_REASON].to_str().unwrap(),
        "ROUTE_UNASSIGNED"
    );
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["error"]["code"], "ROUTE_UNASSIGNED");
}

#[tokio::test]
async fn idempotency_key_replays_result() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let body = json!({
        "agent_id": aid,
        "kind": "x",
        "payload": {"v": 1},
        "artifact_refs": [],
        "source_refs": []
    });

    let r1: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let r2: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(r1["event_id"], r2["event_id"]);
    assert_eq!(r1["seq"], r2["seq"]);
}

#[tokio::test]
async fn idempotency_key_conflict_on_payload_change() {
    let app = spawn_app().await;
    let (tid, aid, sid) = bootstrap_session(&app).await;

    let r1 = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"v": 1},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert!(r1.status().is_success());

    let r2 = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .header("Idempotency-Key", "k1")
        .json(&json!({
            "agent_id": aid,
            "kind": "x",
            "payload": {"v": 2},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status().as_u16(), 409);
    let body: serde_json::Value = r2.json().await.unwrap();
    assert_eq!(body["code"], "IDEMPOTENCY_CONFLICT");
}

#[tokio::test]
async fn unknown_tenant_returns_404() {
    let app = spawn_app().await;
    let bogus = "ten_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .get(format!("{}/api/tenants/{bogus}", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["code"], "TENANT_NOT_FOUND");
}

#[tokio::test]
async fn create_agent_under_unknown_tenant_returns_404() {
    let app = spawn_app().await;
    let bogus = "ten_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .post(format!("{}/api/tenants/{bogus}/agents", app.base))
        .json(&json!({"alias": null, "config": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

#[tokio::test]
async fn append_to_unknown_session_returns_404() {
    let app = spawn_app().await;
    let (tid, _aid, _sid) = bootstrap_session(&app).await;
    let bogus = "ses_018f0000-0000-7000-8000-000000000000";
    let r = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{bogus}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": "agt_018f0000-0000-7000-8000-000000000000",
            "kind": "x",
            "payload": {},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

async fn bootstrap_session(app: &TestApp) -> (String, String, String) {
    let tenant = app
        .runtime
        .create_tenant(NewTenant {
            external_key: "test".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let agent = app
        .runtime
        .create_agent(NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let session = app
        .runtime
        .create_session(NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: agent.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();
    (
        tenant.tenant_id.as_str().to_string(),
        agent.agent_id.as_str().to_string(),
        session.session_id.as_str().to_string(),
    )
}

async fn spawn_routed_app(owner_node_id: &str) -> TestApp {
    let runtime = Runtime::in_memory();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some(owner_node_id))])
        .with_nodes(vec![
            node("node-a", "http://127.0.0.1:18080"),
            node("node-b", "http://127.0.0.1:18081"),
        ]);
    let state = AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", snapshot));
    spawn_app_with_state(state).await
}

async fn spawn_proxy_pair() -> (TestApp, TestApp) {
    spawn_proxy_pair_with_policy(16 * 1024 * 1024).await
}

async fn spawn_proxy_pair_with_policy(max_response_bytes: u64) -> (TestApp, TestApp) {
    let runtime = Runtime::in_memory();
    let owner_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-b"))])
        .with_nodes(vec![node("node-b", "http://127.0.0.1:18081")]);
    let owner = spawn_app_with_state(
        AppState::new(runtime.clone())
            .with_routing(RouteRuntime::snapshot("node-b", owner_snapshot)),
    )
    .await;
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, Some("node-b"))])
        .with_nodes(vec![
            node("node-a", "http://127.0.0.1:18080"),
            node("node-b", &owner.base),
        ]);
    let local = spawn_app_with_state(
        AppState::new(runtime)
            .with_routing(RouteRuntime::snapshot("node-a", snapshot))
            .with_proxy_policy(ProxyPolicy {
                timeout_ms: 5_000,
                max_response_bytes,
            }),
    )
    .await;
    (local, owner)
}

async fn spawn_stale_epoch_proxy_pair() -> (TestApp, TestApp) {
    let runtime = Runtime::in_memory();
    let owner_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet_with_epoch(0, Some("node-b"), 2)])
        .with_nodes(vec![node("node-b", "http://127.0.0.1:18081")]);
    let owner = spawn_app_with_state(
        AppState::new(runtime.clone())
            .with_routing(RouteRuntime::snapshot("node-b", owner_snapshot)),
    )
    .await;
    let local_snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet_with_epoch(0, Some("node-b"), 1)])
        .with_nodes(vec![
            node("node-a", "http://127.0.0.1:18080"),
            node("node-b", &owner.base),
        ]);
    let local = spawn_app_with_state(
        AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", local_snapshot)),
    )
    .await;
    (local, owner)
}

async fn spawn_unassigned_routed_app() -> TestApp {
    let runtime = Runtime::in_memory();
    let snapshot = RouteSnapshot::new(1)
        .with_tablets(vec![tablet(0, None)])
        .with_nodes(vec![node("node-a", "http://127.0.0.1:18080")]);
    let state = AppState::new(runtime).with_routing(RouteRuntime::snapshot("node-a", snapshot));
    spawn_app_with_state(state).await
}

fn node(id: &str, address: &str) -> Node {
    node_with_state_and_heartbeat(id, address, "alive", now_ms_i64())
}

fn node_with_state_and_heartbeat(
    id: &str,
    address: &str,
    state: &str,
    last_heartbeat_ms: i64,
) -> Node {
    Node {
        node_id: id.into(),
        address: address.into(),
        mongo_address: None,
        state: state.into(),
        last_heartbeat_ms,
        joined_at_ms: 100,
        availability_zone: Some("us-west-2a".into()),
        region: Some("us-west-2".into()),
        cell_id: agentdb_meta::cells::DEFAULT_CELL_ID.into(),
        capacity_class: agentdb_meta::cells::DEFAULT_CAPACITY_CLASS.into(),
    }
}

fn now_ms_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn tablet(id: i64, owner: Option<&str>) -> Tablet {
    tablet_with_epoch(id, owner, 1)
}

fn tablet_with_epoch(id: i64, owner: Option<&str>, epoch: i64) -> Tablet {
    Tablet {
        tablet_id: id,
        owner_node_id: owner.map(str::to_string),
        epoch,
        lease_expires_at_ms: i64::MAX,
        created_at_ms: 0,
        placement_scope_id: "default".into(),
        state: "serving".into(),
        raft_group_id: None,
        encryption_policy_version: 0,
        handoff_target_node_id: None,
    }
}
