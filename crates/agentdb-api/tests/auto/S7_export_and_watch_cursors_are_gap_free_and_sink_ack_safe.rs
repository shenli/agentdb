// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S7 export_and_watch_cursors_are_gap_free_and_sink_ack_safe
// Falsifies: C11, C12, C17
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

#[path = "../common/mod.rs"]
mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

use agentdb_api::AppState;
use agentdb_core::Runtime;
use agentdb_storage::{ContentBackend, InMemoryContentBackend, KvStore, RocksKvStore};
use common::{spawn_app_with_state, TestApp};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    static STORES: OnceLock<Mutex<BTreeMap<PathBuf, Arc<dyn KvStore + Send + Sync>>>> =
        OnceLock::new();
    let stores = STORES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut stores = stores.lock().expect("kv registry poisoned");
    if let Some(kv) = stores.get(path) {
        return kv.clone();
    }
    let kv: Arc<dyn KvStore + Send + Sync> =
        Arc::new(RocksKvStore::open(path).expect("open S7 rocks kv"));
    stores.insert(path.to_path_buf(), kv.clone());
    kv
}

async fn spawn_persistent_app() -> (TestApp, TempDir) {
    let dir = TempDir::new().unwrap();
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());
    let runtime = Runtime::with_kv(open_kv(dir.path()), content);
    let state = AppState::new(runtime);
    (spawn_app_with_state(state).await, dir)
}

async fn seed(app: &TestApp, key: &str) -> (String, String, String) {
    let tenant: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants", app.base))
        .json(&json!({"external_key": key, "policy": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let tid = tenant["tenant_id"].as_str().unwrap().to_string();
    let agent: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/agents", app.base))
        .json(&json!({"alias": format!("{key}-agent"), "config": {}}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let aid = agent["agent_id"].as_str().unwrap().to_string();
    let session: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/agents/{aid}/sessions",
            app.base
        ))
        .json(&json!({"alias": format!("{key}-session")}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        tid,
        aid,
        session["session_id"].as_str().unwrap().to_string(),
    )
}

async fn append(app: &TestApp, tid: &str, aid: &str, sid: &str, n: u64) {
    app.client
        .post(format!(
            "{}/api/tenants/{tid}/sessions/{sid}/events",
            app.base
        ))
        .json(&json!({
            "agent_id": aid,
            "kind": "message",
            "payload": {"n": n},
            "artifact_refs": [],
            "source_refs": []
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

fn export_request(
    app: &TestApp,
    tid: &str,
    sid: &str,
    limit: usize,
    cursor: Option<&str>,
) -> reqwest::RequestBuilder {
    let mut query = vec![
        ("scope", "session".to_string()),
        ("session_id", sid.to_string()),
        ("include", "events".to_string()),
        ("limit", limit.to_string()),
    ];
    if let Some(cursor) = cursor {
        query.push(("from_cursor", cursor.to_string()));
    }
    app.client
        .get(format!("{}/api/tenants/{tid}/exports/watch", app.base))
        .query(&query)
}

fn tenant_export_request(
    app: &TestApp,
    tid: &str,
    limit: usize,
    cursor: Option<&str>,
) -> reqwest::RequestBuilder {
    let mut query = vec![
        ("scope", "tenant".to_string()),
        ("include", "events".to_string()),
        ("limit", limit.to_string()),
    ];
    if let Some(cursor) = cursor {
        query.push(("from_cursor", cursor.to_string()));
    }
    app.client
        .get(format!("{}/api/tenants/{tid}/exports/watch", app.base))
        .query(&query)
}

async fn export_page(
    app: &TestApp,
    tid: &str,
    sid: &str,
    limit: usize,
    cursor: Option<&str>,
) -> serde_json::Value {
    export_request(app, tid, sid, limit, cursor)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn tenant_export_page(
    app: &TestApp,
    tid: &str,
    limit: usize,
    cursor: Option<&str>,
) -> serde_json::Value {
    tenant_export_request(app, tid, limit, cursor)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn create_consumer(app: &TestApp, tid: &str) {
    let created: serde_json::Value = app
        .client
        .post(format!("{}/api/tenants/{tid}/exports/consumers", app.base))
        .json(&json!({
            "name": "auto-clickhouse",
            "scope": "tenant",
            "include": ["events"]
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(created["last_acked_cursor"].is_null());
}

async fn claim_consumer(app: &TestApp, tid: &str, owner: &str, lease_ms: u64) {
    let claimed: serde_json::Value = app
        .client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/auto-clickhouse/claim",
            app.base
        ))
        .json(&json!({"lease_owner": owner, "lease_ms": lease_ms}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(claimed["lease_owner"], owner);
}

async fn ack_consumer(app: &TestApp, tid: &str, owner: &str, cursor: &str) -> serde_json::Value {
    app.client
        .post(format!(
            "{}/api/tenants/{tid}/exports/consumers/auto-clickhouse/ack",
            app.base
        ))
        .json(&json!({
            "lease_owner": owner,
            "cursor": cursor,
            "snapshot_high_watermark": cursor
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn consumer_status(app: &TestApp, tid: &str) -> serde_json::Value {
    app.client
        .get(format!(
            "{}/api/tenants/{tid}/exports/consumers/auto-clickhouse",
            app.base
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[derive(Default)]
struct Sink {
    accepted: BTreeMap<String, serde_json::Value>,
    partial_attempts: Vec<String>,
}

impl Sink {
    fn accept_full(&mut self, item: &serde_json::Value) {
        let cursor = item["cursor"].as_str().unwrap().to_string();
        self.accepted.entry(cursor).or_insert_with(|| item.clone());
    }

    fn accept_partial_without_commit(&mut self, item: &serde_json::Value) {
        self.partial_attempts
            .push(item["cursor"].as_str().unwrap().to_string());
    }

    fn cursors(&self) -> BTreeSet<String> {
        self.accepted.keys().cloned().collect()
    }
}

#[tokio::test]
async fn export_and_watch_cursors_are_gap_free_and_sink_ack_safe() {
    let (mut app, _dir) = spawn_persistent_app().await;
    let (tid, aid, sid) = seed(&app, "auto-export").await;
    for n in 0..6_u64 {
        append(&app, &tid, &aid, &sid, n).await;
    }

    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = export_page(&app, &tid, &sid, 2, cursor.as_deref()).await;
        let items = page["items"].as_array().unwrap();
        if items.is_empty() {
            break;
        }
        for item in items {
            let cursor = item["cursor"].as_str().unwrap().to_string();
            assert!(
                !seen.iter().any(|prior: &String| prior == &cursor),
                "cursor must not duplicate: {cursor}"
            );
            seen.push(cursor);
        }
        cursor = page["next_cursor"]
            .as_str()
            .or_else(|| page["high_watermark"].as_str())
            .map(str::to_string);
        assert!(
            cursor.is_some(),
            "export page did not carry a cursor: {page}"
        );
        if seen.len() >= 6 {
            break;
        }
    }
    assert_eq!(seen.len(), 6);
    assert_eq!(seen.iter().collect::<BTreeSet<_>>().len(), 6);

    let (other_tid, other_aid, other_sid) = seed(&app, "auto-export-other-tenant").await;
    append(&app, &other_tid, &other_aid, &other_sid, 99).await;
    let wrong_scope = export_request(
        &app,
        &other_tid,
        &other_sid,
        10,
        seen.first().map(String::as_str),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(
        wrong_scope.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    create_consumer(&app, &tid).await;
    let mut sink = Sink::default();

    claim_consumer(&app, &tid, "worker-before-sink", 1).await;
    let first_page = tenant_export_page(&app, &tid, 3, None).await;
    let first_items = first_page["items"].as_array().unwrap();
    assert_eq!(first_items.len(), 3);
    let before_sink_status = consumer_status(&app, &tid).await;
    assert!(before_sink_status["last_acked_cursor"].is_null());
    assert_eq!(before_sink_status["pending_items"], 6);

    sleep(Duration::from_millis(5)).await;
    claim_consumer(&app, &tid, "worker-partial-sink", 1).await;
    sink.accept_partial_without_commit(&first_items[0]);
    let partial_status = consumer_status(&app, &tid).await;
    assert!(partial_status["last_acked_cursor"].is_null());
    assert!(sink.accepted.is_empty());

    sleep(Duration::from_millis(5)).await;
    claim_consumer(&app, &tid, "worker-full-sink-crash", 1).await;
    for item in first_items {
        sink.accept_full(item);
    }
    let full_sink_status = consumer_status(&app, &tid).await;
    assert!(full_sink_status["last_acked_cursor"].is_null());
    assert_eq!(sink.accepted.len(), 3);

    sleep(Duration::from_millis(5)).await;
    let runtime = app.runtime.clone();
    drop(app);
    app = spawn_app_with_state(AppState::new(runtime)).await;
    claim_consumer(&app, &tid, "worker-ack-after-replay", 60_000).await;
    for item in first_items {
        sink.accept_full(item);
    }
    let first_ack_cursor = first_items.last().unwrap()["cursor"].as_str().unwrap();
    let acked = ack_consumer(&app, &tid, "worker-ack-after-replay", first_ack_cursor).await;
    assert_eq!(acked["last_acked_cursor"], first_ack_cursor);
    assert_eq!(sink.accepted.len(), 3);

    let remaining_page = tenant_export_page(&app, &tid, 10, Some(first_ack_cursor)).await;
    let remaining_items = remaining_page["items"].as_array().unwrap();
    assert_eq!(remaining_items.len(), 3);
    for item in remaining_items {
        sink.accept_full(item);
    }
    let final_cursor = remaining_items.last().unwrap()["cursor"].as_str().unwrap();
    let final_ack = ack_consumer(&app, &tid, "worker-ack-after-replay", final_cursor).await;
    assert_eq!(final_ack["last_acked_cursor"], final_cursor);

    let no_replay_page = tenant_export_page(&app, &tid, 10, Some(final_cursor)).await;
    assert!(no_replay_page["items"].as_array().unwrap().is_empty());
    let final_status = consumer_status(&app, &tid).await;
    assert_eq!(final_status["last_acked_cursor"], final_cursor);
    assert_eq!(final_status["pending_items"], 0);
    let expected_sink_cursors = first_items
        .iter()
        .chain(remaining_items.iter())
        .map(|item| item["cursor"].as_str().unwrap().to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(sink.cursors(), expected_sink_cursors);
}
