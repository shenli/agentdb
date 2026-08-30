// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S2 idempotent_replay_after_lost_ack_restart_and_handoff
// Falsifies: C3, C4, C15, C17, C20
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

use std::sync::Arc;

use agentdb_core::{
    errors::ErrorCode,
    runtime::{AppendRequest, FetchRequest, Runtime},
};
use agentdb_storage::{ContentBackend, InMemoryContentBackend, KvStore, RocksKvStore};
use tempfile::TempDir;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    // A real restart: close-and-reopen from disk, exercising WAL recovery.
    // The previous runtime\'s adaptive-write thread releases its KV handle
    // asynchronously after drop, so briefly retry while RocksDB\'s LOCK file
    // is still held rather than caching a warm handle (a cached handle would
    // turn every "restart" test into a re-hydration test).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match RocksKvStore::open(path) {
            Ok(kv) => return Arc::new(kv),
            Err(err) => {
                let message = err.to_string();
                if std::time::Instant::now() >= deadline
                    || !(message.contains("lock") || message.contains("LOCK"))
                {
                    panic!("open rocks kv: {message}");
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
        }
    }
}

fn content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

#[tokio::test]
async fn idempotent_replay_after_lost_ack_restart_and_handoff() {
    let dir = TempDir::new().unwrap();
    let rt = Runtime::with_kv(open_kv(dir.path()), content());
    let (tenant, agent, session) = rt.bootstrap_test_session().await;

    let mut append = AppendRequest::simple(
        &tenant,
        &agent,
        &session,
        "tool_result",
        serde_json::json!({ "call_id": "call-1", "result": "ok" }),
    );
    append.idempotency_key = Some("lost-ack-append".into());
    let first = rt.append(append.clone()).await.unwrap();

    let fetched = rt
        .append_and_fetch(
            AppendRequest::simple(&tenant, &agent, &session, "tail", serde_json::json!({})),
            10,
            1024 * 1024,
        )
        .await
        .unwrap();
    assert_eq!(fetched.seq, 1);
    drop(rt);

    let reopened = Runtime::with_kv(open_kv(dir.path()), content());
    let replay = reopened.append(append.clone()).await.unwrap();
    assert_eq!(replay.event_id, first.event_id);
    assert_eq!(replay.seq, first.seq);

    let mut conflicting = append;
    conflicting.payload = serde_json::json!({ "call_id": "call-1", "result": "different" });
    let err = reopened.append(conflicting).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::IdempotencyConflict);

    let events = reopened
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id,
            session_id: session.session_id,
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "tool_result")
            .count(),
        1,
        "idempotent retry after restart must not create a second effect"
    );
}
