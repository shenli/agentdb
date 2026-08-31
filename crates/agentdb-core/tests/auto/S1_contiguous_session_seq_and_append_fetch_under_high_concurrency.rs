// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S1 contiguous_session_seq_and_append_fetch_under_high_concurrency
// Falsifies: C1, C2
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

use std::{collections::BTreeSet, sync::Arc};

use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn contiguous_session_seq_and_append_fetch_under_high_concurrency() {
    let dir = TempDir::new().unwrap();
    let rt = Runtime::with_kv(open_kv(dir.path()), content());
    let (tenant, agent, session) = rt.bootstrap_test_session().await;

    let mut tasks = Vec::new();
    for i in 0..96_u64 {
        let rt = rt.clone();
        let tenant = tenant.clone();
        let agent = agent.clone();
        let session = session.clone();
        tasks.push(tokio::spawn(async move {
            if i % 10 == 0 {
                let fetched = rt
                    .fetch_from_seq(FetchRequest {
                        tenant_id: tenant.tenant_id.clone(),
                        session_id: session.session_id.clone(),
                        from_seq: 0,
                        limit: 512,
                    })
                    .await
                    .unwrap();
                return (None, fetched.len());
            }
            let response = rt
                .append_and_fetch(
                    AppendRequest::simple(
                        &tenant,
                        &agent,
                        &session,
                        "stress",
                        serde_json::json!({ "i": i }),
                    ),
                    8,
                    1024 * 1024,
                )
                .await
                .unwrap();
            assert!(
                response
                    .recent
                    .iter()
                    .any(|event| event.event_id == response.event_id && event.seq == response.seq),
                "append-and-fetch response must contain its own committed event"
            );
            (
                Some((response.seq, response.event_id)),
                response.recent.len(),
            )
        }));
    }

    let mut committed = Vec::new();
    let mut oracle_checks = 0_usize;
    for task in tasks {
        let (appended, checks) = task.await.unwrap();
        oracle_checks += checks.max(1);
        if let Some(appended) = appended {
            committed.push(appended);
        }
    }
    assert_eq!(committed.len(), 86);

    let mut seqs = committed.iter().map(|(seq, _)| *seq).collect::<Vec<_>>();
    seqs.sort_unstable();
    assert_eq!(seqs, (0..committed.len() as u64).collect::<Vec<_>>());
    assert_eq!(
        committed
            .iter()
            .map(|(_, event_id)| event_id.as_str().to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        committed.len(),
        "event IDs must be unique"
    );
    assert!(oracle_checks >= committed.len());

    drop(rt);
    let reopened = Runtime::with_kv(open_kv(dir.path()), content());
    let events = reopened
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id,
            session_id: session.session_id,
            from_seq: 0,
            limit: 512,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), committed.len());
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (0..committed.len() as u64).collect::<Vec<_>>()
    );
}
