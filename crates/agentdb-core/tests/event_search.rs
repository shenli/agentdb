//! Scoped, text-only event-log search (`Runtime::search_events`) — the core
//! "an agent queries its own past sessions" capability. Exercised here over a
//! persistent runtime; the API tests cover the in-memory path.

use agentdb_core::runtime::{AppendRequest, EventSearchRequest, Runtime};
use agentdb_core::{NewAgent, NewSession, NewTenant};
use agentdb_storage::{InMemoryContentBackend, InMemoryKvStore};
use std::sync::Arc;

async fn persistent_runtime() -> Arc<Runtime> {
    Runtime::with_kv(
        Arc::new(InMemoryKvStore::new()),
        Arc::new(InMemoryContentBackend::default()),
    )
}

fn req(tenant_id: &agentdb_core::Id, query: &str) -> EventSearchRequest {
    EventSearchRequest {
        tenant_id: tenant_id.clone(),
        session_id: None,
        agent_id: None,
        query: query.into(),
        kind: None,
        limit: 20,
    }
}

#[tokio::test]
async fn search_events_matches_and_ranks_over_persistent_log() {
    let rt = persistent_runtime().await;
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "ev-search".into(),
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

    for (kind, text) in [
        ("message", "deploy the service"),
        ("message", "deploy the service to staging"),
        ("note", "unrelated"),
    ] {
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            kind,
            serde_json::json!({ "text": text }),
        ))
        .await
        .unwrap();
    }

    // Two events mention "deploy"; the one that also contains "staging" ranks
    // first by token coverage.
    let matches = rt
        .search_events(req(&tenant.tenant_id, "deploy staging"))
        .await
        .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0].event.payload["text"], "deploy the service to staging");
    assert!(matches[0].score > matches[1].score);

    // Empty query and no-hit query both return no matches (not an error).
    assert!(rt
        .search_events(req(&tenant.tenant_id, "   "))
        .await
        .unwrap()
        .is_empty());
    assert!(rt
        .search_events(req(&tenant.tenant_id, "absent"))
        .await
        .unwrap()
        .is_empty());

    // kind filter restricts the match set.
    let mut kind_req = req(&tenant.tenant_id, "deploy");
    kind_req.kind = Some("note".into());
    assert!(rt.search_events(kind_req).await.unwrap().is_empty());
}
