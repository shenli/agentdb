//! Tenant-isolation tests for the V2 Runtime.
//!
//! These guard the invariant that a caller in tenant A cannot reach into
//! tenant B's resources by passing IDs they obtained out-of-band — even when
//! the IDs are valid and the underlying record exists.

use agentdb_core::agent::NewAgent;
use agentdb_core::errors::ErrorCode;
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;

#[tokio::test]
async fn create_session_rejects_cross_tenant_agent() {
    let rt = Runtime::in_memory();
    let t1 = rt
        .create_tenant(NewTenant {
            external_key: "t1".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let t2 = rt
        .create_tenant(NewTenant {
            external_key: "t2".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let a1 = rt
        .create_agent(NewAgent {
            tenant_id: t1.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    // Try to create a session under t2 using a1 (which belongs to t1). The
    // runtime must refuse — and the error should not leak the fact that a1
    // exists under a different tenant.
    let err = rt
        .create_session(NewSession {
            tenant_id: t2.tenant_id.clone(),
            agent_id: a1.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AgentNotFound);
}

#[tokio::test]
async fn append_rejects_cross_tenant_session() {
    let rt = Runtime::in_memory();
    let (_t1, _a1, s1) = rt.bootstrap_test_session().await;
    let t2 = rt
        .create_tenant(NewTenant {
            external_key: "t2".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let a2 = rt
        .create_agent(NewAgent {
            tenant_id: t2.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let req = AppendRequest {
        tenant_id: t2.tenant_id.clone(),
        agent_id: a2.agent_id.clone(),
        session_id: s1.session_id.clone(),
        kind: "x".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
    };
    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionNotFound);
}

#[tokio::test]
async fn append_rejects_wrong_agent_for_session() {
    let rt = Runtime::in_memory();
    let (t1, _a1, s1) = rt.bootstrap_test_session().await;
    let a2 = rt
        .create_agent(NewAgent {
            tenant_id: t1.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let req = AppendRequest {
        tenant_id: t1.tenant_id.clone(),
        agent_id: a2.agent_id.clone(),
        session_id: s1.session_id.clone(),
        kind: "x".into(),
        payload: serde_json::json!({}),
        artifact_refs: vec![],
        source_refs: vec![],
        idempotency_key: None,
    };
    let err = rt.append(req).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::AgentNotFound);
}

#[tokio::test]
async fn fetch_from_seq_rejects_cross_tenant() {
    let rt = Runtime::in_memory();
    let (_t1, _a1, s1) = rt.bootstrap_test_session().await;
    let t2 = rt
        .create_tenant(NewTenant {
            external_key: "t2".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let err = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t2.tenant_id.clone(),
            session_id: s1.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SessionNotFound);
}
