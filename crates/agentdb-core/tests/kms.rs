use std::sync::Arc;

use agentdb_core::{
    runtime::AppendRequest, ErrorCode, NewAgent, NewArtifact, NewSession, NewTenant, Runtime,
};
use agentdb_storage::{StaticKeyState, StaticKmsKeyResolver};

async fn bootstrap(
    rt: &Arc<Runtime>,
) -> (
    agentdb_core::Tenant,
    agentdb_core::Agent,
    agentdb_core::Session,
) {
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "kms-tenant".into(),
            policy: Default::default(),
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
    (tenant, agent, session)
}

#[tokio::test]
async fn unavailable_key_rejects_artifact_content_write() {
    let kms = Arc::new(StaticKmsKeyResolver::new());
    let rt = Runtime::in_memory_with_kms(kms.clone());
    let (tenant, _agent, _session) = bootstrap(&rt).await;
    kms.set_state(
        tenant.tenant_id.as_str(),
        StaticKeyState::Unavailable {
            reason: "key disabled".into(),
        },
    );

    let err = rt
        .create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: None,
            },
            b"blocked".to_vec(),
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::KeyUnavailable);
    assert!(err.retryable);
    assert!(err.message.contains("content_write"));
    assert_eq!(
        err.suggestion.as_deref(),
        Some("restore or rotate the tenant encryption key before retrying")
    );
}

#[tokio::test]
async fn unavailable_key_rejects_event_metadata_write() {
    let kms = Arc::new(StaticKmsKeyResolver::new());
    let rt = Runtime::in_memory_with_kms(kms.clone());
    let (tenant, agent, session) = bootstrap(&rt).await;
    kms.set_state(
        tenant.tenant_id.as_str(),
        StaticKeyState::Unavailable {
            reason: "pending rotation".into(),
        },
    );

    let err = rt
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"text": "blocked"}),
        ))
        .await
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::KeyUnavailable);
    assert!(err.message.contains("metadata_write"));
}
