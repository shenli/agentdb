use agentdb_core::artifact::NewArtifact;
use agentdb_core::errors::ErrorCode;
use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{Runtime, SearchRequest};
use agentdb_core::session::Lifecycle;
use agentdb_core::{NewAgent, NewSession, NewTenant, Tenant};

async fn tenant_with_policy(rt: &Runtime, key: &str, policy: serde_json::Value) -> Tenant {
    rt.create_tenant(NewTenant {
        external_key: key.into(),
        policy,
    })
    .await
    .unwrap()
}

async fn session_for(rt: &Runtime, tenant: &Tenant) -> agentdb_core::Session {
    let agent = rt
        .create_agent(NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    rt.create_session(NewSession {
        tenant_id: tenant.tenant_id.clone(),
        agent_id: agent.agent_id,
        alias: None,
    })
    .await
    .unwrap()
}

fn new_record(tenant: &Tenant, text: &str) -> NewRecord {
    NewRecord {
        tenant_id: tenant.tenant_id.clone(),
        namespace: vec!["docs".into()],
        scope: serde_json::json!({}),
        text: Some(text.into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn session_lifecycle_transitions_from_policy() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "lifecycle",
        serde_json::json!({
            "version": 9,
            "lifecycle": {"cold_after_ms": 10, "archive_after_ms": 20}
        }),
    )
    .await;
    let session = session_for(&rt, &tenant).await;

    let report = rt
        .sweep_lifecycle_at(session.last_active_at_ms + 10)
        .await
        .unwrap();
    assert_eq!(report.sessions_transitioned, 1);
    let cold = rt
        .get_session(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(cold.lifecycle, Lifecycle::Cold);
    assert_eq!(cold.policy_version, 9);

    let report = rt
        .sweep_lifecycle_at(session.last_active_at_ms + 20)
        .await
        .unwrap();
    assert_eq!(report.sessions_transitioned, 1);
    let archived = rt
        .get_session(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(archived.lifecycle, Lifecycle::Archive);
}

#[tokio::test]
async fn derivative_only_purges_raw_artifact_but_keeps_metadata() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "derivative-only",
        serde_json::json!({
            "lifecycle": {"derivative_only_after_ms": 0}
        }),
    )
    .await;
    let artifact = rt
        .create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: Some("text/plain".into()),
                expected_sha256: None,
            },
            b"raw bytes".to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(
        rt.get_artifact_blob(&tenant.tenant_id, &artifact.artifact_id)
            .await
            .unwrap(),
        b"raw bytes".to_vec()
    );

    let report = rt.sweep_lifecycle_at(artifact.created_at_ms).await.unwrap();
    assert_eq!(report.artifacts_raw_purged, 1);
    let metadata = rt
        .get_artifact(&tenant.tenant_id, &artifact.artifact_id)
        .await
        .unwrap();
    assert_eq!(metadata.artifact_id, artifact.artifact_id);
    assert!(metadata.raw_purged_at_ms.is_some());

    let err = rt
        .get_artifact_blob(&tenant.tenant_id, &artifact.artifact_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);
}

#[tokio::test]
async fn gc_purges_soft_deleted_record_after_retention() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "record-gc",
        serde_json::json!({"retention": {"retention_ms": 0}}),
    )
    .await;
    let record = rt.write_record(new_record(&tenant, "alpha")).await.unwrap();
    rt.delete_record(&tenant.tenant_id, &record.record_id)
        .await
        .unwrap();
    let deleted = rt
        .get_record_with_options(&tenant.tenant_id, &record.record_id, true)
        .await
        .unwrap();
    assert!(deleted.deleted_at_ms.is_some());

    let report = rt.gc_sweep_at(u64::MAX).await.unwrap();
    assert_eq!(report.records_purged, 1);
    let err = rt
        .get_record_with_options(&tenant.tenant_id, &record.record_id, true)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);
    let hits = rt
        .search_records(SearchRequest::text_only(
            tenant.tenant_id,
            vec!["docs".into()],
            "alpha",
            10,
        ))
        .await
        .unwrap();
    assert!(hits.is_empty());
}

#[tokio::test]
async fn gc_purges_soft_deleted_artifact_after_retention() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "artifact-gc",
        serde_json::json!({"retention": {"retention_ms": 0}}),
    )
    .await;
    let artifact = rt
        .create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            b"delete me".to_vec(),
        )
        .await
        .unwrap();
    rt.delete_artifact(&tenant.tenant_id, &artifact.artifact_id)
        .await
        .unwrap();

    let report = rt.gc_sweep_at(u64::MAX).await.unwrap();
    assert_eq!(report.artifacts_purged, 1);
    let err = rt
        .get_artifact(&tenant.tenant_id, &artifact.artifact_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);
    let err = rt
        .get_artifact_blob(&tenant.tenant_id, &artifact.artifact_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactNotFound);
}

#[tokio::test]
async fn gc_respects_legal_hold() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "legal-hold",
        serde_json::json!({"retention": {"retention_ms": 0, "legal_hold": true}}),
    )
    .await;
    let record = rt.write_record(new_record(&tenant, "hold")).await.unwrap();
    rt.delete_record(&tenant.tenant_id, &record.record_id)
        .await
        .unwrap();

    let report = rt.gc_sweep_at(u64::MAX).await.unwrap();
    assert_eq!(report.records_purged, 0);
    let deleted = rt
        .get_record_with_options(&tenant.tenant_id, &record.record_id, true)
        .await
        .unwrap();
    assert!(deleted.deleted_at_ms.is_some());
}

#[tokio::test]
async fn tenant_scoped_sweeps_do_not_touch_other_tenants() {
    let rt = Runtime::in_memory();
    let t1 = tenant_with_policy(
        &rt,
        "scope-1",
        serde_json::json!({
            "lifecycle": {"cold_after_ms": 0},
            "retention": {"retention_ms": 0}
        }),
    )
    .await;
    let t2 = tenant_with_policy(
        &rt,
        "scope-2",
        serde_json::json!({
            "lifecycle": {"cold_after_ms": 0},
            "retention": {"retention_ms": 0}
        }),
    )
    .await;
    let s1 = session_for(&rt, &t1).await;
    let s2 = session_for(&rt, &t2).await;

    let report = rt
        .sweep_lifecycle_for_tenant_at(&t1.tenant_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(report.sessions_transitioned, 1);
    assert_eq!(
        rt.get_session(&t1.tenant_id, &s1.session_id)
            .await
            .unwrap()
            .lifecycle,
        Lifecycle::Cold
    );
    assert_eq!(
        rt.get_session(&t2.tenant_id, &s2.session_id)
            .await
            .unwrap()
            .lifecycle,
        Lifecycle::Warm
    );

    let r1 = rt
        .write_record(new_record(&t1, "tenant one"))
        .await
        .unwrap();
    let r2 = rt
        .write_record(new_record(&t2, "tenant two"))
        .await
        .unwrap();
    rt.delete_record(&t1.tenant_id, &r1.record_id)
        .await
        .unwrap();
    rt.delete_record(&t2.tenant_id, &r2.record_id)
        .await
        .unwrap();

    let report = rt
        .gc_sweep_for_tenant_at(&t1.tenant_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(report.records_purged, 1);
    assert_eq!(
        rt.get_record_with_options(&t1.tenant_id, &r1.record_id, true)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RecordNotFound
    );
    assert!(rt
        .get_record_with_options(&t2.tenant_id, &r2.record_id, true)
        .await
        .unwrap()
        .deleted_at_ms
        .is_some());
}
