use agentdb_core::artifact::NewArtifact;
use agentdb_core::errors::ErrorCode;
use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime, SearchRequest};
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
    assert_eq!(report.seal_segment_actions, 1);
    assert_eq!(report.move_to_cold_actions, 1);
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
    assert_eq!(report.archive_actions, 1);
    let archived = rt
        .get_session(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(archived.lifecycle, Lifecycle::Archive);
}

#[tokio::test]
async fn cold_transition_seals_history_and_append_reopens_warm_head() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "tiering",
        serde_json::json!({
            "version": 3,
            "lifecycle": {"cold_after_ms": 10}
        }),
    )
    .await;
    let agent = rt
        .create_agent(agentdb_core::NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let session = rt
        .create_session(agentdb_core::NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: agent.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    for i in 0..3 {
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }
    let baseline = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(baseline.len(), 3);

    let storage = rt
        .transition_session_lifecycle(&tenant.tenant_id, &session.session_id, Lifecycle::Cold)
        .await
        .unwrap();
    assert_eq!(storage.lifecycle, Lifecycle::Cold);
    assert_eq!(storage.sealed_segment_count, 1);
    assert_eq!(storage.warm_head_from_seq, 3);

    let cold_fetch = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(cold_fetch.len(), 3);
    assert_eq!(
        cold_fetch.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let resumed = rt
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"i": 99}),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.seq, 3);

    let warm_storage = rt
        .get_session_storage(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(warm_storage.lifecycle, Lifecycle::Warm);
    assert_eq!(warm_storage.warm_head_from_seq, 3);
    assert_eq!(warm_storage.sealed_segment_count, 1);

    let merged = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(merged.len(), 4);
    assert_eq!(
        merged.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

#[tokio::test]
async fn archive_transition_compacts_history_and_append_reopens_warm_head() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "archive-tiering",
        serde_json::json!({
            "version": 4,
            "lifecycle": {"cold_after_ms": 10, "archive_after_ms": 20}
        }),
    )
    .await;
    let agent = rt
        .create_agent(agentdb_core::NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let session = rt
        .create_session(agentdb_core::NewSession {
            tenant_id: tenant.tenant_id.clone(),
            agent_id: agent.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    for i in 0..3 {
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"i": i}),
        ))
        .await
        .unwrap();
    }

    let storage = rt
        .transition_session_lifecycle(&tenant.tenant_id, &session.session_id, Lifecycle::Archive)
        .await
        .unwrap();
    assert_eq!(storage.lifecycle, Lifecycle::Archive);
    assert_eq!(storage.sealed_segment_count, 0);
    assert!(storage.archive_manifest.is_some());

    let archived_fetch = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        archived_fetch
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let resumed = rt
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"i": 99}),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.seq, 3);

    let warm_storage = rt
        .get_session_storage(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(warm_storage.lifecycle, Lifecycle::Warm);
    assert!(warm_storage.archive_manifest.is_some());
    assert_eq!(warm_storage.warm_head_from_seq, 3);

    let merged = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        merged.iter().map(|event| event.seq).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
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

#[tokio::test]
async fn session_delete_blocks_reads_and_gc_purges_events() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "session-gc",
        serde_json::json!({"retention": {"retention_ms": 0}}),
    )
    .await;
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
            alias: Some("gone".into()),
        })
        .await
        .unwrap();
    let appended = rt
        .append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "x",
            serde_json::json!({"i": 1}),
        ))
        .await
        .unwrap();

    rt.delete_session(&tenant.tenant_id, &session.session_id)
        .await
        .unwrap();
    assert_eq!(
        rt.get_session(&tenant.tenant_id, &session.session_id)
            .await
            .unwrap_err()
            .code,
        ErrorCode::SessionNotFound
    );
    assert_eq!(
        rt.fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::SessionNotFound
    );
    assert_eq!(
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "x",
            serde_json::json!({"i": 2}),
        ))
        .await
        .unwrap_err()
        .code,
        ErrorCode::SessionNotFound
    );

    let report = rt
        .gc_sweep_for_tenant_at(&tenant.tenant_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(report.sessions_purged, 1);
    assert_eq!(report.events_purged, 1);
    assert_eq!(
        rt.lookup_event_by_id(&tenant.tenant_id, &appended.event_id)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn agent_soft_delete_gc_cascades_sessions_and_bindings() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "agent-gc",
        serde_json::json!({"retention": {"retention_ms": 0}}),
    )
    .await;
    let agent = rt
        .create_agent(NewAgent {
            tenant_id: tenant.tenant_id.clone(),
            alias: Some("worker".into()),
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
    rt.append(AppendRequest::simple(
        &tenant,
        &agent,
        &session,
        "x",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    let skill = rt
        .register_skill(
            agentdb_core::NewSkill {
                tenant_id: tenant.tenant_id.clone(),
                name: "tool".into(),
                version: "1.0.0".into(),
                manifest: serde_json::json!({}),
                artifact_refs: vec![],
            },
            None,
        )
        .await
        .unwrap();
    rt.bind_skill(agentdb_core::BindSkillRequest {
        tenant_id: tenant.tenant_id.clone(),
        agent_id: agent.agent_id.clone(),
        skill_id: skill.skill_id,
        idempotency_key: None,
    })
    .await
    .unwrap();

    rt.delete_agent(&tenant.tenant_id, &agent.agent_id)
        .await
        .unwrap();
    assert_eq!(
        rt.get_agent(&tenant.tenant_id, &agent.agent_id)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AgentNotFound
    );

    let report = rt.gc_sweep_at(u64::MAX).await.unwrap();
    assert_eq!(report.agents_purged, 1);
    assert_eq!(report.sessions_purged, 1);
    assert_eq!(report.events_purged, 1);
    assert_eq!(report.skill_bindings_purged, 1);
    assert_eq!(report.changefeed_purge_events, 2);
}

#[tokio::test]
async fn tenant_soft_delete_gc_expands_to_children_then_tenant() {
    let rt = Runtime::in_memory();
    let tenant = tenant_with_policy(
        &rt,
        "tenant-gc",
        serde_json::json!({"retention": {"retention_ms": 0}}),
    )
    .await;
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
    rt.append(AppendRequest::simple(
        &tenant,
        &agent,
        &session,
        "x",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    let record = rt
        .write_record(new_record(&tenant, "tenant record"))
        .await
        .unwrap();
    let artifact = rt
        .create_artifact(
            NewArtifact {
                tenant_id: tenant.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            b"tenant artifact".to_vec(),
        )
        .await
        .unwrap();

    rt.delete_tenant(&tenant.tenant_id).await.unwrap();
    assert_eq!(
        rt.get_agent(&tenant.tenant_id, &agent.agent_id)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AgentNotFound
    );
    assert_eq!(
        rt.fetch_from_seq(FetchRequest {
            tenant_id: tenant.tenant_id.clone(),
            session_id: session.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::SessionNotFound
    );
    assert_eq!(
        rt.get_record_with_options(&tenant.tenant_id, &record.record_id, true)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RecordNotFound
    );
    assert_eq!(
        rt.get_artifact(&tenant.tenant_id, &artifact.artifact_id)
            .await
            .unwrap_err()
            .code,
        ErrorCode::ArtifactNotFound
    );
    let report = rt.gc_sweep_at(u64::MAX).await.unwrap();
    assert_eq!(report.tenants_purged, 1);
    assert_eq!(report.agents_purged, 1);
    assert_eq!(report.sessions_purged, 1);
    assert_eq!(report.events_purged, 1);
    assert_eq!(report.records_purged, 1);
    assert_eq!(report.artifacts_purged, 1);
    assert_eq!(
        rt.get_tenant(&tenant.tenant_id).await.unwrap_err().code,
        ErrorCode::TenantNotFound
    );
    assert_eq!(
        rt.get_record_with_options(&tenant.tenant_id, &record.record_id, true)
            .await
            .unwrap_err()
            .code,
        ErrorCode::RecordNotFound
    );
    assert_eq!(
        rt.get_artifact(&tenant.tenant_id, &artifact.artifact_id)
            .await
            .unwrap_err()
            .code,
        ErrorCode::ArtifactNotFound
    );
}
