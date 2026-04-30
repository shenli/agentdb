use agentdb_core::artifact::NewArtifact;
use agentdb_core::errors::ErrorCode;
use agentdb_core::policy::{Priority, RuntimePolicy};
use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::{NewAgent, NewSession, NewTenant};

#[tokio::test]
async fn tenant_policy_enforces_append_quota_and_stamps_version() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "free".into(),
            policy: serde_json::json!({
                "version": 7,
                "quota": {"max_events_per_session": 1}
            }),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    let ok = rt
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(ok.seq, 0);

    let err = rt
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "x",
            serde_json::json!({}),
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PolicyDenied);

    let events = rt
        .fetch_from_seq(agentdb_core::runtime::FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(events[0].policy_version, 7);
}

#[tokio::test]
async fn agent_policy_override_takes_precedence() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "tenant".into(),
            policy: serde_json::json!({
                "version": 1,
                "quota": {"max_events_per_session": 1}
            }),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({
                "policy": {
                    "version": 2,
                    "quota": {"max_events_per_session": 2}
                }
            }),
        })
        .await
        .unwrap();
    let policy = rt
        .resolve_policy_for_agent(&t.tenant_id, &a.agent_id)
        .await
        .unwrap();
    assert_eq!(policy.version, 2);
    assert_eq!(policy.quota.max_events_per_session, Some(2));
}

#[tokio::test]
async fn artifact_size_policy_is_tenant_scoped() {
    let rt = Runtime::in_memory();
    let small = rt
        .create_tenant(NewTenant {
            external_key: "small".into(),
            policy: serde_json::json!({
                "artifact_size": {"max_artifact_bytes": 4, "max_inline_event_bytes": 65536}
            }),
        })
        .await
        .unwrap();
    let large = rt
        .create_tenant(NewTenant {
            external_key: "large".into(),
            policy: serde_json::json!({
                "artifact_size": {"max_artifact_bytes": 16, "max_inline_event_bytes": 65536}
            }),
        })
        .await
        .unwrap();

    let err = rt
        .create_artifact(
            NewArtifact {
                tenant_id: small.tenant_id,
                content_type: None,
                expected_sha256: None,
            },
            b"12345".to_vec(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::ArtifactTooLarge);

    rt.create_artifact(
        NewArtifact {
            tenant_id: large.tenant_id,
            content_type: None,
            expected_sha256: None,
        },
        b"12345".to_vec(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn record_count_quota_is_enforced() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "records".into(),
            policy: serde_json::json!({"quota": {"max_records": 1}}),
        })
        .await
        .unwrap();

    let make_record = || NewRecord {
        tenant_id: t.tenant_id.clone(),
        namespace: vec!["n".into()],
        scope: serde_json::json!({}),
        text: Some("x".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    };

    rt.write_record(make_record()).await.unwrap();
    let err = rt.write_record(make_record()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::PolicyDenied);
}

#[test]
fn policy_defaults_and_priority_parse() {
    let p = RuntimePolicy::from_json(&serde_json::json!({"priority": "high"}));
    assert_eq!(p.priority, Priority::High);
    assert_eq!(p.artifact_size.max_inline_event_bytes, 64 * 1024);
}

#[test]
fn tier_template_resolves_and_allows_overrides() {
    let free = RuntimePolicy::from_json(&serde_json::json!({"tier": "free"}));
    assert_eq!(free.tier.as_deref(), Some("free"));
    assert_eq!(free.version, 10_001);
    assert_eq!(free.priority, Priority::Low);
    assert_eq!(free.quota.max_records, Some(100));

    let another_free = RuntimePolicy::from_json(&serde_json::json!({"tier": "free"}));
    assert_eq!(another_free.version, free.version);

    let pro_override = RuntimePolicy::from_json(&serde_json::json!({
        "tier": "pro",
        "version": 77,
        "quota": {"max_records": 2}
    }));
    assert_eq!(pro_override.tier.as_deref(), Some("pro"));
    assert_eq!(pro_override.version, 77);
    assert_eq!(pro_override.priority, Priority::Normal);
    assert_eq!(pro_override.quota.max_records, Some(2));
    assert_eq!(pro_override.quota.soft_max_records, Some(8_000));
    assert_eq!(pro_override.quota.max_active_sessions, Some(1_000));
}

#[tokio::test]
async fn active_session_quota_enforces_hard_limit_and_reports_soft_limit() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "sessions".into(),
            policy: serde_json::json!({
                "quota": {
                    "soft_max_active_sessions": 1,
                    "max_active_sessions": 1
                }
            }),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();

    rt.create_session(NewSession {
        tenant_id: t.tenant_id.clone(),
        agent_id: a.agent_id.clone(),
        alias: None,
    })
    .await
    .unwrap();
    let report = rt.backpressure_for_tenant(&t.tenant_id).await.unwrap();
    assert_eq!(
        report.header_value().as_deref(),
        Some("active_sessions=100")
    );

    let err = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id,
            alias: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PolicyDenied);
}

#[tokio::test]
async fn session_event_backpressure_reports_soft_limit() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "session-events".into(),
            policy: serde_json::json!({
                "quota": {"soft_max_events_per_session": 1, "max_events_per_session": 10}
            }),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();

    rt.append(AppendRequest::simple(
        &t,
        &a,
        &s,
        "x",
        serde_json::json!({}),
    ))
    .await
    .unwrap();
    let report = rt
        .backpressure_for_session(&t.tenant_id, &s.session_id)
        .await
        .unwrap();
    assert_eq!(report.header_value().as_deref(), Some("session_events=100"));
}

#[tokio::test]
async fn backpressure_reports_soft_record_quota() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "bp-records".into(),
            policy: serde_json::json!({
                "quota": {"soft_max_records": 1, "max_records": 10}
            }),
        })
        .await
        .unwrap();

    let req = NewRecord {
        tenant_id: t.tenant_id.clone(),
        namespace: vec!["n".into()],
        scope: serde_json::json!({}),
        text: Some("x".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    };
    rt.write_record(req).await.unwrap();

    let report = rt.backpressure_for_tenant(&t.tenant_id).await.unwrap();
    assert_eq!(report.header_value().as_deref(), Some("records=100"));
}

#[tokio::test]
async fn artifact_storage_quota_enforces_hard_limit_and_reports_soft_limit() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "bp-artifacts".into(),
            policy: serde_json::json!({
                "quota": {
                    "soft_max_artifact_storage_bytes": 4,
                    "max_artifact_storage_bytes": 6
                }
            }),
        })
        .await
        .unwrap();

    rt.create_artifact(
        NewArtifact {
            tenant_id: t.tenant_id.clone(),
            content_type: None,
            expected_sha256: None,
        },
        b"1234".to_vec(),
    )
    .await
    .unwrap();
    let report = rt.backpressure_for_tenant(&t.tenant_id).await.unwrap();
    assert_eq!(
        report.header_value().as_deref(),
        Some("artifact_storage=100")
    );

    let err = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            b"123".to_vec(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PolicyDenied);
}
