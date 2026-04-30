use agentdb_core::artifact::NewArtifact;
use agentdb_core::errors::ErrorCode;
use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::skill::{BindSkillRequest, NewSkill};

#[tokio::test]
async fn register_skill_assigns_typed_id_and_enforces_unique_version() {
    let rt = Runtime::in_memory();
    let (t, _, _) = rt.bootstrap_test_session().await;

    let skill = rt
        .register_skill(
            NewSkill {
                tenant_id: t.tenant_id.clone(),
                name: "summarize".into(),
                version: "1.0.0".into(),
                manifest: serde_json::json!({"entrypoint": "run"}),
                artifact_refs: vec![],
            },
            None,
        )
        .await
        .unwrap();
    assert!(skill.skill_id.as_str().starts_with("skl_"));
    assert_eq!(skill.schema_version, agentdb_core::VERSION);

    let err = rt
        .register_skill(
            NewSkill {
                tenant_id: t.tenant_id.clone(),
                name: "summarize".into(),
                version: "1.0.0".into(),
                manifest: serde_json::json!({}),
                artifact_refs: vec![],
            },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);
}

#[tokio::test]
async fn bind_skill_lists_exact_version_for_agent() {
    let rt = Runtime::in_memory();
    let (t, a, _) = rt.bootstrap_test_session().await;
    let skill = rt
        .register_skill(
            NewSkill {
                tenant_id: t.tenant_id.clone(),
                name: "summarize".into(),
                version: "1.0.0".into(),
                manifest: serde_json::json!({}),
                artifact_refs: vec![],
            },
            None,
        )
        .await
        .unwrap();

    let binding = rt
        .bind_skill(BindSkillRequest {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            skill_id: skill.skill_id.clone(),
            idempotency_key: None,
        })
        .await
        .unwrap();
    assert_eq!(binding.name, "summarize");
    assert_eq!(binding.version, "1.0.0");

    let listed = rt
        .list_agent_skills(&t.tenant_id, &a.agent_id)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].skill_id, skill.skill_id);
}

#[tokio::test]
async fn register_skill_validates_artifact_refs() {
    let rt = Runtime::in_memory();
    let (t, _, _) = rt.bootstrap_test_session().await;
    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: Some("application/gzip".into()),
                expected_sha256: None,
            },
            b"package".to_vec(),
        )
        .await
        .unwrap();

    rt.register_skill(
        NewSkill {
            tenant_id: t.tenant_id.clone(),
            name: "packaged".into(),
            version: "1.0.0".into(),
            manifest: serde_json::json!({}),
            artifact_refs: vec![agentdb_core::ArtifactRef {
                artifact_id: art.artifact_id,
                content_type: None,
                bytes: None,
                sha256: None,
            }],
        },
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn skill_used_event_requires_exact_registered_version() {
    let rt = Runtime::in_memory();
    let (t, a, s) = rt.bootstrap_test_session().await;
    let skill = rt
        .register_skill(
            NewSkill {
                tenant_id: t.tenant_id.clone(),
                name: "tool".into(),
                version: "1.0.0".into(),
                manifest: serde_json::json!({}),
                artifact_refs: vec![],
            },
            None,
        )
        .await
        .unwrap();
    rt.register_skill(
        NewSkill {
            tenant_id: t.tenant_id.clone(),
            name: "tool".into(),
            version: "2.0.0".into(),
            manifest: serde_json::json!({}),
            artifact_refs: vec![],
        },
        None,
    )
    .await
    .unwrap();

    let resp = rt
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "skill_used",
            serde_json::json!({
                "skill_id": skill.skill_id.as_str(),
                "version": "1.0.0"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.seq, 0);

    let err = rt
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "skill_used",
            serde_json::json!({
                "skill_id": skill.skill_id.as_str(),
                "version": "9.9.9"
            }),
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SkillNotFound);
}
