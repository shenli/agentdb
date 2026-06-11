// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S6 record_visibility_and_policy_gc_match_committed_model_under_lag
// Falsifies: C8, C9, C13, C18
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

use agentdb_core::{
    errors::ErrorCode,
    record::{IndexingPolicy, NewRecord},
    runtime::{Runtime, SearchRequest},
    NewTenant,
};

fn record(tenant_id: agentdb_core::Id, text: &str, tag: &str) -> NewRecord {
    NewRecord {
        tenant_id,
        namespace: vec!["policy".into(), "records".into()],
        scope: serde_json::json!({}),
        text: Some(text.into()),
        metadata: serde_json::json!({"title": text}),
        tags: vec![tag.into()],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    }
}

#[tokio::test]
async fn record_visibility_and_policy_gc_match_committed_model_under_lag() {
    let rt = Runtime::in_memory();
    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "record-policy-auto".into(),
            policy: serde_json::json!({"quota": {"max_records": 2}}),
        })
        .await
        .unwrap();

    let first = rt
        .write_record_with_commit(record(
            tenant.tenant_id.clone(),
            "fresh lagged needle",
            "visible",
        ))
        .await
        .unwrap();
    rt.simulate_record_index_lag_for_test(&first.record.record_id, 0)
        .await;
    let repaired = rt
        .search_records(SearchRequest {
            tenant_id: tenant.tenant_id.clone(),
            namespace: vec!["policy".into(), "records".into()],
            sharding_key: Some("policy/records".into()),
            text: Some("needle".into()),
            embedding: None,
            tags: vec!["visible".into()],
            source_filter: None,
            metadata_filter: None,
            limit: 10,
            min_visible_record_commit_seq: Some(first.commit_token.seq),
        })
        .await
        .unwrap();
    assert_eq!(repaired.len(), 1);
    assert_eq!(repaired[0].record_id, first.record.record_id);

    let deleted = rt
        .write_record(record(
            tenant.tenant_id.clone(),
            "delete me needle",
            "deleted",
        ))
        .await
        .unwrap();
    rt.delete_record(&tenant.tenant_id, &deleted.record_id)
        .await
        .unwrap();
    let after_delete = rt
        .search_records(SearchRequest::text_only(
            tenant.tenant_id.clone(),
            vec!["policy".into(), "records".into()],
            "delete",
            10,
        ))
        .await
        .unwrap();
    assert!(
        after_delete
            .iter()
            .all(|hit| hit.record_id != deleted.record_id),
        "deleted record must not remain searchable"
    );

    let quota_tenant = rt
        .create_tenant(NewTenant {
            external_key: "record-policy-quota-auto".into(),
            policy: serde_json::json!({"quota": {"max_records": 1}}),
        })
        .await
        .unwrap();
    rt.write_record(record(
        quota_tenant.tenant_id.clone(),
        "first quota record",
        "quota",
    ))
    .await
    .unwrap();
    let over_quota = rt
        .write_record(record(
            quota_tenant.tenant_id.clone(),
            "second quota denied",
            "denied",
        ))
        .await
        .unwrap_err();
    assert_eq!(over_quota.code, ErrorCode::PolicyDenied);

    let stats = rt.record_index_stats().await;
    assert!(
        stats.local_index_watermark >= first.commit_token.seq,
        "local index watermark should report progress through committed records"
    );
}
