//! Runtime-level tests for the V2.2 SearchableRecord API.
//!
//! Covers write/get/search/delete on the in-memory backend: tenant
//! validation, indexing-policy semantics (text vs metadata-only vs
//! selected-fields), namespace isolation, vector denial, soft-delete +
//! expiration filtering, and tag/source post-filtering. Restart
//! safety lives in `runtime_restart.rs`.

use agentdb_core::errors::ErrorCode;
use agentdb_core::event::SourceRef;
use agentdb_core::id::{Id, IdKind};
use agentdb_core::record::{IndexingMode, IndexingPolicy, NewRecord};
use agentdb_core::runtime::{Runtime, SearchRequest};
use agentdb_core::tenant::NewTenant;

async fn fresh_tenant(rt: &Runtime, key: &str) -> agentdb_core::tenant::Tenant {
    rt.create_tenant(NewTenant {
        external_key: key.into(),
        policy: serde_json::json!({}),
    })
    .await
    .unwrap()
}

fn req(
    tenant_id: Id,
    namespace: Vec<&str>,
    text: Option<&str>,
    metadata: serde_json::Value,
    policy: IndexingPolicy,
) -> NewRecord {
    NewRecord {
        tenant_id,
        external_id: None,
        namespace: namespace.into_iter().map(String::from).collect(),
        scope: serde_json::json!({}),
        text: text.map(String::from),
        metadata,
        tags: vec![],
        source: None,
        embedding: None,
        policy,
        expires_at_ms: None,
    }
}

fn metadata_policy() -> IndexingPolicy {
    IndexingPolicy {
        mode: IndexingMode::MetadataOnly,
        fields: vec![],
        vector_allowed: false,
    }
}

fn selected_policy(fields: Vec<&str>) -> IndexingPolicy {
    IndexingPolicy {
        mode: IndexingMode::SelectedFields,
        fields: fields.into_iter().map(String::from).collect(),
        vector_allowed: false,
    }
}

fn summary_policy() -> IndexingPolicy {
    IndexingPolicy {
        mode: IndexingMode::SummaryOnly,
        fields: vec![],
        vector_allowed: false,
    }
}

fn vector_metadata_policy() -> IndexingPolicy {
    IndexingPolicy {
        mode: IndexingMode::MetadataOnly,
        fields: vec![],
        vector_allowed: true,
    }
}

#[tokio::test]
async fn write_and_get_record_round_trip() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let r = rt
        .write_record(req(
            t.tenant_id.clone(),
            vec!["agent", "memory"],
            Some("hello"),
            serde_json::json!({"title": "alpha"}),
            metadata_policy(),
        ))
        .await
        .unwrap();
    assert!(r.record_id.as_str().starts_with("rec_"));
    assert_eq!(r.tenant_id.as_str(), t.tenant_id.as_str());

    let got = rt.get_record(&t.tenant_id, &r.record_id).await.unwrap();
    assert_eq!(got.record_id.as_str(), r.record_id.as_str());
    assert_eq!(
        got.namespace,
        vec!["agent".to_string(), "memory".to_string()]
    );
}

#[tokio::test]
async fn search_repairs_same_client_visibility_when_local_index_lags() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let written = rt
        .write_record_with_commit(req(
            t.tenant_id.clone(),
            vec!["agent", "memory"],
            None,
            serde_json::json!({"title": "needle phrase"}),
            metadata_policy(),
        ))
        .await
        .unwrap();

    rt.simulate_record_index_lag_for_test(&written.record.record_id, 0)
        .await;

    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["agent".into(), "memory".into()],
            sharding_key: Some("agent/memory".into()),
            text: Some("needle".into()),
            embedding: None,
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 10,
            min_visible_record_commit_seq: Some(written.commit_token.seq),
        })
        .await
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].record_id.as_str(),
        written.record.record_id.as_str()
    );
}

#[tokio::test]
async fn write_record_rejects_unknown_tenant() {
    let rt = Runtime::in_memory();
    let bogus = Id::new(IdKind::Tenant);
    let err = rt
        .write_record(req(
            bogus,
            vec!["ns"],
            Some("x"),
            serde_json::json!({}),
            metadata_policy(),
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::TenantNotFound);
}

#[tokio::test]
async fn get_record_returns_not_found_for_missing_or_wrong_tenant() {
    let rt = Runtime::in_memory();
    let t1 = fresh_tenant(&rt, "t1").await;
    let t2 = fresh_tenant(&rt, "t2").await;
    let r = rt
        .write_record(req(
            t1.tenant_id.clone(),
            vec!["ns"],
            Some("x"),
            serde_json::json!({}),
            metadata_policy(),
        ))
        .await
        .unwrap();

    // Missing record_id under a real tenant.
    let bogus_rec = Id::new(IdKind::Record);
    let err = rt.get_record(&t1.tenant_id, &bogus_rec).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);

    // Real record_id but wrong tenant — collapsed into RecordNotFound.
    let err = rt
        .get_record(&t2.tenant_id, &r.record_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);
}

#[tokio::test]
async fn search_metadata_finds_match_on_metadata_field() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["kb"],
        Some("aside"),
        serde_json::json!({"title": "refund policy", "body": "shipping rules"}),
        metadata_policy(),
    ))
    .await
    .unwrap();
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["kb"],
        Some("other"),
        serde_json::json!({"title": "carrots are orange"}),
        metadata_policy(),
    ))
    .await
    .unwrap();

    let hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "refund",
            10,
        ))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].metadata["title"], "refund policy");
}

#[tokio::test]
async fn metadata_only_policy_does_not_index_text_field() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["kb"],
        Some("secret password handshake"),
        serde_json::json!({"title": "mundane"}),
        metadata_policy(),
    ))
    .await
    .unwrap();

    let hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "secret",
            10,
        ))
        .await
        .unwrap();
    assert!(
        hits.is_empty(),
        "metadata_only must NOT index the text field"
    );
}

#[tokio::test]
async fn selected_fields_policy_indexes_only_named_fields() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["kb"],
        Some("ignored text"),
        serde_json::json!({"title": "alpha quartet", "body": "beta rhythm"}),
        selected_policy(vec!["title"]),
    ))
    .await
    .unwrap();

    let hit_title = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "alpha",
            10,
        ))
        .await
        .unwrap();
    assert_eq!(hit_title.len(), 1);

    let hit_body = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "beta",
            10,
        ))
        .await
        .unwrap();
    assert!(hit_body.is_empty(), "body field is not in selected fields");

    let hit_text = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "ignored",
            10,
        ))
        .await
        .unwrap();
    assert!(
        hit_text.is_empty(),
        "text field is not indexed under SelectedFields"
    );
}

#[tokio::test]
async fn summary_only_policy_indexes_text_field() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["kb"],
        Some("alpha bravo charlie"),
        serde_json::json!({"title": "ignored"}),
        summary_policy(),
    ))
    .await
    .unwrap();

    let hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "bravo",
            10,
        ))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);

    let no_hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "ignored",
            10,
        ))
        .await
        .unwrap();
    assert!(
        no_hits.is_empty(),
        "metadata is not indexed under SummaryOnly"
    );
}

#[tokio::test]
async fn no_indexing_policy_returns_no_text_hits() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["kb"],
        Some("very searchable text"),
        serde_json::json!({"title": "very searchable metadata"}),
        IndexingPolicy {
            mode: IndexingMode::NoIndexing,
            fields: vec![],
            vector_allowed: false,
        },
    ))
    .await
    .unwrap();

    let hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["kb".into()],
            "searchable",
            10,
        ))
        .await
        .unwrap();
    assert!(hits.is_empty());
}

#[tokio::test]
async fn search_filters_by_namespace() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["nsa"],
        None,
        serde_json::json!({"title": "shared term"}),
        metadata_policy(),
    ))
    .await
    .unwrap();
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["nsb"],
        None,
        serde_json::json!({"title": "shared term"}),
        metadata_policy(),
    ))
    .await
    .unwrap();

    let hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["nsa".into()],
            "shared",
            10,
        ))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].namespace, vec!["nsa".to_string()]);
}

#[tokio::test]
async fn search_does_not_leak_across_tenants() {
    let rt = Runtime::in_memory();
    let t1 = fresh_tenant(&rt, "t1").await;
    let t2 = fresh_tenant(&rt, "t2").await;
    rt.write_record(req(
        t1.tenant_id.clone(),
        vec!["ns"],
        None,
        serde_json::json!({"title": "shared term"}),
        metadata_policy(),
    ))
    .await
    .unwrap();

    let hits = rt
        .search_records(SearchRequest::text_only(
            t2.tenant_id.clone(),
            vec!["ns".into()],
            "shared",
            10,
        ))
        .await
        .unwrap();
    assert!(hits.is_empty(), "t2 must not see t1's records");
}

#[tokio::test]
async fn search_without_query_returns_namespace_records() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    for i in 0..3 {
        rt.write_record(req(
            t.tenant_id.clone(),
            vec!["ns"],
            None,
            serde_json::json!({"i": i}),
            metadata_policy(),
        ))
        .await
        .unwrap();
    }
    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["other"],
        None,
        serde_json::json!({}),
        metadata_policy(),
    ))
    .await
    .unwrap();

    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["ns".into()],
            sharding_key: Some("ns".into()),
            text: None,
            embedding: None,
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 100,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);
    assert!(hits.iter().all(|r| r.namespace == vec!["ns".to_string()]));
}

#[tokio::test]
async fn vector_search_finds_nearest_when_policy_allows() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;

    // Three records with distinct unit-ish vectors. Query closest to the
    // second.
    let vectors = [[1.0_f32, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]];
    let mut ids = Vec::new();
    for v in &vectors {
        let mut r = req(
            t.tenant_id.clone(),
            vec!["ns"],
            None,
            serde_json::json!({}),
            vector_metadata_policy(),
        );
        r.embedding = Some(v.to_vec());
        let rec = rt.write_record(r).await.unwrap();
        ids.push(rec.record_id);
    }

    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["ns".into()],
            sharding_key: Some("ns".into()),
            text: None,
            embedding: Some(vec![0.0, 1.0, 0.0]),
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 3,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].record_id.as_str(), ids[1].as_str());
}

#[tokio::test]
async fn vector_denied_policy_skips_vector_index() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let mut r = req(
        t.tenant_id.clone(),
        vec!["ns"],
        None,
        serde_json::json!({}),
        // policy explicitly does NOT allow vectors
        metadata_policy(),
    );
    r.embedding = Some(vec![1.0, 0.0, 0.0]);
    rt.write_record(r).await.unwrap();

    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["ns".into()],
            sharding_key: Some("ns".into()),
            text: None,
            embedding: Some(vec![1.0, 0.0, 0.0]),
            tags: vec![],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 5,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert!(
        hits.is_empty(),
        "vector_allowed=false must keep the record out of the vector index"
    );
}

#[tokio::test]
async fn search_filters_by_tag() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let mut r1 = req(
        t.tenant_id.clone(),
        vec!["ns"],
        None,
        serde_json::json!({"title": "shared term"}),
        metadata_policy(),
    );
    r1.tags = vec!["red".into()];
    rt.write_record(r1).await.unwrap();

    let mut r2 = req(
        t.tenant_id.clone(),
        vec!["ns"],
        None,
        serde_json::json!({"title": "shared term"}),
        metadata_policy(),
    );
    r2.tags = vec!["blue".into()];
    rt.write_record(r2).await.unwrap();

    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["ns".into()],
            sharding_key: Some("ns".into()),
            text: Some("shared".into()),
            embedding: None,
            tags: vec!["red".into()],
            source_filter: None,
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits[0].tags.contains(&"red".to_string()));
}

#[tokio::test]
async fn search_filters_by_source_event_id() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let evt_id = Id::new(IdKind::Event);

    let mut with_source = req(
        t.tenant_id.clone(),
        vec!["ns"],
        None,
        serde_json::json!({"title": "shared term"}),
        metadata_policy(),
    );
    with_source.source = Some(SourceRef {
        session_id: None,
        seq: None,
        event_id: Some(evt_id.clone()),
        artifact_id: None,
        record_id: None,
    });
    rt.write_record(with_source).await.unwrap();

    rt.write_record(req(
        t.tenant_id.clone(),
        vec!["ns"],
        None,
        serde_json::json!({"title": "shared term"}),
        metadata_policy(),
    ))
    .await
    .unwrap();

    let filter = SourceRef {
        session_id: None,
        seq: None,
        event_id: Some(evt_id.clone()),
        artifact_id: None,
        record_id: None,
    };
    let hits = rt
        .search_records(SearchRequest {
            tenant_id: t.tenant_id.clone(),
            namespace: vec!["ns".into()],
            sharding_key: Some("ns".into()),
            text: Some("shared".into()),
            embedding: None,
            tags: vec![],
            source_filter: Some(filter),
            metadata_filter: None,
            include_superseded: false,
            min_score: None,
            created_after_ms: None,
            created_before_ms: None,
            touch: false,
            limit: 10,
            min_visible_record_commit_seq: None,
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0]
            .source
            .as_ref()
            .unwrap()
            .event_id
            .as_ref()
            .unwrap()
            .as_str(),
        evt_id.as_str()
    );
}

#[tokio::test]
async fn delete_record_removes_from_search_and_get() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let r = rt
        .write_record(req(
            t.tenant_id.clone(),
            vec!["ns"],
            None,
            serde_json::json!({"title": "deletable"}),
            metadata_policy(),
        ))
        .await
        .unwrap();

    rt.delete_record(&t.tenant_id, &r.record_id).await.unwrap();

    let err = rt.get_record(&t.tenant_id, &r.record_id).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);

    let hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["ns".into()],
            "deletable",
            10,
        ))
        .await
        .unwrap();
    assert!(hits.is_empty());
}

#[tokio::test]
async fn delete_record_returns_not_found_for_missing_or_wrong_tenant() {
    let rt = Runtime::in_memory();
    let t1 = fresh_tenant(&rt, "t1").await;
    let t2 = fresh_tenant(&rt, "t2").await;

    let bogus = Id::new(IdKind::Record);
    let err = rt.delete_record(&t1.tenant_id, &bogus).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);

    let r = rt
        .write_record(req(
            t1.tenant_id.clone(),
            vec!["ns"],
            None,
            serde_json::json!({}),
            metadata_policy(),
        ))
        .await
        .unwrap();
    let err = rt
        .delete_record(&t2.tenant_id, &r.record_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);
}

#[tokio::test]
async fn expired_record_is_excluded_from_get_and_search() {
    let rt = Runtime::in_memory();
    let t = fresh_tenant(&rt, "t").await;
    let mut r = req(
        t.tenant_id.clone(),
        vec!["ns"],
        None,
        serde_json::json!({"title": "expirable"}),
        metadata_policy(),
    );
    r.expires_at_ms = Some(0); // already expired
    let rec = rt.write_record(r).await.unwrap();

    let err = rt
        .get_record(&t.tenant_id, &rec.record_id)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::RecordNotFound);

    let hits = rt
        .search_records(SearchRequest::text_only(
            t.tenant_id.clone(),
            vec!["ns".into()],
            "expirable",
            10,
        ))
        .await
        .unwrap();
    assert!(hits.is_empty());
}
