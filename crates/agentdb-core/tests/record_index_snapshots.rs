//! Persisted record-index snapshots: startup restores the text/vector/global
//! indexes, doc-id maps, and external-id map from a snapshot blob and
//! delta-replays only the records whose commit sequence moved past the
//! snapshot watermark, instead of scanning and re-tokenizing every record.
//! These tests pin the restore + delta semantics, including the awkward
//! cases: post-snapshot updates, deletes, GC purges, and corrupted blobs.

use std::sync::Arc;

use agentdb_core::record::{IndexingMode, IndexingPolicy, NewRecord};
use agentdb_core::runtime::{HydrationMode, RecordUpdate, Runtime, SearchRequest};
use agentdb_core::tenant::NewTenant;
use agentdb_core::Id;
use agentdb_storage::{ContentBackend, InMemoryContentBackend, InMemoryKvStore, KvStore};

fn new_record(tenant_id: &Id, i: usize, text: &str) -> NewRecord {
    NewRecord {
        tenant_id: tenant_id.clone(),
        external_id: Some(format!("ext-{i}")),
        namespace: vec!["notes".into()],
        scope: serde_json::json!({}),
        text: Some(text.to_string()),
        metadata: serde_json::json!({ "i": i }),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy {
            mode: IndexingMode::SummaryOnly,
            fields: vec![],
            vector_allowed: false,
        },
        expires_at_ms: None,
    }
}

fn text_search(tenant_id: &Id, query: &str) -> SearchRequest {
    SearchRequest {
        tenant_id: tenant_id.clone(),
        namespace: vec!["notes".into()],
        sharding_key: None,
        text: Some(query.to_string()),
        embedding: None,
        tags: vec![],
        source_filter: None,
        metadata_filter: None,
        include_superseded: false,
        min_score: None,
        created_after_ms: None,
        created_before_ms: None,
        touch: false,
        limit: 50,
        min_visible_record_commit_seq: None,
    }
}

/// Restart from a snapshot with NO deltas: search parity with the full
/// rebuild, snapshot reported as loaded, and record rows not resident.
#[tokio::test]
async fn restart_from_snapshot_matches_full_rebuild() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());

    let tenant_id = {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "snap".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        for i in 0..5 {
            rt.upsert_record_with_commit(new_record(
                &t.tenant_id,
                i,
                &format!("alpha document number {i}"),
            ))
            .await
            .unwrap();
        }
        rt.apply_record_index_outbox_once().await.unwrap();
        assert!(rt.write_record_index_snapshot(true).await.unwrap());
        let stats = rt.record_index_stats().await;
        assert_eq!(stats.snapshot_writes_total, 1);
        assert!(!stats.snapshot_loaded_at_startup);
        t.tenant_id
    };

    for mode in [HydrationMode::Full, HydrationMode::Lazy] {
        let rt = Runtime::with_kv_and_hydration_mode(kv.clone(), content.clone(), mode);
        let stats = rt.record_index_stats().await;
        assert!(
            stats.snapshot_loaded_at_startup,
            "{mode:?} startup should restore from the snapshot"
        );
        let matches = rt
            .search_records(text_search(&tenant_id, "alpha document"))
            .await
            .unwrap();
        assert_eq!(matches.len(), 5, "{mode:?}: snapshot-restored text search");
        assert_eq!(rt.list_records(&tenant_id).await.unwrap().len(), 5);
    }
}

/// Records written, updated, and deleted AFTER the snapshot must be
/// reconciled by the delta replay on the next startup.
#[tokio::test]
async fn delta_replay_covers_post_snapshot_writes_updates_and_deletes() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());

    let (tenant_id, updated_id, deleted_id) = {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "snap-delta".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let mut ids = Vec::new();
        for i in 0..3 {
            let written = rt
                .upsert_record_with_commit(new_record(
                    &t.tenant_id,
                    i,
                    &format!("stable content {i}"),
                ))
                .await
                .unwrap();
            ids.push(written.record.record_id.clone());
        }
        rt.apply_record_index_outbox_once().await.unwrap();
        assert!(rt.write_record_index_snapshot(true).await.unwrap());

        // Post-snapshot: one new record, one update, one delete.
        rt.upsert_record_with_commit(new_record(&t.tenant_id, 90, "freshly added afterwards"))
            .await
            .unwrap();
        rt.update_record_with_commit(RecordUpdate {
            tenant_id: t.tenant_id.clone(),
            record_id: ids[0].clone(),
            expected_version: None,
            namespace: None,
            scope: None,
            text: Some(Some("rewritten content zero".into())),
            metadata: None,
            tags: None,
            source: None,
            embedding: None,
            policy: None,
            status: None,
            expires_at_ms: None,
        })
        .await
        .unwrap();
        rt.delete_record(&t.tenant_id, &ids[1]).await.unwrap();
        (t.tenant_id, ids[0].clone(), ids[1].clone())
    };

    let rt = Runtime::with_kv_and_hydration_mode(kv, content, HydrationMode::Lazy);
    assert!(rt.record_index_stats().await.snapshot_loaded_at_startup);

    // The new record and the rewritten text are searchable.
    let fresh = rt
        .search_records(text_search(&tenant_id, "freshly added"))
        .await
        .unwrap();
    assert_eq!(fresh.len(), 1);
    let rewritten = rt
        .search_records(text_search(&tenant_id, "rewritten content"))
        .await
        .unwrap();
    assert_eq!(rewritten.len(), 1);
    assert_eq!(rewritten[0].record_id, updated_id);

    // The deleted record is gone from search results and reads.
    let deleted_hits = rt
        .search_records(text_search(&tenant_id, "stable content"))
        .await
        .unwrap();
    assert!(
        deleted_hits.iter().all(|r| r.record_id != deleted_id),
        "deleted record must not surface from the snapshot-era index"
    );
    assert!(rt.get_record(&tenant_id, &deleted_id).await.is_err());
    assert_eq!(rt.list_records(&tenant_id).await.unwrap().len(), 3);

    // External-id dedupe still works after restore: re-upserting ext-0 must
    // update the rewritten record, not duplicate it.
    rt.upsert_record_with_commit(new_record(&tenant_id, 0, "external id takes this over"))
        .await
        .unwrap();
    assert_eq!(rt.list_records(&tenant_id).await.unwrap().len(), 3);

    // And the external id freed by the delete is reusable.
    rt.upsert_record_with_commit(new_record(&tenant_id, 1, "second life for ext one"))
        .await
        .unwrap();
    assert_eq!(rt.list_records(&tenant_id).await.unwrap().len(), 4);
}

/// A corrupted snapshot blob must fall back to the full-scan rebuild, not
/// fail startup or serve an empty index.
#[tokio::test]
async fn corrupted_snapshot_falls_back_to_full_rebuild() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());

    let tenant_id = {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "snap-corrupt".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        for i in 0..4 {
            rt.upsert_record_with_commit(new_record(
                &t.tenant_id,
                i,
                &format!("resilient text {i}"),
            ))
            .await
            .unwrap();
        }
        assert!(rt.write_record_index_snapshot(true).await.unwrap());
        t.tenant_id
    };

    // Overwrite the snapshot key with garbage, in the same namespace the
    // runtime reads it from.
    {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        rt.corrupt_record_index_snapshot_for_test().unwrap();
    }

    let rt = Runtime::with_kv_and_hydration_mode(kv, content, HydrationMode::Lazy);
    let stats = rt.record_index_stats().await;
    assert!(
        !stats.snapshot_loaded_at_startup,
        "corrupted snapshot must be ignored"
    );
    let matches = rt
        .search_records(text_search(&tenant_id, "resilient text"))
        .await
        .unwrap();
    assert_eq!(matches.len(), 4, "full rebuild still serves every record");
}

/// The min-advance gate: unchanged watermark means no rewrite; enough new
/// commits re-arm the writer.
#[tokio::test]
async fn snapshot_writer_skips_until_watermark_advances() {
    std::env::set_var("AGENTDB_RECORD_INDEX_SNAPSHOT_MIN_ADVANCE", "2");
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv, content);
    let t = rt
        .create_tenant(NewTenant {
            external_key: "snap-gate".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    assert!(
        !rt.write_record_index_snapshot(false).await.unwrap(),
        "nothing to snapshot yet"
    );
    rt.upsert_record_with_commit(new_record(&t.tenant_id, 0, "first"))
        .await
        .unwrap();
    assert!(
        !rt.write_record_index_snapshot(false).await.unwrap(),
        "one commit is under the min advance"
    );
    rt.upsert_record_with_commit(new_record(&t.tenant_id, 1, "second"))
        .await
        .unwrap();
    assert!(rt.write_record_index_snapshot(false).await.unwrap());
    assert!(
        !rt.write_record_index_snapshot(false).await.unwrap(),
        "no rewrite while the watermark is unchanged"
    );
    assert_eq!(rt.record_index_stats().await.snapshot_writes_total, 1);
    std::env::remove_var("AGENTDB_RECORD_INDEX_SNAPSHOT_MIN_ADVANCE");
}

/// A record soft-deleted AND hard-purged by GC after the snapshot: the delta
/// replay sees a bumped commit seq with no row at all and must scrub every
/// snapshot-era trace, including the external-id mapping.
#[tokio::test]
async fn delta_replay_scrubs_records_purged_after_the_snapshot() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());

    let (tenant_id, purged_id) = {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "snap-purge".into(),
                policy: serde_json::json!({"retention": {"retention_ms": 0}}),
            })
            .await
            .unwrap();
        let kept = rt
            .upsert_record_with_commit(new_record(&t.tenant_id, 0, "keeper text"))
            .await
            .unwrap();
        let doomed = rt
            .upsert_record_with_commit(new_record(&t.tenant_id, 1, "doomed text"))
            .await
            .unwrap();
        rt.apply_record_index_outbox_once().await.unwrap();
        assert!(rt.write_record_index_snapshot(true).await.unwrap());

        rt.delete_record(&t.tenant_id, &doomed.record.record_id)
            .await
            .unwrap();
        let report = rt.gc_sweep_at(u64::MAX).await.unwrap();
        assert_eq!(report.records_purged, 1);
        drop(kept);
        (t.tenant_id, doomed.record.record_id)
    };

    let rt = Runtime::with_kv_and_hydration_mode(kv, content, HydrationMode::Lazy);
    assert!(rt.record_index_stats().await.snapshot_loaded_at_startup);

    let hits = rt
        .search_records(text_search(&tenant_id, "doomed text"))
        .await
        .unwrap();
    assert!(hits.is_empty(), "purged record must not resurface");
    assert!(rt.get_record(&tenant_id, &purged_id).await.is_err());
    assert_eq!(rt.list_records(&tenant_id).await.unwrap().len(), 1);

    // The purged record's external id is fully released: a new record can
    // claim ext-1 without tripping the snapshot-era mapping.
    rt.upsert_record_with_commit(new_record(&tenant_id, 1, "reclaimed external id"))
        .await
        .unwrap();
    assert_eq!(rt.list_records(&tenant_id).await.unwrap().len(), 2);
}

/// Regression (found by S18): `search_record_matches` is the public funnel
/// the API calls, and it must pull the tenant row through the KV fallback —
/// a cold lazy restart used to 404 the first search.
#[tokio::test]
async fn search_record_matches_works_on_a_cold_tenant_cache() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());
    let tenant_id = {
        let rt = Runtime::with_kv(kv.clone(), content.clone());
        let t = rt
            .create_tenant(NewTenant {
                external_key: "cold-search".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        rt.upsert_record_with_commit(new_record(&t.tenant_id, 0, "cold cache search"))
            .await
            .unwrap();
        t.tenant_id
    };
    let rt = Runtime::with_kv_and_hydration_mode(kv, content, HydrationMode::Lazy);
    let matches = rt
        .search_record_matches(text_search(&tenant_id, "cold cache"))
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
}

/// The outbox is a catch-up queue, not a rebuild source: once a mutation is
/// reflected in both the local and global indexes, its row is trimmed so the
/// per-drain scan stays proportional to new mutations.
#[tokio::test]
async fn outbox_rows_are_trimmed_once_both_indexes_reflect_them() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv, content);
    let t = rt
        .create_tenant(NewTenant {
            external_key: "outbox-trim".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    for i in 0..4 {
        rt.upsert_record_with_commit(new_record(&t.tenant_id, i, &format!("trim me {i}")))
            .await
            .unwrap();
    }
    let (upserts, _) = rt.record_index_outbox_counts_for_test().unwrap();
    assert_eq!(upserts, 4, "rows queued before any drain");

    // First drain applies everything (advancing the global watermark);
    // second drain finds all rows at or below both watermarks and trims.
    rt.apply_record_index_outbox_once().await.unwrap();
    rt.apply_record_index_outbox_once().await.unwrap();
    let (upserts, deletes) = rt.record_index_outbox_counts_for_test().unwrap();
    assert_eq!((upserts, deletes), (0, 0), "drained rows are deleted");

    // And search still works, of course.
    let matches = rt
        .search_records(text_search(&t.tenant_id, "trim me"))
        .await
        .unwrap();
    assert_eq!(matches.len(), 4);
}
