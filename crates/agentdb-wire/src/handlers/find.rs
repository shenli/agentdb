use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use agentdb_core::{
    scan_session_all_tiers, BoxRegistry, Box_, Event, ReadCtx, SessionRegistry, TenantRegistry,
};
use bson::{doc, Bson, Document};
use serde_json::Value;

use crate::bsonx::json_to_bson;
use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

/// Cap on the locator-fanout list we'll pull from meta in a
/// single sweep. The query is paginated, so this isn't a hard
/// limit on tenant size — it's just the per-call batch.
/// Sized so a tenant with thousands of sessions still fits in
/// one round-trip.
const FANOUT_LOCATOR_LIMIT: i64 = 10_000;

/// `raw: true` returns un-folded events (corrections +
/// tombstones visible). Default is `false` so clients that
/// wrote via `update` / `delete` see the post-mutation view.
#[allow(clippy::too_many_arguments)]
pub async fn find_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    filter_doc: &Document,
    limit: Option<i64>,
    raw: bool,
) -> WireResult<Document> {
    let started = std::time::Instant::now();
    // M20 multi-node: if the filter pins a specific session
    // and that session's shard is owned by another node, the
    // read is routed wrong and we'd silently return empty
    // from the tenant-hash box. Fail loudly with a
    // NotWritablePrimary-shaped error that tells the client
    // where to reconnect. Mongo-wire cross-node forwarding is
    // deferred (binary stateful protocol; see plan doc).
    // If the filter pins a session, respect the shard
    // override / tablet ownership: remote → cross-node signal
    // (or forwarding at the connection layer); local but on a
    // different box than tenant-hash → use the session's box
    // so a find after an override doesn't silently miss.
    let pinned_session: Option<String> = Filter::parse(filter_doc)
        .ok()
        .and_then(|f| f.top_level_eq("session_id").and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        }));

    // Cross-node early return for pinned sessions whose shard
    // is owned by a peer. Has to fire before any tenant
    // lookup: a peer-owned session whose tenant has never
    // been materialised on this node still needs to surface
    // NotWritablePrimary so the client reconnects to the
    // right peer (rather than seeing an empty batch).
    if let Some(s) = pinned_session.as_deref() {
        if let agentdb_core::RouteDecision::Remote { target, tablet_id } =
            registry.route_session_for_io(db, s)
        {
            return Ok(cross_node_error(&target.node_id, &target.address, tablet_id));
        }
    }

    // Rate-limit + metrics bookkeeping anchors on the
    // tenant-hash box even when the actual read fans out
    // across multiple boxes. This keeps QPS gating semantics
    // unchanged across the M26 transition: one client request
    // counts as one regardless of how many tablets it
    // ultimately touches.
    let admit_box = registry.route_tenant(db);
    admit_box.metrics.record_query_find();
    let admit_tenant = tenants.lookup(admit_box.id, db)?;
    let admit_id = admit_tenant.as_ref().map(|t| t.internal_id).unwrap_or(0);
    admit_box
        .admit(admit_id)
        .map_err(|d| agentdb_core::Error::TooManyRequests {
            scope: d.scope,
            retry_after_ms: 1000,
        })?;
    // Don't early-return on `admit_tenant.is_none()`: even
    // when the tenant has no row on the tenant-hash box, the
    // pinned-session path (or the locator fanout below) may
    // still resolve to a different local box that DOES have
    // the tenant. The targets vec ends up empty in the truly-
    // unknown case, which produces an empty batch naturally
    // at the end of the function.

    // Build the list of (host_box, session_external) targets.
    // Three sources:
    //
    //   1. **Pinned session**: filter has session_id == "X".
    //      Route to that one session via the existing shard /
    //      tablet lookup. Cross-node returns the same
    //      NotWritablePrimary-shaped error as before — full
    //      cross-node Mongo wire forwarding for reads is the
    //      explicit Phase 3 follow-up (binary-protocol).
    //
    //   2. **Locator fanout** (M26 Phase 3): meta-wired
    //      registries enumerate sessions via
    //      session_locators::list_by_tenant_external. Each
    //      session's hosting box is the one whose id matches
    //      the locator's tablet_id; cross-node sessions are
    //      skipped with a tracing warn.
    //
    //   3. **Tenant-hash-box-local back-compat**: sessions
    //      created pre-M26 (or pre-locator-population) only
    //      exist in the tenant-hash box's local KV. Union
    //      these in so the read coverage is the strict
    //      superset of the pre-M26 behavior.
    let filter = Filter::parse(filter_doc)?;
    let store = registry.object_store();
    let mut batch: Vec<Value> = Vec::new();
    let cap = limit.unwrap_or(i64::MAX).max(0) as usize;
    let mut targets: Vec<(Arc<Box_>, String)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    if let Some(s) = pinned_session.as_deref() {
        // Local-only at this point — the cross-node case
        // returned above. `route_session_for_io` resolves the
        // shard override (or tenant-hash fallback) to a
        // local box.
        let box_ = match registry.route_session_for_io(db, s) {
            agentdb_core::RouteDecision::Remote { .. } => unreachable!(
                "remote pin already handled above the admit block"
            ),
            agentdb_core::RouteDecision::Local(b) => b,
        };
        seen.insert(s.to_string());
        targets.push((box_, s.to_string()));
    } else {
        if let Some(meta) = registry.meta_store() {
            match agentdb_meta::session_locators::list_by_tenant_external(
                meta.pool(),
                db,
                None,
                FANOUT_LOCATOR_LIMIT,
            )
            .await
            {
                Ok(locators) => {
                    for loc in locators {
                        let Some(tab) = loc.tablet_id else { continue };
                        let Some(host) =
                            u16::try_from(tab).ok().and_then(|t| registry.box_by_id(t))
                        else {
                            tracing::warn!(
                                session_id = %loc.session_id,
                                tablet_id = tab,
                                "find: locator points at non-local tablet; cross-node Mongo \
                                 wire fanout reads are a Phase 3 follow-up — session skipped"
                            );
                            continue;
                        };
                        if seen.insert(loc.session_id.clone()) {
                            targets.push((host, loc.session_id));
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        %err,
                        "find: locator fanout query failed; falling back to tenant-hash box only"
                    );
                }
            }
        }
        // Back-compat: include tenant-hash-box-local sessions
        // that the locator hasn't captured yet (pre-M26 or
        // pre-populate-on-mint sessions). Union semantics —
        // dedup via `seen`.
        if let Some(t) = &admit_tenant {
            for s_ext in sessions.list_in_tenant(admit_box.id, t.internal_id)? {
                if seen.insert(s_ext.clone()) {
                    targets.push((admit_box.clone(), s_ext));
                }
            }
        }
    }

    // Per-box memoised ReadCtx + tenant_internal_id. Avoids
    // re-opening the same box's snapshot when several sessions
    // happen to share a host (the common case post-M26 once
    // placement settles).
    let mut box_caches: HashMap<u16, (ReadCtx, Option<u64>)> = HashMap::new();
    let kv = registry.kv();

    match collection {
        "events" => {
            for (host, s_ext) in &targets {
                let entry = match box_caches.get(&host.id) {
                    Some(e) => e,
                    None => {
                        let ctx = ReadCtx::open(host, kv)?;
                        let tenant_internal = tenants.lookup(host.id, db)?.map(|t| t.internal_id);
                        box_caches.insert(host.id, (ctx, tenant_internal));
                        box_caches
                            .get(&host.id)
                            .expect("just inserted")
                    }
                };
                let (ctx, tenant_internal) = entry;
                let Some(tenant_internal_id) = *tenant_internal else { continue };
                let Some(session) = ctx.session(sessions, tenant_internal_id, s_ext)? else {
                    continue;
                };
                let mut events = scan_session_all_tiers(ctx, store, &session).await?;
                if !raw {
                    events = agentdb_core::fold_events(events);
                }
                for ev in events {
                    let row = project_event(&session.external_id, ev);
                    if filter.matches(&row) {
                        batch.push(row);
                        if batch.len() >= cap {
                            break;
                        }
                    }
                }
                if batch.len() >= cap {
                    break;
                }
            }
        }
        "sessions" => {
            for (host, s_ext) in &targets {
                let entry = match box_caches.get(&host.id) {
                    Some(e) => e,
                    None => {
                        let ctx = ReadCtx::open(host, kv)?;
                        let tenant_internal = tenants.lookup(host.id, db)?.map(|t| t.internal_id);
                        box_caches.insert(host.id, (ctx, tenant_internal));
                        box_caches
                            .get(&host.id)
                            .expect("just inserted")
                    }
                };
                let (ctx, tenant_internal) = entry;
                let Some(tenant_internal_id) = *tenant_internal else { continue };
                let Some(session) = ctx.session(sessions, tenant_internal_id, s_ext)? else {
                    continue;
                };
                let row = serde_json::to_value(&session)?;
                if filter.matches(&row) {
                    batch.push(row);
                    if batch.len() >= cap {
                        break;
                    }
                }
            }
        }
        other => {
            return Err(WireError::Protocol(format!(
                "unknown collection {other}; only 'events' and 'sessions' exist"
            )));
        }
    }

    let first_batch: Vec<Bson> = batch.into_iter().map(json_to_bson).collect();
    let batch_len = first_batch.len();
    let response = doc! {
        "cursor": {
            "id": 0_i64,
            "ns": format!("{}.{}", db, collection),
            "firstBatch": Bson::Array(first_batch),
        },
        "ok": 1.0,
    };
    // Slow-query log — buffered only when elapsed clears the
    // operator threshold. Off-path cost is one atomic load.
    if let Some(buf) = registry.slow_queries() {
        let ns = started.elapsed().as_nanos() as u64;
        let pinned_session = Filter::parse(filter_doc)
            .ok()
            .and_then(|f| {
                f.top_level_eq("session_id").and_then(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
            })
            .unwrap_or_default();
        let filter_json = serde_json::to_string(&bson::Bson::Document(filter_doc.clone()))
            .unwrap_or_default();
        buf.maybe_record(
            agentdb_core::slow_query::SlowQueryKind::Find,
            ns,
            wall_ms(),
            admit_box.id,
            db,
            &pinned_session,
            || {
                format!(
                    "collection={collection}, batch={batch_len}, limit={}, filter={}",
                    limit.unwrap_or(-1),
                    truncate(&filter_json, 256),
                )
            },
        );
    }
    Ok(response)
}

fn wall_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut out = s[..max].to_string();
        out.push('…');
        out
    }
}

/// Mongo-shaped error body for the session-pinned-on-a-peer
/// case. `code: 10107` (NotWritablePrimary) is the canonical
/// signal for "your connection is to the wrong node" — drivers
/// interpret it as "reconnect." We include the owning node's
/// address so a human operator sees it immediately and a
/// driver that logs errmsg has a breadcrumb.
pub fn cross_node_error(node_id: &str, address: &str, tablet_id: i64) -> Document {
    doc! {
        "ok": 0.0,
        "code": 10107_i32,
        "codeName": "NotWritablePrimary",
        "errmsg": format!(
            "session shard is owned by node {node_id} at {address} (tablet {tablet_id}); \
             reconnect there. Cross-node Mongo-wire forwarding is not implemented; \
             see docs/ARCHITECTURE.md M20 multi-node section."
        ),
    }
}

fn project_event(session_id: &str, ev: Event) -> Value {
    serde_json::json!({
        "session_id": session_id,
        "seq": ev.seq,
        "ts": ev.ts,
        "payload": ev.payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::{KvStore, RocksKvStore};
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup_with_events() -> (
        TempDir,
        TempDir,
        Arc<BoxRegistry>,
        Arc<TenantRegistry>,
        Arc<SessionRegistry>,
    ) {
        use object_store::local::LocalFileSystem;
        use object_store::ObjectStore;
        let dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        for seq in 1..=3u64 {
            agentdb_core::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-1",
                json!({"kind": "msg", "i": seq}),
                0,
                seq,
            )
            .await
            .unwrap();
        }
        (dir, obj_dir, registry, tenants, sessions)
    }

    #[tokio::test]
    async fn find_events_returns_all_for_tenant() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            None,
            false,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn find_events_with_session_id_filter_scans_only_that_session() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        // create a second session
        for _ in 0..2 {
            agentdb_core::append_and_fetch(
                &registry,
                &tenants,
                &sessions,
                "t-1",
                "s-2",
                json!({"kind": "other"}),
                0,
                0,
            )
            .await
            .unwrap();
        }
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! { "session_id": "s-1" },
            None,
            false,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn find_respects_limit() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            Some(2),
            false,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn find_unknown_tenant_returns_empty() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "nope",
            "events",
            &doc! {},
            None,
            false,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn find_on_sessions_collection_returns_session_records() {
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "sessions",
            &doc! {},
            None,
            false,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(batch.len(), 1);
        let rec = batch[0].as_document().unwrap();
        assert_eq!(rec.get_str("external_id").unwrap(), "s-1");
    }

    #[tokio::test]
    async fn find_events_on_cold_session_returns_segment_contents() {
        // M17 regression: before this milestone, find over `events` on
        // a cold session returned empty because scan_session_events
        // only walked KV (already evicted on tier transition).
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            agentdb_core::cold_transition(
                registry.kv(),
                registry.object_store(),
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        assert_eq!(session.tier, agentdb_core::Tier::Cold);

        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            None,
            false,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(
            batch.len(),
            3,
            "cold session should surface 3 events via segment read"
        );
    }

    #[tokio::test]
    async fn find_events_on_archive_session_returns_segment_contents() {
        // M17 core regression: find on an archived session returned
        // empty pre-M17 because `Tier::Archive => Vec::new()`. Now it
        // must return the same events as the cold case.
        let (_dir, _obj_dir, registry, tenants, sessions) = setup_with_events().await;
        let box_ = registry.route_tenant("t-1");
        let tenant = tenants.get_or_mint(box_.id, "t-1", 0).unwrap();
        let mut session = sessions
            .lookup(box_.id, tenant.internal_id, "s-1")
            .unwrap()
            .unwrap();
        // Cold then flip to Archive (metadata-only in this prototype).
        {
            let guard = box_.tenant_write_lock("t-1").lock_owned().await;
            agentdb_core::cold_transition(
                registry.kv(),
                registry.object_store(),
                &sessions,
                &guard,
                &mut session,
            )
            .await
            .unwrap();
        }
        session.tier = agentdb_core::Tier::Archive;
        sessions.save(&session).unwrap();

        let resp = find_response(
            &registry,
            &tenants,
            &sessions,
            "t-1",
            "events",
            &doc! {},
            None,
            false,
        )
        .await
        .unwrap();
        let batch = resp
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        assert_eq!(
            batch.len(),
            3,
            "archive session should surface 3 events via unified segment read (M17)"
        );
    }
}
