//! Mongo `delete` command handler.
//!
//! AgentDB is append-only, so literal deletion would rewrite
//! the log — we don't. Instead a `delete` appends a
//! **tombstone event** carrying `_tombstone_of: <seq>` in its
//! payload. The original event stays in the log; read-side
//! projections may choose to hide any event referenced by a
//! later tombstone in the same session.
//!
//! The response's `n` reports the number of tombstones
//! appended — matching Mongo's "documents removed" semantic
//! closely enough for client libraries that branch on it.
//!
//! Scope of this implementation:
//!
//! - Target must be the `events` collection.
//! - Each delete MUST carry a top-level `session_id` equality
//!   in its filter. Unscoped deletes would scan every session.
//! - `limit` semantics: `1` → delete the first match only,
//!   `0` → delete every match in the session. This matches
//!   the MongoDB wire protocol's `limit` field on a delete
//!   element (values other than 0 and 1 are Mongo-illegal
//!   and we reject them).

use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::{
    scan_session_all_tiers, BoxRegistry, Event, ReadCtx, SessionRegistry, TenantRegistry,
};
use bson::{doc, Bson, Document};
use serde_json::{json, Value};

use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

pub async fn delete_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    deletes: &[Document],
) -> WireResult<Document> {
    let box_ = registry.route_tenant(db);
    box_.metrics.record_delete();

    if collection != "events" {
        return Err(WireError::Protocol(format!(
            "delete on {collection} not supported; only 'events' is a writable collection"
        )));
    }

    let mut n_deleted: i32 = 0;
    let mut write_errors: Vec<Bson> = Vec::new();

    for (idx, spec) in deletes.iter().enumerate() {
        match apply_one_delete(registry, tenants, sessions, db, spec).await {
            Ok(count) => {
                n_deleted += count as i32;
            }
            Err(e) => {
                let code = error_code_for(&e);
                write_errors.push(Bson::Document(doc! {
                    "index": idx as i32,
                    "code": code,
                    "errmsg": e.to_string(),
                }));
            }
        }
    }

    let mut resp = doc! {
        "n": n_deleted,
        "ok": 1.0,
    };
    if !write_errors.is_empty() {
        resp.insert("writeErrors", Bson::Array(write_errors));
    }
    Ok(resp)
}

async fn apply_one_delete(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    spec: &Document,
) -> WireResult<u32> {
    let q = spec
        .get_document("q")
        .map_err(|_| WireError::Protocol("delete spec missing 'q'".into()))?;

    // `limit`: 0 = all, 1 = first. Anything else is invalid.
    let limit = match spec.get("limit") {
        Some(Bson::Int32(0)) | Some(Bson::Int64(0)) | None => 0i32,
        Some(Bson::Int32(1)) | Some(Bson::Int64(1)) => 1,
        Some(other) => {
            return Err(WireError::Protocol(format!(
                "delete 'limit' must be 0 (all) or 1 (one); got {other:?}"
            )))
        }
    };

    let session_id = q
        .get_str("session_id")
        .map_err(|_| WireError::Protocol(
            "delete on events requires the filter to include a top-level \
             'session_id' equality — an unscoped delete would scan every session"
                .into(),
        ))?
        .to_string();
    let filter = Filter::parse(q)?;

    let box_ = registry.route_tenant(db);
    let Some(tenant) = tenants.lookup(box_.id, db)? else {
        return Ok(0);
    };
    let ctx = ReadCtx::open(&box_, registry.kv())?;
    let Some(session) = ctx.session(sessions, tenant.internal_id, &session_id)? else {
        return Ok(0);
    };

    let events = scan_session_all_tiers(&ctx, registry.object_store(), &session).await?;
    // Don't hold the read context while we take the write lock.
    drop(ctx);

    let mut deleted: u32 = 0;
    for ev in events {
        let row = project_event(&session.external_id, &ev);
        if !filter.matches(&row) {
            continue;
        }

        let tombstone = json!({
            "_tombstone_of": ev.seq,
        });
        let now = now_ms();
        agentdb_core::append_and_fetch_with_actor(
            registry,
            tenants,
            sessions,
            db,
            &session_id,
            tombstone,
            ev.actor_id.clone(),
            0,
            now,
        )
        .await?;
        deleted += 1;

        if limit == 1 {
            break;
        }
    }
    Ok(deleted)
}

fn project_event(session_id: &str, ev: &Event) -> Value {
    json!({
        "session_id": session_id,
        "seq": ev.seq,
        "ts": ev.ts,
        "payload": ev.payload,
    })
}

fn error_code_for(e: &WireError) -> i32 {
    match e {
        WireError::Core(agentdb_core::Error::TooManyRequests { .. }) => 82,
        WireError::Protocol(_) => 9,
        _ => 11,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
