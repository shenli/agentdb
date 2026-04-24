//! Mongo `update` command handler.
//!
//! AgentDB is an event log, so literal in-place mutation isn't
//! possible. The honest translation: an `update` appends a
//! **correction event** — a new event that carries the merged
//! fields plus a `_update_of: <seq>` marker pointing at the
//! original. Applications (and a future projection layer) can
//! fold corrections into the latest view; the raw log keeps
//! the full history.
//!
//! This model differs from MongoDB on one important dimension:
//! **the response's `nModified` counts appended corrections,
//! not mutated rows.** Drivers that just check `ok: 1` work as
//! expected; drivers that branch on `nModified` still get a
//! truthful answer (one correction event appended per match).
//!
//! Scope of this implementation:
//!
//! - `update` must target the `events` collection. `sessions`
//!   and any other target are rejected with a Protocol error.
//! - The filter MUST include a top-level `session_id` equality.
//!   Unscoped updates would require a cross-session scan,
//!   which at event-log scale is an accident-prone footgun.
//! - Supported operators: `$set`, `$unset`, `$inc`. Replacement
//!   documents (no update operator) are rejected — the append-
//!   on-update model only makes sense when the client is
//!   asking to change specific fields.
//! - `upsert: true` with a non-matching filter is rejected
//!   (the correct path for inserts is the `insert` command).
//! - `multi: false` (the default) updates at most one matched
//!   event (the first one scanned); `multi: true` updates all
//!   matches in the scoped session.

use std::time::{SystemTime, UNIX_EPOCH};

use agentdb_core::{
    scan_session_all_tiers, BoxRegistry, Event, ReadCtx, SessionRegistry, TenantRegistry,
};
use bson::{doc, Bson, Document};
use serde_json::{json, Value};

use crate::bsonx::bson_to_json;
use crate::error::{WireError, WireResult};
use crate::handlers::filter::Filter;

pub async fn update_response(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    collection: &str,
    updates: &[Document],
) -> WireResult<Document> {
    let box_ = registry.route_tenant(db);
    box_.metrics.record_update();

    if collection != "events" {
        return Err(WireError::Protocol(format!(
            "update on {collection} not supported; only 'events' is a writable collection"
        )));
    }

    let mut n_matched: i32 = 0;
    let mut n_modified: i32 = 0;
    let mut write_errors: Vec<Bson> = Vec::new();

    for (idx, spec) in updates.iter().enumerate() {
        match apply_one_update(registry, tenants, sessions, db, spec).await {
            Ok(counts) => {
                n_matched += counts.matched as i32;
                n_modified += counts.modified as i32;
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
        "n": n_matched,
        "nModified": n_modified,
        "ok": 1.0,
    };
    if !write_errors.is_empty() {
        resp.insert("writeErrors", Bson::Array(write_errors));
    }
    Ok(resp)
}

#[derive(Debug, Default)]
struct UpdateCounts {
    matched: u32,
    modified: u32,
}

async fn apply_one_update(
    registry: &BoxRegistry,
    tenants: &TenantRegistry,
    sessions: &SessionRegistry,
    db: &str,
    spec: &Document,
) -> WireResult<UpdateCounts> {
    if spec.get_bool("upsert").unwrap_or(false) {
        return Err(WireError::Protocol(
            "upsert not supported on an event log; use 'insert' to append a new event".into(),
        ));
    }
    let multi = spec.get_bool("multi").unwrap_or(false);

    let q = spec
        .get_document("q")
        .map_err(|_| WireError::Protocol("update spec missing 'q'".into()))?;
    let u = spec
        .get_document("u")
        .map_err(|_| WireError::Protocol("update spec missing 'u'".into()))?;
    let ops = UpdateOps::parse(u)?;

    let session_id = q
        .get_str("session_id")
        .map_err(|_| WireError::Protocol(
            "update on events requires the filter to include a top-level \
             'session_id' equality — an unscoped update would scan every session"
                .into(),
        ))?
        .to_string();
    let filter = Filter::parse(q)?;

    // Resolve tenant + session. Missing tenant or session is a
    // legitimate "0 matched", not an error — mirrors Mongo's
    // behavior on an unknown namespace.
    let box_ = registry.route_tenant(db);
    let Some(tenant) = tenants.lookup(box_.id, db)? else {
        return Ok(UpdateCounts::default());
    };
    let ctx = ReadCtx::open(&box_, registry.kv())?;
    let Some(session) = ctx.session(sessions, tenant.internal_id, &session_id)? else {
        return Ok(UpdateCounts::default());
    };

    let events = scan_session_all_tiers(&ctx, registry.object_store(), &session).await?;
    // Close the read context before we do any writes to the
    // same session — the write path takes an exclusive tenant
    // lock.
    drop(ctx);

    // Match the filter against the FOLDED view so chained
    // `update` / `delete` sequences compose correctly: an
    // `updateOne({v:1})` after a prior `updateOne({v:0}→{v:1})`
    // must see v=1, not the raw v=0 event. We preserve the
    // folded event's `seq` which points at the underlying
    // target, so the correction we append still references
    // the right original.
    let folded = agentdb_core::fold_events(events);

    let mut counts = UpdateCounts::default();
    for ev in folded {
        let row = project_event(&session.external_id, &ev);
        if !filter.matches(&row) {
            continue;
        }
        counts.matched += 1;

        // Build the correction payload: start from the folded
        // payload (post any prior correction), apply ops,
        // attach the `_update_of` marker pointing at the
        // original target seq.
        let mut payload = ev.payload.clone();
        if !payload.is_object() {
            payload = Value::Object(Default::default());
        }
        ops.apply(&mut payload);
        if let Value::Object(obj) = &mut payload {
            obj.insert("_update_of".into(), Value::from(ev.seq));
        }

        let now = now_ms();
        agentdb_core::append_and_fetch_with_actor(
            registry,
            tenants,
            sessions,
            db,
            &session_id,
            payload,
            ev.actor_id.clone(),
            0,
            now,
        )
        .await?;
        counts.modified += 1;

        if !multi {
            break;
        }
    }

    Ok(counts)
}

/// Parsed update operators. Simple-minded: top-level only, no
/// dotted paths (future work).
struct UpdateOps {
    set: Vec<(String, Value)>,
    unset: Vec<String>,
    inc: Vec<(String, f64)>,
}

impl UpdateOps {
    fn parse(u: &Document) -> WireResult<Self> {
        // Reject replacement documents — a doc without any `$`
        // operator would replace the whole payload, which does
        // not map onto the append-on-update semantics.
        let has_op = u.keys().any(|k| k.starts_with('$'));
        if !has_op {
            return Err(WireError::Protocol(
                "replacement updates not supported; use $set / $unset / $inc".into(),
            ));
        }
        let mut ops = UpdateOps {
            set: Vec::new(),
            unset: Vec::new(),
            inc: Vec::new(),
        };
        for (key, val) in u.iter() {
            match key.as_str() {
                "$set" => {
                    let d = val.as_document().ok_or_else(|| {
                        WireError::Protocol("$set requires a document".into())
                    })?;
                    for (k, v) in d.iter() {
                        ops.set.push((k.clone(), bson_to_json(v.clone())));
                    }
                }
                "$unset" => {
                    let d = val.as_document().ok_or_else(|| {
                        WireError::Protocol("$unset requires a document".into())
                    })?;
                    for (k, _) in d.iter() {
                        ops.unset.push(k.clone());
                    }
                }
                "$inc" => {
                    let d = val.as_document().ok_or_else(|| {
                        WireError::Protocol("$inc requires a document".into())
                    })?;
                    for (k, v) in d.iter() {
                        let n = match v {
                            Bson::Double(x) => *x,
                            Bson::Int32(x) => *x as f64,
                            Bson::Int64(x) => *x as f64,
                            _ => {
                                return Err(WireError::Protocol(format!(
                                    "$inc.{k} must be numeric"
                                )))
                            }
                        };
                        ops.inc.push((k.clone(), n));
                    }
                }
                other if other.starts_with('$') => {
                    return Err(WireError::Protocol(format!(
                        "update operator {other} not supported; only $set / $unset / $inc"
                    )));
                }
                _ => {
                    // Non-$ keys in a doc that also has $
                    // operators are a legacy shape we don't
                    // support.
                    return Err(WireError::Protocol(
                        "mixed operator and non-operator update doc not supported".into(),
                    ));
                }
            }
        }
        Ok(ops)
    }

    fn apply(&self, payload: &mut Value) {
        let Value::Object(obj) = payload else {
            return;
        };
        for (k, v) in &self.set {
            obj.insert(k.clone(), v.clone());
        }
        for k in &self.unset {
            obj.remove(k);
        }
        for (k, delta) in &self.inc {
            let current = obj.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let next = current + delta;
            // Keep the field an integer when the result is
            // exact — drivers read `{count: 3}` as Int32, so
            // an unnecessary float upgrade would surprise.
            if next.fract() == 0.0 && next.abs() < (i64::MAX as f64) {
                obj.insert(k.clone(), Value::from(next as i64));
            } else {
                obj.insert(k.clone(), json!(next));
            }
        }
    }
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
        WireError::Protocol(_) => 9,  // FailedToParse
        _ => 11,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
