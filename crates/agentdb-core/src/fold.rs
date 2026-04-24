//! Materialized read view: fold correction + tombstone
//! events into a latest-wins view of the session.
//!
//! The wire `update` handler appends **correction events**
//! whose payload carries `_update_of: <target_seq>`; the
//! `delete` handler appends **tombstone events** whose payload
//! carries `_tombstone_of: <target_seq>`. Both are regular
//! events in the log. This module collapses them so a client
//! doing `insert → update → find` sees the post-update
//! payload, and `insert → delete → find` sees nothing.
//!
//! Semantics:
//!
//! - A tombstone hides both the target event and the tombstone
//!   itself. Tombstoning a non-existent seq is a no-op (the
//!   tombstone event still disappears).
//! - A correction folds its payload onto the target event,
//!   replacing it. When multiple corrections target the same
//!   seq, the one with the highest seq wins (most recent).
//! - Correction events themselves disappear from the output —
//!   they're implementation detail.
//! - If a correction's own seq is later tombstoned, the
//!   correction is ignored (the target stays with its
//!   previous payload; if that older payload is the original,
//!   the original is restored). If the final tombstone
//!   targets the original, everything in the chain drops.
//!
//! Input order: whatever the caller supplies, typically
//! ascending seq within a session (that's what
//! `scan_session_all_tiers` produces). The function is
//! two-pass over the slice and preserves input order on
//! emitted events.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::types::Event;

/// Marker key in a correction event's payload.
pub const UPDATE_OF_MARKER: &str = "_update_of";
/// Marker key in a tombstone event's payload.
pub const TOMBSTONE_OF_MARKER: &str = "_tombstone_of";

/// Fold correction + tombstone events into a latest-wins view
/// of `events`. See the module docs for the full contract.
///
/// Runs in `O(n)` time + `O(n)` extra space. Two passes over
/// the input: first to collect markers, second to emit.
pub fn fold_events(events: Vec<Event>) -> Vec<Event> {
    // Pass 1: collect markers.
    let mut tombstoned: HashSet<u64> = HashSet::new();
    // target_seq -> (winning_correction_seq, winning_payload)
    let mut corrections: HashMap<u64, (u64, Value)> = HashMap::new();

    for ev in &events {
        if let Some(target) = marker(&ev.payload, TOMBSTONE_OF_MARKER) {
            tombstoned.insert(target);
        } else if let Some(target) = marker(&ev.payload, UPDATE_OF_MARKER) {
            // Skip the correction entirely if a later event
            // tombstones the correction itself.
            let entry = corrections.entry(target).or_insert((0, Value::Null));
            if ev.seq > entry.0 {
                let mut p = ev.payload.clone();
                if let Value::Object(o) = &mut p {
                    o.remove(UPDATE_OF_MARKER);
                }
                *entry = (ev.seq, p);
            }
        }
    }

    // Second tombstone pass: a tombstoned correction means we
    // fall back to the next-newest correction for the same
    // target (or no correction if none). Iterate events in
    // reverse-seq order and pick the newest non-tombstoned
    // correction for each target.
    let mut rebuilt: HashMap<u64, (u64, Value)> = HashMap::new();
    for ev in &events {
        if tombstoned.contains(&ev.seq) {
            continue;
        }
        if let Some(target) = marker(&ev.payload, UPDATE_OF_MARKER) {
            let entry = rebuilt.entry(target).or_insert((0, Value::Null));
            if ev.seq > entry.0 {
                let mut p = ev.payload.clone();
                if let Value::Object(o) = &mut p {
                    o.remove(UPDATE_OF_MARKER);
                }
                *entry = (ev.seq, p);
            }
        }
    }

    // Pass 2: emit.
    let mut out = Vec::with_capacity(events.len());
    for ev in events {
        if tombstoned.contains(&ev.seq) {
            continue;
        }
        if marker(&ev.payload, TOMBSTONE_OF_MARKER).is_some() {
            continue;
        }
        if marker(&ev.payload, UPDATE_OF_MARKER).is_some() {
            continue;
        }
        if let Some((_, new_payload)) = rebuilt.get(&ev.seq) {
            out.push(Event {
                payload: new_payload.clone(),
                ..ev
            });
        } else {
            out.push(ev);
        }
    }
    out
}

fn marker(v: &Value, key: &str) -> Option<u64> {
    match v.get(key)? {
        Value::Number(n) => n.as_u64().or_else(|| n.as_i64().map(|i| i.max(0) as u64)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::HlcTimestamp;
    use serde_json::json;

    fn ev(seq: u64, payload: Value) -> Event {
        Event {
            seq,
            ts: seq,
            hlc_ts: HlcTimestamp::ZERO,
            actor_id: None,
            payload,
        }
    }

    #[test]
    fn no_markers_is_identity() {
        let input = vec![
            ev(1, json!({"a": 1})),
            ev(2, json!({"a": 2})),
            ev(3, json!({"a": 3})),
        ];
        let out = fold_events(input.clone());
        assert_eq!(out, input);
    }

    #[test]
    fn correction_replaces_target_payload() {
        let input = vec![
            ev(1, json!({"status": "pending", "order": 42})),
            ev(2, json!({"status": "shipped", "order": 42, "_update_of": 1})),
        ];
        let out = fold_events(input);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].seq, 1);
        assert_eq!(out[0].payload["status"], json!("shipped"));
        // The `_update_of` marker itself is stripped.
        assert!(out[0].payload.get("_update_of").is_none());
    }

    #[test]
    fn tombstone_hides_target_and_itself() {
        let input = vec![
            ev(1, json!({"kind": "keep"})),
            ev(2, json!({"kind": "drop"})),
            ev(3, json!({"_tombstone_of": 2})),
        ];
        let out = fold_events(input);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].seq, 1);
    }

    #[test]
    fn latest_correction_wins() {
        let input = vec![
            ev(1, json!({"v": 0})),
            ev(2, json!({"v": 1, "_update_of": 1})),
            ev(3, json!({"v": 2, "_update_of": 1})),
            ev(4, json!({"v": 3, "_update_of": 1})),
        ];
        let out = fold_events(input);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["v"], json!(3));
    }

    #[test]
    fn tombstoned_correction_falls_back_to_prior_correction() {
        let input = vec![
            ev(1, json!({"v": 0})),
            ev(2, json!({"v": 1, "_update_of": 1})),
            ev(3, json!({"v": 2, "_update_of": 1})),
            ev(4, json!({"_tombstone_of": 3})), // cancel the v=2 correction
        ];
        let out = fold_events(input);
        assert_eq!(out.len(), 1);
        // Falls back to the v=1 correction (event 2).
        assert_eq!(out[0].payload["v"], json!(1));
    }

    #[test]
    fn tombstoning_the_target_wins_over_corrections() {
        let input = vec![
            ev(1, json!({"v": 0})),
            ev(2, json!({"v": 1, "_update_of": 1})),
            ev(3, json!({"_tombstone_of": 1})),
        ];
        let out = fold_events(input);
        assert!(out.is_empty(), "tombstone on target hides everything: {out:?}");
    }

    #[test]
    fn tombstone_on_unknown_seq_is_a_noop() {
        let input = vec![
            ev(1, json!({"v": 0})),
            ev(2, json!({"_tombstone_of": 999})),
        ];
        let out = fold_events(input);
        // Event 1 stays; the tombstone itself disappears
        // (its own payload carries the marker).
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].seq, 1);
    }

    #[test]
    fn correction_referring_to_unknown_seq_is_dropped() {
        let input = vec![
            ev(1, json!({"v": 0})),
            ev(2, json!({"v": "orphan", "_update_of": 999})),
        ];
        let out = fold_events(input);
        // Event 1 stays; the correction targets nothing so
        // it's simply omitted (no ghost event).
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].seq, 1);
    }

    #[test]
    fn preserves_input_order_on_emitted_events() {
        let input = vec![
            ev(10, json!({"n": 10})),
            ev(20, json!({"n": 20})),
            ev(25, json!({"_tombstone_of": 20})),
            ev(30, json!({"n": 30})),
            ev(35, json!({"n": 33, "_update_of": 30})),
        ];
        let out = fold_events(input);
        let seqs: Vec<u64> = out.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![10, 30]);
        assert_eq!(out[1].payload["n"], json!(33));
    }
}
