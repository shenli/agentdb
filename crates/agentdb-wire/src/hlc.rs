//! HLC ↔ Mongo `$clusterTime` bridge.
//!
//! The Mongo wire protocol exposes causal consistency via the
//! `$clusterTime` metadata field on every request and response. Mongo 3.6+
//! drivers use it to gossip a `ClusterTime` value shaped as a
//! `BsonTimestamp { time: u32 seconds, increment: u32 }`. That is
//! structurally HLC, and AgentDB's internal `HlcTimestamp` maps directly
//! to it — at seconds granularity in the physical component, which is the
//! same granularity MongoDB itself uses.
//!
//! See `docs/agent-db-v2.md` §4.1, and `docs/superpowers/notes/2026-04-21-hlc-clock-decision.md` §3.6.

use agentdb_core::{HlcClock, HlcError, HlcTimestamp};
use bson::{doc, Bson, Document};

/// Convert a received Mongo `Timestamp(t, i)` into an `HlcTimestamp`.
/// Seconds are scaled to nanoseconds; increment becomes `logical`.
///
/// A sentinel all-zero timestamp is returned as `HlcTimestamp::ZERO`; the
/// caller should treat that as "no HLC present" rather than feeding it to
/// `HlcClock::update`.
pub fn hlc_from_bson_timestamp(ts: bson::Timestamp) -> HlcTimestamp {
    HlcTimestamp {
        physical_ns: (ts.time as u64) * 1_000_000_000,
        logical: ts.increment,
    }
}

/// Convert an `HlcTimestamp` into a Mongo `Timestamp` suitable for wrapping
/// in a `$clusterTime`. Drops sub-second precision (nanoseconds mod 1e9),
/// which is acceptable because MongoDB's own `$clusterTime` operates at
/// seconds granularity.
///
/// Saturates to `u32::MAX` seconds for HLCs beyond 2106-02-07. Practical
/// applications are not affected; guarding is cheaper than panicking.
pub fn bson_timestamp_from_hlc(hlc: HlcTimestamp) -> bson::Timestamp {
    let secs = hlc.physical_ns / 1_000_000_000;
    bson::Timestamp {
        time: secs.min(u32::MAX as u64) as u32,
        increment: hlc.logical,
    }
}

/// Extract a caller-supplied HLC from a request body's `$clusterTime`
/// sub-document and feed it to the local clock.
///
/// Returns `Ok(())` on success, including the no-HLC case (nothing to
/// update). Returns `Err(...)` if the caller's HLC is beyond the local
/// max-offset window; callers should reply with a Mongo error document
/// (see `error_reply_for_hlc_rejection`).
pub fn update_hlc_from_request(
    body: &Document,
    hlc: &HlcClock,
) -> Result<(), HlcError> {
    let Some(ct) = body.get_document("$clusterTime").ok() else {
        return Ok(());
    };
    let Ok(ts) = ct.get_timestamp("clusterTime") else {
        return Ok(());
    };
    // Sentinel all-zero → nothing to propagate.
    if ts.time == 0 && ts.increment == 0 {
        return Ok(());
    }
    hlc.update(hlc_from_bson_timestamp(ts))?;
    Ok(())
}

/// Inject the local HLC as `$clusterTime` on a response body, mirroring
/// what mongod does on every reply. Advances the clock (`HlcClock::now`)
/// so every response carries a strictly-fresh timestamp — matches
/// MongoDB's per-message `$clusterTime` semantics and guarantees the
/// emitted value is strictly greater than any inbound we absorbed.
/// Idempotent — overwrites any existing `$clusterTime` already in the
/// response.
///
/// Includes a zero-signature sub-document (empty hash + keyId 0). The
/// Rust `mongodb` driver and others validate `$clusterTime` structure
/// strictly and reject responses whose signature is missing; a
/// well-formed-but-unsigned structure is the idiomatic way to advertise
/// "this server does not sign cluster times".
pub fn inject_cluster_time(response: &mut Document, hlc: &HlcClock) {
    let ts = bson_timestamp_from_hlc(hlc.now());
    response.insert(
        "$clusterTime",
        Bson::Document(doc! {
            "clusterTime": ts,
            "signature": {
                "hash": Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: Vec::new(),
                }),
                "keyId": 0_i64,
            },
        }),
    );
}

/// Build a Mongo-style error document for an HLC max-offset rejection.
/// Code 71 (`InvalidOptions`) is the closest standard Mongo error for
/// "this request's metadata is out of range".
pub fn error_reply_for_hlc_rejection(err: &HlcError) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": err.to_string(),
        "code": 71_i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_preserves_seconds_and_logical() {
        let original = HlcTimestamp {
            physical_ns: 1_700_000_123_000_000_000, // exactly 1_700_000_123 seconds
            logical: 42,
        };
        let bson_ts = bson_timestamp_from_hlc(original);
        assert_eq!(bson_ts.time, 1_700_000_123);
        assert_eq!(bson_ts.increment, 42);
        let back = hlc_from_bson_timestamp(bson_ts);
        assert_eq!(back, original);
    }

    #[test]
    fn outbound_drops_sub_second_nanos() {
        // Sub-second precision is deliberately not preserved by the bridge.
        let hlc = HlcTimestamp {
            physical_ns: 1_700_000_123_999_999_999,
            logical: 7,
        };
        let ts = bson_timestamp_from_hlc(hlc);
        assert_eq!(ts.time, 1_700_000_123); // floor of nanos/1e9
        assert_eq!(ts.increment, 7);
    }

    #[test]
    fn update_accepts_reasonable_remote() {
        let hlc = HlcClock::new(10);
        let body = doc! {
            "find": "x",
            "$clusterTime": {
                "clusterTime": bson::Timestamp { time: 100, increment: 3 },
            },
        };
        update_hlc_from_request(&body, &hlc).expect("within offset");
        // The clock should have caught up past the incoming physical.
        assert!(
            hlc.current().physical_ns >= 100 * 1_000_000_000,
            "hlc did not absorb incoming clusterTime: {:?}",
            hlc.current()
        );
    }

    #[test]
    fn update_rejects_remote_beyond_max_offset() {
        let hlc = HlcClock::new(10);
        // 2_000_000_000 seconds ≈ year 2033; well beyond now + 10s.
        let far_future = bson::Timestamp {
            time: 2_000_000_000,
            increment: 0,
        };
        let body = doc! {
            "insert": "x",
            "$clusterTime": { "clusterTime": far_future },
        };
        let err = update_hlc_from_request(&body, &hlc).expect_err("rejected");
        match err {
            HlcError::OffsetTooLarge { .. } => {}
        }
    }

    #[test]
    fn update_ignores_missing_cluster_time() {
        let hlc = HlcClock::new(10);
        let body = doc! { "hello": 1 };
        update_hlc_from_request(&body, &hlc).expect("no clusterTime is fine");
    }

    #[test]
    fn update_ignores_sentinel_zero_timestamp() {
        let hlc = HlcClock::new(10);
        let body = doc! {
            "hello": 1,
            "$clusterTime": {
                "clusterTime": bson::Timestamp { time: 0, increment: 0 },
            },
        };
        // Advance local once so we can detect an erroneous update.
        let before = hlc.now();
        update_hlc_from_request(&body, &hlc).expect("sentinel is fine");
        let after = hlc.current();
        // Local clock must not have jumped backward (obvious) nor forward
        // purely because of the sentinel.
        assert!(after >= before);
    }

    #[test]
    fn inject_writes_cluster_time_into_response() {
        let hlc = HlcClock::new(10);
        let _ = hlc.now();
        let mut resp = doc! { "ok": 1.0 };
        inject_cluster_time(&mut resp, &hlc);
        let ct = resp.get_document("$clusterTime").unwrap();
        let ts = ct.get_timestamp("clusterTime").unwrap();
        // Coarse sanity: emitted timestamp should be near "now".
        let now_s = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(
            (now_s as i64 - ts.time as i64).abs() < 60,
            "emitted ts {:?} drifted from wall clock {}",
            ts,
            now_s
        );
    }

    #[test]
    fn error_reply_uses_code_71() {
        let err = HlcError::OffsetTooLarge {
            inbound_ns: 1,
            local_ns: 0,
            max_offset_ns: 0,
        };
        let doc = error_reply_for_hlc_rejection(&err);
        assert_eq!(doc.get_f64("ok").unwrap(), 0.0);
        assert_eq!(doc.get_i32("code").unwrap(), 71);
        assert!(doc.get_str("errmsg").unwrap().contains("max_offset"));
    }
}
