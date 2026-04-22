//! M14 Phase H — skewed-clock integration scenarios.
//!
//! These tests exercise HLC semantics across multiple `HlcClock`
//! instances — the approximation of multiple processes or nodes inside
//! one test binary. Unit-level HLC tests live in `agentdb-core::hlc`;
//! this file covers the cross-clock properties that single-clock tests
//! cannot.

use agentdb_core::{HlcClock, HlcError, HlcTimestamp};

/// Two clocks, starting independently, converge when they exchange
/// timestamps via `update()`. After one round trip, the "slow" clock
/// has absorbed the "fast" clock's physical component.
#[test]
fn two_clocks_converge_under_exchange() {
    let fast = HlcClock::new(10);
    let slow = HlcClock::new(10);

    // Advance the fast clock a bit.
    let fast_ts = fast.now();

    // Slow absorbs fast via update.
    let merged = slow.update(fast_ts).expect("within max_offset");
    assert!(
        merged.physical_ns >= fast_ts.physical_ns,
        "slow clock did not catch up to fast: fast={:?}, slow_after={:?}",
        fast_ts,
        merged,
    );

    // Now slow advances; fast absorbs.
    let slow_ts = slow.now();
    let fast_merged = fast.update(slow_ts).expect("within max_offset");
    assert!(fast_merged.physical_ns >= slow_ts.physical_ns);

    // After convergence, either clock's `now()` is ≥ both earlier timestamps.
    let after = fast.now();
    assert!(after > fast_ts);
    assert!(after > slow_ts);
}

/// A peer in the future beyond max_offset cannot poison the local clock.
/// Repeated rejects leave local state unchanged and increment the
/// rejects counter.
#[test]
fn future_skew_beyond_max_offset_cannot_poison_local() {
    let local = HlcClock::new(10);
    let before = local.now();

    // Peer is 1 hour ahead.
    let rogue = HlcTimestamp {
        physical_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + 3_600_000_000_000,
        logical: 0,
    };

    // Fire 100 rejections.
    for _ in 0..100 {
        let err = local.update(rogue).expect_err("must reject");
        match err {
            HlcError::OffsetTooLarge { .. } => {}
        }
    }
    assert_eq!(local.rejects_count(), 100);

    // Local clock must not have been bumped by any of these.
    let after = local.now();
    let offset_ns = (after.physical_ns as i128 - before.physical_ns as i128).abs();
    assert!(
        offset_ns < 10_000_000_000, // within 10s of original
        "rogue remote poisoned local clock by {} ns",
        offset_ns
    );
}

/// A peer well within max_offset (5s ahead) advances local cleanly.
/// This is the "my neighbor's clock is slightly fast" scenario — the
/// design explicitly accepts it.
#[test]
fn moderate_future_skew_is_absorbed() {
    let local = HlcClock::new(10);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let ahead = HlcTimestamp {
        physical_ns: now_ns + 5_000_000_000, // +5s
        logical: 0,
    };
    let merged = local.update(ahead).expect("5s is within 10s max_offset");
    assert!(merged >= ahead);
    assert_eq!(local.rejects_count(), 0);
}

/// After a rejected update, future well-behaved updates still work.
/// A rejection is a per-request failure; it does not corrupt state.
#[test]
fn reject_does_not_break_subsequent_updates() {
    let local = HlcClock::new(10);

    // 1-hour-future peer → reject.
    let rogue = HlcTimestamp {
        physical_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + 3_600_000_000_000,
        logical: 0,
    };
    let _ = local.update(rogue);
    assert_eq!(local.rejects_count(), 1);

    // Well-behaved peer immediately after → accepted.
    let ok = HlcTimestamp {
        physical_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + 100_000_000, // +100ms
        logical: 0,
    };
    let merged = local.update(ok).expect("within offset");
    assert!(merged >= ok);
    assert_eq!(local.rejects_count(), 1);
}
