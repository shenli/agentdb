//! Hybrid Logical Clock (HLC).
//!
//! Implements the clock model committed to in `docs/agent-db-v2.md` §4 and
//! described in detail in `docs/superpowers/notes/2026-04-21-hlc-clock-decision.md`.
//!
//! The primitive: a process-global `HlcClock` provides causality-respecting
//! timestamps with bounded drift from wall clock. Each event appended in the
//! system carries an `HlcTimestamp`; cross-placement snapshot reads pin an
//! HLC cutoff to define a consistent read horizon; inbound timestamps from
//! other processes update the local clock via `update()` subject to a
//! max-offset guard that protects against rogue peers.
//!
//! Algorithm reference: Kulkarni, Demirbas, Madappa, Avva, Leone. "Logical
//! Physical Clocks." OPODIS 2014.

use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

/// A hybrid logical clock timestamp.
///
/// Encoded as 12 bytes on the wire: an 8-byte physical component
/// (nanoseconds since UNIX epoch) and a 4-byte logical counter that breaks
/// ties when the physical component does not advance between events.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize, ToSchema)]
pub struct HlcTimestamp {
    /// Nanoseconds since UNIX epoch. Tracks wall-clock time with
    /// causality-respecting floors enforced on `update()`.
    pub physical_ns: u64,
    /// Tie-breaker counter. Incremented when two events land in the same
    /// physical nanosecond.
    pub logical: u32,
}

impl HlcTimestamp {
    /// The zero timestamp — used as a default for events read from legacy
    /// v1 segments that predate HLC (see M14 Phase B).
    pub const ZERO: Self = Self {
        physical_ns: 0,
        logical: 0,
    };

    /// Pack into a single u128 for ordered comparison. High 96 bits are
    /// physical_ns (shifted), low 32 bits are logical.
    pub fn to_u128(self) -> u128 {
        ((self.physical_ns as u128) << 32) | (self.logical as u128)
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.physical_ns
            .cmp(&other.physical_ns)
            .then_with(|| self.logical.cmp(&other.logical))
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Errors returned from HLC operations.
#[derive(Debug, Error)]
pub enum HlcError {
    /// An inbound timestamp's physical component exceeds our local wall
    /// clock by more than `max_offset_ns`. Protects against a rogue peer
    /// poisoning the global clock.
    #[error(
        "inbound HLC physical {inbound_ns}ns exceeds local wall {local_ns}ns by more than max_offset {max_offset_ns}ns"
    )]
    OffsetTooLarge {
        inbound_ns: u64,
        local_ns: u64,
        max_offset_ns: u64,
    },
}

/// A process-global hybrid logical clock.
///
/// Thread-safe. All methods are cheap: `now()` and `update()` take a single
/// short mutex. If contention ever becomes visible in profiling, the
/// internal representation can be swapped for a packed `AtomicU128` CAS
/// loop without API changes.
pub struct HlcClock {
    state: Mutex<HlcTimestamp>,
    max_offset_ns: u64,
}

impl HlcClock {
    /// Build a new clock. `max_offset_secs` bounds how far in the future an
    /// inbound timestamp is allowed to be; defaults to 10s per the design
    /// doc (CockroachDB's `max_offset` has the same meaning).
    pub fn new(max_offset_secs: u64) -> Self {
        Self {
            state: Mutex::new(HlcTimestamp::ZERO),
            max_offset_ns: max_offset_secs.saturating_mul(1_000_000_000),
        }
    }

    /// Allocate a new timestamp for a local event. Monotonically advances
    /// both the physical and logical components so that repeated calls
    /// always return strictly-increasing values.
    pub fn now(&self) -> HlcTimestamp {
        let wall = wall_clock_ns();
        let mut s = self.state.lock();
        let phys = s.physical_ns.max(wall);
        let logical = if phys == s.physical_ns {
            s.logical.saturating_add(1)
        } else {
            0
        };
        *s = HlcTimestamp {
            physical_ns: phys,
            logical,
        };
        *s
    }

    /// Observe a remote timestamp (carried on an RPC) and advance the local
    /// clock accordingly. Returns the new local timestamp.
    ///
    /// Rejects with `OffsetTooLarge` if the remote is too far in the
    /// future — this protects the local clock from being poisoned by a
    /// rogue or misconfigured peer.
    pub fn update(&self, remote: HlcTimestamp) -> Result<HlcTimestamp, HlcError> {
        let wall = wall_clock_ns();
        if remote.physical_ns > wall.saturating_add(self.max_offset_ns) {
            return Err(HlcError::OffsetTooLarge {
                inbound_ns: remote.physical_ns,
                local_ns: wall,
                max_offset_ns: self.max_offset_ns,
            });
        }
        let mut s = self.state.lock();
        let prev = *s;
        let phys = prev.physical_ns.max(remote.physical_ns).max(wall);
        let logical = if phys == prev.physical_ns && phys == remote.physical_ns {
            prev.logical.max(remote.logical).saturating_add(1)
        } else if phys == prev.physical_ns {
            prev.logical.saturating_add(1)
        } else if phys == remote.physical_ns {
            remote.logical.saturating_add(1)
        } else {
            0
        };
        *s = HlcTimestamp {
            physical_ns: phys,
            logical,
        };
        Ok(*s)
    }

    /// Read the current clock state without advancing it. Useful for
    /// metrics and for anchoring a `ReadCtx` HLC cutoff.
    pub fn current(&self) -> HlcTimestamp {
        *self.state.lock()
    }

    /// Current offset from wall clock, in nanoseconds. Positive when HLC
    /// has been pushed ahead of wall clock by a peer; negative when wall
    /// clock has jumped ahead and HLC has not yet caught up.
    pub fn offset_from_wall_ns(&self) -> i64 {
        let hlc = self.current().physical_ns as i128;
        let wall = wall_clock_ns() as i128;
        (hlc - wall) as i64
    }

    /// The configured max-offset in seconds. Exposed for admin endpoints.
    pub fn max_offset_secs(&self) -> u64 {
        self.max_offset_ns / 1_000_000_000
    }
}

impl Default for HlcClock {
    /// Default clock: 10s max-offset, matching the design doc's default and
    /// CockroachDB's convention.
    fn default() -> Self {
        Self::new(10)
    }
}

fn wall_clock_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must not be before UNIX epoch")
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn timestamp_ordering_is_lexicographic() {
        let a = HlcTimestamp {
            physical_ns: 100,
            logical: 5,
        };
        let b = HlcTimestamp {
            physical_ns: 100,
            logical: 6,
        };
        let c = HlcTimestamp {
            physical_ns: 101,
            logical: 0,
        };
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn zero_is_below_everything() {
        let t = HlcTimestamp {
            physical_ns: 1,
            logical: 0,
        };
        assert!(HlcTimestamp::ZERO < t);
    }

    #[test]
    fn now_is_strictly_monotonic_in_tight_loop() {
        let clock = HlcClock::new(10);
        let mut last = clock.now();
        for _ in 0..10_000 {
            let next = clock.now();
            assert!(
                next > last,
                "HlcClock::now() not monotonic: prev={:?} next={:?}",
                last,
                next
            );
            last = next;
        }
    }

    #[test]
    fn now_bumps_logical_when_physical_stuck() {
        // Seed state with a future physical so wall clock can't advance past it
        // in the short time of this test, forcing `now()` to bump `logical`.
        let clock = HlcClock::new(10);
        let future_ns = wall_clock_ns() + 10_000_000_000; // +10s, still within max_offset
        {
            let mut s = clock.state.lock();
            *s = HlcTimestamp {
                physical_ns: future_ns,
                logical: 0,
            };
        }
        let t1 = clock.now();
        let t2 = clock.now();
        let t3 = clock.now();
        assert_eq!(t1.physical_ns, future_ns);
        assert_eq!(t2.physical_ns, future_ns);
        assert_eq!(t3.physical_ns, future_ns);
        assert_eq!(t1.logical, 1);
        assert_eq!(t2.logical, 2);
        assert_eq!(t3.logical, 3);
    }

    #[test]
    fn update_advances_local_past_ahead_remote() {
        let clock = HlcClock::new(10);
        let _ = clock.now();
        let remote = HlcTimestamp {
            physical_ns: wall_clock_ns() + 1_000_000_000, // +1s, within max_offset
            logical: 42,
        };
        let merged = clock.update(remote).expect("within max_offset");
        assert!(merged >= remote);
    }

    #[test]
    fn update_rejects_remote_beyond_max_offset() {
        let clock = HlcClock::new(10);
        let before = clock.current();
        let remote = HlcTimestamp {
            physical_ns: wall_clock_ns() + 3_600_000_000_000, // +1h
            logical: 0,
        };
        let err = clock.update(remote).expect_err("beyond max_offset");
        match err {
            HlcError::OffsetTooLarge { .. } => {}
        }
        // Local state must not have been poisoned.
        let after = clock.current();
        assert!(
            after.physical_ns <= wall_clock_ns() + 10_000_000_000,
            "local HLC must not have been bumped by rejected remote: after={:?}",
            after
        );
        // And it should not have regressed from `before`.
        assert!(after >= before);
    }

    #[test]
    fn update_accepts_remote_within_max_offset() {
        let clock = HlcClock::new(10);
        let remote = HlcTimestamp {
            physical_ns: wall_clock_ns() + 5_000_000_000, // +5s
            logical: 7,
        };
        let merged = clock.update(remote).expect("within 10s max_offset");
        assert!(merged.physical_ns >= remote.physical_ns);
    }

    #[test]
    fn concurrent_now_produces_unique_strictly_ordered_timestamps() {
        let clock = Arc::new(HlcClock::new(10));
        let threads = 8;
        let per_thread = 1_000;
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let c = clock.clone();
            handles.push(thread::spawn(move || {
                let mut out = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    out.push(c.now());
                }
                out
            }));
        }

        let mut all = Vec::with_capacity(threads * per_thread);
        for h in handles {
            all.extend(h.join().unwrap());
        }

        // Any two calls to now() must return distinct timestamps regardless
        // of which thread they came from.
        all.sort();
        for pair in all.windows(2) {
            assert!(
                pair[0] < pair[1],
                "duplicate or reversed pair: {:?} then {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn current_does_not_advance_clock() {
        let clock = HlcClock::new(10);
        let _ = clock.now();
        let a = clock.current();
        let b = clock.current();
        assert_eq!(a, b);
    }

    #[test]
    fn offset_from_wall_within_reasonable_bounds() {
        let clock = HlcClock::new(10);
        let _ = clock.now();
        let off = clock.offset_from_wall_ns();
        // After one `now()`, HLC should be ~equal to wall clock.
        assert!(
            off.abs() < 1_000_000_000,
            "offset larger than 1s right after now(): {}",
            off
        );
    }

    #[test]
    fn max_offset_secs_roundtrips() {
        let clock = HlcClock::new(30);
        assert_eq!(clock.max_offset_secs(), 30);
    }

    #[test]
    fn default_clock_has_10s_max_offset() {
        let clock = HlcClock::default();
        assert_eq!(clock.max_offset_secs(), 10);
    }
}
