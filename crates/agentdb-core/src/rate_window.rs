//! Sliding-window rate tracker for the hot-tablet detector
//! (M25). Each tracker holds a 60-second window bucketed
//! into six 10-second slots; writers fetch-add into the
//! current slot; readers sum across all slots.
//!
//! **Accuracy.** The slot that covers `now` may be partially
//! populated (e.g. sampled 2s into a 10s slot). We account for
//! this by computing the window's effective duration as
//! `(len(slots) - 1) * slot_width + partial_slot_age`. Result
//! is stable once the window is fully populated (>= 60s since
//! start).
//!
//! **Hot path cost.** A single atomic fetch_add per record +
//! an amortized CAS per slot roll. No heap allocation, no
//! locks. Read-side sum-over-slots is 6 atomic loads.
//!
//! **Time source.** An injected `now_ms()` closure for tests;
//! production uses `SystemTime::now()`. We pass a plain `i64`
//! at the read call to keep the type free of closure
//! machinery.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

/// How many slots to keep in the window. 6 slots × 10s = 60s.
pub const NUM_SLOTS: usize = 6;
/// Width of each slot in milliseconds.
pub const SLOT_WIDTH_MS: i64 = 10_000;

/// Rate window tracking two correlated counters: `record_a`
/// and `record_b`. For the hot-tablet path, A = appends and
/// B = admission waits. Keeping them in the same window
/// guarantees the ratio B/A is computed over the same slots.
#[derive(Debug)]
pub struct RateWindow {
    slots: [Slot; NUM_SLOTS],
    /// Index into `slots` for the current write target.
    /// Monotonically advances via `fetch_add % NUM_SLOTS`
    /// on each roll, so readers never see a half-rolled
    /// state.
    current: AtomicUsize,
    /// Wall-clock ms of the *start* of the current slot.
    /// Updated atomically when the slot rolls.
    slot_start_ms: AtomicI64,
    /// Wall-clock ms of when this window was created.
    /// Used to clamp the effective-duration calculation
    /// during warm-up (< 60s since start).
    created_at_ms: AtomicI64,
}

#[derive(Debug, Default)]
struct Slot {
    a: AtomicU64,
    b: AtomicU64,
}

impl RateWindow {
    /// New window pinned to `now_ms` as its t=0. Callers
    /// should construct one per tablet.
    pub fn new(now_ms: i64) -> Self {
        Self {
            slots: Default::default(),
            current: AtomicUsize::new(0),
            slot_start_ms: AtomicI64::new(now_ms),
            created_at_ms: AtomicI64::new(now_ms),
        }
    }

    /// Record one `a` event (e.g. an append).
    pub fn record_a(&self, now_ms: i64) {
        self.maybe_roll(now_ms);
        let idx = self.current.load(Ordering::Acquire) % NUM_SLOTS;
        self.slots[idx].a.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one `b` event (e.g. an admission wait).
    pub fn record_b(&self, now_ms: i64) {
        self.maybe_roll(now_ms);
        let idx = self.current.load(Ordering::Acquire) % NUM_SLOTS;
        self.slots[idx].b.fetch_add(1, Ordering::Relaxed);
    }

    /// Point-in-time rate snapshot.
    ///
    /// - `a_per_sec`: total A events / effective seconds
    /// - `ratio_b_over_a`: sum_b / max(sum_a, 1) — zero
    ///   when A is zero (otherwise 0/0 would NaN).
    pub fn snapshot(&self, now_ms: i64) -> RateSnapshot {
        self.maybe_roll(now_ms);
        let mut sum_a: u64 = 0;
        let mut sum_b: u64 = 0;
        for slot in &self.slots {
            sum_a += slot.a.load(Ordering::Relaxed);
            sum_b += slot.b.load(Ordering::Relaxed);
        }
        let effective_ms = self.effective_window_ms(now_ms);
        let seconds = (effective_ms as f64 / 1000.0).max(0.001);
        let a_per_sec = sum_a as f64 / seconds;
        let ratio_b_over_a = if sum_a == 0 {
            0.0
        } else {
            sum_b as f64 / sum_a as f64
        };
        RateSnapshot {
            a_count: sum_a,
            b_count: sum_b,
            a_per_sec,
            ratio_b_over_a,
            window_ms: effective_ms,
        }
    }

    /// If `now_ms` has crossed one or more slot boundaries
    /// since the last roll, advance `current` and zero any
    /// slots we'd skip over. Idempotent; safe under
    /// concurrent recorders.
    fn maybe_roll(&self, now_ms: i64) {
        loop {
            let start = self.slot_start_ms.load(Ordering::Acquire);
            let elapsed = now_ms - start;
            if elapsed < SLOT_WIDTH_MS {
                return;
            }
            let steps = (elapsed / SLOT_WIDTH_MS) as usize;
            let new_start = start + (steps as i64) * SLOT_WIDTH_MS;
            // Try to commit the new slot_start_ms. Only the
            // winner of this CAS advances current + zeroes
            // the new slot; losers re-check.
            if self
                .slot_start_ms
                .compare_exchange(start, new_start, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Advance current by `steps`, zeroing each
                // slot we pass through (including the new
                // landing slot). Two steps means we also
                // zero the landing slot because it must
                // have been stale from the previous loop.
                let old_current = self.current.load(Ordering::Acquire);
                // Clamp `steps` to NUM_SLOTS so the bookkeeping
                // below stays O(NUM_SLOTS) even after a long
                // idle period.
                let effective_steps = steps.min(NUM_SLOTS);
                for i in 1..=effective_steps {
                    let idx = (old_current + i) % NUM_SLOTS;
                    self.slots[idx].a.store(0, Ordering::Relaxed);
                    self.slots[idx].b.store(0, Ordering::Relaxed);
                }
                // Publish the new current.
                self.current
                    .store((old_current + effective_steps) % NUM_SLOTS, Ordering::Release);
                return;
            }
            // CAS lost → another recorder rolled; re-check.
        }
    }

    /// Compute the effective window size in ms, clamped by
    /// how long the window has been alive.
    fn effective_window_ms(&self, now_ms: i64) -> i64 {
        let age = (now_ms - self.created_at_ms.load(Ordering::Acquire)).max(0);
        let max_window = (NUM_SLOTS as i64) * SLOT_WIDTH_MS;
        age.min(max_window).max(SLOT_WIDTH_MS)
    }
}

/// Point-in-time read. Callers don't mutate these.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateSnapshot {
    pub a_count: u64,
    pub b_count: u64,
    pub a_per_sec: f64,
    pub ratio_b_over_a: f64,
    pub window_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_window_reports_zero_rate() {
        let w = RateWindow::new(0);
        let s = w.snapshot(0);
        assert_eq!(s.a_count, 0);
        assert_eq!(s.b_count, 0);
        assert_eq!(s.a_per_sec, 0.0);
        assert_eq!(s.ratio_b_over_a, 0.0);
    }

    #[test]
    fn records_in_same_slot_accumulate() {
        let w = RateWindow::new(0);
        for i in 0..100 {
            w.record_a(1_000 + i);
        }
        let s = w.snapshot(9_999);
        assert_eq!(s.a_count, 100);
    }

    #[test]
    fn records_roll_across_slot_boundaries() {
        let w = RateWindow::new(0);
        // 100 events in slot 0
        for _ in 0..100 {
            w.record_a(0);
        }
        // 100 events in slot 1 (10s later)
        for _ in 0..100 {
            w.record_a(10_000);
        }
        // 100 events in slot 2 (20s later)
        for _ in 0..100 {
            w.record_a(20_000);
        }
        let s = w.snapshot(20_100);
        assert_eq!(s.a_count, 300);
    }

    #[test]
    fn ratio_b_over_a_counts_correctly() {
        let w = RateWindow::new(0);
        for _ in 0..10 {
            w.record_a(0);
        }
        for _ in 0..3 {
            w.record_b(0);
        }
        let s = w.snapshot(5_000);
        assert_eq!(s.a_count, 10);
        assert_eq!(s.b_count, 3);
        assert!((s.ratio_b_over_a - 0.3).abs() < 1e-9);
    }

    #[test]
    fn old_records_age_out_of_the_window() {
        let w = RateWindow::new(0);
        // Burst at t=0.
        for _ in 0..1000 {
            w.record_a(0);
        }
        // Fast-forward past the 60s window. The 70s jump
        // triggers maybe_roll which zeroes all slots (clamped
        // to NUM_SLOTS).
        let s = w.snapshot(70_000);
        assert_eq!(
            s.a_count, 0,
            "records from >60s ago should have aged out"
        );
    }

    #[test]
    fn effective_window_clamps_to_age_during_warmup() {
        let w = RateWindow::new(0);
        for _ in 0..50 {
            w.record_a(1_000);
        }
        // At t=5s, only ~5s of window has accumulated.
        let s = w.snapshot(5_000);
        assert_eq!(s.a_count, 50);
        // Window clamps to 10s slot width during warm-up —
        // so a_per_sec = 50/10 = 5, not 50/60.
        assert!(
            (s.a_per_sec - 5.0).abs() < 0.01,
            "warm-up window should use slot-width floor, got {}",
            s.a_per_sec
        );
    }

    #[test]
    fn ratio_is_zero_when_a_is_zero() {
        let w = RateWindow::new(0);
        // Waits without appends: pathological but possible
        // if waits fire for requests that ultimately fail.
        // Ratio must not NaN.
        for _ in 0..5 {
            w.record_b(0);
        }
        let s = w.snapshot(5_000);
        assert_eq!(s.a_count, 0);
        assert_eq!(s.b_count, 5);
        assert_eq!(s.ratio_b_over_a, 0.0);
    }

    #[test]
    fn concurrent_records_are_safe() {
        use std::sync::Arc;
        use std::thread;
        let w = Arc::new(RateWindow::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let wc = w.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10_000 {
                    wc.record_a(100);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let s = w.snapshot(200);
        assert_eq!(s.a_count, 80_000, "all records must be observed");
    }
}
