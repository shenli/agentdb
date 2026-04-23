//! Fixed-bucket latency histogram.
//!
//! Per-bucket `AtomicU64` counters, plus a running sum and
//! count. Lock-free, allocation-free on the hot path.
//!
//! Buckets are approximately-exponential and cover the
//! latency shape we actually see on this prototype
//! (sub-ms through many-seconds). If we ever need
//! finer-grained p-values, swap in an HDR-histogram crate;
//! for now, fixed buckets are the zero-dependency choice.
//!
//! Exposition is Prometheus-compatible: the bucket counts
//! are cumulative by upper bound, matching the
//! `<metric>_bucket{le="..."}` shape. `<metric>_sum` is
//! emitted from `sum_ns`; `<metric>_count` from `count`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Upper bounds (inclusive) of each bucket in nanoseconds.
/// The last row is `u64::MAX`, standing in for `+Inf`.
pub const BUCKET_NS: [u64; 14] = [
    1_000_000,      // 1 ms
    2_000_000,      // 2 ms
    5_000_000,      // 5 ms
    10_000_000,     // 10 ms
    20_000_000,     // 20 ms
    50_000_000,     // 50 ms
    100_000_000,    // 100 ms
    200_000_000,    // 200 ms
    500_000_000,    // 500 ms
    1_000_000_000,  // 1 s
    2_000_000_000,  // 2 s
    5_000_000_000,  // 5 s
    10_000_000_000, // 10 s
    u64::MAX,       // +Inf
];

/// Lock-free histogram. Each `record(ns)` picks the first
/// bucket whose upper bound is `>= ns` and fetch-adds its
/// counter, plus updates `count` and `sum_ns`.
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; BUCKET_NS.len()],
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a single observation in nanoseconds.
    pub fn record(&self, ns: u64) {
        let idx = Self::bucket_index(ns);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Point-in-time snapshot. Reads each atomic once; a
    /// concurrent writer may land observations between the
    /// buckets read and the count/sum read, so the
    /// snapshot is "approximately consistent" — totals can
    /// drift by a few records across races. Matches
    /// Prometheus histogram semantics.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut buckets = [0u64; BUCKET_NS.len()];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Ordering::Relaxed);
        }
        // Make cumulative (Prometheus `_bucket` shape).
        let mut running = 0u64;
        for b in buckets.iter_mut() {
            running += *b;
            *b = running;
        }
        HistogramSnapshot {
            cumulative_buckets: buckets,
            count: self.count.load(Ordering::Relaxed),
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
        }
    }

    fn bucket_index(ns: u64) -> usize {
        // Linear scan is fine — 14 buckets, no measurable
        // overhead vs. binary search at this size.
        for (i, &upper) in BUCKET_NS.iter().enumerate() {
            if ns <= upper {
                return i;
            }
        }
        BUCKET_NS.len() - 1
    }
}

/// Point-in-time histogram values.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    /// Cumulative bucket counts (Prometheus `le`).
    pub cumulative_buckets: [u64; BUCKET_NS.len()],
    pub count: u64,
    pub sum_ns: u64,
}

impl HistogramSnapshot {
    /// Approximate `quantile` in nanoseconds, using linear
    /// interpolation inside the hit bucket. `q` in 0..1.
    /// Returns 0 when the histogram is empty.
    pub fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let q = q.clamp(0.0, 1.0);
        let target = (q * self.count as f64).ceil() as u64;
        let mut prev_upper = 0u64;
        let mut prev_cumul = 0u64;
        for (i, &cumul) in self.cumulative_buckets.iter().enumerate() {
            if cumul >= target {
                let upper = BUCKET_NS[i];
                let bucket_upper = if upper == u64::MAX {
                    // Unbounded; report the lower edge +
                    // a token cushion rather than u64::MAX.
                    prev_upper.saturating_add(prev_upper / 2).max(prev_upper + 1)
                } else {
                    upper
                };
                let observations_in_bucket = cumul - prev_cumul;
                if observations_in_bucket == 0 {
                    return bucket_upper;
                }
                // Linear interpolation between prev_upper
                // and bucket_upper based on how far into
                // the bucket `target` lands.
                let into_bucket = target - prev_cumul;
                let span = bucket_upper.saturating_sub(prev_upper);
                let interp = prev_upper
                    + (span as f64 * into_bucket as f64 / observations_in_bucket as f64) as u64;
                return interp;
            }
            prev_cumul = cumul;
            prev_upper = BUCKET_NS[i];
        }
        // Fall-through (shouldn't happen given the +Inf
        // bucket).
        prev_upper
    }

    pub fn p50(&self) -> u64 {
        self.quantile(0.50)
    }
    pub fn p95(&self) -> u64 {
        self.quantile(0.95)
    }
    pub fn p99(&self) -> u64 {
        self.quantile(0.99)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_reports_zero_quantiles() {
        let h = LatencyHistogram::new();
        let s = h.snapshot();
        assert_eq!(s.count, 0);
        assert_eq!(s.p50(), 0);
        assert_eq!(s.p95(), 0);
        assert_eq!(s.p99(), 0);
    }

    #[test]
    fn bucket_index_picks_first_ge_upper_bound() {
        assert_eq!(LatencyHistogram::bucket_index(0), 0);
        assert_eq!(LatencyHistogram::bucket_index(500_000), 0); // 0.5ms → 1ms bucket
        assert_eq!(LatencyHistogram::bucket_index(1_000_000), 0); // exactly 1ms
        assert_eq!(LatencyHistogram::bucket_index(1_500_000), 1); // 1.5ms → 2ms
        assert_eq!(
            LatencyHistogram::bucket_index(u64::MAX),
            BUCKET_NS.len() - 1
        );
    }

    #[test]
    fn record_moves_counts_and_sum() {
        let h = LatencyHistogram::new();
        h.record(500_000); // 0.5ms
        h.record(1_500_000); // 1.5ms
        h.record(1_500_000); // 1.5ms
        let s = h.snapshot();
        assert_eq!(s.count, 3);
        assert_eq!(s.sum_ns, 500_000 + 1_500_000 + 1_500_000);
        // cumulative: bucket 0 has 1 (0.5ms ≤ 1ms), bucket 1
        // has 1+2 = 3 (all three are ≤ 2ms).
        assert_eq!(s.cumulative_buckets[0], 1);
        assert_eq!(s.cumulative_buckets[1], 3);
    }

    #[test]
    fn p50_of_unimodal_distribution_lands_in_the_spike() {
        let h = LatencyHistogram::new();
        for _ in 0..1000 {
            h.record(7_000_000); // 7ms — lands in the 10ms bucket
        }
        let s = h.snapshot();
        let p50 = s.p50();
        assert!(
            (5_000_000..=10_000_000).contains(&p50),
            "p50 should fall inside the hit bucket; got {p50}"
        );
    }

    #[test]
    fn p99_picks_tail() {
        // 95 fast + 5 slow: p95 should be slow. p99 also
        // slow (target = ceil(0.99*100) = 99, landing in
        // the second bucket where the 5 slow observations
        // live).
        let h = LatencyHistogram::new();
        for _ in 0..95 {
            h.record(1_000_000);
        }
        for _ in 0..5 {
            h.record(500_000_000);
        }
        let s = h.snapshot();
        let p99 = s.p99();
        assert!(
            p99 >= 100_000_000,
            "p99 should reflect the tail observation; got {p99}"
        );
    }

    #[test]
    fn record_is_safe_under_concurrency() {
        use std::sync::Arc;
        use std::thread;
        let h = Arc::new(LatencyHistogram::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let hc = h.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10_000 {
                    hc.record(3_000_000);
                }
            }));
        }
        for j in handles {
            j.join().unwrap();
        }
        let s = h.snapshot();
        assert_eq!(s.count, 80_000, "all records must be observed");
    }

    #[test]
    fn quantile_clamps_to_valid_range() {
        let h = LatencyHistogram::new();
        for _ in 0..100 {
            h.record(1_000_000);
        }
        let s = h.snapshot();
        // Negative / > 1 quantiles clamp; no panic.
        assert!(s.quantile(-0.5) <= s.p50());
        assert!(s.quantile(2.0) > 0);
    }
}
