//! Slow query log — a bounded in-memory ring buffer of
//! requests whose end-to-end latency exceeded an operator-
//! configured threshold.
//!
//! Complements `agentdb_append_latency_seconds` (p50/p95/p99
//! from a histogram): the histogram answers "how fast is the
//! system?", the slow-query log answers "which specific
//! requests were slow, and for whom?"
//!
//! Configuration comes from env vars (read once at server
//! startup):
//!
//!   - `AGENTDB_SLOW_QUERY_THRESHOLD_MS` — record any request
//!     whose elapsed time is >= this value. Default 100.
//!     `0` records every request (useful for debugging;
//!     operators can flood the buffer quickly).
//!   - `AGENTDB_SLOW_QUERY_BUFFER_CAPACITY` — ring buffer size.
//!     Default 1024. When full, the oldest record is evicted.
//!   - `AGENTDB_SLOW_QUERY_DISABLE` — if set to any non-empty
//!     value, the server binary skips attaching the buffer
//!     entirely. All `maybe_record` calls become no-ops at
//!     the `registry.slow_queries().is_none()` check.
//!
//! Cardinality: unlike `/metrics` labels, ring-buffer records
//! carry free-form strings (tenant id, session id, detail).
//! That's fine — the buffer is in memory only, bounded, and
//! flows through REST JSON, not Prometheus.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// What kind of request this record describes. Used for
/// filtering in the admin endpoint and as a label on the
/// recorded-count counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SlowQueryKind {
    /// REST `POST /append_and_fetch` (and its Mongo-wire
    /// insert translation).
    Append,
    /// Mongo-wire `find` response.
    Find,
    /// Mongo-wire `aggregate` response.
    Aggregate,
}

impl SlowQueryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SlowQueryKind::Append => "append",
            SlowQueryKind::Find => "find",
            SlowQueryKind::Aggregate => "aggregate",
        }
    }
}

/// One recorded slow request.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SlowQueryRecord {
    /// Monotonically-increasing id assigned at record time.
    /// Exposed so clients can page via `after_id` without
    /// fighting timestamp ties.
    pub id: u64,
    /// Wall-clock ms when the request completed.
    pub ts_ms: i64,
    pub kind: SlowQueryKind,
    pub box_id: u16,
    /// External tenant id (the URL path segment).
    pub tenant_id: String,
    /// Session or collection identifier, if the request
    /// carries one. Find/aggregate on an unscoped tenant
    /// leaves this empty.
    #[serde(default)]
    pub session_id: String,
    pub latency_ms: u64,
    /// Short request summary. For append this is the
    /// payload byte size; for find/aggregate it's a
    /// truncated filter/pipeline BSON excerpt. Never
    /// includes secrets — callers are responsible for the
    /// redaction boundary.
    pub detail: String,
}

/// Filter for [`SlowQueryBuffer::snapshot`]. All fields
/// compose with AND. Non-matching records are skipped while
/// newest-first traversal continues until `limit`.
#[derive(Debug, Clone, Default)]
pub struct SlowQueryFilter {
    pub kind: Option<SlowQueryKind>,
    pub tenant_id: Option<String>,
    /// Cap on returned rows. `None` defaults to
    /// [`DEFAULT_SNAPSHOT_LIMIT`] and values above
    /// [`MAX_SNAPSHOT_LIMIT`] are clamped.
    pub limit: Option<usize>,
    /// Return records with `id > after_id`. Pair with the
    /// largest id from a previous page for pagination.
    pub after_id: Option<u64>,
}

pub const DEFAULT_SNAPSHOT_LIMIT: usize = 100;
pub const MAX_SNAPSHOT_LIMIT: usize = 10_000;

/// Default buffer capacity when env is unset.
pub const DEFAULT_BUFFER_CAPACITY: usize = 1024;
/// Default threshold when env is unset. Requests faster
/// than this are not recorded.
pub const DEFAULT_THRESHOLD_MS: u64 = 100;

/// In-memory, bounded ring buffer. Thread-safe: the single
/// Mutex is fine at prototype scale (slow records are rare by
/// definition; the hot path is the pre-record threshold
/// compare, which is lock-free).
pub struct SlowQueryBuffer {
    inner: Mutex<Inner>,
    threshold_ns: u64,
    next_id: AtomicU64,
    /// Running totals by kind. Exported to `/metrics` as
    /// `agentdb_slow_queries_recorded_total{kind="..."}`.
    recorded_append: AtomicU64,
    recorded_find: AtomicU64,
    recorded_aggregate: AtomicU64,
}

struct Inner {
    records: VecDeque<SlowQueryRecord>,
    capacity: usize,
}

impl SlowQueryBuffer {
    /// Build a buffer with explicit parameters. `threshold_ms
    /// == 0` records every request (for debugging). To fully
    /// disable recording, don't attach the buffer on the
    /// `BoxRegistry` at all.
    pub fn new(threshold_ms: u64, capacity: usize) -> Self {
        // Guard against pathological configs. A zero capacity
        // with a non-zero threshold would accept records and
        // immediately drop them; treat that the same as
        // threshold=0 by keeping a nominal capacity of 1.
        let cap = capacity.max(1);
        Self {
            inner: Mutex::new(Inner {
                records: VecDeque::with_capacity(cap),
                capacity: cap,
            }),
            threshold_ns: threshold_ms.saturating_mul(1_000_000),
            next_id: AtomicU64::new(1),
            recorded_append: AtomicU64::new(0),
            recorded_find: AtomicU64::new(0),
            recorded_aggregate: AtomicU64::new(0),
        }
    }

    /// Construct from env vars. Falls back to defaults on
    /// missing / unparseable values. `AGENTDB_SLOW_QUERY_THRESHOLD_MS`
    /// of `0` yields a disabled buffer (records skipped).
    pub fn from_env() -> Self {
        let threshold = std::env::var("AGENTDB_SLOW_QUERY_THRESHOLD_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_THRESHOLD_MS);
        let capacity = std::env::var("AGENTDB_SLOW_QUERY_BUFFER_CAPACITY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_BUFFER_CAPACITY);
        Self::new(threshold, capacity)
    }

    /// Threshold in milliseconds. Zero means "record everything";
    /// absence of the buffer entirely (via the registry
    /// `OnceLock` staying unset) is the only way to disable.
    pub fn threshold_ms(&self) -> u64 {
        self.threshold_ns / 1_000_000
    }

    /// Record a completed request if it meets the threshold.
    /// `elapsed_ns` is typically `Instant::elapsed().as_nanos()`.
    /// Fast path when below-threshold: one atomic load + compare.
    ///
    /// `build_detail` is invoked lazily so callers only pay for
    /// building the detail string on the slow path.
    pub fn maybe_record<F: FnOnce() -> String>(
        &self,
        kind: SlowQueryKind,
        elapsed_ns: u64,
        ts_ms: i64,
        box_id: u16,
        tenant_id: &str,
        session_id: &str,
        build_detail: F,
    ) {
        if elapsed_ns < self.threshold_ns {
            return;
        }
        let record = SlowQueryRecord {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            ts_ms,
            kind,
            box_id,
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            latency_ms: elapsed_ns / 1_000_000,
            detail: build_detail(),
        };
        match kind {
            SlowQueryKind::Append => {
                self.recorded_append.fetch_add(1, Ordering::Relaxed);
            }
            SlowQueryKind::Find => {
                self.recorded_find.fetch_add(1, Ordering::Relaxed);
            }
            SlowQueryKind::Aggregate => {
                self.recorded_aggregate.fetch_add(1, Ordering::Relaxed);
            }
        };
        let mut inner = self.inner.lock();
        if inner.records.len() >= inner.capacity {
            inner.records.pop_front();
        }
        inner.records.push_back(record);
    }

    /// Return records newest-first, subject to [`SlowQueryFilter`].
    /// Clones the records so callers don't hold the buffer lock.
    pub fn snapshot(&self, filter: &SlowQueryFilter) -> Vec<SlowQueryRecord> {
        let limit = filter
            .limit
            .unwrap_or(DEFAULT_SNAPSHOT_LIMIT)
            .clamp(1, MAX_SNAPSHOT_LIMIT);
        let inner = self.inner.lock();
        let mut out: Vec<SlowQueryRecord> = Vec::with_capacity(limit.min(inner.records.len()));
        // Iterate newest → oldest. The ring is insertion-
        // ordered, so rev() is newest-first.
        for r in inner.records.iter().rev() {
            if let Some(kind) = filter.kind {
                if r.kind != kind {
                    continue;
                }
            }
            if let Some(tid) = &filter.tenant_id {
                if &r.tenant_id != tid {
                    continue;
                }
            }
            if let Some(after) = filter.after_id {
                if r.id <= after {
                    continue;
                }
            }
            out.push(r.clone());
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Per-kind recorded counts since process start. Emitted
    /// on `/metrics` as `agentdb_slow_queries_recorded_total`.
    pub fn counts(&self) -> [(SlowQueryKind, u64); 3] {
        [
            (
                SlowQueryKind::Append,
                self.recorded_append.load(Ordering::Relaxed),
            ),
            (
                SlowQueryKind::Find,
                self.recorded_find.load(Ordering::Relaxed),
            ),
            (
                SlowQueryKind::Aggregate,
                self.recorded_aggregate.load(Ordering::Relaxed),
            ),
        ]
    }

    /// Current number of records held in the ring. Used for
    /// tests; operators see this through the snapshot
    /// endpoint.
    pub fn len(&self) -> usize {
        self.inner.lock().records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec_append(b: &SlowQueryBuffer, elapsed_ns: u64, tenant: &str, session: &str) {
        b.maybe_record(
            SlowQueryKind::Append,
            elapsed_ns,
            1000,
            0,
            tenant,
            session,
            || "payload=42".to_string(),
        );
    }

    #[test]
    fn threshold_zero_records_everything() {
        let b = SlowQueryBuffer::new(0, 16);
        rec_append(&b, 0, "t", "s");
        rec_append(&b, 1_000, "t", "s");
        rec_append(&b, 1_000_000_000, "t", "s");
        assert_eq!(b.len(), 3);
    }

    #[test]
    fn records_above_threshold_only() {
        // threshold = 100ms
        let b = SlowQueryBuffer::new(100, 16);
        rec_append(&b, 50_000_000, "fast", "s"); // 50ms → skipped
        rec_append(&b, 100_000_000, "exact", "s"); // 100ms → recorded (>=)
        rec_append(&b, 500_000_000, "slow", "s"); // 500ms → recorded
        assert_eq!(b.len(), 2);
        let snap = b.snapshot(&SlowQueryFilter::default());
        let tenants: Vec<&str> = snap.iter().map(|r| r.tenant_id.as_str()).collect();
        assert_eq!(tenants, vec!["slow", "exact"]);
    }

    #[test]
    fn ring_evicts_oldest_when_full() {
        let b = SlowQueryBuffer::new(100, 3);
        for i in 0..5u64 {
            b.maybe_record(
                SlowQueryKind::Append,
                200_000_000,
                1000 + i as i64,
                0,
                &format!("t-{i}"),
                "s",
                || "".to_string(),
            );
        }
        let ids: Vec<u64> = b
            .snapshot(&SlowQueryFilter::default())
            .into_iter()
            .map(|r| r.id)
            .collect();
        // Newest three records kept, newest first.
        assert_eq!(ids, vec![5, 4, 3]);
    }

    #[test]
    fn filter_by_kind() {
        let b = SlowQueryBuffer::new(100, 16);
        rec_append(&b, 200_000_000, "t", "s");
        b.maybe_record(
            SlowQueryKind::Find,
            200_000_000,
            1,
            0,
            "t",
            "",
            || "".into(),
        );
        b.maybe_record(
            SlowQueryKind::Aggregate,
            200_000_000,
            1,
            0,
            "t",
            "",
            || "".into(),
        );
        let kinds: Vec<SlowQueryKind> = b
            .snapshot(&SlowQueryFilter {
                kind: Some(SlowQueryKind::Find),
                ..Default::default()
            })
            .into_iter()
            .map(|r| r.kind)
            .collect();
        assert_eq!(kinds, vec![SlowQueryKind::Find]);
    }

    #[test]
    fn filter_by_tenant() {
        let b = SlowQueryBuffer::new(100, 16);
        rec_append(&b, 200_000_000, "alice", "s");
        rec_append(&b, 200_000_000, "bob", "s");
        rec_append(&b, 200_000_000, "alice", "s");
        let rows = b.snapshot(&SlowQueryFilter {
            tenant_id: Some("alice".into()),
            ..Default::default()
        });
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.tenant_id == "alice"));
    }

    #[test]
    fn filter_after_id_skips_older() {
        let b = SlowQueryBuffer::new(100, 16);
        for i in 0..4u64 {
            rec_append(&b, 200_000_000, &format!("t-{i}"), "s");
        }
        let rows = b.snapshot(&SlowQueryFilter {
            after_id: Some(2),
            ..Default::default()
        });
        let ids: Vec<u64> = rows.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![4, 3], "only ids > 2 returned");
    }

    #[test]
    fn counts_track_per_kind_totals() {
        let b = SlowQueryBuffer::new(100, 16);
        rec_append(&b, 200_000_000, "t", "s");
        rec_append(&b, 200_000_000, "t", "s");
        b.maybe_record(
            SlowQueryKind::Find,
            200_000_000,
            1,
            0,
            "t",
            "",
            || "".into(),
        );
        let counts: std::collections::HashMap<_, _> = b.counts().into_iter().collect();
        assert_eq!(counts.get(&SlowQueryKind::Append).copied(), Some(2));
        assert_eq!(counts.get(&SlowQueryKind::Find).copied(), Some(1));
        assert_eq!(counts.get(&SlowQueryKind::Aggregate).copied(), Some(0));
    }

    #[test]
    fn limit_clamps_and_defaults() {
        let b = SlowQueryBuffer::new(100, 32);
        for _ in 0..10 {
            rec_append(&b, 200_000_000, "t", "s");
        }
        // limit 0 → clamped to 1
        let r = b.snapshot(&SlowQueryFilter {
            limit: Some(0),
            ..Default::default()
        });
        assert_eq!(r.len(), 1);
        // None limit → default (100), but only 10 records exist
        let r = b.snapshot(&SlowQueryFilter::default());
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn detail_closure_runs_only_on_slow_path() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let ran = AtomicBool::new(false);
        let b = SlowQueryBuffer::new(100, 16);
        b.maybe_record(
            SlowQueryKind::Append,
            50_000_000, // below threshold
            1,
            0,
            "t",
            "s",
            || {
                ran.store(true, Ordering::Relaxed);
                "detail".into()
            },
        );
        assert!(
            !ran.load(Ordering::Relaxed),
            "detail builder must not run when below threshold"
        );
    }
}
