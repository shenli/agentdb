//! `GET /metrics` — Prometheus text-format exposition of every
//! per-box counter and gauge. Designed to be scraped by a
//! Prometheus-compatible agent (Prometheus itself, Grafana Agent,
//! VictoriaMetrics, etc.) without any extra config: just point at
//! `http://<node>:7878/metrics`.
//!
//! Why not the `prometheus` crate? Two reasons:
//!
//! 1. It pulls a moderate dep tree for something that's a trivial
//!    text format (one line per sample). We format by hand.
//! 2. Its metric registry model doesn't match ours — `BoxMetrics`
//!    already lives on `Box_` as atomics. Copying into a second
//!    registry at scrape time would double-buffer every gauge.
//!
//! Contract: the text body is UTF-8, uses the exposition format
//! described at <https://prometheus.io/docs/instrumenting/exposition_formats/>,
//! and carries `Content-Type: text/plain; version=0.0.4`. One
//! HELP/TYPE block per metric name; per-box samples use the
//! `box` label.

use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use std::fmt::Write;

use crate::AppState;

/// Per-metric-name metadata emitted exactly once (HELP + TYPE)
/// before the samples.
struct MetricMeta {
    name: &'static str,
    help: &'static str,
    kind: MetricKind,
}

#[derive(Clone, Copy)]
enum MetricKind {
    Counter,
    Gauge,
}

impl MetricKind {
    fn as_str(&self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
        }
    }
}

// The catalog of metrics we export. Kept in code rather than derived
// from the snapshot so naming is stable across struct-field renames
// and so HELP strings can be tuned without touching the snapshot
// type.
const METRICS: &[MetricMeta] = &[
    MetricMeta {
        name: "agentdb_appends_total",
        help: "Total append operations per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_forks_total",
        help: "Total fork operations per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_inserts_total",
        help: "Total insert operations per box (Mongo wire insert)",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_queries_find_total",
        help: "Total Mongo find queries per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_queries_aggregate_total",
        help: "Total Mongo aggregate queries per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_tier_transitions_total",
        help: "Total warm/cold/archive tier transitions per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_quota_hits_total",
        help: "Total quota / admission token-bucket denials per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_faults_injected_total",
        help: "Total admin-injected faults served per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_hedged_triggers_total",
        help: "Total cold-read secondary (hedge) requests fired per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_hedged_primary_wins_total",
        help: "Hedged reads where the primary completed first per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_hedged_secondary_wins_total",
        help: "Hedged reads where the secondary completed first per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_events_indexed_total",
        help: "Events inserted into the live per-box index (post-initial-build)",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_index_snapshots_written_total",
        help: "Successful index snapshot uploads per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_index_snapshot_failures_total",
        help: "Failed index snapshot uploads per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_lease_renewals_total",
        help: "Successful lease renewals (including initial acquire) per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_lease_renewal_failures_total",
        help: "Failed lease renewals per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_lease_lost_total",
        help: "Per-box count of write-path LeaseLost refusals",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_lease_epoch",
        help: "Current lease epoch per box; 0 means no lease held",
        kind: MetricKind::Gauge,
    },
    MetricMeta {
        name: "agentdb_admission_waits_total",
        help: "Write-path requests that had to wait for an admission semaphore permit",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_forwarded_writes_total",
        help: "Writes this node forwarded to a peer node because the target tablet is owned elsewhere (M20 multi-node)",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_forwarded_write_failures_total",
        help: "Forwarded writes where the peer returned non-2xx or the request failed",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_forwarded_reads_total",
        help: "Session-scoped reads (admin session detail, fork) this node forwarded to a peer (M20 multi-node)",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_forwarded_read_failures_total",
        help: "Forwarded reads where the peer returned non-2xx or the request failed",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_webhook_fires_total",
        help: "Total tier-change webhooks fired per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_webhook_failures_total",
        help: "Total tier-change webhook delivery failures per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_open_snapshots",
        help: "Currently-open MVCC read snapshots per box",
        kind: MetricKind::Gauge,
    },
    MetricMeta {
        name: "agentdb_segments_pinned",
        help: "Cold segments currently pinned by live snapshots per box",
        kind: MetricKind::Gauge,
    },
    MetricMeta {
        name: "agentdb_segment_gc_reclaimed_total",
        help: "Segments reaped by the segment garbage collector per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_segment_gc_skipped_pinned_total",
        help: "Segment GC attempts skipped because of a live pin per box",
        kind: MetricKind::Counter,
    },
    MetricMeta {
        name: "agentdb_warm_sessions",
        help: "Current warm-tier sessions per box",
        kind: MetricKind::Gauge,
    },
    MetricMeta {
        name: "agentdb_cold_sessions",
        help: "Current cold-tier sessions per box",
        kind: MetricKind::Gauge,
    },
    MetricMeta {
        name: "agentdb_archive_sessions",
        help: "Current archive-tier sessions per box",
        kind: MetricKind::Gauge,
    },
];

/// Render the exposition text. Owns the whole body as a String —
/// simpler than streaming and cheap at prototype scale (one KB per
/// box for ~30 metrics).
fn render(state: &AppState) -> String {
    let mut out = String::with_capacity(16 * 1024);
    // Snapshot every box up front so we walk them in a stable order
    // and don't re-read atomics per metric.
    struct BoxRow {
        box_id: u16,
        snap: agentdb_core::MetricsSnapshot,
        append: agentdb_core::HistogramSnapshot,
        admission: agentdb_core::HistogramSnapshot,
    }
    let mut rows: Vec<BoxRow> = Vec::new();
    for i in 0..state.registry.num_boxes() {
        let Some(b) = state.registry.box_by_id(i) else {
            continue;
        };
        rows.push(BoxRow {
            box_id: i,
            snap: b.metrics.snapshot(),
            append: b.metrics.append_latency.snapshot(),
            admission: b.metrics.admission_wait_latency.snapshot(),
        });
    }

    for m in METRICS {
        writeln!(out, "# HELP {} {}", m.name, m.help).ok();
        writeln!(out, "# TYPE {} {}", m.name, m.kind.as_str()).ok();
        for row in &rows {
            let value = value_for(m.name, &row.snap);
            writeln!(out, "{}{{box=\"{}\"}} {}", m.name, row.box_id, value).ok();
        }
    }

    // Histograms. Prometheus histograms emit one row per
    // bucket (`_bucket{le="..."}`) plus `_sum` and
    // `_count`. The `le` label is in seconds to match
    // Prometheus convention (convert ns → s at exposition
    // time).
    render_histogram(
        &mut out,
        "agentdb_append_latency_seconds",
        "End-to-end append_and_fetch latency",
        &rows.iter().map(|r| (r.box_id, &r.append)).collect::<Vec<_>>(),
    );
    render_histogram(
        &mut out,
        "agentdb_admission_wait_seconds",
        "Time spent blocked on the per-box admission semaphore (only contended writes)",
        &rows.iter().map(|r| (r.box_id, &r.admission)).collect::<Vec<_>>(),
    );

    // Slow-query recorded counts. One series per kind
    // (append / find / aggregate). Constant cardinality.
    if let Some(sq) = state.registry.slow_queries() {
        writeln!(
            out,
            "# HELP agentdb_slow_queries_recorded_total Requests whose elapsed time exceeded the slow-query threshold since process start"
        )
        .ok();
        writeln!(out, "# TYPE agentdb_slow_queries_recorded_total counter").ok();
        for (kind, count) in sq.counts() {
            writeln!(
                out,
                "agentdb_slow_queries_recorded_total{{kind=\"{}\"}} {}",
                kind.as_str(),
                count,
            )
            .ok();
        }
    }

    // Global per-tenant rate-limit denies. One series per
    // tenant that has ever denied on this process; absent
    // series (no denies yet) are Prometheus-equivalent to
    // zero and we don't emit them. Cardinality is bounded by
    // operator intent — only tenants with configured quotas
    // can deny.
    if let Some(quotas) = state.registry.tenant_quotas() {
        let denies = quotas.deny_snapshot();
        if !denies.is_empty() {
            writeln!(
                out,
                "# HELP agentdb_tenant_quota_denies_total Global per-tenant rate-limit denials since process start"
            )
            .ok();
            writeln!(out, "# TYPE agentdb_tenant_quota_denies_total counter").ok();
            for (tenant_id, count) in denies {
                writeln!(
                    out,
                    "agentdb_tenant_quota_denies_total{{tenant=\"{}\"}} {}",
                    escape_prom_label(&tenant_id),
                    count,
                )
                .ok();
            }
        }
    }

    out
}

/// Minimal Prometheus label-value escaping per the exposition
/// format: backslash, double-quote, newline. No label we emit
/// currently contains these, but tenant ids are operator-
/// supplied strings so guarding against malformed input here
/// keeps the exposition parseable.
fn escape_prom_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str(r"\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

fn render_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    rows: &[(u16, &agentdb_core::HistogramSnapshot)],
) {
    writeln!(out, "# HELP {name} {help}").ok();
    writeln!(out, "# TYPE {name} histogram").ok();
    for (box_id, snap) in rows {
        for (i, &upper_ns) in agentdb_core::BUCKET_NS.iter().enumerate() {
            let le = if upper_ns == u64::MAX {
                "+Inf".to_string()
            } else {
                // ns → seconds with enough precision to keep
                // sub-ms buckets distinguishable.
                format!("{:.6}", upper_ns as f64 / 1_000_000_000.0)
            };
            let cumul = snap.cumulative_buckets[i];
            writeln!(
                out,
                "{name}_bucket{{box=\"{box_id}\",le=\"{le}\"}} {cumul}"
            )
            .ok();
        }
        let sum_seconds = snap.sum_ns as f64 / 1_000_000_000.0;
        writeln!(out, "{name}_sum{{box=\"{box_id}\"}} {sum_seconds:.9}").ok();
        writeln!(
            out,
            "{name}_count{{box=\"{box_id}\"}} {}",
            snap.count
        )
        .ok();
    }
}

/// Map metric name to the correct field on the snapshot. Keeping
/// this in one place rather than per-MetricMeta closures keeps the
/// METRICS table const-friendly.
fn value_for(name: &str, s: &agentdb_core::MetricsSnapshot) -> u64 {
    match name {
        "agentdb_appends_total" => s.appends,
        "agentdb_forks_total" => s.forks,
        "agentdb_inserts_total" => s.inserts,
        "agentdb_queries_find_total" => s.queries_find,
        "agentdb_queries_aggregate_total" => s.queries_aggregate,
        "agentdb_tier_transitions_total" => s.tier_transitions,
        "agentdb_quota_hits_total" => s.quota_hits,
        "agentdb_faults_injected_total" => s.faults_injected,
        "agentdb_hedged_triggers_total" => s.hedged_triggers,
        "agentdb_hedged_primary_wins_total" => s.hedged_primary_wins,
        "agentdb_hedged_secondary_wins_total" => s.hedged_secondary_wins,
        "agentdb_events_indexed_total" => s.events_indexed,
        "agentdb_index_snapshots_written_total" => s.index_snapshots_written,
        "agentdb_index_snapshot_failures_total" => s.index_snapshot_failures,
        "agentdb_lease_renewals_total" => s.lease_renewals,
        "agentdb_lease_renewal_failures_total" => s.lease_renewal_failures,
        "agentdb_lease_lost_total" => s.lease_lost,
        "agentdb_lease_epoch" => s.lease_epoch,
        "agentdb_admission_waits_total" => s.admission_waits_total,
        "agentdb_forwarded_writes_total" => s.forwarded_writes_total,
        "agentdb_forwarded_write_failures_total" => s.forwarded_write_failures_total,
        "agentdb_forwarded_reads_total" => s.forwarded_reads_total,
        "agentdb_forwarded_read_failures_total" => s.forwarded_read_failures_total,
        "agentdb_webhook_fires_total" => s.webhook_fires,
        "agentdb_webhook_failures_total" => s.webhook_failures,
        "agentdb_open_snapshots" => s.open_snapshots,
        "agentdb_segments_pinned" => s.segments_pinned_by_snapshots,
        "agentdb_segment_gc_reclaimed_total" => s.segment_gc_reclaimed,
        "agentdb_segment_gc_skipped_pinned_total" => s.segment_gc_skipped_pinned,
        "agentdb_warm_sessions" => s.gauges.warm_sessions,
        "agentdb_cold_sessions" => s.gauges.cold_sessions,
        "agentdb_archive_sessions" => s.gauges.archive_sessions,
        _ => 0,
    }
}

/// Axum handler. Returns `text/plain` with the Prometheus exposition.
pub async fn metrics(State(state): State<AppState>) -> Response {
    let body = render(&state);
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
    use agentdb_storage::{KvStore, RocksKvStore};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn state_with_two_boxes() -> (TempDir, TempDir, AppState) {
        let kv_dir = TempDir::new().unwrap();
        let obj_dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(kv_dir.path()).unwrap());
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
        let reg = Arc::new(BoxRegistry::new(kv.clone(), store, 2).unwrap());
        let tenants = Arc::new(TenantRegistry::new(kv.clone()));
        let sessions = Arc::new(SessionRegistry::new(kv));
        let stream = crate::stream::StreamBus::default();
        let http = crate::webhook::new_client();
        (
            kv_dir,
            obj_dir,
            AppState {
                registry: reg,
                tenants,
                sessions,
                stream,
                http,
                auth: None,
        admin_auth: None,
            },
        )
    }

    // Two histogram families (append latency, admission wait) add
    // two HELP/TYPE blocks on top of the counter/gauge catalog.
    const HISTOGRAM_FAMILIES: usize = 2;

    // Per histogram per box: one sample line per bucket upper
    // bound plus `_sum` + `_count`.
    fn histogram_lines_per_box() -> usize {
        agentdb_core::BUCKET_NS.len() + 2
    }

    #[test]
    fn render_emits_help_and_type_per_metric() {
        let (_k, _o, state) = state_with_two_boxes();
        let body = render(&state);
        let expected = METRICS.len() + HISTOGRAM_FAMILIES;
        assert_eq!(
            body.matches("# HELP agentdb_").count(),
            expected,
            "expected one HELP line per metric family (catalog + histograms)"
        );
        assert_eq!(
            body.matches("# TYPE agentdb_").count(),
            expected,
            "expected one TYPE line per metric family (catalog + histograms)"
        );
    }

    #[test]
    fn render_emits_one_sample_per_box_per_metric() {
        let (_k, _o, state) = state_with_two_boxes();
        let body = render(&state);
        let sample_count = body
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .count();
        // Two boxes * (scalar metrics + histogram bucket/sum/count lines).
        let expected = 2
            * (METRICS.len() + HISTOGRAM_FAMILIES * histogram_lines_per_box());
        assert_eq!(sample_count, expected);
    }

    #[test]
    fn samples_carry_box_label_and_numeric_value() {
        let (_k, _o, state) = state_with_two_boxes();
        // Bump appends on box 0 so the value is non-zero; verify
        // text body reflects that without affecting box 1.
        state.registry.box_by_id(0).unwrap().metrics.record_append();
        state.registry.box_by_id(0).unwrap().metrics.record_append();
        let body = render(&state);
        assert!(
            body.contains("agentdb_appends_total{box=\"0\"} 2"),
            "box 0 appends counter missing from body:\n{body}"
        );
        assert!(
            body.contains("agentdb_appends_total{box=\"1\"} 0"),
            "box 1 appends counter missing from body"
        );
    }

    #[tokio::test]
    async fn content_type_is_prometheus_exposition() {
        let (_k, _o, state) = state_with_two_boxes();
        let resp = metrics(axum::extract::State(state)).await;
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap_or("").to_string())
            .unwrap_or_default();
        assert!(
            ct.starts_with("text/plain"),
            "expected text/plain content-type; got {ct}"
        );
        assert!(
            ct.contains("version=0.0.4"),
            "expected Prometheus version tag; got {ct}"
        );
    }
}
