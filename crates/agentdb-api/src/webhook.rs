//! Lifecycle webhook delivery. Fire-and-forget HTTP POST on each
//! tier transition, to the caller's per-`Box_` configured URL.
//! No retries; failures are logged + counted on `BoxMetrics`.

use std::sync::Arc;
use std::time::Duration;

use agentdb_core::{Box_, Session, Tier};
use serde_json::json;

/// 5s connect+read timeout. We don't want a slow receiver blocking a
/// box's lifecycle worker forever.
pub const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// Build the shared HTTP client. `reqwest::Client` is internally
/// reference-counted and connection-pooled; clone it into `AppState`
/// once and share.
pub fn new_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(WEBHOOK_TIMEOUT)
        .build()
        .expect("reqwest client builds with default config")
}

fn tier_str(t: Tier) -> &'static str {
    match t {
        Tier::Warm => "warm",
        Tier::Cold => "cold",
        Tier::Archive => "archive",
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Read the box's configured webhook URL. If `Some(url)`,
/// `tokio::spawn` a task that POSTs a JSON payload and bumps the
/// box's metrics counters based on the outcome. Returns immediately.
pub fn fire_tier_change(
    client: &reqwest::Client,
    box_: &Arc<Box_>,
    tenant_external: &str,
    session: Session,
    from: Tier,
    to: Tier,
) {
    let url = { box_.webhook.read().clone() };
    let Some(url) = url else {
        return;
    };
    let client = client.clone();
    let box_ = box_.clone();
    let tenant_id = tenant_external.to_string();
    let session_id = session.external_id.clone();
    let segment_path = session.segment_path.clone();
    tokio::spawn(async move {
        let body = json!({
            "event": "TierChange",
            "ts_ms": now_ms(),
            "box_id": box_.id,
            "tenant_id": tenant_id,
            "session_id": session_id,
            "from": tier_str(from),
            "to": tier_str(to),
            "session": {
                "internal_id": session.internal_id,
                "next_seq": session.next_seq,
                "last_active_at": session.last_active_at,
                "segment_path": segment_path,
            },
        });
        match client.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {
                box_.metrics.record_webhook_fire();
            }
            Ok(resp) => {
                box_.metrics.record_webhook_failure();
                tracing::warn!(
                    box_id = box_.id,
                    status = resp.status().as_u16(),
                    url = %url,
                    "webhook returned non-2xx"
                );
            }
            Err(err) => {
                box_.metrics.record_webhook_failure();
                tracing::warn!(
                    box_id = box_.id,
                    ?err,
                    url = %url,
                    "webhook POST failed"
                );
            }
        }
    });
}
