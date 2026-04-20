use std::convert::Infallible;
use std::time::{SystemTime, UNIX_EPOCH};

use async_broadcast::{Receiver, Sender};
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::Stream;
use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum StreamEvent {
    MetricsTick {
        ts_ms: u64,
        boxes: Vec<TickBox>,
    },
    TierChange {
        ts_ms: u64,
        box_id: u16,
        session_id: String,
        from: String,
        to: String,
    },
    ForkCreated {
        ts_ms: u64,
        box_id: u16,
        parent: String,
        child: String,
    },
    QuotaHit {
        ts_ms: u64,
        box_id: u16,
        kind: String,
    },
    FaultInjected {
        ts_ms: u64,
        box_id: u16,
        kind: String,
        duration_s: u64,
    },
    SegmentGcRun {
        ts_ms: u64,
        box_id: u16,
        reclaimed: u64,
        skipped_pinned: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickBox {
    pub id: u16,
    pub warm_sessions: u64,
    pub cold_sessions: u64,
    pub appends: u64,
    pub forks: u64,
    pub fault_active: bool,
    pub open_snapshots: u64,
    pub segments_pinned_by_snapshots: u64,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
pub struct StreamBus {
    pub tx: Sender<StreamEvent>,
}

impl StreamBus {
    pub fn new(capacity: usize) -> Self {
        let (mut tx, rx) = async_broadcast::broadcast(capacity);
        tx.set_overflow(true);
        // Keep a receiver alive so the channel doesn't close when no subscribers are connected.
        std::mem::forget(rx);
        Self { tx }
    }

    pub fn subscribe(&self) -> Receiver<StreamEvent> {
        self.tx.new_receiver()
    }

    pub fn emit(&self, event: StreamEvent) {
        let _ = self.tx.try_broadcast(event);
    }
}

impl Default for StreamBus {
    fn default() -> Self {
        Self::new(1024)
    }
}

pub async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    use futures_util::StreamExt;
    let rx = state.stream.subscribe();
    let stream = rx.map(|ev| {
        let data = serde_json::to_string(&ev).unwrap_or_else(|_| "{}".into());
        Ok(Event::default().data(data))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
