use async_broadcast::{Receiver, Sender};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub tenant: String,
    pub session: String,
    pub seq: u64,
    pub ts_ms: u64,
    pub hlc_ts: agentdb_core::hlc::HlcTimestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Clone)]
pub struct EventBus {
    tx: Sender<EventEnvelope>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (mut tx, rx) = async_broadcast::broadcast(capacity);
        tx.set_overflow(true);
        std::mem::forget(rx);
        Self { tx }
    }

    pub fn subscribe(&self) -> Receiver<EventEnvelope> {
        self.tx.new_receiver()
    }

    pub fn emit(&self, env: EventEnvelope) {
        let _ = self.tx.try_broadcast(env);
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(16384)
    }
}

pub fn envelope_from_event(
    tenant: &str,
    session: &str,
    event: &agentdb_core::types::Event,
) -> EventEnvelope {
    let kind = event
        .payload
        .get("kind")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    EventEnvelope {
        tenant: tenant.to_string(),
        session: session.to_string(),
        seq: event.seq,
        ts_ms: event.ts,
        hlc_ts: event.hlc_ts,
        actor_id: event.actor_id.clone(),
        kind,
        payload: event.payload.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_env(seq: u64) -> EventEnvelope {
        EventEnvelope {
            tenant: "t-1".into(),
            session: "s-1".into(),
            seq,
            ts_ms: 1_000 + seq,
            hlc_ts: agentdb_core::hlc::HlcTimestamp {
                physical_ns: 0,
                logical: 0,
            },
            actor_id: None,
            kind: Some("message".into()),
            payload: serde_json::json!({"text": format!("evt-{seq}")}),
        }
    }

    #[tokio::test]
    async fn emit_then_receive_in_order() {
        let bus = EventBus::new(64);
        let mut rx = bus.subscribe();
        bus.emit(make_env(1));
        bus.emit(make_env(2));
        bus.emit(make_env(3));
        let a = rx.recv().await.unwrap();
        let b = rx.recv().await.unwrap();
        let c = rx.recv().await.unwrap();
        assert_eq!(a.seq, 1);
        assert_eq!(b.seq, 2);
        assert_eq!(c.seq, 3);
    }

    #[tokio::test]
    async fn slow_subscriber_lags_when_capacity_exhausted() {
        let bus = EventBus::new(2);
        let mut rx = bus.subscribe();
        for i in 1..=5 {
            bus.emit(make_env(i));
        }
        let mut errors = 0;
        for _ in 0..5 {
            match rx.try_recv() {
                Ok(_) => {}
                Err(async_broadcast::TryRecvError::Overflowed(_)) => errors += 1,
                Err(_) => break,
            }
        }
        assert!(errors >= 1, "expected at least one Overflowed error");
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let env = make_env(42);
        let s = serde_json::to_string(&env).unwrap();
        let back: EventEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(env, back);
    }
}
