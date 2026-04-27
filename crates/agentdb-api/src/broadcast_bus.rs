//! Phase 2 ephemeral broadcast bus (spec § 5.4).
//!
//! Per-channel `async_broadcast` senders, keyed by `(tenant, channel)`,
//! lazy-allocated on first publish or subscribe. Smaller capacity (256)
//! than Phase 1's `EventBus` because broadcast is ephemeral — buffering
//! doesn't help slow subscribers since there's no replay.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_broadcast::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Channel-keyed broadcast envelope. Tenant-scoped, no seq, no replay.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BroadcastEnvelope {
    pub tenant: String,
    pub channel: String,
    /// Set by the publish handler (not the bus) at publish time.
    pub ts_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ChannelKey {
    pub tenant: String,
    pub channel: String,
}

/// In-memory broadcast bus keyed by `(tenant, channel)` (spec § 5.4).
///
/// Per-channel `async_broadcast::Sender` with capacity 256, lazy-allocated
/// on first publish or subscribe. Subscribers are ref-counted; a
/// [`BroadcastSubscription`]'s `Drop` decrements the count.
///
/// **`Default::default()` constructs a bus with NO GC task running.**
/// Callers must spawn a periodic [`BroadcastBus::gc`] sweep — `agentdb-server`
/// does this at startup. Without GC, idle channel entries accumulate forever.
#[derive(Clone, Default)]
pub struct BroadcastBus {
    inner: Arc<RwLock<HashMap<ChannelKey, ChannelEntry>>>,
}

#[derive(Clone)]
struct ChannelEntry {
    tx: Sender<BroadcastEnvelope>,
    subscriber_count: Arc<AtomicUsize>,
    last_publish_ts: Arc<AtomicU64>,
}

const PER_CHANNEL_CAPACITY: usize = 256;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl BroadcastBus {
    /// Allocate (if needed) and return an entry for the channel.
    /// Internal helper used by both publish and subscribe.
    async fn ensure_entry(&self, key: &ChannelKey) -> ChannelEntry {
        {
            let g = self.inner.read().await;
            if let Some(e) = g.get(key) {
                return e.clone();
            }
        }
        let mut g = self.inner.write().await;
        // Double-check after upgrading the lock.
        if let Some(e) = g.get(key) {
            return e.clone();
        }
        let (mut tx, rx) = async_broadcast::broadcast(PER_CHANNEL_CAPACITY);
        tx.set_overflow(true);
        // Keep a receiver alive so the channel doesn't close when no subscribers are connected.
        std::mem::forget(rx);
        let entry = ChannelEntry {
            tx,
            subscriber_count: Arc::new(AtomicUsize::new(0)),
            last_publish_ts: Arc::new(AtomicU64::new(now_ms())),
        };
        g.insert(key.clone(), entry.clone());
        entry
    }

    /// Publish an envelope to a channel. Always non-blocking; on capacity
    /// overflow the oldest message is dropped (`set_overflow(true)`).
    ///
    /// If GC drops the channel entry between this call's `ensure_entry` read
    /// and `try_broadcast`, the publish lands on the now-orphaned `Sender`
    /// and reaches no subscribers. That's correct behavior for an ephemeral
    /// channel — no subscribers, nothing to deliver to.
    pub async fn publish(&self, key: &ChannelKey, env: BroadcastEnvelope) {
        let entry = self.ensure_entry(key).await;
        entry.last_publish_ts.store(now_ms(), Ordering::Relaxed);
        let _ = entry.tx.try_broadcast(env);
    }

    pub async fn subscribe(&self, key: &ChannelKey) -> BroadcastSubscription {
        let entry = self.ensure_entry(key).await;
        entry.subscriber_count.fetch_add(1, Ordering::Relaxed);
        BroadcastSubscription {
            rx: entry.tx.new_receiver(),
            count: entry.subscriber_count.clone(),
        }
    }

    /// Drop entries with zero subscribers and last publish older than `idle_ms`.
    /// Returns the number of entries reclaimed.
    pub async fn gc(&self, idle_ms: u64) -> usize {
        let cutoff = now_ms().saturating_sub(idle_ms);
        let mut g = self.inner.write().await;
        let before = g.len();
        g.retain(|_k, entry| {
            let subs = entry.subscriber_count.load(Ordering::Relaxed);
            let last_pub = entry.last_publish_ts.load(Ordering::Relaxed);
            subs > 0 || last_pub >= cutoff
        });
        before - g.len()
    }
}

pub struct BroadcastSubscription {
    rx: Receiver<BroadcastEnvelope>,
    count: Arc<AtomicUsize>,
}

impl BroadcastSubscription {
    pub async fn recv(&mut self) -> Result<BroadcastEnvelope, async_broadcast::RecvError> {
        self.rx.recv().await
    }
}

impl Drop for BroadcastSubscription {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Validate a channel name. Spec § 5.7: `^[a-zA-Z0-9._:-]{1,256}$`.
pub fn validate_channel_name(name: &str) -> bool {
    let len = name.len();
    if len == 0 || len > 256 {
        return false;
    }
    name.bytes().all(|b| {
        b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b':' || b == b'-'
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(tenant: &str, channel: &str) -> ChannelKey {
        ChannelKey { tenant: tenant.into(), channel: channel.into() }
    }

    fn env(channel: &str, payload: serde_json::Value) -> BroadcastEnvelope {
        BroadcastEnvelope {
            tenant: "t-1".into(),
            channel: channel.into(),
            ts_ms: 1_000,
            sender: None,
            payload,
        }
    }

    #[tokio::test]
    async fn publish_then_receive_in_order() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-1");
        let mut sub = bus.subscribe(&k).await;
        bus.publish(&k, env("c-1", serde_json::json!({"n": 1}))).await;
        bus.publish(&k, env("c-1", serde_json::json!({"n": 2}))).await;
        let a = sub.recv().await.unwrap();
        let b = sub.recv().await.unwrap();
        assert_eq!(a.payload["n"], 1);
        assert_eq!(b.payload["n"], 2);
    }

    #[tokio::test]
    async fn channels_are_isolated() {
        let bus = BroadcastBus::default();
        let k1 = key("t-1", "c-1");
        let k2 = key("t-1", "c-2");
        let mut sub1 = bus.subscribe(&k1).await;
        let mut sub2 = bus.subscribe(&k2).await;
        bus.publish(&k1, env("c-1", serde_json::json!({}))).await;
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), sub1.recv())
            .await
            .is_ok());
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), sub2.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn tenants_are_isolated() {
        let bus = BroadcastBus::default();
        let ka = key("t-a", "c-1");
        let kb = key("t-b", "c-1");
        let mut suba = bus.subscribe(&ka).await;
        let mut subb = bus.subscribe(&kb).await;
        bus.publish(&ka, env("c-1", serde_json::json!({}))).await;
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), suba.recv())
            .await
            .is_ok());
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), subb.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn slow_subscriber_overflows_silently() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-1");
        let mut sub = bus.subscribe(&k).await;
        for i in 0..(PER_CHANNEL_CAPACITY * 4) {
            bus.publish(&k, env("c-1", serde_json::json!({"n": i}))).await;
        }
        // With overflow=true the channel drops oldest items so publish never
        // blocks; the receiver may surface `RecvError::Overflowed(n)` once to
        // signal the gap, then continue delivering newer envelopes. We only
        // care that at least one envelope makes it through.
        let mut got = 0;
        for _ in 0..(PER_CHANNEL_CAPACITY * 2) {
            match tokio::time::timeout(std::time::Duration::from_millis(20), sub.recv()).await {
                Ok(Ok(_)) => got += 1,
                Ok(Err(async_broadcast::RecvError::Overflowed(_))) => continue,
                _ => break,
            }
        }
        assert!(got > 0, "expected at least one delivered envelope");
    }

    #[tokio::test]
    async fn gc_drops_idle_entries_with_zero_subscribers() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-idle");
        bus.publish(&k, env("c-idle", serde_json::json!({}))).await;
        // No subscriber held. Wait > idle threshold and GC.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let reclaimed = bus.gc(10).await;
        assert_eq!(reclaimed, 1);
    }

    #[tokio::test]
    async fn gc_keeps_entries_with_active_subscribers() {
        let bus = BroadcastBus::default();
        let k = key("t-1", "c-live");
        let _sub = bus.subscribe(&k).await;
        bus.publish(&k, env("c-live", serde_json::json!({}))).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let reclaimed = bus.gc(10).await;
        assert_eq!(reclaimed, 0);
    }

    #[test]
    fn channel_name_validation() {
        assert!(validate_channel_name("room-42"));
        assert!(validate_channel_name("room:42"));
        assert!(validate_channel_name("team.alpha.standup"));
        assert!(validate_channel_name("a"));
        assert!(validate_channel_name(&"x".repeat(256)));
        assert!(!validate_channel_name(""));
        assert!(!validate_channel_name(&"x".repeat(257)));
        assert!(!validate_channel_name("with/slash"));
        assert!(!validate_channel_name("with space"));
        assert!(!validate_channel_name("emoji😀"));
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let e = BroadcastEnvelope {
            tenant: "t-1".into(),
            channel: "c-1".into(),
            ts_ms: 42,
            sender: Some("sub".into()),
            payload: serde_json::json!({"k": "v"}),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: BroadcastEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }
}
