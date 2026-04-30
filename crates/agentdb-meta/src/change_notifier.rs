//! Unified change-notification stream across dialects.
//!
//! Postgres delivers changes via `LISTEN/NOTIFY` triggers set
//! up by migrations 009 and 015 on `shards`, `nodes`, `tablets`,
//! and `session_locators`.
//! SQLite has no such primitive, so the SQLite variant
//! returns a stream that never yields — consumers fall back
//! to their existing periodic refresh loops.
//!
//! Consumers (LocationProxy, NodeAddressProxy, etc.) treat
//! the stream as a "maybe-refresh now" signal: a hint that
//! they should re-pull from meta sooner than the next
//! periodic tick. They never read the payload for
//! correctness — the trigger payload carries only the
//! primary-key of the changed row, which is enough for a
//! cache to refetch.
//!
//! `Channel` is a namespaced string (`shards`, `nodes`,
//! `tablets`, `session_locators`).

use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::{MetaDialect, MetaStore};

/// Logical channel name. Maps to a `agentdb_<channel>_changed`
/// Postgres NOTIFY channel via migration 009.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    Shards,
    Nodes,
    Tablets,
    SessionLocators,
}

impl Channel {
    /// Native Postgres channel name (as populated in the
    /// triggers). Kept in sync with migration 009.
    pub fn pg_name(self) -> &'static str {
        match self {
            Channel::Shards => "agentdb_shards_changed",
            Channel::Nodes => "agentdb_nodes_changed",
            Channel::Tablets => "agentdb_tablets_changed",
            Channel::SessionLocators => "agentdb_session_locators_changed",
        }
    }
}

/// A single change event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEvent {
    pub channel: Channel,
    /// Primary key of the row that changed. Opaque to the
    /// consumer — cache layers refetch in bulk anyway.
    pub payload: String,
}

/// Subscribe to every change across the given channels.
///
/// Returns an `mpsc::Receiver<ChangeEvent>`. The underlying
/// connection is owned by a spawned task; dropping the
/// receiver drops the connection.
///
/// On Postgres: each channel gets a `LISTEN`, and the task
/// relays notifications as they arrive.
///
/// On SQLite: the channel is an inert receiver (never yields,
/// never closes). Callers fall back to their periodic refresh
/// loops for correctness.
pub fn subscribe(store: &MetaStore, channels: &[Channel]) -> mpsc::Receiver<ChangeEvent> {
    let (tx, rx) = mpsc::channel::<ChangeEvent>(64);
    match store.dialect() {
        MetaDialect::Postgres => {
            let pool = store
                .postgres_pool()
                .expect("postgres dialect without PgPool")
                .clone();
            let channels: Vec<Channel> = channels.to_vec();
            tokio::spawn(async move {
                run_pg_listen(pool, channels, tx).await;
            });
        }
        MetaDialect::Sqlite => {
            // No underlying mechanism — keep the sender
            // alive inside a sleeping task so the receiver
            // sees a never-yielding stream (rather than an
            // immediate closure).
            tokio::spawn(async move {
                let _keepalive = tx;
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            });
        }
    }
    rx
}

async fn run_pg_listen(
    pool: sqlx::postgres::PgPool,
    channels: Vec<Channel>,
    tx: mpsc::Sender<ChangeEvent>,
) {
    use sqlx::postgres::PgListener;
    let mut listener = match PgListener::connect_with(&pool).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, "change_notifier: connect_with failed; listener task exits");
            return;
        }
    };
    let names: Vec<&str> = channels.iter().map(|c| c.pg_name()).collect();
    if let Err(e) = listener.listen_all(names).await {
        tracing::warn!(error = %e, "change_notifier: listen_all failed");
        return;
    }
    let mut stream = listener.into_stream();
    while let Some(event) = stream.next().await {
        match event {
            Ok(notif) => {
                let channel_name = notif.channel();
                let channel = match channel_name {
                    "agentdb_shards_changed" => Channel::Shards,
                    "agentdb_nodes_changed" => Channel::Nodes,
                    "agentdb_tablets_changed" => Channel::Tablets,
                    "agentdb_session_locators_changed" => Channel::SessionLocators,
                    other => {
                        tracing::debug!(
                            channel = other,
                            "change_notifier: unexpected channel; ignoring"
                        );
                        continue;
                    }
                };
                let ev = ChangeEvent {
                    channel,
                    payload: notif.payload().to_string(),
                };
                if tx.send(ev).await.is_err() {
                    // Receiver dropped; nothing to do.
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "change_notifier: listen stream error; retrying");
                // Best-effort: sleep a beat then continue.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_backend_is_inert() {
        let store = MetaStore::open_sqlite(":memory:").await.unwrap();
        let mut rx = subscribe(&store, &[Channel::Shards, Channel::Nodes]);
        // No underlying mechanism; a short real wait + a
        // try_recv gives us enough to verify the stream
        // doesn't yield. `recv()` with a timeout would hang
        // forever without a real signal.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "sqlite stream must not yield");
    }

    #[test]
    fn channel_name_matches_trigger_convention() {
        assert_eq!(Channel::Shards.pg_name(), "agentdb_shards_changed");
        assert_eq!(Channel::Nodes.pg_name(), "agentdb_nodes_changed");
        assert_eq!(Channel::Tablets.pg_name(), "agentdb_tablets_changed");
        assert_eq!(
            Channel::SessionLocators.pg_name(),
            "agentdb_session_locators_changed"
        );
    }
}
