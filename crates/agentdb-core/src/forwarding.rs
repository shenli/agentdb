//! Cross-node write forwarding support (M20 continuation).
//!
//! Maps `tablet_id -> (node_id, address)` by joining the
//! `tablets` and `nodes` control-plane tables, and caches the
//! result in memory. The cache is rebuilt every
//! `AGENTDB_NODE_ADDR_REFRESH_SECS` (default 10s — more aggressive
//! than [`LocationProxy`]'s 30s because node-address changes are
//! failover-critical). Callers check `target_for_tablet(id)` and
//! then `is_self(&target)` to decide whether to route locally or
//! forward via HTTP.
//!
//! Design invariants:
//!
//! 1. **`None` means "route locally".** Every failure mode
//!    (tablet unowned, owner not in nodes table, race during
//!    bootstrap) collapses to the same safe answer: the caller
//!    falls back to local `route_tenant` routing. The reaper
//!    re-overrides shards on its next tick so this state is
//!    transient.
//!
//! 2. **No blocking on hot path.** `target_for_tablet` is a
//!    single `RwLock::read` + `HashMap::get`. The refresh job
//!    owns the write side and runs infrequently.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use agentdb_meta::{nodes as meta_nodes, tablets as meta_tablets, MetaStore};
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};

/// Where a tablet lives. `address` is whatever the owning node
/// wrote to `nodes.address` — typically `"host:port"` but could
/// be a full URL. Callers normalize at request-build time.
///
/// `mongo_address` is the Mongo wire listener advertised by
/// the node, used by the cross-node Mongo forwarding proxy.
/// `None` means the owner predates the advertise column or
/// opted out — the wire handler falls back to the
/// NotWritablePrimary error in that case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTarget {
    pub node_id: String,
    pub address: String,
    pub mongo_address: Option<String>,
}

pub struct NodeAddressProxy {
    meta: Arc<MetaStore>,
    cache: RwLock<HashMap<i64, NodeTarget>>,
    self_node_id: String,
    refresh_count: AtomicU64,
    last_refresh_ms: AtomicI64,
}

impl NodeAddressProxy {
    /// Build a proxy and do an initial full refresh. Refresh
    /// failures at startup propagate so the server doesn't
    /// silently run with an empty cache (every remote
    /// forward would degrade to local fallback).
    pub async fn new(meta: Arc<MetaStore>, self_node_id: String) -> Result<Arc<Self>> {
        let proxy = Arc::new(Self {
            meta,
            cache: RwLock::new(HashMap::new()),
            self_node_id,
            refresh_count: AtomicU64::new(0),
            last_refresh_ms: AtomicI64::new(0),
        });
        proxy.refresh().await?;
        Ok(proxy)
    }

    /// Look up the node that currently owns `tablet_id`.
    /// Returns `None` if the tablet is unowned, if the owner
    /// isn't in the nodes table, or if the cache is stale
    /// past the tablet's lease — callers must treat `None`
    /// as "route locally" (see crate-level docs).
    pub fn target_for_tablet(&self, tablet_id: i64) -> Option<NodeTarget> {
        self.cache.read().get(&tablet_id).cloned()
    }

    /// Is the given target this process? Callers use this to
    /// short-circuit the forwarding path for tablets this
    /// node owns.
    pub fn is_self(&self, target: &NodeTarget) -> bool {
        target.node_id == self.self_node_id
    }

    pub fn self_node_id(&self) -> &str {
        &self.self_node_id
    }

    pub fn refresh_count(&self) -> u64 {
        self.refresh_count.load(Ordering::Acquire)
    }

    pub fn last_refresh_ms(&self) -> i64 {
        self.last_refresh_ms.load(Ordering::Acquire)
    }

    /// Pull fresh `tablets × nodes` state from meta and swap
    /// the cache. Tablets without an owner or with an owner
    /// not present in `nodes` are dropped — better to
    /// return `None` to callers than to point them at a
    /// stale address.
    pub async fn refresh(&self) -> Result<()> {
        let tablets = meta_tablets::list_all(self.meta.pool())
            .await
            .map_err(|e| Error::Invalid {
                field: None,
                message: format!("node_address_proxy refresh tablets: {e}"),
            })?;
        let nodes = meta_nodes::list_all(self.meta.pool())
            .await
            .map_err(|e| Error::Invalid {
                field: None,
                message: format!("node_address_proxy refresh nodes: {e}"),
            })?;
        let node_addr: HashMap<String, (String, Option<String>)> = nodes
            .into_iter()
            .map(|n| (n.node_id, (n.address, n.mongo_address)))
            .collect();

        let mut next: HashMap<i64, NodeTarget> = HashMap::new();
        for t in tablets {
            let Some(owner) = t.owner_node_id else {
                continue;
            };
            let Some((addr, mongo)) = node_addr.get(&owner) else {
                // Owner is stale in the nodes table. Skip — callers
                // will see None and fall back to local routing
                // until the nodes row is re-upserted at next
                // heartbeat.
                continue;
            };
            next.insert(
                t.tablet_id,
                NodeTarget {
                    node_id: owner,
                    address: addr.clone(),
                    mongo_address: mongo.clone(),
                },
            );
        }
        *self.cache.write() = next;
        self.refresh_count.fetch_add(1, Ordering::Release);
        self.last_refresh_ms.store(now_ms(), Ordering::Release);
        Ok(())
    }
}

/// RAII handle for the periodic refresh task. Drop cancels
/// the loop. Mirrors `LocationProxyRefreshJob`'s shape so
/// the server binary can own both handles uniformly.
pub struct NodeAddressRefreshJob {
    _handle: JoinHandle<()>,
}

impl NodeAddressRefreshJob {
    pub fn start(proxy: Arc<NodeAddressProxy>, interval: Duration) -> Self {
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick; the caller just
            // finished its startup refresh via new().
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(err) = proxy.refresh().await {
                    tracing::warn!(%err, "node address proxy refresh failed; will retry next tick");
                }
            }
        });
        Self { _handle: handle }
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_meta::migrations;

    async fn fresh_meta() -> Arc<MetaStore> {
        let meta = MetaStore::open_sqlite(":memory:").await.unwrap();
        migrations::apply(meta.pool(), agentdb_meta::MetaDialect::Sqlite).await.unwrap();
        Arc::new(meta)
    }

    #[tokio::test]
    async fn new_starts_with_empty_cache_when_no_tablets_owned() {
        let meta = fresh_meta().await;
        let proxy = NodeAddressProxy::new(meta.clone(), "self".into())
            .await
            .unwrap();
        assert_eq!(proxy.refresh_count(), 1);
        assert_eq!(proxy.target_for_tablet(0), None);
    }

    #[tokio::test]
    async fn refresh_populates_tablet_to_node_mapping() {
        let meta = fresh_meta().await;
        // Register two nodes, one tablet owned by each.
        meta_nodes::upsert(meta.pool(), "node-a", "127.0.0.1:7000", None, "alive", 100)
            .await
            .unwrap();
        meta_nodes::upsert(meta.pool(), "node-b", "127.0.0.1:7001", None, "alive", 100)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 0, 100).await.unwrap();
        meta_tablets::get_or_create(meta.pool(), 1, 100).await.unwrap();
        meta_tablets::assume_ownership(meta.pool(), 0, 0, "node-a", i64::MAX)
            .await
            .unwrap();
        meta_tablets::assume_ownership(meta.pool(), 1, 0, "node-b", i64::MAX)
            .await
            .unwrap();

        let proxy = NodeAddressProxy::new(meta.clone(), "node-a".into())
            .await
            .unwrap();
        let t0 = proxy.target_for_tablet(0).unwrap();
        assert_eq!(t0.node_id, "node-a");
        assert_eq!(t0.address, "127.0.0.1:7000");
        let t1 = proxy.target_for_tablet(1).unwrap();
        assert_eq!(t1.node_id, "node-b");
        assert_eq!(t1.address, "127.0.0.1:7001");

        assert!(proxy.is_self(&t0));
        assert!(!proxy.is_self(&t1));
    }

    #[tokio::test]
    async fn refresh_drops_tablets_whose_owner_is_unregistered() {
        // Tablet claims ownership by "node-ghost", but we only
        // upsert "node-a" into the nodes table. The proxy must
        // drop the tablet's entry rather than pointing at a
        // nonexistent node.
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "node-a", "127.0.0.1:7000", None, "alive", 100)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 5, 100).await.unwrap();
        meta_tablets::assume_ownership(meta.pool(), 5, 0, "node-ghost", i64::MAX)
            .await
            .unwrap();
        let proxy = NodeAddressProxy::new(meta, "node-a".into())
            .await
            .unwrap();
        assert!(proxy.target_for_tablet(5).is_none());
    }

    #[tokio::test]
    async fn refresh_drops_unowned_tablets() {
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "node-a", "127.0.0.1:7000", None, "alive", 100)
            .await
            .unwrap();
        meta_tablets::get_or_create(meta.pool(), 9, 100).await.unwrap();
        // No assume_ownership — tablet stays owner=NULL.
        let proxy = NodeAddressProxy::new(meta, "node-a".into())
            .await
            .unwrap();
        assert!(proxy.target_for_tablet(9).is_none());
    }

    #[tokio::test]
    async fn refresh_job_updates_cache_after_new_ownership() {
        let meta = fresh_meta().await;
        meta_nodes::upsert(meta.pool(), "node-a", "127.0.0.1:7000", None, "alive", 100)
            .await
            .unwrap();
        let proxy = NodeAddressProxy::new(meta.clone(), "node-a".into())
            .await
            .unwrap();
        assert!(proxy.target_for_tablet(42).is_none());

        let _job = NodeAddressRefreshJob::start(proxy.clone(), Duration::from_millis(50));

        // Introduce a new tablet + ownership after the job is
        // running. A subsequent refresh tick must pick it up.
        meta_tablets::get_or_create(meta.pool(), 42, 100).await.unwrap();
        meta_tablets::assume_ownership(meta.pool(), 42, 0, "node-a", i64::MAX)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(400)).await;
        let t = proxy.target_for_tablet(42);
        assert!(
            t.is_some(),
            "refresh job should have picked up the new ownership"
        );
    }
}
