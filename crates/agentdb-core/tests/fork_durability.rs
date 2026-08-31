//! Fork follows the memory-follows-durable-commit rule: a fork whose persist
//! fails must leave no visible child session and must release its alias.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::runtime::{AppendRequest, ForkSessionRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{ContentBackend, InMemoryContentBackend, InMemoryKvStore, KvStore, WriteOp};

/// KV double that fails every write while armed; reads always succeed.
struct FailingWritesKv {
    inner: InMemoryKvStore,
    fail_writes: AtomicBool,
}

impl FailingWritesKv {
    fn err() -> agentdb_storage::Error {
        agentdb_storage::Error::Other("injected write failure".into())
    }
    fn check(&self) -> agentdb_storage::Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            Err(Self::err())
        } else {
            Ok(())
        }
    }
}

impl KvStore for FailingWritesKv {
    fn get(&self, key: &[u8]) -> agentdb_storage::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn put(&self, key: &[u8], value: &[u8]) -> agentdb_storage::Result<()> {
        self.check()?;
        self.inner.put(key, value)
    }
    fn delete(&self, key: &[u8]) -> agentdb_storage::Result<()> {
        self.check()?;
        self.inner.delete(key)
    }
    fn scan_prefix(&self, prefix: &[u8]) -> agentdb_storage::Result<agentdb_storage::KvIter> {
        self.inner.scan_prefix(prefix)
    }
    fn write_batch(&self, ops: Vec<WriteOp>) -> agentdb_storage::Result<()> {
        self.check()?;
        self.inner.write_batch(ops)
    }
    fn snapshot<'a>(&'a self) -> agentdb_storage::Result<Box<dyn agentdb_storage::KvSnapshot + 'a>> {
        self.inner.snapshot()
    }
    fn snapshot_owned(
        self: Arc<Self>,
    ) -> agentdb_storage::Result<Arc<dyn agentdb_storage::KvSnapshot>> {
        // The inner store's snapshot borrows; clone rows into an owned map.
        let rows: Vec<(Vec<u8>, Vec<u8>)> = self
            .inner
            .scan_prefix(b"")?
            .collect::<agentdb_storage::Result<_>>()?;
        let owned = InMemoryKvStore::default();
        for (k, v) in rows {
            owned.put(&k, &v)?;
        }
        Arc::new(owned).snapshot_owned()
    }
}

#[tokio::test]
async fn failed_fork_persist_leaves_no_child_and_releases_alias() {
    let kv = Arc::new(FailingWritesKv {
        inner: InMemoryKvStore::default(),
        fail_writes: AtomicBool::new(false),
    });
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv(kv.clone(), content);

    let t = rt
        .create_tenant(NewTenant {
            external_key: "fork-durability".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();
    rt.append(AppendRequest::simple(&t, &a, &s, "x", serde_json::json!({})))
        .await
        .unwrap();

    // Arm the fault: the fork's durable write fails, so no child may be
    // published and the alias must be reusable.
    kv.fail_writes.store(true, Ordering::SeqCst);
    let err = rt
        .fork_session(ForkSessionRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            at_seq: 0,
            alias: Some("fork-alias".into()),
        })
        .await
        .unwrap_err();
    assert!(err.message.contains("injected write failure"), "{err:?}");

    // Recovered: the same alias must be free again and the fork must succeed.
    kv.fail_writes.store(false, Ordering::SeqCst);
    let child = rt
        .fork_session(ForkSessionRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            at_seq: 0,
            alias: Some("fork-alias".into()),
        })
        .await
        .unwrap();
    assert_eq!(child.alias.as_deref(), Some("fork-alias"));

    // Exactly one child exists: replay of the child sees parent history.
    let replay = rt
        .replay_session(&t.tenant_id, &child.session_id)
        .await
        .unwrap();
    assert_eq!(replay.events.len(), 1);
}
