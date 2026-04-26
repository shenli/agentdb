//! Shared harness for pub/sub-flavored integration tests.
//!
//! Spins up an in-process axum server backed by RocksDB tempdirs.
//! Exposes the live `AppState` so tests can subscribe to the
//! `EventBus` directly while issuing HTTP requests against the
//! same server. Pattern mirrors the per-file `spawn()` helpers
//! used elsewhere in this crate (e.g. `http.rs`); this version
//! is centralized so the pub/sub tests don't have to reinvent it.
//!
//! Not exported from the crate — `tests/common/mod.rs` is only
//! visible to integration test binaries that declare `mod common;`.

#![allow(dead_code)]

use std::sync::Arc;

use agentdb_api::{router, AppState, EventBus, StreamBus};
use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_storage::{KvStore, RocksKvStore};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

/// Live in-process server for an integration test. The `state`
/// field is the same `AppState` the router was built with — tests
/// can call `state.event_bus.subscribe()` and observe envelopes
/// emitted by handler invocations on this server.
pub struct TestServer {
    pub state: AppState,
    pub base: String,
    /// Held until drop so RocksDB + object-store dirs survive
    /// for the test's lifetime. `_` prefixed because tests don't
    /// touch them directly.
    _kv_dir: TempDir,
    _obj_dir: TempDir,
}

impl TestServer {
    pub async fn start() -> Self {
        let kv_dir = TempDir::new().expect("kv tempdir");
        let obj_dir = TempDir::new().expect("obj tempdir");
        let kv: Arc<dyn KvStore> = Arc::new(
            RocksKvStore::open(kv_dir.path()).expect("rocks open"),
        );
        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(obj_dir.path()).expect("local fs"),
        );
        let state = AppState {
            registry: Arc::new(BoxRegistry::new(kv.clone(), store, 4).expect("registry")),
            tenants: Arc::new(TenantRegistry::new(kv.clone())),
            sessions: Arc::new(SessionRegistry::new(kv)),
            stream: StreamBus::default(),
            event_bus: EventBus::default(),
            http: agentdb_api::webhook::new_client(),
            auth: None,
            admin_auth: None,
        };
        let app = router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        Self {
            state,
            base: format!("http://{addr}"),
            _kv_dir: kv_dir,
            _obj_dir: obj_dir,
        }
    }

    /// POST a JSON body to a path on the running server. Panics
    /// on transport errors so tests' assertion lines stay focused
    /// on the response, not the plumbing.
    pub async fn post_json(&self, path: &str, body: serde_json::Value) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}{}", self.base, path))
            .json(&body)
            .send()
            .await
            .expect("send")
    }
}
