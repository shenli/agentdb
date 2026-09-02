//! Shared integration-test harness for the V2.0 HTTP routes.
//!
//! Spawns the router on a random TCP port backed by an in-memory `Runtime`
//! and hands back a `reqwest::Client` plus the base URL. The graceful-shutdown
//! channel is held by `TestApp`; dropping it lets axum stop the listener so we
//! don't leak tasks across tests.

use agentdb_api::{build_router, AppState};
use agentdb_core::Runtime;
use std::sync::Arc;
use tokio::net::TcpListener;

pub struct TestApp {
    pub base: String,
    pub client: reqwest::Client,
    #[allow(dead_code)]
    pub runtime: Arc<Runtime>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

#[allow(dead_code)]
pub async fn spawn_app() -> TestApp {
    let runtime = Runtime::in_memory();
    // Plain single-node harness: cluster/distributed routes are not mounted.
    spawn_raw(AppState::new(runtime)).await
}

/// Advanced harness for tests that inject custom state. These exercise the
/// cluster / distributed / admin surfaces, so the app runs in cluster mode and
/// mounts the full route set. Use `spawn_raw` directly to test single-node
/// route gating with a custom state.
#[allow(dead_code)]
pub async fn spawn_app_with_state(state: AppState) -> TestApp {
    spawn_raw(state.with_cluster_mode(true)).await
}

#[allow(dead_code)]
pub async fn spawn_raw(state: AppState) -> TestApp {
    let runtime = state.runtime.clone();
    let router = build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });

    TestApp {
        base: format!("http://{addr}"),
        client: reqwest::Client::new(),
        runtime,
        _shutdown: tx,
    }
}
