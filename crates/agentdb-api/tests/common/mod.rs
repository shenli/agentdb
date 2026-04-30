//! Shared integration-test harness for the V2.0 HTTP routes.
//!
//! Spawns the router on a random TCP port backed by an in-memory `Runtime`
//! and hands back a `reqwest::Client` plus the base URL. The graceful-shutdown
//! channel is held by `TestApp`; dropping it lets axum stop the listener so we
//! don't leak tasks across tests.

use agentdb_api::{build_router, AppState};
use agentdb_core::Runtime;
use tokio::net::TcpListener;

pub struct TestApp {
    pub base: String,
    pub client: reqwest::Client,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

pub async fn spawn_app() -> TestApp {
    let runtime = Runtime::in_memory();
    let state = AppState::new(runtime);
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
        _shutdown: tx,
    }
}
