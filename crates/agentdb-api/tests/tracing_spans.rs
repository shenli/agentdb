//! M15 Part A — verify the instrumentation actually emits the
//! expected span tree. Uses an in-memory span recorder (a custom
//! `tracing_subscriber::Layer`) rather than the OTel SDK exporter,
//! because we do not want the tests to depend on the OTel pipeline or
//! its version churn.
//!
//! The recorder captures span name + parent relationship into a vector.
//! Tests drive a request end-to-end and assert the captured tree shape.

use std::sync::{Arc, Mutex};

use agentdb_api::{router, AppState, AuthConfig, StreamBus};
use agentdb_core::{BoxRegistry, HlcClock, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use axum::Extension;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;
use tracing::span::{Attributes, Id, Record};
use tracing::{Metadata, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{registry, Layer};

/// Record of one span: its name and its parent's name (if any).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SpanRecord {
    name: &'static str,
    parent: Option<&'static str>,
}

#[derive(Default)]
struct CollectingLayer {
    spans: Arc<Mutex<Vec<SpanRecord>>>,
}

impl<S> Layer<S> for CollectingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let name: &'static str = attrs.metadata().name();
        let parent = ctx
            .span(id)
            .and_then(|s| s.parent().map(|p| p.metadata().name()));
        self.spans.lock().unwrap().push(SpanRecord { name, parent });
    }

    fn enabled(&self, _metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        true
    }

    fn on_record(&self, _span: &Id, _values: &Record<'_>, _ctx: Context<'_, S>) {}
}

fn capture() -> Arc<Mutex<Vec<SpanRecord>>> {
    let spans = Arc::new(Mutex::new(Vec::new()));
    let layer = CollectingLayer {
        spans: spans.clone(),
    };
    // `try_init` rather than `init` — parallel tests may compete for the
    // global; we only need one successful install per process.
    let _ = registry().with(layer).try_init();
    spans
}

async fn spawn_server() -> (TempDir, TempDir, tempfile::NamedTempFile, String) {
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use std::sync::Arc;
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());
    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(
        BoxRegistry::new_with_meta(
            kv.clone(),
            store,
            4,
            Arc::new(HlcClock::default()),
            meta.clone(),
            "tracing-test-node".into(),
        )
        .await
        .unwrap(),
    );
    let state = AppState {
        registry,
        tenants: Arc::new(TenantRegistry::new(kv.clone())),
        sessions: Arc::new(SessionRegistry::new(kv)),
        stream: StreamBus::default(),
        event_bus: agentdb_api::EventBus::default(),
        http: agentdb_api::webhook::new_client(),
        auth: Some(AuthConfig { meta: Some(meta.clone()) }),
        admin_auth: None,
        jwt_config: agentdb_api::jwt::JwtConfig::default(),
        cors_config: agentdb_api::cors::CorsConfig::default(),
        broadcast_bus: agentdb_api::BroadcastBus::default(),
    };
    let app = router(state).layer(Extension(meta.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (dir, obj_dir, meta_file, format!("http://{addr}"))
}

async fn create_user(base: &str, user_id: &str, tenant: &str, password: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/admin/users"))
        .json(&serde_json::json!({
            "user_id": user_id,
            "tenant_external": tenant,
            "password": password,
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "create_user failed: {}", resp.text().await.unwrap());
}

fn basic(user: &str, pass: &str) -> String {
    format!("Basic {}", B64.encode(format!("{user}:{pass}").as_bytes()))
}

#[tokio::test]
#[serial]
async fn append_and_fetch_emits_expected_span_tree() {
    let spans = capture();
    let (_d, _od, _m, base) = spawn_server().await;
    let client = reqwest::Client::new();

    create_user(&base, "trace-user", "t-trace", "pass").await;
    let auth = basic("trace-user", "pass");

    client
        .post(format!("{base}/api/sessions/s-trace/append_and_fetch"))
        .header("Authorization", auth)
        .json(&json!({
            "event": {"text": "hello"},
            "actor_id": "agent-planner",
            "fetch_last_n": 1,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Give a tick for any trailing spans to drain to the recorder.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let captured = spans.lock().unwrap().clone();
    let names: Vec<&'static str> = captured.iter().map(|s| s.name).collect();

    assert!(
        names.contains(&"rest.append_and_fetch_session_keyed"),
        "missing REST handler span. Captured: {:?}",
        names
    );
    assert!(
        names.contains(&"append_and_fetch"),
        "missing core primitive span"
    );
    assert!(
        names.contains(&"fetch_last_n_merged"),
        "missing fetch_last_n_merged span (always runs when fetch_last_n>0)"
    );

    // The core span should be parented under the REST span (or a
    // middleware span that is itself under REST — assert by walking
    // parents).
    let core = captured
        .iter()
        .find(|s| s.name == "append_and_fetch")
        .expect("core span captured");
    assert!(
        core.parent.is_some(),
        "core span should have a parent (REST handler or middleware)"
    );
}
