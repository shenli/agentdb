//! M19: Mongo wire SASL end-to-end.
//!
//! Uses the real `mongodb` Rust driver so we exercise the actual
//! SCRAM-SHA-256 handshake bytes, not a hand-crafted mock. The
//! server runs with `require_auth=true`, meaning:
//!
//! - connections that auth successfully can run `insert`/`find`
//! - connections that skip auth are rejected on the first non-SASL
//!   command
//! - a valid user whose `$db` != their tenant is rejected with
//!   code 13 (Unauthorized)

use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use agentdb_meta::MetaStore;
use agentdb_storage::{KvStore, RocksKvStore};
use agentdb_wire::{serve, WireAuthConfig, WireState};
use bson::doc;
use mongodb::options::{ClientOptions, Credential};
use mongodb::Client;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

async fn spawn_with_auth() -> (
    TempDir,
    TempDir,
    tempfile::NamedTempFile,
    u16,
    Arc<MetaStore>,
) {
    let dir = TempDir::new().unwrap();
    let obj_dir = TempDir::new().unwrap();
    let meta_file = tempfile::NamedTempFile::new().unwrap();
    let meta_path = meta_file.path().to_string_lossy().to_string();
    let meta = Arc::new(MetaStore::open_sqlite(&meta_path).await.unwrap());

    let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(obj_dir.path()).unwrap());
    let registry = Arc::new(BoxRegistry::new(kv.clone(), store, 4).unwrap());
    let tenants = Arc::new(TenantRegistry::new(kv.clone()));
    let sessions = Arc::new(SessionRegistry::new(kv));

    let state = WireState::with_auth(
        registry,
        tenants,
        sessions,
        WireAuthConfig {
            meta: meta.clone(),
            require_auth: true,
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = serve(listener, state).await;
    });
    (dir, obj_dir, meta_file, port, meta)
}

/// Build a MongoDB client with SCRAM-SHA-256 credentials for
/// `user:password` on the `admin` auth source (the standard
/// pre-X.509 Mongo convention).
async fn auth_client(port: u16, user: &str, password: &str) -> Client {
    let mut opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .await
        .unwrap();
    opts.credential = Some(
        Credential::builder()
            .username(user.to_string())
            .password(password.to_string())
            .mechanism(mongodb::options::AuthMechanism::ScramSha256)
            .source("admin".to_string())
            .build(),
    );
    // Keep the driver from retrying forever against an auth error.
    opts.server_selection_timeout = Some(std::time::Duration::from_secs(3));
    Client::with_options(opts).unwrap()
}

#[tokio::test]
async fn scram_handshake_completes_and_enables_inserts() {
    let (_d, _od, _m, port, meta) = spawn_with_auth().await;
    agentdb_meta::users::create(meta.pool(), "alice", "t-alice", "pencil", 4096, 100)
        .await
        .unwrap();

    let client = auth_client(port, "alice", "pencil").await;
    // An insert on t-alice should succeed.
    let coll = client
        .database("t-alice")
        .collection::<bson::Document>("events");
    coll.insert_one(doc! { "session_id": "s-1", "payload": "hi" })
        .await
        .expect("insert after successful SASL must succeed");
}

#[tokio::test]
async fn scram_with_wrong_password_fails() {
    let (_d, _od, _m, port, meta) = spawn_with_auth().await;
    agentdb_meta::users::create(meta.pool(), "bob", "t-bob", "pencil", 4096, 100)
        .await
        .unwrap();

    let client = auth_client(port, "bob", "wrong").await;
    let coll = client
        .database("t-bob")
        .collection::<bson::Document>("events");
    let err = coll
        .insert_one(doc! { "session_id": "s-1", "payload": "hi" })
        .await
        .expect_err("wrong password must fail");
    let msg = err.to_string();
    // The driver maps our CODE_AUTH_FAILED=18 into its Authentication kind.
    assert!(
        msg.to_lowercase().contains("auth") || msg.contains("18"),
        "expected auth-failed error, got: {msg}"
    );
}

#[tokio::test]
async fn scram_cross_tenant_db_is_forbidden() {
    let (_d, _od, _m, port, meta) = spawn_with_auth().await;
    // alice belongs to t-alice; she tries to insert into t-bob.
    agentdb_meta::users::create(meta.pool(), "alice", "t-alice", "pencil", 4096, 100)
        .await
        .unwrap();

    let client = auth_client(port, "alice", "pencil").await;
    let coll = client
        .database("t-bob")
        .collection::<bson::Document>("events");
    let err = coll
        .insert_one(doc! { "session_id": "s-1", "payload": "x" })
        .await
        .expect_err("cross-tenant access must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("cannot access") || msg.contains("13"),
        "expected Unauthorized (code 13), got: {msg}"
    );
}
