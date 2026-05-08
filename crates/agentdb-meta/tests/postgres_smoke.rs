//! Postgres smoke tests. Gated on `AGENTDB_TEST_PG_URL` so
//! CI / local workflows that don't run a Postgres instance
//! silently skip this file. Set the env to a
//! `postgres://user:pass@host/db` URL pointing at a throwaway
//! database (the tests drop+recreate every schema object on
//! each run via migrations plus explicit cleanup).
//!
//! What each test verifies:
//!
//! 1. Migrations apply cleanly against a fresh DB.
//! 2. A re-open is idempotent (no duplicate schema rows).
//! 3. Core CRUD paths round-trip (admin_tokens, nodes).
//! 4. LISTEN/NOTIFY: writes to `nodes` and tablet replicas fire
//!    their expected change events.

use std::time::Duration;

use agentdb_meta::change_notifier::{subscribe, Channel};
use agentdb_meta::{admin_tokens, nodes, session_locators, tablet_replicas, tenants, MetaStore};

fn pg_url() -> Option<String> {
    std::env::var("AGENTDB_TEST_PG_URL").ok()
}

async fn cleanup_schema(url: &str) {
    // Drop every relation the migrations create so the next
    // test run starts from a clean slate. One statement per
    // line — executed against a short-lived direct PgPool to
    // keep the main test path uncluttered.
    let pool = sqlx::postgres::PgPool::connect(url).await.unwrap();
    let drop_sql = [
        "DROP TRIGGER IF EXISTS trg_agentdb_shards_notify ON shards",
        "DROP TRIGGER IF EXISTS trg_agentdb_nodes_notify ON nodes",
        "DROP TRIGGER IF EXISTS trg_agentdb_tablets_notify ON tablets",
        "DROP TRIGGER IF EXISTS trg_agentdb_tablet_replicas_notify ON tablet_replicas",
        "DROP TRIGGER IF EXISTS trg_agentdb_session_locators_notify ON session_locators",
        "DROP FUNCTION IF EXISTS agentdb_notify_shards()",
        "DROP FUNCTION IF EXISTS agentdb_notify_nodes()",
        "DROP FUNCTION IF EXISTS agentdb_notify_tablets()",
        "DROP FUNCTION IF EXISTS agentdb_notify_tablet_replicas()",
        "DROP FUNCTION IF EXISTS agentdb_notify_session_locators()",
        "DROP TABLE IF EXISTS tenant_quotas CASCADE",
        "DROP TABLE IF EXISTS admin_audit CASCADE",
        "DROP TABLE IF EXISTS admin_tokens CASCADE",
        "DROP TABLE IF EXISTS users CASCADE",
        "DROP TABLE IF EXISTS config CASCADE",
        "DROP TABLE IF EXISTS nodes CASCADE",
        "DROP TABLE IF EXISTS session_locators CASCADE",
        "DROP TABLE IF EXISTS session_overrides CASCADE",
        "DROP TABLE IF EXISTS shards CASCADE",
        "DROP TABLE IF EXISTS tablet_movements CASCADE",
        "DROP TABLE IF EXISTS tablet_replicas CASCADE",
        "DROP TABLE IF EXISTS tablets CASCADE",
        "DROP TABLE IF EXISTS tenants CASCADE",
        "DROP TABLE IF EXISTS scheduler_leader CASCADE",
        "DROP TABLE IF EXISTS schema_migrations CASCADE",
    ];
    for sql in drop_sql {
        // Ignore errors — dropping objects that don't exist
        // is the point of this fixture.
        let _ = sqlx::query(sql).execute(&pool).await;
    }
}

#[tokio::test]
async fn pg_migrations_apply_cleanly() {
    let Some(url) = pg_url() else {
        eprintln!("AGENTDB_TEST_PG_URL unset; skipping pg smoke test");
        return;
    };
    cleanup_schema(&url).await;
    let store = MetaStore::open_postgres(&url).await.unwrap();

    // Every expected table exists.
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables \
           WHERE table_schema = current_schema() ORDER BY table_name",
    )
    .fetch_all(store.pool())
    .await
    .unwrap();
    let names: Vec<String> = rows.into_iter().map(|(n,)| n).collect();
    for expected in [
        "admin_audit",
        "admin_tokens",
        "config",
        "nodes",
        "schema_migrations",
        "session_locators",
        "scheduler_leader",
        "session_overrides",
        "shards",
        "tablet_movements",
        "tablets",
        "tenant_quotas",
        "tenants",
        "users",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "expected table {expected}; got {names:?}"
        );
    }

    cleanup_schema(&url).await;
}

#[tokio::test]
async fn pg_reopen_is_idempotent() {
    let Some(url) = pg_url() else {
        return;
    };
    cleanup_schema(&url).await;
    let _a = MetaStore::open_postgres(&url).await.unwrap();
    let _b = MetaStore::open_postgres(&url).await.unwrap();
    let c = MetaStore::open_postgres(&url).await.unwrap();
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM schema_migrations")
        .fetch_one(c.pool())
        .await
        .unwrap();
    // At least 9 migrations (we have 001-009); the exact count
    // is checked via this lower bound so future migrations
    // don't force a test update.
    assert!(n >= 9, "got {n}");
    cleanup_schema(&url).await;
}

#[tokio::test]
async fn pg_admin_token_roundtrip() {
    let Some(url) = pg_url() else {
        return;
    };
    cleanup_schema(&url).await;
    let store = MetaStore::open_postgres(&url).await.unwrap();
    let minted = admin_tokens::mint(store.pool(), "alice", admin_tokens::AdminScope::Admin, 1000)
        .await
        .unwrap();
    let v = admin_tokens::verify(store.pool(), &minted.plaintext)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.0, minted.token_id);
    assert_eq!(v.1, admin_tokens::AdminScope::Admin);

    let active = admin_tokens::list_active(store.pool()).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].description, "alice");
    cleanup_schema(&url).await;
}

#[tokio::test]
async fn pg_listen_notify_delivers_a_node_change() {
    let Some(url) = pg_url() else {
        return;
    };
    cleanup_schema(&url).await;
    let store = MetaStore::open_postgres(&url).await.unwrap();
    let mut rx = subscribe(&store, &[Channel::Nodes]);
    // Give the listener loop a moment to set up LISTEN
    // before we fire the write.
    tokio::time::sleep(Duration::from_millis(150)).await;

    nodes::upsert(
        store.pool(),
        "pg-notify-test",
        "127.0.0.1:1",
        Some("127.0.0.1:2"),
        "alive",
        10,
        None,
        None,
    )
    .await
    .unwrap();

    let evt = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("no NOTIFY delivered within 3s")
        .expect("sender closed");
    assert_eq!(evt.channel, Channel::Nodes);
    assert_eq!(evt.payload, "pg-notify-test");

    cleanup_schema(&url).await;
}

#[tokio::test]
async fn pg_listen_notify_delivers_a_session_locator_change() {
    let Some(url) = pg_url() else {
        return;
    };
    cleanup_schema(&url).await;
    let store = MetaStore::open_postgres(&url).await.unwrap();
    let tenant = tenants::get_or_create(store.pool(), "ten_1", 10)
        .await
        .unwrap();
    let mut rx = subscribe(&store, &[Channel::SessionLocators]);
    tokio::time::sleep(Duration::from_millis(150)).await;

    session_locators::upsert(
        store.pool(),
        "pg-session-notify-test",
        tenant.internal_id,
        "ten_1",
        Some(7),
        0,
        10,
    )
    .await
    .unwrap();

    let evt = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("no NOTIFY delivered within 3s")
        .expect("sender closed");
    assert_eq!(evt.channel, Channel::SessionLocators);
    assert_eq!(evt.payload, "pg-session-notify-test");

    cleanup_schema(&url).await;
}

#[tokio::test]
async fn pg_listen_notify_delivers_a_tablet_replica_change() {
    let Some(url) = pg_url() else {
        return;
    };
    cleanup_schema(&url).await;
    let store = MetaStore::open_postgres(&url).await.unwrap();
    let mut rx = subscribe(&store, &[Channel::Tablets]);
    tokio::time::sleep(Duration::from_millis(150)).await;

    tablet_replicas::upsert(
        store.pool(),
        7,
        "pg-replica-notify-test",
        "learner",
        "lagging",
        0,
        0,
        10,
    )
    .await
    .unwrap();

    let evt = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("no NOTIFY delivered within 3s")
        .expect("sender closed");
    assert_eq!(evt.channel, Channel::Tablets);
    assert_eq!(evt.payload, "7");

    cleanup_schema(&url).await;
}
