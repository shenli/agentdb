//! Alias / external-key uniqueness tests for the V2 Runtime.
//!
//! Tenant external_key is global; agent + session aliases are tenant-scoped.
//! Conflicts surface as `ErrorCode::AliasConflict` (HTTP 409).

use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::errors::ErrorCode;
use agentdb_core::runtime::Runtime;
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{ContentBackend, InMemoryContentBackend, KvStore, RocksKvStore};
use tempfile::TempDir;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    Arc::new(RocksKvStore::open(path).expect("open rocks kv"))
}

fn in_memory_content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

#[tokio::test]
async fn create_tenant_rejects_duplicate_external_key() {
    let rt = Runtime::in_memory();
    rt.create_tenant(NewTenant {
        external_key: "acme".into(),
        policy: serde_json::json!({}),
    })
    .await
    .unwrap();
    let err = rt
        .create_tenant(NewTenant {
            external_key: "acme".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);
}

#[tokio::test]
async fn create_tenant_allows_empty_external_key_repeat() {
    // Empty external_key is treated as "unset" — many can coexist.
    let rt = Runtime::in_memory();
    rt.create_tenant(NewTenant {
        external_key: "".into(),
        policy: serde_json::json!({}),
    })
    .await
    .unwrap();
    rt.create_tenant(NewTenant {
        external_key: "".into(),
        policy: serde_json::json!({}),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn create_agent_rejects_duplicate_alias_within_tenant() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "t1".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    rt.create_agent(NewAgent {
        tenant_id: t.tenant_id.clone(),
        alias: Some("rep-1".into()),
        config: serde_json::json!({}),
    })
    .await
    .unwrap();
    let err = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id,
            alias: Some("rep-1".into()),
            config: serde_json::json!({}),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);
}

#[tokio::test]
async fn create_agent_allows_same_alias_across_tenants() {
    let rt = Runtime::in_memory();
    let t1 = rt
        .create_tenant(NewTenant {
            external_key: "t1".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let t2 = rt
        .create_tenant(NewTenant {
            external_key: "t2".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    rt.create_agent(NewAgent {
        tenant_id: t1.tenant_id,
        alias: Some("rep-1".into()),
        config: serde_json::json!({}),
    })
    .await
    .unwrap();
    rt.create_agent(NewAgent {
        tenant_id: t2.tenant_id,
        alias: Some("rep-1".into()),
        config: serde_json::json!({}),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn create_session_rejects_duplicate_alias_within_tenant() {
    let rt = Runtime::in_memory();
    let t = rt
        .create_tenant(NewTenant {
            external_key: "t1".into(),
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
    rt.create_session(NewSession {
        tenant_id: t.tenant_id.clone(),
        agent_id: a.agent_id.clone(),
        alias: Some("session-a".into()),
    })
    .await
    .unwrap();
    let err = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id,
            agent_id: a.agent_id,
            alias: Some("session-a".into()),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);
}

#[tokio::test]
async fn alias_uniqueness_survives_restart() {
    let dir = TempDir::new().unwrap();
    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv(kv, in_memory_content());
    let t = rt
        .create_tenant(NewTenant {
            external_key: "acme".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let tenant_id = t.tenant_id.clone();
    rt.create_agent(NewAgent {
        tenant_id: t.tenant_id,
        alias: Some("rep-1".into()),
        config: serde_json::json!({}),
    })
    .await
    .unwrap();
    drop(rt);

    // Reopen — alias maps must rehydrate from on-disk alias index rows.
    let kv2 = open_kv(dir.path());
    let rt2 = Runtime::with_kv(kv2, in_memory_content());
    let err = rt2
        .create_tenant(NewTenant {
            external_key: "acme".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::AliasConflict);

    // Agent alias also rehydrated: creating another agent under the same
    // tenant with alias "rep-1" must fail.
    let err2 = rt2
        .create_agent(NewAgent {
            tenant_id,
            alias: Some("rep-1".into()),
            config: serde_json::json!({}),
        })
        .await
        .unwrap_err();
    assert_eq!(err2.code, ErrorCode::AliasConflict);
}
