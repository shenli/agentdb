//! Encryption-at-rest M5 — encryption precedes the Raft log (design stage 5,
//! "Verify Raft and snapshot ordering"). A replicated runtime with the keyring
//! enabled must seal an event value *before* it enters the replicated log, so
//! the log (and any tablet snapshot built from it) holds ciphertext, never
//! plaintext.

use std::collections::BTreeMap;
use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;
use agentdb_core::RUNTIME_PRIMARY_TABLET_ID;
use agentdb_storage::{
    AeadTenantKeyring, ContentBackend, DekCacheConfig, EnvelopeCrypto, InMemoryContentBackend,
    InMemoryKvStore, KvStore, LocalKeyProvider, ReplicatedLog, SingleNodeReplicatedLog, Suite,
};

#[tokio::test]
async fn events_are_ciphertext_in_the_replicated_log() {
    // One KV backs both the replicated log entries and the applied tablet
    // state, so a single scan covers both.
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
    let keyring: Arc<dyn EnvelopeCrypto> = Arc::new(AeadTenantKeyring::new(
        Arc::new(LocalKeyProvider::from_secret(b"raft-ordering-root")),
        Suite::XChaCha20Poly1305,
        DekCacheConfig::default(),
    ));
    let rt = Runtime::with_kv_content_keyring_and_replicated_logs(
        kv.clone(),
        Arc::new(InMemoryContentBackend::default()) as Arc<dyn ContentBackend>,
        keyring,
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]),
    );

    let t = rt
        .create_tenant(NewTenant {
            external_key: "raft".into(),
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

    const MARKER: &str = "RAFT-EVENT-MARKER-77c4";
    rt.append(AppendRequest::simple(
        &t,
        &a,
        &s,
        "message",
        serde_json::json!({ "text": MARKER }),
    ))
    .await
    .unwrap();

    // The marker must not appear anywhere in the KV — neither the replicated
    // log entries nor the applied state — proving the event was sealed before
    // it entered the log.
    let leaked = kv.scan_prefix(b"").unwrap().any(|r| {
        let (_k, v) = r.unwrap();
        v.windows(MARKER.len()).any(|w| w == MARKER.as_bytes())
    });
    assert!(
        !leaked,
        "event plaintext appears in the replicated log or applied state"
    );

    // And it still round-trips through the raft apply path.
    let events = rt
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["text"], MARKER);
}
