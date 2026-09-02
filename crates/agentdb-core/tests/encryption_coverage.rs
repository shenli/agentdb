//! Encryption-at-rest raw-storage marker coverage map (design "Proposed Code
//! Changes" §4 / delivery-plan stage 5). Writes a unique secret marker through
//! every durable path and asserts, per path, whether that marker appears in the
//! raw bytes on disk.
//!
//! This is the regression detector the design calls for. Every durable path
//! whose payload is sealed at rest must show NO marker in raw storage — a leak
//! is a regression. As of M17 that includes events, artifact content, and the
//! agent/record/skill primary records (M17). Any still-plaintext path added
//! here should assert `marker_in` as a tripwire, so encrypting it later forces
//! this coverage map to be updated deliberately.

use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::artifact::NewArtifact;
use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::skill::NewSkill;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{
    AeadTenantKeyring, ContentBackend, DekCacheConfig, EnvelopeCrypto, InMemoryContentBackend,
    InMemoryKvStore, KvStore, LocalKeyProvider, Suite,
};

fn envelope_keyring() -> Arc<dyn EnvelopeCrypto> {
    Arc::new(AeadTenantKeyring::new(
        Arc::new(LocalKeyProvider::from_secret(b"coverage-root")),
        Suite::XChaCha20Poly1305,
        DekCacheConfig::default(),
    ))
}

fn marker_in(values: &[Vec<u8>], marker: &str) -> bool {
    let m = marker.as_bytes();
    values.iter().any(|v| v.windows(m.len()).any(|w| w == m))
}

#[tokio::test]
async fn raw_storage_marker_coverage_map() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content.clone() as Arc<dyn ContentBackend>,
        envelope_keyring(),
    );

    // Distinct marker per durable path.
    const EVENT_MARKER: &str = "COVERAGE-EVENT-1a";
    const ARTIFACT_MARKER: &str = "COVERAGE-ARTIFACT-2b";
    const AGENT_MARKER: &str = "COVERAGE-AGENT-3c";
    const RECORD_MARKER: &str = "COVERAGE-RECORD-4d";
    const SKILL_MARKER: &str = "COVERAGE-SKILL-5e";
    const TENANT_MARKER: &str = "COVERAGE-TENANT-6f";

    let t = rt
        .create_tenant(NewTenant {
            external_key: "coverage".into(),
            policy: serde_json::json!({ "secret": TENANT_MARKER }),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: Some("agent".into()),
            config: serde_json::json!({ "secret": AGENT_MARKER }),
        })
        .await
        .unwrap();
    let s = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: Some("session".into()),
        })
        .await
        .unwrap();
    rt.append(AppendRequest::simple(
        &t,
        &a,
        &s,
        "message",
        serde_json::json!({ "text": EVENT_MARKER }),
    ))
    .await
    .unwrap();
    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            format!("blob {ARTIFACT_MARKER} end").into_bytes(),
        )
        .await
        .unwrap();
    rt.write_record(NewRecord {
        tenant_id: t.tenant_id.clone(),
        external_id: None,
        namespace: vec!["n".into()],
        scope: serde_json::json!({}),
        text: Some(RECORD_MARKER.into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    })
    .await
    .unwrap();
    rt.register_skill(
        NewSkill {
            tenant_id: t.tenant_id.clone(),
            name: "skill".into(),
            version: "1.0.0".into(),
            manifest: serde_json::json!({ "secret": SKILL_MARKER }),
            artifact_refs: vec![],
        },
        None,
    )
    .await
    .unwrap();

    let kv_values: Vec<Vec<u8>> = kv.scan_prefix(b"").unwrap().map(|r| r.unwrap().1).collect();
    let raw_artifact = content
        .get(t.tenant_id.as_str(), art.artifact_id.as_str())
        .await
        .unwrap();

    // --- Encrypted paths: a marker here would be a regression. ---
    assert!(
        !marker_in(&kv_values, EVENT_MARKER),
        "event payload leaked into a raw KV value (M3 regression)"
    );
    assert!(
        !raw_artifact
            .windows(ARTIFACT_MARKER.len())
            .any(|w| w == ARTIFACT_MARKER.as_bytes()),
        "artifact content leaked into raw content storage (M2 regression)"
    );

    // --- Primary records encrypted in M17: a marker here would be a regression.
    //     (Their export-ledger copies were already encrypted earlier.) ---
    assert!(
        !marker_in(&kv_values, AGENT_MARKER),
        "agent config leaked into a raw KV value (M17 regression)"
    );
    assert!(
        !marker_in(&kv_values, RECORD_MARKER),
        "searchable record leaked into a raw KV value (M17 regression)"
    );
    assert!(
        !marker_in(&kv_values, SKILL_MARKER),
        "skill manifest leaked into a raw KV value (M17 regression)"
    );
    assert!(
        !marker_in(&kv_values, TENANT_MARKER),
        "tenant policy leaked into a raw KV value (M18 regression)"
    );
}

/// The M17 primary-record encryption must round-trip through a *fresh* runtime
/// that hydrates by scanning + decrypting the sealed rows — the read side of the
/// coverage above. A second runtime over the same store with the same keyring
/// reconstructs every agent/record/skill and its secret payload.
#[tokio::test]
async fn primary_records_round_trip_through_hydration() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content.clone() as Arc<dyn ContentBackend>,
        envelope_keyring(),
    );

    let t = rt
        .create_tenant(NewTenant {
            external_key: "roundtrip".into(),
            policy: serde_json::json!({ "secret": "TENANT-SECRET-payload" }),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: Some("agent".into()),
            config: serde_json::json!({ "secret": "AGENT-SECRET-payload" }),
        })
        .await
        .unwrap();
    let sess = rt
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: Some("session".into()),
        })
        .await
        .unwrap();
    rt.write_record(NewRecord {
        tenant_id: t.tenant_id.clone(),
        external_id: None,
        namespace: vec!["n".into()],
        scope: serde_json::json!({}),
        text: Some("RECORD-SECRET-payload".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    })
    .await
    .unwrap();
    rt.register_skill(
        NewSkill {
            tenant_id: t.tenant_id.clone(),
            name: "skill".into(),
            version: "1.0.0".into(),
            manifest: serde_json::json!({ "secret": "SKILL-SECRET-payload" }),
            artifact_refs: vec![],
        },
        None,
    )
    .await
    .unwrap();

    // A brand-new runtime over the SAME store + an equivalent keyring hydrates by
    // scanning and decrypting the sealed primary rows.
    let rt2 = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content.clone() as Arc<dyn ContentBackend>,
        envelope_keyring(),
    );

    let agents = rt2.list_agents(&t.tenant_id).await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].agent_id, a.agent_id);
    assert_eq!(agents[0].config["secret"], "AGENT-SECRET-payload");

    let records = rt2.list_records(&t.tenant_id).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].text.as_deref(), Some("RECORD-SECRET-payload"));

    let skills = rt2.list_skills(&t.tenant_id).await.unwrap();
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].manifest["secret"], "SKILL-SECRET-payload");

    let sessions = rt2.list_sessions(&t.tenant_id).await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, sess.session_id);

    // The tenant primary row also survives hydration (policy decrypts).
    let tenant = rt2.get_tenant(&t.tenant_id).await.unwrap();
    assert_eq!(tenant.policy["secret"], "TENANT-SECRET-payload");
}
