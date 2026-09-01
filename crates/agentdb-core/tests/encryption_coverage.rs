//! Encryption-at-rest raw-storage marker coverage map (design "Proposed Code
//! Changes" §4 / delivery-plan stage 5). Writes a unique secret marker through
//! every durable path and asserts, per path, whether that marker appears in the
//! raw bytes on disk.
//!
//! This is the regression detector the design calls for. It is intentionally
//! two-sided:
//!   - Paths that ARE encrypted (events, artifact content) must show NO marker
//!     in raw storage — a leak here is a regression.
//!   - Paths NOT yet encrypted (agent config, searchable records, skills) are
//!     asserted to STILL leak, so this test doubles as a tripwire: when a later
//!     milestone encrypts one of them, this assertion flips and forces the
//!     coverage map to be updated deliberately.

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

    let t = rt
        .create_tenant(NewTenant {
            external_key: "coverage".into(),
            policy: serde_json::json!({}),
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

    // --- Not-yet-encrypted paths: tripwires. When a later milestone encrypts
    //     one of these, the assertion flips — update this coverage map then. ---
    assert!(
        marker_in(&kv_values, AGENT_MARKER),
        "agent config is now encrypted — extend the coverage map"
    );
    assert!(
        marker_in(&kv_values, RECORD_MARKER),
        "searchable record is now encrypted — extend the coverage map (note: also the export ledger)"
    );
    assert!(
        marker_in(&kv_values, SKILL_MARKER),
        "skill manifest is now encrypted — extend the coverage map"
    );
}
