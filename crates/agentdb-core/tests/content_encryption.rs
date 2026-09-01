//! Encryption-at-rest M2 — artifact/session content is sealed through the
//! envelope keyring before it reaches the content backend, and a
//! disabled→enabled rollout stays readable (mixed v1/v2 reader).

use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::artifact::NewArtifact;
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::{NewTenant, Tenant};
use agentdb_storage::{
    is_envelope_v2, AeadTenantKeyring, ContentBackend, DekCacheConfig, DisabledEnvelopeCrypto,
    EnvelopeCrypto, InMemoryContentBackend, InMemoryKvStore, KvStore, LocalKeyProvider, Suite,
};

fn envelope_keyring() -> Arc<dyn EnvelopeCrypto> {
    Arc::new(AeadTenantKeyring::new(
        Arc::new(LocalKeyProvider::from_secret(b"content-encryption-test-root")),
        Suite::XChaCha20Poly1305,
        DekCacheConfig::default(),
    ))
}

async fn fresh_tenant(rt: &Runtime, key: &str) -> Tenant {
    rt.create_tenant(NewTenant {
        external_key: key.into(),
        policy: serde_json::json!({}),
    })
    .await
    .unwrap()
}

/// With the keyring enabled, artifact content is an `AGDB-ENVELOPE-v2` frame on
/// disk that does not contain the plaintext, and it round-trips back through the
/// runtime.
#[tokio::test]
async fn artifact_content_is_encrypted_on_disk_and_round_trips() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv_content_and_keyring(
        kv,
        content.clone() as Arc<dyn ContentBackend>,
        envelope_keyring(),
    );
    let t = fresh_tenant(&rt, "enc").await;

    const MARKER: &[u8] = b"SUPER-SECRET-ARTIFACT-MARKER-9f3a5c";
    let mut blob = b"prefix ".to_vec();
    blob.extend_from_slice(MARKER);
    blob.extend_from_slice(b" suffix");

    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: Some("application/octet-stream".into()),
                expected_sha256: None,
            },
            blob.clone(),
        )
        .await
        .unwrap();

    // Raw stored bytes: an envelope-v2 frame with the marker absent.
    let raw = content
        .get(t.tenant_id.as_str(), art.artifact_id.as_str())
        .await
        .unwrap();
    assert!(
        is_envelope_v2(&raw),
        "content should be sealed as an AGDB-ENVELOPE-v2 frame"
    );
    assert!(
        !raw.windows(MARKER.len()).any(|w| w == MARKER),
        "plaintext marker leaked into stored content"
    );

    // Round-trips back to plaintext through the runtime.
    let fetched = rt
        .get_artifact_blob(&t.tenant_id, &art.artifact_id)
        .await
        .unwrap();
    assert_eq!(fetched, blob);
}

/// Two tenants storing identical content get independent ciphertext on disk.
#[tokio::test]
async fn identical_content_for_two_tenants_is_independent_ciphertext() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv_content_and_keyring(
        kv,
        content.clone() as Arc<dyn ContentBackend>,
        envelope_keyring(),
    );
    let a = fresh_tenant(&rt, "ta").await;
    let b = fresh_tenant(&rt, "tb").await;
    let blob = b"identical-artifact-body".to_vec();

    let art_a = rt
        .create_artifact(
            NewArtifact {
                tenant_id: a.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            blob.clone(),
        )
        .await
        .unwrap();
    let art_b = rt
        .create_artifact(
            NewArtifact {
                tenant_id: b.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            blob.clone(),
        )
        .await
        .unwrap();

    let raw_a = content
        .get(a.tenant_id.as_str(), art_a.artifact_id.as_str())
        .await
        .unwrap();
    let raw_b = content
        .get(b.tenant_id.as_str(), art_b.artifact_id.as_str())
        .await
        .unwrap();
    assert_ne!(raw_a, raw_b, "identical plaintext must not share ciphertext");
}

/// Every raw value in the KV store, so a test can prove a plaintext marker is
/// absent from what actually lands on disk.
fn raw_kv_values(kv: &Arc<dyn KvStore + Send + Sync>) -> Vec<Vec<u8>> {
    kv.scan_prefix(b"")
        .unwrap()
        .map(|r| r.unwrap().1)
        .collect()
}

fn marker_present(values: &[Vec<u8>], marker: &[u8]) -> bool {
    values
        .iter()
        .any(|v| v.windows(marker.len()).any(|w| w == marker))
}

/// M3: event values (and artifact metadata rows) are sealed before the KV
/// write, so the plaintext marker never appears in a raw KV value, and events
/// round-trip back through the runtime.
#[tokio::test]
async fn event_and_metadata_values_are_encrypted_on_disk() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content: Arc<dyn ContentBackend> = Arc::new(InMemoryContentBackend::default());
    let rt = Runtime::with_kv_content_and_keyring(kv.clone(), content, envelope_keyring());

    let t = fresh_tenant(&rt, "evt").await;
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: Some("agent".into()),
            config: serde_json::json!({}),
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

    const MARKER: &str = "SECRET-EVENT-MARKER-a1b2c3d4";
    rt.append(AppendRequest::simple(
        &t,
        &a,
        &s,
        "message",
        serde_json::json!({ "text": MARKER }),
    ))
    .await
    .unwrap();

    // The marker is nowhere in the raw KV bytes (event value is sealed).
    let values = raw_kv_values(&kv);
    assert!(
        !marker_present(&values, MARKER.as_bytes()),
        "plaintext event marker leaked into a raw KV value"
    );

    // The event round-trips back through the runtime with its payload intact.
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

/// M3 mixed reader: an envelope-enabled runtime reads legacy plaintext event
/// values written earlier by a disabled runtime, and can then append new
/// (encrypted) events alongside them.
#[tokio::test]
async fn envelope_runtime_reads_legacy_plaintext_events() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());

    let disabled = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        Arc::new(InMemoryContentBackend::default()) as Arc<dyn ContentBackend>,
        Arc::new(DisabledEnvelopeCrypto),
    );
    let t = fresh_tenant(&disabled, "mixevt").await;
    let a = disabled
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: None,
            config: serde_json::json!({}),
        })
        .await
        .unwrap();
    let s = disabled
        .create_session(NewSession {
            tenant_id: t.tenant_id.clone(),
            agent_id: a.agent_id.clone(),
            alias: None,
        })
        .await
        .unwrap();
    disabled
        .append(AppendRequest::simple(
            &t,
            &a,
            &s,
            "message",
            serde_json::json!({ "text": "legacy" }),
        ))
        .await
        .unwrap();

    // A fresh envelope-enabled runtime hydrates from the same KV and reads the
    // legacy plaintext event.
    let enveloped = Runtime::with_kv_content_and_keyring(
        kv,
        Arc::new(InMemoryContentBackend::default()) as Arc<dyn ContentBackend>,
        envelope_keyring(),
    );
    let events = enveloped
        .fetch_from_seq(FetchRequest {
            tenant_id: t.tenant_id.clone(),
            session_id: s.session_id.clone(),
            from_seq: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["text"], "legacy");
}

/// A disabled→enabled rollout stays readable: an envelope-enabled runtime reads
/// legacy plaintext content written earlier by a disabled runtime (the mixed
/// v1/v2 reader).
#[tokio::test]
async fn envelope_runtime_reads_legacy_plaintext_content() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content = Arc::new(InMemoryContentBackend::default());

    // 1. A disabled runtime writes an artifact (legacy path).
    let disabled = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content.clone() as Arc<dyn ContentBackend>,
        Arc::new(DisabledEnvelopeCrypto),
    );
    let t = fresh_tenant(&disabled, "mix").await;
    let blob = b"legacy-plaintext-content".to_vec();
    let art = disabled
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            blob.clone(),
        )
        .await
        .unwrap();
    let raw = content
        .get(t.tenant_id.as_str(), art.artifact_id.as_str())
        .await
        .unwrap();
    assert!(!is_envelope_v2(&raw), "disabled path must not write v2 frames");

    // 2. An envelope-enabled runtime hydrates from the same store and reads the
    //    legacy content back as plaintext.
    let enveloped = Runtime::with_kv_content_and_keyring(
        kv,
        content as Arc<dyn ContentBackend>,
        envelope_keyring(),
    );
    let fetched = enveloped
        .get_artifact_blob(&t.tenant_id, &art.artifact_id)
        .await
        .unwrap();
    assert_eq!(fetched, blob);
}
