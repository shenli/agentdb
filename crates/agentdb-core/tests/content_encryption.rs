//! Encryption-at-rest M2 — artifact/session content is sealed through the
//! envelope keyring before it reaches the content backend, and a
//! disabled→enabled rollout stays readable (mixed v1/v2 reader).

use std::sync::Arc;

use agentdb_core::artifact::NewArtifact;
use agentdb_core::runtime::Runtime;
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
