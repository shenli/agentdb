//! Encryption-at-rest M20 — the data-plane DEK re-encryption walk. After a DEK
//! rotation, existing rows stay sealed under the old (now read-only) version;
//! this walk rewrites them under the new active version so the old version can
//! eventually be retired.

use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::artifact::NewArtifact;
use agentdb_core::record::{IndexingPolicy, NewRecord};
use agentdb_core::runtime::{AppendRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{
    envelope_dek_version, is_envelope_v2, AeadTenantKeyring, CatalogKeyProvider, ContentBackend,
    DekCacheConfig, EnvelopeCrypto, InMemoryContentBackend, InMemoryKvStore, KeyCatalog, KeyPurpose,
    KvStore, LocalKekProvider, Suite,
};

/// The DEK version of every sealed metadata row in the raw store. Persist writes
/// through a tablet-KV wrapper that physically prefixes keys, so we scan all raw
/// values and pick out the `AGDB-ENVELOPE-v2` frames (the metadata rows — the KEK
/// catalog stores wrapped-DEK JSON, not envelope frames, so it is skipped).
fn metadata_row_versions(kv: &Arc<dyn KvStore + Send + Sync>) -> Vec<u32> {
    let mut versions = Vec::new();
    for res in kv.scan_prefix(b"").unwrap() {
        let (_k, v) = res.unwrap();
        if is_envelope_v2(&v) {
            if let Some(ver) = envelope_dek_version(&v) {
                versions.push(ver);
            }
        }
    }
    versions
}

#[tokio::test]
async fn reencrypt_walk_moves_metadata_from_the_old_dek_version_to_the_active_one() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content = Arc::new(InMemoryContentBackend::default());
    // A catalog-backed keyring so the Metadata DEK can be rotated (the local
    // HKDF provider has no versioning).
    let provider = Arc::new(CatalogKeyProvider::new(
        KeyCatalog::new(kv.clone()),
        Arc::new(LocalKekProvider::from_secret(b"kek-secret", "local:v1")),
    ));
    let keyring = Arc::new(AeadTenantKeyring::new(
        provider.clone(),
        Suite::XChaCha20Poly1305,
        DekCacheConfig::default(),
    ));
    let rt = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content.clone() as Arc<dyn ContentBackend>,
        keyring.clone(),
    );

    let t = rt
        .create_tenant(NewTenant {
            external_key: "reenc".into(),
            policy: serde_json::json!({ "secret": "TENANT-SECRET" }),
        })
        .await
        .unwrap();
    let a = rt
        .create_agent(NewAgent {
            tenant_id: t.tenant_id.clone(),
            alias: Some("agent".into()),
            config: serde_json::json!({ "secret": "AGENT-SECRET" }),
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
        serde_json::json!({ "text": "EVENT-SECRET" }),
    ))
    .await
    .unwrap();
    rt.write_record(NewRecord {
        tenant_id: t.tenant_id.clone(),
        external_id: None,
        namespace: vec!["n".into()],
        scope: serde_json::json!({}),
        text: Some("RECORD-SECRET".into()),
        metadata: serde_json::json!({}),
        tags: vec![],
        source: None,
        embedding: None,
        policy: IndexingPolicy::default(),
        expires_at_ms: None,
    })
    .await
    .unwrap();

    // Everything is sealed under Metadata DEK v1 (tenant `t` plus the auto-created
    // system tenant, whose rows this walk will leave untouched).
    let before = metadata_row_versions(&kv);
    assert!(!before.is_empty(), "expected some encrypted metadata rows");
    assert!(before.iter().all(|&v| v == 1), "all rows start at v1: {before:?}");
    let total_before = before.len();

    // Rotate the tenant's Metadata DEK to v2 and drop the cached active lease so
    // new seals use v2 (Runtime::rotate_tenant_dek does this in production; here
    // the single-node provider is driven directly).
    provider.rotate(t.tenant_id.as_str(), KeyPurpose::Metadata).unwrap();
    keyring.invalidate(t.tenant_id.as_str());

    // Re-encrypt: every v1 row is rewritten under v2.
    let report = rt.reencrypt_tenant_dek(t.tenant_id.as_str()).await.unwrap();
    assert_eq!(report.errors, 0, "no row should fail to re-seal");
    assert!(report.reencrypted >= 4, "tenant+agent+session+record+event rewritten: {report:?}");

    // Tenant t's rows moved to v2; the system tenant's rows (not rotated) stay
    // v1. So the v2 count equals what the walk rewrote, and the leftover v1 count
    // is exactly the rows the walk did not touch.
    let after = metadata_row_versions(&kv);
    let v2_after = after.iter().filter(|&&v| v == 2).count();
    let v1_after = after.iter().filter(|&&v| v == 1).count();
    assert_eq!(v2_after, report.reencrypted, "v2 rows == rows the walk rewrote");
    assert_eq!(
        v1_after,
        total_before - report.reencrypted,
        "remaining v1 rows are exactly the untouched (system-tenant) ones"
    );

    // Idempotent: a second pass rewrites nothing.
    let again = rt.reencrypt_tenant_dek(t.tenant_id.as_str()).await.unwrap();
    assert_eq!(again.reencrypted, 0, "already-current rows are not rewritten");

    // The data is still readable: a fresh runtime over the same store + an
    // equivalent catalog-backed keyring hydrates by decrypting the v2 rows.
    let provider2 = Arc::new(CatalogKeyProvider::new(
        KeyCatalog::new(kv.clone()),
        Arc::new(LocalKekProvider::from_secret(b"kek-secret", "local:v1")),
    ));
    let keyring2 = Arc::new(AeadTenantKeyring::new(
        provider2,
        Suite::XChaCha20Poly1305,
        DekCacheConfig::default(),
    ));
    let rt2 = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content as Arc<dyn ContentBackend>,
        keyring2,
    );
    let agents = rt2.list_agents(&t.tenant_id).await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].config["secret"], "AGENT-SECRET");
    let records = rt2.list_records(&t.tenant_id).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].text.as_deref(), Some("RECORD-SECRET"));
}

#[tokio::test]
async fn reencrypt_walk_moves_content_blobs_to_the_active_content_dek() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let content = Arc::new(InMemoryContentBackend::default());
    let provider = Arc::new(CatalogKeyProvider::new(
        KeyCatalog::new(kv.clone()),
        Arc::new(LocalKekProvider::from_secret(b"kek-secret", "local:v1")),
    ));
    let keyring = Arc::new(AeadTenantKeyring::new(
        provider.clone(),
        Suite::XChaCha20Poly1305,
        DekCacheConfig::default(),
    ));
    let rt = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content.clone() as Arc<dyn ContentBackend>,
        keyring.clone(),
    );

    let t = rt
        .create_tenant(NewTenant {
            external_key: "reenc-content".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();
    let art = rt
        .create_artifact(
            NewArtifact {
                tenant_id: t.tenant_id.clone(),
                content_type: None,
                expected_sha256: None,
            },
            b"ARTIFACT-SECRET-blob".to_vec(),
        )
        .await
        .unwrap();

    // The artifact content is sealed under Content DEK v1.
    let stored_v1 = content
        .get(t.tenant_id.as_str(), art.artifact_id.as_str())
        .await
        .unwrap();
    assert!(is_envelope_v2(&stored_v1));
    assert_eq!(envelope_dek_version(&stored_v1), Some(1));

    // Rotate the Content DEK to v2 and drop the cached active lease.
    provider.rotate(t.tenant_id.as_str(), KeyPurpose::Content).unwrap();
    keyring.invalidate(t.tenant_id.as_str());

    // Re-encrypt: the artifact blob is rewritten under Content v2.
    let report = rt.reencrypt_tenant_dek(t.tenant_id.as_str()).await.unwrap();
    assert_eq!(report.errors, 0, "no blob should fail to re-seal");
    assert!(report.reencrypted >= 1, "the artifact blob was rewritten: {report:?}");

    let stored_v2 = content
        .get(t.tenant_id.as_str(), art.artifact_id.as_str())
        .await
        .unwrap();
    assert_eq!(envelope_dek_version(&stored_v2), Some(2), "blob now under v2");

    // Still readable: a fresh runtime over the same store + an equivalent keyring
    // decrypts the re-sealed blob.
    let provider2 = Arc::new(CatalogKeyProvider::new(
        KeyCatalog::new(kv.clone()),
        Arc::new(LocalKekProvider::from_secret(b"kek-secret", "local:v1")),
    ));
    let keyring2 = Arc::new(AeadTenantKeyring::new(
        provider2,
        Suite::XChaCha20Poly1305,
        DekCacheConfig::default(),
    ));
    let rt2 = Runtime::with_kv_content_and_keyring(
        kv.clone(),
        content as Arc<dyn ContentBackend>,
        keyring2,
    );
    let blob = rt2.get_artifact_blob(&t.tenant_id, &art.artifact_id).await.unwrap();
    assert_eq!(blob, b"ARTIFACT-SECRET-blob");
}
