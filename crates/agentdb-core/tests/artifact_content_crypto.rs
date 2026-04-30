use std::io;
use std::sync::Arc;

use agentdb_core::artifact::NewArtifact;
use agentdb_core::runtime::Runtime;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::{
    sha256_hex, AllowAllKmsKeyResolver, ContentBackend, ContentCryptoAdapter, ContentCryptoContext,
    KvStore, LocalFsContentBackend, RocksKvStore,
};
use tempfile::TempDir;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    Arc::new(RocksKvStore::open(path).expect("open rocks kv"))
}

#[derive(Debug)]
struct PrefixReverseCrypto;

impl ContentCryptoAdapter for PrefixReverseCrypto {
    fn encrypt(&self, context: &ContentCryptoContext<'_>, plaintext: &[u8]) -> io::Result<Vec<u8>> {
        let mut out = format!("enc:{}:{}:", context.tenant_id, context.key.key_id).into_bytes();
        out.extend(plaintext.iter().rev());
        Ok(out)
    }

    fn decrypt(
        &self,
        context: &ContentCryptoContext<'_>,
        ciphertext: &[u8],
    ) -> io::Result<Vec<u8>> {
        let prefix = format!("enc:{}:{}:", context.tenant_id, context.key.key_id);
        let Some(body) = ciphertext.strip_prefix(prefix.as_bytes()) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing test envelope prefix",
            ));
        };
        Ok(body.iter().rev().copied().collect())
    }
}

#[tokio::test]
async fn artifact_content_crypto_wraps_storage_bytes_but_preserves_plaintext_metadata() {
    let dir = TempDir::new().unwrap();
    let kv_path = dir.path().join("kv");
    let content_root = dir.path().join("content");
    let plaintext = b"secret artifact bytes";

    let (tenant_id, artifact_id, sha256) = {
        let kv = open_kv(&kv_path);
        let content: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
        let rt = Runtime::with_kv_kms_and_content_crypto(
            kv,
            content,
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(PrefixReverseCrypto),
        );
        let tenant = rt
            .create_tenant(NewTenant {
                external_key: "crypto-artifacts".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let artifact = rt
            .create_artifact(
                NewArtifact {
                    tenant_id: tenant.tenant_id.clone(),
                    content_type: Some("application/octet-stream".into()),
                    expected_sha256: Some(sha256_hex(plaintext)),
                },
                plaintext.to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(artifact.bytes, plaintext.len() as u64);
        assert_eq!(artifact.sha256, sha256_hex(plaintext));
        assert!(artifact.content_key_id.as_deref().is_some());
        assert!(artifact.metadata_key_id.as_deref().is_some());

        let stored_path = content_root
            .join("artifact_content")
            .join(tenant.tenant_id.as_str())
            .join(artifact.artifact_id.as_str())
            .join("blob");
        let stored = std::fs::read(stored_path).unwrap();
        assert_ne!(stored, plaintext);
        assert!(stored.starts_with(b"AGDB-ENVELOPE-v1\n"));
        assert!(stored.windows(b"tenant:".len()).any(|w| w == b"tenant:"));

        let round_trip = rt
            .get_artifact_blob(&tenant.tenant_id, &artifact.artifact_id)
            .await
            .unwrap();
        assert_eq!(round_trip, plaintext);

        (
            tenant.tenant_id.clone(),
            artifact.artifact_id.clone(),
            artifact.sha256,
        )
    };

    let kv = open_kv(&kv_path);
    let content: Arc<dyn ContentBackend> = Arc::new(LocalFsContentBackend::new(&content_root));
    let rt = Runtime::with_kv_kms_and_content_crypto(
        kv,
        content,
        Arc::new(AllowAllKmsKeyResolver),
        Arc::new(PrefixReverseCrypto),
    );

    let metadata = rt.get_artifact(&tenant_id, &artifact_id).await.unwrap();
    assert_eq!(metadata.bytes, plaintext.len() as u64);
    assert_eq!(metadata.sha256, sha256);
    assert!(metadata.content_key_id.as_deref().is_some());
    assert!(metadata.metadata_key_id.as_deref().is_some());

    let reopened_blob = rt
        .get_artifact_blob(&tenant_id, &artifact_id)
        .await
        .unwrap();
    assert_eq!(reopened_blob, plaintext);
}
