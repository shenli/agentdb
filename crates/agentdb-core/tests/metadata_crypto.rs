use std::io;
use std::sync::Arc;

use agentdb_core::agent::NewAgent;
use agentdb_core::artifact::NewArtifact;
use agentdb_core::runtime::{AppendRequest, FetchRequest, Runtime};
use agentdb_core::session::NewSession;
use agentdb_core::tenant::NewTenant;
use agentdb_storage::keys::{artifact_record_key, event_key};
use agentdb_storage::{
    AllowAllKmsKeyResolver, ContentBackend, InMemoryContentBackend, KvStore, MetadataCryptoAdapter,
    MetadataCryptoContext, RocksKvStore, TabletKvStore,
};
use tempfile::TempDir;

const RUNTIME_PRIMARY_TABLET_ID: i64 = 1;

fn open_kv(path: &std::path::Path) -> Arc<dyn KvStore + Send + Sync> {
    Arc::new(RocksKvStore::open(path).expect("open rocks kv"))
}

#[derive(Debug)]
struct PrefixReverseMetadataCrypto;

impl MetadataCryptoAdapter for PrefixReverseMetadataCrypto {
    fn encrypt(
        &self,
        context: &MetadataCryptoContext<'_>,
        plaintext: &[u8],
    ) -> io::Result<Vec<u8>> {
        let mut out = format!(
            "meta:{}:{}:{}:",
            context.tenant_id,
            context.object_kind.as_str(),
            context.object_id
        )
        .into_bytes();
        out.extend(plaintext.iter().rev());
        Ok(out)
    }

    fn decrypt(
        &self,
        context: &MetadataCryptoContext<'_>,
        ciphertext: &[u8],
    ) -> io::Result<Vec<u8>> {
        let prefix = format!(
            "meta:{}:{}:{}:",
            context.tenant_id,
            context.object_kind.as_str(),
            context.object_id
        );
        let Some(body) = ciphertext.strip_prefix(prefix.as_bytes()) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing test metadata envelope prefix",
            ));
        };
        Ok(body.iter().rev().copied().collect())
    }
}

fn content() -> Arc<dyn ContentBackend> {
    Arc::new(InMemoryContentBackend::default())
}

#[tokio::test]
async fn metadata_crypto_wraps_artifact_and_event_rows_and_hydrates_them() {
    let dir = TempDir::new().unwrap();
    let (tenant_id, session_id, artifact_id) = {
        let kv = open_kv(dir.path());
        let rt = Runtime::with_kv_kms_content_and_metadata_crypto(
            kv.clone(),
            content(),
            Arc::new(AllowAllKmsKeyResolver),
            Arc::new(agentdb_storage::IdentityContentCryptoAdapter),
            Arc::new(PrefixReverseMetadataCrypto),
        );
        let tenant = rt
            .create_tenant(NewTenant {
                external_key: "metadata-crypto".into(),
                policy: serde_json::json!({}),
            })
            .await
            .unwrap();
        let agent = rt
            .create_agent(NewAgent {
                tenant_id: tenant.tenant_id.clone(),
                alias: Some("agent".into()),
                config: serde_json::json!({}),
            })
            .await
            .unwrap();
        let session = rt
            .create_session(NewSession {
                tenant_id: tenant.tenant_id.clone(),
                agent_id: agent.agent_id.clone(),
                alias: Some("session".into()),
            })
            .await
            .unwrap();
        let artifact = rt
            .create_artifact(
                NewArtifact {
                    tenant_id: tenant.tenant_id.clone(),
                    content_type: Some("text/plain".into()),
                    expected_sha256: None,
                },
                b"hello".to_vec(),
            )
            .await
            .unwrap();
        rt.append(AppendRequest::simple(
            &tenant,
            &agent,
            &session,
            "message",
            serde_json::json!({"body": "secret event metadata"}),
        ))
        .await
        .unwrap();

        let tablet = TabletKvStore::new(kv.clone(), RUNTIME_PRIMARY_TABLET_ID).unwrap();
        let artifact_raw = tablet
            .get(
                artifact_record_key(tenant.tenant_id.as_str(), artifact.artifact_id.as_str())
                    .as_bytes(),
            )
            .unwrap()
            .unwrap();
        assert!(artifact_raw.starts_with(b"AGDB-ENVELOPE-v1\n"));
        assert!(!artifact_raw.starts_with(b"{"));
        assert!(artifact.metadata_key_id.as_deref().is_some());

        let event_raw = tablet
            .get(event_key(tenant.tenant_id.as_str(), session.session_id.as_str(), 0).as_bytes())
            .unwrap()
            .unwrap();
        assert!(event_raw.starts_with(b"AGDB-ENVELOPE-v1\n"));
        assert!(!event_raw
            .windows(b"secret event metadata".len())
            .any(|w| { w == b"secret event metadata" }));

        (
            tenant.tenant_id.clone(),
            session.session_id.clone(),
            artifact.artifact_id.clone(),
        )
    };

    let kv = open_kv(dir.path());
    let rt = Runtime::with_kv_kms_content_and_metadata_crypto(
        kv,
        content(),
        Arc::new(AllowAllKmsKeyResolver),
        Arc::new(agentdb_storage::IdentityContentCryptoAdapter),
        Arc::new(PrefixReverseMetadataCrypto),
    );

    let artifact = rt.get_artifact(&tenant_id, &artifact_id).await.unwrap();
    assert_eq!(artifact.bytes, 5);
    assert_eq!(artifact.content_type.as_deref(), Some("text/plain"));
    assert!(artifact.metadata_key_id.as_deref().is_some());

    let fetched = rt
        .fetch_from_seq(FetchRequest {
            tenant_id,
            session_id,
            from_seq: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].kind, "message");
    assert_eq!(fetched[0].payload["body"], "secret event metadata");
    assert!(fetched[0].metadata_key_id.as_deref().is_some());
}
