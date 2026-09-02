//! Encryption-at-rest M16 — the runtime revokes / crypto-shreds a tenant's data
//! keys on the admin path (the `TenantKeyRevoker` hook).

use std::sync::{Arc, Mutex};

use agentdb_core::errors::{ErrorCode, Result};
use agentdb_core::runtime::Runtime;
use agentdb_core::TenantKeyRevoker;

#[derive(Default)]
struct RecordingRevoker {
    revoked: Mutex<Vec<(String, bool)>>,
}

#[async_trait::async_trait]
impl TenantKeyRevoker for RecordingRevoker {
    async fn revoke_tenant(&self, tenant_id: &str, destroy: bool) -> Result<()> {
        self.revoked
            .lock()
            .unwrap()
            .push((tenant_id.to_string(), destroy));
        Ok(())
    }
}

#[tokio::test]
async fn revoke_tenant_dek_invokes_the_installed_revoker_with_the_destroy_flag() {
    let rt = Runtime::in_memory();
    let revoker = Arc::new(RecordingRevoker::default());
    rt.set_key_revoker(revoker.clone());

    rt.revoke_tenant_dek("ten_a", false).await.unwrap();
    rt.revoke_tenant_dek("ten_b", true).await.unwrap();

    let seen = revoker.revoked.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![("ten_a".to_string(), false), ("ten_b".to_string(), true)]
    );
}

#[tokio::test]
async fn revoke_tenant_dek_fails_closed_without_a_revoker() {
    let rt = Runtime::in_memory();
    let err = rt.revoke_tenant_dek("ten_a", false).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::KeyUnavailable);
}
