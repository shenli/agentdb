//! Encryption-at-rest M15 — the runtime rotates a tenant's data keys on the
//! admin path (the `TenantKeyRotator` hook).

use std::sync::{Arc, Mutex};

use agentdb_core::errors::{ErrorCode, Result};
use agentdb_core::runtime::Runtime;
use agentdb_core::TenantKeyRotator;

#[derive(Default)]
struct RecordingRotator {
    rotated: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TenantKeyRotator for RecordingRotator {
    async fn rotate_tenant(&self, tenant_id: &str) -> Result<()> {
        self.rotated.lock().unwrap().push(tenant_id.to_string());
        Ok(())
    }
}

#[tokio::test]
async fn rotate_tenant_dek_invokes_the_installed_rotator() {
    let rt = Runtime::in_memory();
    let rotator = Arc::new(RecordingRotator::default());
    rt.set_key_rotator(rotator.clone());

    rt.rotate_tenant_dek("ten_a").await.unwrap();

    let seen = rotator.rotated.lock().unwrap().clone();
    assert_eq!(seen, vec!["ten_a".to_string()]);
}

#[tokio::test]
async fn rotate_tenant_dek_fails_closed_without_a_rotator() {
    // No rotator installed (encryption disabled) → the admin path fails closed
    // rather than silently no-op.
    let rt = Runtime::in_memory();
    let err = rt.rotate_tenant_dek("ten_a").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::KeyUnavailable);
}

#[tokio::test]
async fn first_installed_rotator_wins() {
    let rt = Runtime::in_memory();
    let first = Arc::new(RecordingRotator::default());
    let second = Arc::new(RecordingRotator::default());
    rt.set_key_rotator(first.clone());
    rt.set_key_rotator(second.clone()); // ignored

    rt.rotate_tenant_dek("ten_a").await.unwrap();

    assert_eq!(first.rotated.lock().unwrap().len(), 1);
    assert_eq!(second.rotated.lock().unwrap().len(), 0);
}
