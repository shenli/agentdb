//! Encryption-at-rest M12 — the runtime provisions a tenant's data keys at
//! creation time (the cluster key-provisioning hook).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use agentdb_core::errors::Result;
use agentdb_core::runtime::Runtime;
use agentdb_core::tenant::NewTenant;
use agentdb_core::{TenantKeyProvisioner, RUNTIME_PRIMARY_TABLET_ID};
use agentdb_storage::{
    ContentBackend, DisabledEnvelopeCrypto, InMemoryContentBackend, InMemoryKvStore, KvStore,
    ReplicatedLog, SingleNodeReplicatedLog,
};

#[derive(Default)]
struct RecordingProvisioner {
    provisioned: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TenantKeyProvisioner for RecordingProvisioner {
    async fn provision_tenant(&self, tenant_id: &str) -> Result<()> {
        self.provisioned
            .lock()
            .unwrap()
            .push(tenant_id.to_string());
        Ok(())
    }
}

#[tokio::test]
async fn create_tenant_provisions_keys() {
    let kv: Arc<dyn KvStore + Send + Sync> = Arc::new(InMemoryKvStore::default());
    let log: Arc<dyn ReplicatedLog + Send + Sync> =
        Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
    let provisioner = Arc::new(RecordingProvisioner::default());

    let rt = Runtime::with_kv_content_keyring_provisioner_and_replicated_logs(
        kv,
        Arc::new(InMemoryContentBackend::default()) as Arc<dyn ContentBackend>,
        Arc::new(DisabledEnvelopeCrypto),
        provisioner.clone(),
        BTreeMap::from([(RUNTIME_PRIMARY_TABLET_ID, log)]),
    );

    let tenant = rt
        .create_tenant(NewTenant {
            external_key: "prov".into(),
            policy: serde_json::json!({}),
        })
        .await
        .unwrap();

    // The provisioner saw exactly this tenant, minted at creation (before any of
    // its data could be written).
    let seen = provisioner.provisioned.lock().unwrap().clone();
    assert!(
        seen.contains(&tenant.tenant_id.as_str().to_string()),
        "create_tenant should provision the tenant's keys; saw {seen:?}"
    );
}
