use std::sync::Arc;

use agentdb_storage::{BoxId, KvStore, TenantId, WriteOp};

use crate::error::Result;
use crate::meta::{key_next_tenant_id, key_tenant_by_external, key_tenant_record};
use crate::types::Tenant;

pub struct TenantRegistry {
    kv: Arc<dyn KvStore>,
}

impl TenantRegistry {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }

    /// Look up an existing tenant by its external string id.
    pub fn lookup(&self, box_id: BoxId, external: &str) -> Result<Option<Tenant>> {
        let Some(bytes) = self.kv.get(&key_tenant_by_external(box_id, external))? else {
            return Ok(None);
        };
        let internal_id: TenantId = serde_json::from_slice(&bytes)?;
        let record_bytes = self
            .kv
            .get(&key_tenant_record(box_id, internal_id))?
            .ok_or_else(|| {
                crate::error::Error::Invalid(format!(
                    "tenant record missing for box={box_id} tenant={internal_id}"
                ))
            })?;
        let t: Tenant = serde_json::from_slice(&record_bytes)?;
        Ok(Some(t))
    }

    /// Get an existing tenant or mint a new internal id. Persists both the
    /// lookup entry and the full tenant record atomically.
    pub fn get_or_mint(&self, box_id: BoxId, external: &str, now_millis: u64) -> Result<Tenant> {
        if let Some(t) = self.lookup(box_id, external)? {
            return Ok(t);
        }
        let next_key = key_next_tenant_id(box_id);
        let current: TenantId = match self.kv.get(&next_key)? {
            Some(b) => serde_json::from_slice(&b)?,
            None => 1,
        };
        let tenant = Tenant {
            internal_id: current,
            external_id: external.to_string(),
            box_id,
            created_at: now_millis,
        };
        let tenant_bytes = serde_json::to_vec(&tenant)?;
        let lookup_bytes = serde_json::to_vec(&tenant.internal_id)?;
        let next_bytes = serde_json::to_vec(&(current + 1))?;
        self.kv.write_batch(vec![
            WriteOp::Put {
                key: key_tenant_by_external(box_id, external),
                value: lookup_bytes,
            },
            WriteOp::Put {
                key: key_tenant_record(box_id, current),
                value: tenant_bytes,
            },
            WriteOp::Put {
                key: next_key,
                value: next_bytes,
            },
        ])?;
        Ok(tenant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;
    use tempfile::TempDir;

    fn store() -> (TempDir, Arc<dyn KvStore>) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        (dir, kv)
    }

    #[test]
    fn lookup_unknown_tenant_returns_none() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        assert!(reg.lookup(1, "nobody").unwrap().is_none());
    }

    #[test]
    fn first_get_mints_id_one_then_two() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        let a = reg.get_or_mint(1, "t-a", 100).unwrap();
        let b = reg.get_or_mint(1, "t-b", 101).unwrap();
        assert_eq!(a.internal_id, 1);
        assert_eq!(b.internal_id, 2);
        assert_eq!(a.box_id, 1);
        assert_eq!(a.external_id, "t-a");
        assert_eq!(a.created_at, 100);
    }

    #[test]
    fn get_or_mint_is_idempotent_for_same_external_id() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        let a1 = reg.get_or_mint(1, "t-a", 100).unwrap();
        let a2 = reg.get_or_mint(1, "t-a", 200).unwrap();
        assert_eq!(a1, a2); // created_at preserved, no re-mint
    }

    #[test]
    fn different_boxes_mint_independent_id_spaces() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        let a = reg.get_or_mint(1, "same", 0).unwrap();
        let b = reg.get_or_mint(2, "same", 0).unwrap();
        assert_eq!(a.internal_id, 1);
        assert_eq!(b.internal_id, 1); // different box -> fresh counter
        assert_ne!(a.box_id, b.box_id);
    }
}
