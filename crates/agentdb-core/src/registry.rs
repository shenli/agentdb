use std::sync::Arc;

use agentdb_storage::{BoxId, KvStore, TenantId, WriteOp};

use crate::error::Result;
use crate::meta::{
    key_next_tenant_id, key_tenant_by_external, key_tenant_record,
    prefix_tenant_by_external_for_box,
};
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
                crate::error::Error::Invalid {
                    field: None,
                    message: format!("tenant record missing for box={box_id} tenant={internal_id}"),
                }
            })?;
        let t: Tenant = serde_json::from_slice(&record_bytes)?;
        Ok(Some(t))
    }

    /// Reverse lookup: given an internal id, return the full tenant
    /// record. Used by the lifecycle hook to resolve external ids for
    /// webhook payloads.
    pub fn lookup_by_internal(
        &self,
        box_id: BoxId,
        internal_id: TenantId,
    ) -> Result<Option<Tenant>> {
        let Some(bytes) = self.kv.get(&key_tenant_record(box_id, internal_id))? else {
            return Ok(None);
        };
        let t: Tenant = serde_json::from_slice(&bytes)?;
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

    /// Enumerate all external tenant ids minted under this box.
    pub fn list_in_box(&self, box_id: BoxId) -> Result<Vec<String>> {
        let prefix = prefix_tenant_by_external_for_box(box_id);
        let mut out = Vec::new();
        for res in self.kv.scan_prefix(&prefix)? {
            let (k, _v) = res?;
            // key = Meta(1) | TAG(1) | box(2) | external-bytes
            if k.len() <= 4 {
                continue;
            }
            let name = std::str::from_utf8(&k[4..])
                .map_err(|e| crate::error::Error::Invalid { field: Some("tenant_id".into()), message: format!("bad tenant name utf-8: {e}") })?
                .to_string();
            out.push(name);
        }
        Ok(out)
    }
}

use crate::meta::{
    key_next_session_id, key_session_by_external, key_session_record,
    prefix_session_by_external_for_tenant,
};
use crate::types::{Session, Tier};
use agentdb_storage::SessionId;

pub struct SessionRegistry {
    kv: Arc<dyn KvStore>,
}

impl SessionRegistry {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }

    pub fn lookup(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
        external: &str,
    ) -> Result<Option<Session>> {
        let Some(bytes) = self
            .kv
            .get(&key_session_by_external(box_id, tenant_id, external))?
        else {
            return Ok(None);
        };
        let internal_id: SessionId = serde_json::from_slice(&bytes)?;
        let record_bytes = self
            .kv
            .get(&key_session_record(box_id, tenant_id, internal_id))?
            .ok_or_else(|| {
                crate::error::Error::Invalid {
                    field: None,
                    message: format!("session record missing for box={box_id} tenant={tenant_id} session={internal_id}"),
                }
            })?;
        let s: Session = serde_json::from_slice(&record_bytes)?;
        Ok(Some(s))
    }

    pub fn get_or_mint(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
        external: &str,
        now_millis: u64,
    ) -> Result<Session> {
        self.get_or_mint_with_fork(box_id, tenant_id, external, now_millis, None)
    }

    pub fn get_or_mint_with_fork(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
        external: &str,
        now_millis: u64,
        forked_from: Option<crate::types::ForkRef>,
    ) -> Result<Session> {
        if let Some(s) = self.lookup(box_id, tenant_id, external)? {
            return Ok(s);
        }
        let next_key = key_next_session_id(box_id, tenant_id);
        let current: SessionId = match self.kv.get(&next_key)? {
            Some(b) => serde_json::from_slice(&b)?,
            None => 1,
        };
        let session = Session {
            internal_id: current,
            external_id: external.to_string(),
            tenant_internal_id: tenant_id,
            box_id,
            created_at: now_millis,
            last_active_at: now_millis,
            tier: Tier::Warm,
            next_seq: 1,
            segment_path: None,
            forked_from,
        };
        let session_bytes = serde_json::to_vec(&session)?;
        let lookup_bytes = serde_json::to_vec(&session.internal_id)?;
        let next_bytes = serde_json::to_vec(&(current + 1))?;
        self.kv.write_batch(vec![
            WriteOp::Put {
                key: key_session_by_external(box_id, tenant_id, external),
                value: lookup_bytes,
            },
            WriteOp::Put {
                key: key_session_record(box_id, tenant_id, current),
                value: session_bytes,
            },
            WriteOp::Put {
                key: next_key,
                value: next_bytes,
            },
        ])?;
        Ok(session)
    }

    /// Enumerate all external session ids minted under this tenant.
    pub fn list_in_tenant(&self, box_id: BoxId, tenant_id: TenantId) -> Result<Vec<String>> {
        let prefix = prefix_session_by_external_for_tenant(box_id, tenant_id);
        let mut out = Vec::new();
        for res in self.kv.scan_prefix(&prefix)? {
            let (k, _v) = res?;
            if k.len() <= 12 {
                continue;
            }
            let name = std::str::from_utf8(&k[12..])
                .map_err(|e| crate::error::Error::Invalid { field: Some("session_id".into()), message: format!("bad session name utf-8: {e}") })?
                .to_string();
            out.push(name);
        }
        Ok(out)
    }

    /// Lookup a session by its internal id (not its external string). Used by
    /// the planner when resolving DocIds that came back from an index.
    pub fn lookup_by_internal(
        &self,
        box_id: BoxId,
        tenant_id: TenantId,
        internal_id: SessionId,
    ) -> Result<Option<Session>> {
        let key = crate::meta::key_session_record(box_id, tenant_id, internal_id);
        let Some(bytes) = self.kv.get(&key)? else {
            return Ok(None);
        };
        let s: Session = serde_json::from_slice(&bytes)?;
        Ok(Some(s))
    }

    /// Same as `lookup` but reads via `snapshot` instead of the live KV.
    pub fn lookup_at(
        &self,
        snapshot: &dyn agentdb_storage::KvSnapshot,
        box_id: BoxId,
        tenant_id: TenantId,
        external: &str,
    ) -> Result<Option<Session>> {
        let Some(bytes) = snapshot.get(&key_session_by_external(box_id, tenant_id, external))?
        else {
            return Ok(None);
        };
        let internal_id: SessionId = serde_json::from_slice(&bytes)?;
        self.lookup_by_internal_at(snapshot, box_id, tenant_id, internal_id)
    }

    /// Same as `lookup_by_internal` but reads via `snapshot`.
    pub fn lookup_by_internal_at(
        &self,
        snapshot: &dyn agentdb_storage::KvSnapshot,
        box_id: BoxId,
        tenant_id: TenantId,
        internal_id: SessionId,
    ) -> Result<Option<Session>> {
        let key = key_session_record(box_id, tenant_id, internal_id);
        let Some(bytes) = snapshot.get(&key)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// Overwrite the persisted session record. Does NOT rewrite the lookup
    /// entry — that mapping is immutable once created.
    pub fn save(&self, session: &Session) -> Result<()> {
        let bytes = serde_json::to_vec(session)?;
        self.kv.put(
            &key_session_record(
                session.box_id,
                session.tenant_internal_id,
                session.internal_id,
            ),
            &bytes,
        )?;
        Ok(())
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
    fn tenant_lookup_by_internal_round_trips() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        let minted = reg.get_or_mint(1, "t-ext", 42).unwrap();
        let found = reg.lookup_by_internal(1, minted.internal_id).unwrap();
        assert_eq!(found.map(|t| t.external_id), Some("t-ext".to_string()));
        let missing = reg.lookup_by_internal(1, 9999).unwrap();
        assert!(missing.is_none());
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

    #[test]
    fn session_lookup_unknown_returns_none() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        assert!(reg.lookup(1, 1, "none").unwrap().is_none());
    }

    #[test]
    fn session_get_or_mint_creates_default_session() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let s = reg.get_or_mint(1, 10, "s-1", 500).unwrap();
        assert_eq!(s.external_id, "s-1");
        assert_eq!(s.tenant_internal_id, 10);
        assert_eq!(s.box_id, 1);
        assert_eq!(s.internal_id, 1);
        assert_eq!(s.created_at, 500);
        assert_eq!(s.last_active_at, 500);
        assert_eq!(s.tier, crate::types::Tier::Warm);
        assert_eq!(s.next_seq, 1);
    }

    #[test]
    fn session_mint_is_idempotent() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let a = reg.get_or_mint(1, 10, "s-1", 500).unwrap();
        let b = reg.get_or_mint(1, 10, "s-1", 900).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn session_ids_increment_within_same_tenant() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let s1 = reg.get_or_mint(1, 10, "s-a", 0).unwrap();
        let s2 = reg.get_or_mint(1, 10, "s-b", 0).unwrap();
        assert_eq!(s1.internal_id, 1);
        assert_eq!(s2.internal_id, 2);
    }

    #[test]
    fn save_persists_updates_to_session_record() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let mut s = reg.get_or_mint(1, 10, "s-1", 500).unwrap();
        s.next_seq = 99;
        s.last_active_at = 999;
        reg.save(&s).unwrap();
        let reloaded = reg.lookup(1, 10, "s-1").unwrap().unwrap();
        assert_eq!(reloaded.next_seq, 99);
        assert_eq!(reloaded.last_active_at, 999);
    }

    #[test]
    fn session_list_in_tenant_returns_all_externals() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        reg.get_or_mint(0, 10, "s-a", 0).unwrap();
        reg.get_or_mint(0, 10, "s-b", 0).unwrap();
        reg.get_or_mint(0, 11, "elsewhere", 0).unwrap();
        let mut got = reg.list_in_tenant(0, 10).unwrap();
        got.sort();
        assert_eq!(got, vec!["s-a".to_string(), "s-b".to_string()]);
    }

    #[test]
    fn list_in_box_returns_all_minted_externals_sorted() {
        let (_dir, kv) = store();
        let reg = TenantRegistry::new(kv);
        reg.get_or_mint(1, "t-c", 0).unwrap();
        reg.get_or_mint(1, "t-a", 0).unwrap();
        reg.get_or_mint(1, "t-b", 0).unwrap();
        reg.get_or_mint(2, "t-elsewhere", 0).unwrap();

        let mut got = reg.list_in_box(1).unwrap();
        got.sort();
        assert_eq!(
            got,
            vec!["t-a".to_string(), "t-b".to_string(), "t-c".to_string()]
        );
    }

    #[test]
    fn get_or_mint_with_fork_stores_parent_pointer() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let fork_ref = crate::types::ForkRef {
            parent_session_internal_id: 7,
            parent_seq_at_fork: 42,
        };
        let s = reg
            .get_or_mint_with_fork(1, 10, "child", 100, Some(fork_ref.clone()))
            .unwrap();
        assert_eq!(s.forked_from, Some(fork_ref));
    }

    #[test]
    fn get_or_mint_is_unchanged_for_non_forked_session() {
        let (_dir, kv) = store();
        let reg = SessionRegistry::new(kv);
        let s = reg.get_or_mint(1, 10, "plain", 100).unwrap();
        assert!(s.forked_from.is_none());
    }
}
