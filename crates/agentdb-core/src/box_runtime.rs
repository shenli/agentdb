use std::sync::Arc;

use agentdb_storage::{BoxId, KvStore};
use tokio::sync::Mutex;

use crate::error::{Error, Result};

/// One logical box. Owns a mutex that serializes the hot-path writes
/// (append, metadata updates) for tenants it hosts.
pub struct Box_ {
    pub id: BoxId,
    pub write_lock: Mutex<()>,
}

impl Box_ {
    fn new(id: BoxId) -> Self {
        Self {
            id,
            write_lock: Mutex::new(()),
        }
    }
}

/// Owns the pool of boxes and the shared KV store, plus the tenant→box
/// routing function.
pub struct BoxRegistry {
    boxes: Vec<Arc<Box_>>,
    kv: Arc<dyn KvStore>,
}

impl BoxRegistry {
    pub fn new(kv: Arc<dyn KvStore>, num_boxes: u16) -> Result<Self> {
        if num_boxes == 0 {
            return Err(Error::Invalid("num_boxes must be > 0".into()));
        }
        let boxes = (0..num_boxes).map(|i| Arc::new(Box_::new(i))).collect();
        Ok(Self { boxes, kv })
    }

    pub fn kv(&self) -> &Arc<dyn KvStore> {
        &self.kv
    }

    pub fn num_boxes(&self) -> u16 {
        self.boxes.len() as u16
    }

    /// Route a tenant external id to a box using FNV-1a hash mod num_boxes.
    pub fn route_tenant(&self, external: &str) -> Arc<Box_> {
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in external.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let idx = (hash as usize) % self.boxes.len();
        self.boxes[idx].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn registry(num: u16) -> (TempDir, BoxRegistry) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        (dir, BoxRegistry::new(kv, num).unwrap())
    }

    #[test]
    fn zero_boxes_is_rejected() {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        assert!(BoxRegistry::new(kv, 0).is_err());
    }

    #[test]
    fn same_tenant_always_routes_to_same_box() {
        let (_dir, reg) = registry(8);
        let a = reg.route_tenant("t-42").id;
        let b = reg.route_tenant("t-42").id;
        assert_eq!(a, b);
    }

    #[test]
    fn different_tenants_eventually_hit_multiple_boxes() {
        let (_dir, reg) = registry(4);
        let mut hit = HashSet::new();
        for i in 0..200 {
            hit.insert(reg.route_tenant(&format!("t-{i}")).id);
        }
        assert!(
            hit.len() > 1,
            "expected distribution across boxes, got {hit:?}"
        );
    }

    #[test]
    fn num_boxes_reports_correct_count() {
        let (_dir, reg) = registry(5);
        assert_eq!(reg.num_boxes(), 5);
    }
}
