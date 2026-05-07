use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use agentdb_storage::{
    Error as StorageError, RaftRsSharedGroup, RaftRsSnapshotTransfer, RaftRsWireMessage, Result,
};

use crate::state::RaftRsIngress;

#[derive(Default)]
pub struct RaftRsGroupRegistry {
    groups: RwLock<BTreeMap<i64, RaftRsSharedGroup>>,
}

impl RaftRsGroupRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn register(&self, tablet_id: i64, group: RaftRsSharedGroup) -> Result<()> {
        if tablet_id <= 0 {
            return Err(StorageError::Other(
                "raft-rs registry tablet_id must be positive".into(),
            ));
        }
        self.groups
            .write()
            .expect("raft-rs registry lock poisoned")
            .insert(tablet_id, group);
        Ok(())
    }

    pub fn unregister(&self, tablet_id: i64) -> Option<RaftRsSharedGroup> {
        self.groups
            .write()
            .expect("raft-rs registry lock poisoned")
            .remove(&tablet_id)
    }

    pub fn tablet_count(&self) -> usize {
        self.groups
            .read()
            .expect("raft-rs registry lock poisoned")
            .len()
    }

    pub fn tablet_ids(&self) -> Vec<i64> {
        self.groups
            .read()
            .expect("raft-rs registry lock poisoned")
            .keys()
            .copied()
            .collect()
    }

    pub fn groups_snapshot(&self) -> Vec<(i64, RaftRsSharedGroup)> {
        self.groups
            .read()
            .expect("raft-rs registry lock poisoned")
            .iter()
            .map(|(tablet_id, group)| (*tablet_id, group.clone()))
            .collect()
    }

    fn group_for(&self, tablet_id: i64) -> Result<RaftRsSharedGroup> {
        self.groups
            .read()
            .expect("raft-rs registry lock poisoned")
            .get(&tablet_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::Other(format!(
                    "raft-rs group for tablet {tablet_id} is not registered on this node"
                ))
            })
    }
}

impl RaftRsIngress for RaftRsGroupRegistry {
    fn step_wire_message(&self, message: RaftRsWireMessage) -> Result<()> {
        self.group_for(message.tablet_id)?
            .step_wire_message(message)
    }

    fn install_snapshot_transfer(
        &self,
        to: u64,
        transfer: RaftRsSnapshotTransfer,
    ) -> Result<Vec<u8>> {
        self.group_for(transfer.tablet_id)?
            .install_snapshot_transfer(to, transfer)
    }
}
