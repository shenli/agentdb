//! Tablet state-machine boundary.
//!
//! Runtime append/record code still writes through `Persist` today. This module
//! is the target seam: commands are serialized into a tablet log, committed, and
//! then applied deterministically to tablet-local state. The current
//! implementation works with `SingleNodeReplicatedLog`; replacing the log with
//! Raft should not change command application semantics.

use std::sync::{Arc, Mutex};

use agentdb_storage::{
    KvStore, LogCommit, ReplicatedLog, Result as StorageResult, SingleNodeReplicatedLog,
    TabletKvStore, WriteOp,
};

const APPLIED_INDEX_KEY: &[u8] = b"\0agentdb/tablet/applied_index";
const RESERVED_KEY_PREFIX: &[u8] = b"\0agentdb/";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TabletCommand {
    WriteBatch { ops: Vec<TabletWriteOp> },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TabletWriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl From<WriteOp> for TabletWriteOp {
    fn from(op: WriteOp) -> Self {
        match op {
            WriteOp::Put { key, value } => Self::Put { key, value },
            WriteOp::Delete { key } => Self::Delete { key },
        }
    }
}

impl From<TabletWriteOp> for WriteOp {
    fn from(op: TabletWriteOp) -> Self {
        match op {
            TabletWriteOp::Put { key, value } => Self::Put { key, value },
            TabletWriteOp::Delete { key } => Self::Delete { key },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletApplyResult {
    pub tablet_id: i64,
    pub committed_index: u64,
    pub applied_index: u64,
}

pub trait TabletStateMachine: Send + Sync {
    fn commit(&self, command: TabletCommand) -> StorageResult<TabletApplyResult>;
    fn apply_committed(&self) -> StorageResult<usize>;
    fn applied_index(&self) -> u64;
}

pub struct LoggedTabletStateMachine {
    tablet_id: i64,
    log: Arc<dyn ReplicatedLog + Send + Sync>,
    store: TabletKvStore,
    applied_index: Mutex<u64>,
}

impl LoggedTabletStateMachine {
    pub fn single_node(kv: Arc<dyn KvStore + Send + Sync>, tablet_id: i64) -> StorageResult<Self> {
        let log = Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
        Self::new(tablet_id, log, TabletKvStore::new(kv, tablet_id)?)
    }

    pub fn new(
        tablet_id: i64,
        log: Arc<dyn ReplicatedLog + Send + Sync>,
        store: TabletKvStore,
    ) -> StorageResult<Self> {
        if tablet_id < 0 {
            return Err(agentdb_storage::Error::Other(
                "tablet_id must be non-negative".into(),
            ));
        }
        let applied_index = load_applied_index(&store)?;
        Ok(Self {
            tablet_id,
            log,
            store,
            applied_index: Mutex::new(applied_index),
        })
    }

    pub fn store(&self) -> &TabletKvStore {
        &self.store
    }
}

impl TabletStateMachine for LoggedTabletStateMachine {
    fn commit(&self, command: TabletCommand) -> StorageResult<TabletApplyResult> {
        let command_bytes = serde_json::to_vec(&command).map_err(|err| {
            agentdb_storage::Error::Other(format!("encode tablet command: {err}"))
        })?;
        let LogCommit {
            tablet_id, index, ..
        } = self.log.append(self.tablet_id, command_bytes)?;
        self.apply_committed()?;
        Ok(TabletApplyResult {
            tablet_id,
            committed_index: index,
            applied_index: self.applied_index(),
        })
    }

    fn apply_committed(&self) -> StorageResult<usize> {
        let mut applied_index = self
            .applied_index
            .lock()
            .expect("tablet applied-index lock poisoned");
        let entries = self
            .log
            .committed_entries(self.tablet_id, *applied_index, usize::MAX)?;
        let mut applied = 0;
        for entry in entries {
            let command: TabletCommand = serde_json::from_slice(&entry.command).map_err(|err| {
                agentdb_storage::Error::Other(format!(
                    "decode tablet command at index {}: {err}",
                    entry.index
                ))
            })?;
            apply_command(&self.store, command, entry.index)?;
            *applied_index = entry.index;
            applied += 1;
        }
        Ok(applied)
    }

    fn applied_index(&self) -> u64 {
        *self
            .applied_index
            .lock()
            .expect("tablet applied-index lock poisoned")
    }
}

fn load_applied_index(store: &TabletKvStore) -> StorageResult<u64> {
    let Some(bytes) = store.get(APPLIED_INDEX_KEY)? else {
        return Ok(0);
    };
    let raw: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| agentdb_storage::Error::Other("invalid tablet applied index".into()))?;
    Ok(u64::from_be_bytes(raw))
}

fn apply_command(
    store: &TabletKvStore,
    command: TabletCommand,
    applied_index: u64,
) -> StorageResult<()> {
    match command {
        TabletCommand::WriteBatch { ops } => {
            for op in &ops {
                validate_write_op_key(op)?;
            }
            let mut write_ops: Vec<WriteOp> = ops.into_iter().map(WriteOp::from).collect();
            write_ops.push(WriteOp::Put {
                key: APPLIED_INDEX_KEY.to_vec(),
                value: applied_index.to_be_bytes().to_vec(),
            });
            store.write_batch(write_ops)
        }
    }
}

fn validate_write_op_key(op: &TabletWriteOp) -> StorageResult<()> {
    let key = match op {
        TabletWriteOp::Put { key, .. } | TabletWriteOp::Delete { key } => key,
    };
    if key.starts_with(RESERVED_KEY_PREFIX) {
        return Err(agentdb_storage::Error::Other(
            "tablet command targets reserved keyspace".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_storage::RocksKvStore;

    #[test]
    fn commit_appends_and_applies_batch_to_tablet_store() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let machine = LoggedTabletStateMachine::single_node(kv, 7).unwrap();

        let result = machine
            .commit(TabletCommand::WriteBatch {
                ops: vec![
                    TabletWriteOp::Put {
                        key: b"session/s1".to_vec(),
                        value: b"v1".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: b"record/r1".to_vec(),
                        value: b"v2".to_vec(),
                    },
                ],
            })
            .unwrap();

        assert_eq!(result.tablet_id, 7);
        assert_eq!(result.committed_index, 1);
        assert_eq!(result.applied_index, 1);
        assert_eq!(
            machine.store().get(b"session/s1").unwrap().as_deref(),
            Some(&b"v1"[..])
        );
        assert_eq!(
            machine.store().get(b"record/r1").unwrap().as_deref(),
            Some(&b"v2"[..])
        );
    }

    #[test]
    fn delete_command_removes_only_tablet_local_key() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let machine = LoggedTabletStateMachine::single_node(kv.clone(), 7).unwrap();
        let other = TabletKvStore::new(kv, 8).unwrap();

        machine.store().put(b"k", b"v7").unwrap();
        other.put(b"k", b"v8").unwrap();
        machine
            .commit(TabletCommand::WriteBatch {
                ops: vec![TabletWriteOp::Delete { key: b"k".to_vec() }],
            })
            .unwrap();

        assert!(machine.store().get(b"k").unwrap().is_none());
        assert_eq!(other.get(b"k").unwrap().as_deref(), Some(&b"v8"[..]));
    }

    #[test]
    fn apply_committed_replays_existing_log_entries_once() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
        let store = TabletKvStore::new(kv.clone(), 7).unwrap();
        let command = TabletCommand::WriteBatch {
            ops: vec![TabletWriteOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        };
        log.append(7, serde_json::to_vec(&command).unwrap())
            .unwrap();

        let machine = LoggedTabletStateMachine::new(7, log, store).unwrap();
        assert_eq!(machine.apply_committed().unwrap(), 1);
        assert_eq!(machine.apply_committed().unwrap(), 0);
        assert_eq!(
            machine.store().get(b"k").unwrap().as_deref(),
            Some(&b"v"[..])
        );
        assert_eq!(machine.applied_index(), 1);
    }

    #[test]
    fn applied_index_is_loaded_from_tablet_store_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
        let store = TabletKvStore::new(kv.clone(), 7).unwrap();
        let command = TabletCommand::WriteBatch {
            ops: vec![TabletWriteOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        };
        log.append(7, serde_json::to_vec(&command).unwrap())
            .unwrap();

        let first = LoggedTabletStateMachine::new(7, log.clone(), store).unwrap();
        assert_eq!(first.apply_committed().unwrap(), 1);
        assert_eq!(first.applied_index(), 1);

        let restarted =
            LoggedTabletStateMachine::new(7, log, TabletKvStore::new(kv, 7).unwrap()).unwrap();
        assert_eq!(restarted.applied_index(), 1);
        assert_eq!(restarted.apply_committed().unwrap(), 0);
    }

    #[test]
    fn command_cannot_write_internal_tablet_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let machine = LoggedTabletStateMachine::single_node(kv, 7).unwrap();

        let err = machine
            .commit(TabletCommand::WriteBatch {
                ops: vec![TabletWriteOp::Put {
                    key: APPLIED_INDEX_KEY.to_vec(),
                    value: 99_u64.to_be_bytes().to_vec(),
                }],
            })
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("tablet command targets reserved keyspace"));
        assert_eq!(machine.applied_index(), 0);
    }
}
