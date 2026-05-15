//! Tablet state-machine boundary.
//!
//! Tablet write commands are serialized into a tablet log, committed, and then
//! applied deterministically to tablet-local state. The current implementation
//! works with `SingleNodeReplicatedLog`; replacing the log with Raft should not
//! change command application semantics.

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TabletStateSnapshot {
    pub tablet_id: i64,
    pub applied_index: u64,
    pub entries: Vec<TabletSnapshotEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TabletSnapshotEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TabletStorageStats {
    pub tablet_id: i64,
    pub key_count: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
    pub storage_bytes: u64,
    pub applied_index: u64,
    pub compaction_debt_ops: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TabletSnapshotShardFilter {
    pub target_tablet_id: i64,
    pub start_shard: i64,
    pub end_shard: i64,
    pub shard_count: i64,
}

impl TabletSnapshotShardFilter {
    fn validate(&self) -> StorageResult<()> {
        if self.target_tablet_id < 0 {
            return Err(agentdb_storage::Error::Other(
                "target_tablet_id must be non-negative".into(),
            ));
        }
        if self.shard_count <= 0 {
            return Err(agentdb_storage::Error::Other(
                "shard_count must be positive".into(),
            ));
        }
        if self.start_shard < 0 || self.end_shard < self.start_shard {
            return Err(agentdb_storage::Error::Other(format!(
                "invalid shard range {}..{}",
                self.start_shard, self.end_shard
            )));
        }
        if self.end_shard >= self.shard_count {
            return Err(agentdb_storage::Error::Other(format!(
                "shard range {}..{} exceeds shard_count {}",
                self.start_shard, self.end_shard, self.shard_count
            )));
        }
        Ok(())
    }

    fn contains_key(&self, key: &[u8], value: &[u8]) -> StorageResult<bool> {
        self.validate()?;
        let placement = classify_snapshot_entry(key, value)?;
        if placement.routing_key.is_none() {
            return Ok(true);
        }
        let shard = stable_shard(
            &placement.tenant_id,
            placement.routing_key.as_deref(),
            self.shard_count,
        );
        Ok(shard >= self.start_shard && shard <= self.end_shard)
    }
}

impl TabletStateSnapshot {
    pub fn retarget(&self, target_tablet_id: i64) -> StorageResult<Self> {
        if target_tablet_id < 0 {
            return Err(agentdb_storage::Error::Other(
                "target_tablet_id must be non-negative".into(),
            ));
        }
        Ok(Self {
            tablet_id: target_tablet_id,
            applied_index: self.applied_index,
            entries: self.entries.clone(),
        })
    }

    pub fn filter_for_shard_range(&self, filter: TabletSnapshotShardFilter) -> StorageResult<Self> {
        filter.validate()?;
        let mut entries = Vec::new();
        for entry in &self.entries {
            if filter.contains_key(&entry.key, &entry.value)? {
                entries.push(entry.clone());
            }
        }
        Ok(Self {
            tablet_id: filter.target_tablet_id,
            applied_index: self.applied_index,
            entries,
        })
    }
}

pub trait TabletStateMachine: Send + Sync {
    fn commit(&self, command: TabletCommand) -> StorageResult<TabletApplyResult>;
    fn apply_committed(&self) -> StorageResult<usize>;
    fn applied_index(&self) -> u64;
    fn storage_stats(&self) -> StorageResult<TabletStorageStats>;
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

    pub fn export_snapshot(&self) -> StorageResult<TabletStateSnapshot> {
        self.apply_committed()?;
        let mut entries = Vec::new();
        for item in self.store.scan_prefix(b"")? {
            let (key, value) = item?;
            if key.starts_with(RESERVED_KEY_PREFIX) {
                continue;
            }
            entries.push(TabletSnapshotEntry { key, value });
        }
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(TabletStateSnapshot {
            tablet_id: self.tablet_id,
            applied_index: self.applied_index(),
            entries,
        })
    }

    pub fn export_snapshot_for_shard_range(
        &self,
        filter: TabletSnapshotShardFilter,
    ) -> StorageResult<TabletStateSnapshot> {
        self.export_snapshot()?.filter_for_shard_range(filter)
    }

    pub fn install_snapshot(&self, snapshot: &TabletStateSnapshot) -> StorageResult<()> {
        if snapshot.tablet_id != self.tablet_id {
            return Err(agentdb_storage::Error::Other(format!(
                "snapshot tablet_id {} does not match state machine tablet_id {}",
                snapshot.tablet_id, self.tablet_id
            )));
        }
        for entry in &snapshot.entries {
            validate_user_key(&entry.key)?;
        }

        let mut ops = Vec::new();
        for item in self.store.scan_prefix(b"")? {
            let (key, _) = item?;
            ops.push(WriteOp::Delete { key });
        }
        for entry in &snapshot.entries {
            ops.push(WriteOp::Put {
                key: entry.key.clone(),
                value: entry.value.clone(),
            });
        }
        ops.push(WriteOp::Put {
            key: APPLIED_INDEX_KEY.to_vec(),
            value: snapshot.applied_index.to_be_bytes().to_vec(),
        });
        self.store.write_batch(ops)?;
        *self
            .applied_index
            .lock()
            .expect("tablet applied-index lock poisoned") = snapshot.applied_index;
        Ok(())
    }

    pub fn install_snapshot_merge(&self, snapshot: &TabletStateSnapshot) -> StorageResult<()> {
        if snapshot.tablet_id != self.tablet_id {
            return Err(agentdb_storage::Error::Other(format!(
                "snapshot tablet_id {} does not match state machine tablet_id {}",
                snapshot.tablet_id, self.tablet_id
            )));
        }
        for entry in &snapshot.entries {
            validate_user_key(&entry.key)?;
        }

        let next_applied_index = self.applied_index().max(snapshot.applied_index);
        let mut ops = Vec::with_capacity(snapshot.entries.len() + 1);
        for entry in &snapshot.entries {
            if let Some(existing) = self.store.get(&entry.key)? {
                if existing != entry.value {
                    return Err(agentdb_storage::Error::Other(format!(
                        "snapshot merge conflict for key `{}`",
                        String::from_utf8_lossy(&entry.key)
                    )));
                }
            }
            ops.push(WriteOp::Put {
                key: entry.key.clone(),
                value: entry.value.clone(),
            });
        }
        ops.push(WriteOp::Put {
            key: APPLIED_INDEX_KEY.to_vec(),
            value: next_applied_index.to_be_bytes().to_vec(),
        });
        self.store.write_batch(ops)?;
        *self
            .applied_index
            .lock()
            .expect("tablet applied-index lock poisoned") = next_applied_index;
        Ok(())
    }

    pub fn compact_log_through(&self, up_to_index: u64) -> StorageResult<usize> {
        if up_to_index > self.applied_index() {
            return Err(agentdb_storage::Error::Other(format!(
                "cannot compact through index {up_to_index}; applied index is {}",
                self.applied_index()
            )));
        }
        self.log.compact(self.tablet_id, up_to_index)
    }

    pub fn storage_stats(&self) -> StorageResult<TabletStorageStats> {
        self.apply_committed()?;
        let mut key_count = 0u64;
        let mut key_bytes = 0u64;
        let mut value_bytes = 0u64;
        for item in self.store.scan_prefix(b"")? {
            let (key, value) = item?;
            if key.starts_with(RESERVED_KEY_PREFIX) {
                continue;
            }
            key_count = key_count.saturating_add(1);
            key_bytes = key_bytes.saturating_add(key.len() as u64);
            value_bytes = value_bytes.saturating_add(value.len() as u64);
        }
        let compaction_debt_ops = self
            .log
            .committed_entries(self.tablet_id, 0, usize::MAX)?
            .len() as u64;
        Ok(TabletStorageStats {
            tablet_id: self.tablet_id,
            key_count,
            key_bytes,
            value_bytes,
            storage_bytes: key_bytes.saturating_add(value_bytes),
            applied_index: self.applied_index(),
            compaction_debt_ops,
        })
    }
}

impl TabletStateMachine for LoggedTabletStateMachine {
    fn commit(&self, command: TabletCommand) -> StorageResult<TabletApplyResult> {
        let command_bytes = encode_tablet_command(&command)?;
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
            let command = decode_tablet_command(&entry.command).map_err(|err| {
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

    fn storage_stats(&self) -> StorageResult<TabletStorageStats> {
        LoggedTabletStateMachine::storage_stats(self)
    }
}

const TABLET_COMMAND_MAGIC: &[u8; 8] = b"ADBTCM01";
const TABLET_COMMAND_WRITE_BATCH: u8 = 1;
const TABLET_WRITE_PUT: u8 = 1;
const TABLET_WRITE_DELETE: u8 = 2;

fn encode_tablet_command(command: &TabletCommand) -> StorageResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(TABLET_COMMAND_MAGIC);
    match command {
        TabletCommand::WriteBatch { ops } => {
            out.push(TABLET_COMMAND_WRITE_BATCH);
            write_u64(&mut out, ops.len() as u64);
            for op in ops {
                match op {
                    TabletWriteOp::Put { key, value } => {
                        out.push(TABLET_WRITE_PUT);
                        write_bytes(&mut out, key);
                        write_bytes(&mut out, value);
                    }
                    TabletWriteOp::Delete { key } => {
                        out.push(TABLET_WRITE_DELETE);
                        write_bytes(&mut out, key);
                    }
                }
            }
        }
    }
    Ok(out)
}

fn decode_tablet_command(bytes: &[u8]) -> StorageResult<TabletCommand> {
    let mut cursor = TabletCommandCursor::new(bytes);
    cursor.expect_magic(TABLET_COMMAND_MAGIC)?;
    let command_kind = cursor.read_u8()?;
    let command = match command_kind {
        TABLET_COMMAND_WRITE_BATCH => {
            let op_count = usize::try_from(cursor.read_u64()?).map_err(|_| {
                agentdb_storage::Error::Other("tablet command op count too large".into())
            })?;
            let mut ops = Vec::with_capacity(op_count.min(1024));
            for _ in 0..op_count {
                let op_kind = cursor.read_u8()?;
                let key = cursor.read_bytes()?.to_vec();
                let op = match op_kind {
                    TABLET_WRITE_PUT => {
                        let value = cursor.read_bytes()?.to_vec();
                        TabletWriteOp::Put { key, value }
                    }
                    TABLET_WRITE_DELETE => TabletWriteOp::Delete { key },
                    other => {
                        return Err(agentdb_storage::Error::Other(format!(
                            "unknown tablet write op kind {other}"
                        )));
                    }
                };
                ops.push(op);
            }
            TabletCommand::WriteBatch { ops }
        }
        other => {
            return Err(agentdb_storage::Error::Other(format!(
                "unknown tablet command kind {other}"
            )));
        }
    };
    cursor.expect_eof()?;
    Ok(command)
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

struct TabletCommandCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> TabletCommandCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8]) -> StorageResult<()> {
        let found = self.read_exact(magic.len())?;
        if found != magic {
            return Err(agentdb_storage::Error::Other(
                "invalid tablet command magic".into(),
            ));
        }
        Ok(())
    }

    fn expect_eof(&self) -> StorageResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(agentdb_storage::Error::Other(
                "trailing bytes in tablet command".into(),
            ))
        }
    }

    fn read_u8(&mut self) -> StorageResult<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u64(&mut self) -> StorageResult<u64> {
        let raw = self.read_exact(8)?;
        let raw: [u8; 8] = raw
            .try_into()
            .map_err(|_| agentdb_storage::Error::Other("invalid u64 length".into()))?;
        Ok(u64::from_be_bytes(raw))
    }

    fn read_bytes(&mut self) -> StorageResult<&'a [u8]> {
        let len = self.read_u64()?;
        let len = usize::try_from(len).map_err(|_| {
            agentdb_storage::Error::Other("tablet command byte field too large".into())
        })?;
        self.read_exact(len)
    }

    fn read_exact(&mut self, len: usize) -> StorageResult<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            agentdb_storage::Error::Other("tablet command offset overflow".into())
        })?;
        if end > self.bytes.len() {
            return Err(agentdb_storage::Error::Other(
                "truncated tablet command".into(),
            ));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
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
    validate_user_key(key)
}

fn validate_user_key(key: &[u8]) -> StorageResult<()> {
    if key.starts_with(RESERVED_KEY_PREFIX) {
        return Err(agentdb_storage::Error::Other(
            "tablet command targets reserved keyspace".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotEntryPlacement {
    tenant_id: String,
    routing_key: Option<String>,
}

fn classify_snapshot_entry(key: &[u8], value: &[u8]) -> StorageResult<SnapshotEntryPlacement> {
    let key = std::str::from_utf8(key).map_err(|err| {
        agentdb_storage::Error::Other(format!("snapshot entry key is not utf8: {err}"))
    })?;
    if let Some(tenant_id) = key.strip_prefix("record/tenant/") {
        return placement(tenant_id, None);
    }
    if key.starts_with("record/tenant_alias/") {
        let tenant_id = std::str::from_utf8(value).map_err(|err| {
            agentdb_storage::Error::Other(format!("tenant alias snapshot value is not utf8: {err}"))
        })?;
        return placement(tenant_id, None);
    }
    if let Some(rest) = key.strip_prefix("record/agent/") {
        return placement(first_segment(rest)?, None);
    }
    if let Some(rest) = key.strip_prefix("record/agent_alias/") {
        return placement(first_segment(rest)?, None);
    }
    if let Some(rest) = key.strip_prefix("record/session/") {
        let (tenant_id, session_id) = first_two_segments(rest)?;
        return placement(tenant_id, Some(session_id));
    }
    if let Some(rest) = key.strip_prefix("record/session_alias/") {
        let tenant_id = first_segment(rest)?;
        let session_id = std::str::from_utf8(value).map_err(|err| {
            agentdb_storage::Error::Other(format!(
                "session alias snapshot value is not utf8: {err}"
            ))
        })?;
        return placement(tenant_id, Some(session_id));
    }
    if let Some(rest) = key.strip_prefix("record/session_segment/") {
        let (tenant_id, session_id) = first_two_segments(rest)?;
        return placement(tenant_id, Some(session_id));
    }
    if let Some(rest) = key.strip_prefix("record/session_by_agent/") {
        let mut parts = rest.splitn(3, '/');
        let tenant_id = parts.next().unwrap_or_default();
        let _agent_id = parts.next().unwrap_or_default();
        let session_id = parts.next().unwrap_or_default();
        return placement(tenant_id, Some(session_id));
    }
    if let Some(rest) = key.strip_prefix("event/") {
        let (tenant_id, session_id) = first_two_segments(rest)?;
        return placement(tenant_id, Some(session_id));
    }
    if let Some(rest) = key.strip_prefix("append_log/") {
        let (tenant_id, session_id) = first_two_segments(rest)?;
        return placement(tenant_id, Some(session_id));
    }
    if let Some(rest) = key.strip_prefix("event_by_id/") {
        let tenant_id = first_segment(rest)?;
        let pointer: serde_json::Value = serde_json::from_slice(value).map_err(|err| {
            agentdb_storage::Error::Other(format!("event_by_id snapshot value is invalid: {err}"))
        })?;
        let session_id = pointer
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                agentdb_storage::Error::Other(
                    "event_by_id snapshot value is missing session_id".into(),
                )
            })?;
        return placement(tenant_id, Some(session_id));
    }
    if let Some(rest) = key.strip_prefix("record/idempotency/") {
        let mut parts = rest.splitn(4, "::");
        let tenant_id = parts.next().unwrap_or_default();
        let operation = parts.next().unwrap_or_default();
        let target = parts.next().unwrap_or_default();
        let routing_key = (operation == "append").then_some(target);
        return placement(tenant_id, routing_key);
    }
    for prefix in [
        "record/artifact/",
        "record/artifact_by_sha256/",
        "record/searchable/",
        "record/searchable_by_namespace/",
        "record/searchable_commit/",
        "record/searchable_index_outbox/",
        "record/skill/",
        "record/skill_unique/",
        "record/agent_skill/",
    ] {
        if let Some(rest) = key.strip_prefix(prefix) {
            return placement(first_segment(rest)?, None);
        }
    }
    Err(agentdb_storage::Error::Other(format!(
        "snapshot entry key `{key}` cannot be classified for shard movement"
    )))
}

fn placement(tenant_id: &str, routing_key: Option<&str>) -> StorageResult<SnapshotEntryPlacement> {
    if tenant_id.is_empty() {
        return Err(agentdb_storage::Error::Other(
            "snapshot entry tenant_id is empty".into(),
        ));
    }
    Ok(SnapshotEntryPlacement {
        tenant_id: tenant_id.to_string(),
        routing_key: routing_key
            .filter(|value| !value.is_empty())
            .map(str::to_string),
    })
}

fn first_segment(value: &str) -> StorageResult<&str> {
    value
        .split('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| agentdb_storage::Error::Other("missing key segment".into()))
}

fn first_two_segments(value: &str) -> StorageResult<(&str, &str)> {
    let mut parts = value.split('/');
    let first = parts
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| agentdb_storage::Error::Other("missing first key segment".into()))?;
    let second = parts
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| agentdb_storage::Error::Other("missing second key segment".into()))?;
    Ok((first, second))
}

fn stable_shard(tenant_id: &str, routing_key: Option<&str>, shard_count: i64) -> i64 {
    let mut bytes =
        Vec::with_capacity(tenant_id.len() + 1 + routing_key.map(str::len).unwrap_or_default());
    bytes.extend_from_slice(tenant_id.as_bytes());
    if let Some(routing_key) = routing_key.filter(|key| !key.is_empty()) {
        bytes.push(0xff);
        bytes.extend_from_slice(routing_key.as_bytes());
    }
    (fnv1a64(&bytes) % shard_count as u64) as i64
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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
    fn storage_stats_reports_user_key_bytes_and_applied_index() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let machine = LoggedTabletStateMachine::single_node(kv, 7).unwrap();

        machine
            .commit(TabletCommand::WriteBatch {
                ops: vec![
                    TabletWriteOp::Put {
                        key: b"k1".to_vec(),
                        value: b"value-one".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: b"k2".to_vec(),
                        value: b"value-two".to_vec(),
                    },
                ],
            })
            .unwrap();

        let stats = machine.storage_stats().unwrap();
        assert_eq!(stats.tablet_id, 7);
        assert_eq!(stats.key_count, 2);
        assert_eq!(stats.key_bytes, 4);
        assert_eq!(stats.value_bytes, 18);
        assert_eq!(stats.storage_bytes, 22);
        assert_eq!(stats.applied_index, 1);
        assert_eq!(stats.compaction_debt_ops, 1);

        assert_eq!(machine.compact_log_through(1).unwrap(), 1);
        assert_eq!(machine.storage_stats().unwrap().compaction_debt_ops, 0);
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
        log.append(7, encode_tablet_command(&command).unwrap())
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
        log.append(7, encode_tablet_command(&command).unwrap())
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

    #[test]
    fn snapshot_exports_applied_state_without_internal_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let machine = LoggedTabletStateMachine::single_node(kv, 7).unwrap();
        machine
            .commit(TabletCommand::WriteBatch {
                ops: vec![
                    TabletWriteOp::Put {
                        key: b"b".to_vec(),
                        value: b"2".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: b"a".to_vec(),
                        value: b"1".to_vec(),
                    },
                ],
            })
            .unwrap();

        let snapshot = machine.export_snapshot().unwrap();

        assert_eq!(snapshot.tablet_id, 7);
        assert_eq!(snapshot.applied_index, 1);
        assert_eq!(
            snapshot.entries,
            vec![
                TabletSnapshotEntry {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                TabletSnapshotEntry {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                },
            ]
        );
    }

    #[test]
    fn snapshot_install_replaces_tablet_state_and_applied_index() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_kv = Arc::new(RocksKvStore::open(source_dir.path()).unwrap());
        let source = LoggedTabletStateMachine::single_node(source_kv, 7).unwrap();
        source
            .commit(TabletCommand::WriteBatch {
                ops: vec![TabletWriteOp::Put {
                    key: b"k".to_vec(),
                    value: b"fresh".to_vec(),
                }],
            })
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let target_dir = tempfile::tempdir().unwrap();
        let target_kv = Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
        let target = LoggedTabletStateMachine::single_node(target_kv.clone(), 7).unwrap();
        target.store().put(b"stale", b"old").unwrap();

        target.install_snapshot(&snapshot).unwrap();

        assert_eq!(target.applied_index(), snapshot.applied_index);
        assert_eq!(
            target.store().get(b"k").unwrap().as_deref(),
            Some(&b"fresh"[..])
        );
        assert!(target.store().get(b"stale").unwrap().is_none());

        let restarted = LoggedTabletStateMachine::single_node(target_kv.clone(), 7).unwrap();
        assert_eq!(restarted.applied_index(), snapshot.applied_index);
        assert_eq!(
            restarted.store().get(b"k").unwrap().as_deref(),
            Some(&b"fresh"[..])
        );
    }

    #[test]
    fn snapshot_filter_retargets_and_keeps_only_requested_shards() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let source = LoggedTabletStateMachine::single_node(kv, 7).unwrap();
        let session_a = "sess_a";
        let shard_a = stable_shard("tenant-a", Some(session_a), 64);
        let session_b = (0..128)
            .map(|idx| format!("sess_b_{idx}"))
            .find(|session| stable_shard("tenant-a", Some(session), 64) != shard_a)
            .unwrap();
        let shard_b = stable_shard("tenant-a", Some(session_b.as_str()), 64);
        assert_ne!(shard_a, shard_b);
        source
            .commit(TabletCommand::WriteBatch {
                ops: vec![
                    TabletWriteOp::Put {
                        key: b"record/tenant/tenant-a".to_vec(),
                        value: b"tenant-global".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: format!("record/session/tenant-a/{session_a}").into_bytes(),
                        value: b"session-a".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: format!("event/tenant-a/{session_a}/00000000000000000001")
                            .into_bytes(),
                        value: b"event-a".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: format!("append_log/tenant-a/{session_a}/00000000000000000000")
                            .into_bytes(),
                        value: b"append-log-a".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: b"event_by_id/tenant-a/evt_a".to_vec(),
                        value: serde_json::to_vec(&serde_json::json!({
                            "session_id": session_a,
                            "seq": 1,
                        }))
                        .unwrap(),
                    },
                    TabletWriteOp::Put {
                        key: b"record/session_alias/tenant-a/current".to_vec(),
                        value: session_a.as_bytes().to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: format!("record/idempotency/tenant-a::append::{session_a}::idem-a")
                            .into_bytes(),
                        value: b"idempotency-a".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: format!("record/session/tenant-a/{session_b}").into_bytes(),
                        value: b"session-b".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: format!("append_log/tenant-a/{session_b}/00000000000000000000")
                            .into_bytes(),
                        value: b"append-log-b".to_vec(),
                    },
                    TabletWriteOp::Put {
                        key: b"event_by_id/tenant-a/evt_b".to_vec(),
                        value: serde_json::to_vec(&serde_json::json!({
                            "session_id": session_b,
                            "seq": 1,
                        }))
                        .unwrap(),
                    },
                ],
            })
            .unwrap();

        let filtered = source
            .export_snapshot_for_shard_range(TabletSnapshotShardFilter {
                target_tablet_id: 8,
                start_shard: shard_a,
                end_shard: shard_a,
                shard_count: 64,
            })
            .unwrap();

        assert_eq!(filtered.tablet_id, 8);
        assert_eq!(filtered.applied_index, 1);
        let keys: Vec<_> = filtered
            .entries
            .iter()
            .map(|entry| String::from_utf8(entry.key.clone()).unwrap())
            .collect();
        assert_eq!(
            keys,
            vec![
                format!("append_log/tenant-a/{session_a}/00000000000000000000"),
                format!("event/tenant-a/{session_a}/00000000000000000001"),
                "event_by_id/tenant-a/evt_a".to_string(),
                format!("record/idempotency/tenant-a::append::{session_a}::idem-a"),
                format!("record/session/tenant-a/{session_a}"),
                "record/session_alias/tenant-a/current".to_string(),
                "record/tenant/tenant-a".to_string(),
            ]
        );
    }

    #[test]
    fn snapshot_merge_install_preserves_existing_entries() {
        let target_dir = tempfile::tempdir().unwrap();
        let target_kv = Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
        let target = LoggedTabletStateMachine::single_node(target_kv, 9).unwrap();
        target.store().put(b"existing", b"left").unwrap();
        let snapshot = TabletStateSnapshot {
            tablet_id: 9,
            applied_index: 4,
            entries: vec![TabletSnapshotEntry {
                key: b"incoming".to_vec(),
                value: b"right".to_vec(),
            }],
        };

        target.install_snapshot_merge(&snapshot).unwrap();

        assert_eq!(target.applied_index(), 4);
        assert_eq!(
            target.store().get(b"existing").unwrap().as_deref(),
            Some(&b"left"[..])
        );
        assert_eq!(
            target.store().get(b"incoming").unwrap().as_deref(),
            Some(&b"right"[..])
        );
    }

    #[test]
    fn snapshot_merge_install_rejects_conflicting_existing_entries() {
        let target_dir = tempfile::tempdir().unwrap();
        let target_kv = Arc::new(RocksKvStore::open(target_dir.path()).unwrap());
        let target = LoggedTabletStateMachine::single_node(target_kv, 9).unwrap();
        target.store().put(b"shared", b"old").unwrap();
        let snapshot = TabletStateSnapshot {
            tablet_id: 9,
            applied_index: 4,
            entries: vec![TabletSnapshotEntry {
                key: b"shared".to_vec(),
                value: b"new".to_vec(),
            }],
        };

        let err = target.install_snapshot_merge(&snapshot).unwrap_err();

        assert!(err.to_string().contains("snapshot merge conflict"));
        assert_eq!(target.applied_index(), 0);
        assert_eq!(
            target.store().get(b"shared").unwrap().as_deref(),
            Some(&b"old"[..])
        );
    }

    #[test]
    fn snapshot_install_rejects_wrong_tablet() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let machine = LoggedTabletStateMachine::single_node(kv, 7).unwrap();
        let snapshot = TabletStateSnapshot {
            tablet_id: 8,
            applied_index: 1,
            entries: vec![],
        };

        let err = machine.install_snapshot(&snapshot).unwrap_err();

        assert!(err.to_string().contains("does not match state machine"));
    }

    #[test]
    fn compact_log_through_rejects_unapplied_index() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let machine = LoggedTabletStateMachine::single_node(kv, 7).unwrap();

        let err = machine.compact_log_through(1).unwrap_err();

        assert!(err.to_string().contains("cannot compact through index 1"));
    }

    #[test]
    fn compact_log_through_removes_covered_entries() {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(RocksKvStore::open(dir.path()).unwrap());
        let log = Arc::new(SingleNodeReplicatedLog::new(kv.clone()));
        let machine =
            LoggedTabletStateMachine::new(7, log.clone(), TabletKvStore::new(kv, 7).unwrap())
                .unwrap();

        machine
            .commit(TabletCommand::WriteBatch {
                ops: vec![TabletWriteOp::Put {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                }],
            })
            .unwrap();
        machine
            .commit(TabletCommand::WriteBatch {
                ops: vec![TabletWriteOp::Put {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                }],
            })
            .unwrap();

        assert_eq!(machine.compact_log_through(1).unwrap(), 1);

        let entries = log.committed_entries(7, 0, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 2);
    }
}
