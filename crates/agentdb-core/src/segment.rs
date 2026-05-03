use serde::{Deserialize, Serialize};

use crate::event::EventEnvelope;
use crate::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSegment {
    pub segment_id: Id,
    pub tenant_id: Id,
    pub session_id: Id,
    pub seq_start: u64,
    pub seq_end: u64,
    pub event_count: u64,
    pub bytes: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_key_version: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSegmentFile {
    pub runtime_version: String,
    pub segment_id: Id,
    pub tenant_id: Id,
    pub session_id: Id,
    pub seq_start: u64,
    pub seq_end: u64,
    pub events: Vec<EventEnvelope>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionArchiveManifest {
    pub manifest_id: Id,
    pub tenant_id: Id,
    pub session_id: Id,
    pub source_segment_ids: Vec<Id>,
    pub event_count: u64,
    pub bytes: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_key_version: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionArchiveFile {
    pub runtime_version: String,
    pub manifest_id: Id,
    pub tenant_id: Id,
    pub session_id: Id,
    pub source_segment_ids: Vec<Id>,
    pub events: Vec<EventEnvelope>,
}

impl SessionSegment {
    pub fn new(
        tenant_id: crate::Id,
        session_id: crate::Id,
        seq_start: u64,
        seq_end: u64,
        bytes: u64,
        sha256: String,
        content_key_id: Option<String>,
        content_key_version: Option<String>,
    ) -> Self {
        Self {
            segment_id: Id::new(IdKind::Segment),
            tenant_id,
            session_id,
            seq_start,
            seq_end,
            event_count: seq_end.saturating_sub(seq_start) + 1,
            bytes,
            sha256,
            content_key_id,
            content_key_version,
            created_at_ms: crate::now_ms(),
        }
    }
}

impl SessionSegmentFile {
    pub fn from_events(segment: &SessionSegment, events: Vec<EventEnvelope>) -> Self {
        Self {
            runtime_version: crate::RUNTIME_VERSION.to_string(),
            segment_id: segment.segment_id.clone(),
            tenant_id: segment.tenant_id.clone(),
            session_id: segment.session_id.clone(),
            seq_start: segment.seq_start,
            seq_end: segment.seq_end,
            events,
        }
    }
}

impl SessionArchiveFile {
    pub fn from_events(
        manifest: &SessionArchiveManifest,
        source_segment_ids: Vec<Id>,
        events: Vec<EventEnvelope>,
    ) -> Self {
        Self {
            runtime_version: crate::RUNTIME_VERSION.to_string(),
            manifest_id: manifest.manifest_id.clone(),
            tenant_id: manifest.tenant_id.clone(),
            session_id: manifest.session_id.clone(),
            source_segment_ids,
            events,
        }
    }
}
