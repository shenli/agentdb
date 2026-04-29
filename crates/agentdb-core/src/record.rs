//! V2.2 searchable record runtime type.
//!
//! See `docs/v2_design/agent-runtime-database.md` §9.4 + §16. Searchable
//! records are framework-provided records for memory / knowledge / recall
//! use cases. AgentDB stores them with a tenant-scoped namespace + scope and
//! caller-supplied indexable text and embeddings, but does NOT decide what
//! to index — the caller passes an [`IndexingPolicy`] and (optionally) the
//! embedding. Search returns matching record_ids in tenant + namespace
//! scope.

use serde::{Deserialize, Serialize};

use crate::event::SourceRef;
use crate::id::{Id, IdKind};

/// What the runtime is allowed to index for a record.
///
/// V2.2 supports four modes; "selected fields" is the precise option for
/// callers who want to index a specific text column (e.g. `title`) without
/// indexing the rest of the metadata blob. V2.4 will widen this with
/// per-field configuration and dot-path field references.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexingMode {
    /// Caller is opting out of any text indexing (e.g. structured-only
    /// memory records that only need point lookup by `record_id`).
    NoIndexing,
    /// Index every string value reachable from the `metadata` JSON tree
    /// plus the `tags` list. The `text` field is NOT indexed in this
    /// mode — callers who want full text indexing pick a more explicit
    /// mode below.
    MetadataOnly,
    /// Index only the named top-level fields from `metadata`, plus
    /// `tags`. Field names are top-level keys; nested dot-paths are
    /// not interpreted in V2.2.
    SelectedFields,
    /// Index only the `text` field, truncated to the first 256 chars.
    /// The intent is "this `text` is the caller-prepared summary";
    /// V2.4 may distinguish summary from full text more carefully.
    SummaryOnly,
}

/// Caller-supplied indexing policy. The runtime never picks a default
/// other than `IndexingPolicy::default()` (conservative, no free-text
/// indexing); callers must opt in to free-text or vector indexing
/// explicitly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexingPolicy {
    pub mode: IndexingMode,
    /// Field names to index when `mode == SelectedFields`. Ignored
    /// otherwise. Each name is a top-level key in the `metadata` JSON
    /// object; nested dot-paths are not interpreted in V2.2.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    /// If true, the caller-supplied `embedding` is included in the
    /// vector index for nearest-neighbor search. False = vector-denied
    /// even if an embedding is attached to the record.
    pub vector_allowed: bool,
}

impl Default for IndexingPolicy {
    fn default() -> Self {
        // Conservative default: index metadata and tags but not free
        // text. Callers who want full text indexing pass MetadataOnly
        // (already the default), SelectedFields with the text field
        // name, or SummaryOnly. Vector indexing is denied by default —
        // a caller must explicitly opt in to nearest-neighbor lookup.
        Self {
            mode: IndexingMode::MetadataOnly,
            fields: vec![],
            vector_allowed: false,
        }
    }
}

/// A V2.2 searchable record. Stored under
/// `record/searchable/{tenant_id}/{record_id}` and indexed (per
/// [`IndexingPolicy`]) into the per-(tenant, namespace) text and vector
/// indexes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchableRecord {
    pub record_id: Id,
    pub tenant_id: Id,
    /// Namespace path, e.g. `["agent", "support", "kb"]`. Stored as a
    /// `Vec` and joined to a slash-separated string for index path /
    /// KV key encoding via [`Self::namespace_path`].
    pub namespace: Vec<String>,
    /// Optional explicit scope, e.g.
    /// `{"agent_id": "agt_...", "customer_id": "..."}`. Used as a
    /// post-filter on search results; the runtime does NOT validate
    /// the inner shape in V2.2.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub scope: serde_json::Value,
    /// Free-text content. Indexed per `policy.mode`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Free-form metadata. Indexed per `policy.mode`.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub metadata: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Source provenance: where did this record come from
    /// (event/artifact/skill).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceRef>,
    /// Caller-supplied vector embedding. Indexed only when
    /// `policy.vector_allowed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
    pub policy: IndexingPolicy,
    pub created_at_ms: u64,
    /// See `Tenant::deleted_at_ms` for the soft-delete contract that
    /// applies uniformly to every V2 runtime object.
    pub deleted_at_ms: Option<u64>,
    /// Optional caller-set expiration (ms since epoch). Search results
    /// filter expired records; GC physically removes them in a future
    /// milestone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewRecord {
    pub tenant_id: Id,
    pub namespace: Vec<String>,
    pub scope: serde_json::Value,
    pub text: Option<String>,
    pub metadata: serde_json::Value,
    pub tags: Vec<String>,
    pub source: Option<SourceRef>,
    pub embedding: Option<Vec<f32>>,
    pub policy: IndexingPolicy,
    pub expires_at_ms: Option<u64>,
}

impl SearchableRecord {
    pub fn create(req: NewRecord) -> Self {
        Self {
            record_id: Id::new(IdKind::Record),
            tenant_id: req.tenant_id,
            namespace: req.namespace,
            scope: req.scope,
            text: req.text,
            metadata: req.metadata,
            tags: req.tags,
            source: req.source,
            embedding: req.embedding,
            policy: req.policy,
            created_at_ms: super::now_ms(),
            deleted_at_ms: None,
            expires_at_ms: req.expires_at_ms,
        }
    }

    /// Slash-joined namespace for index path / KV key encoding.
    pub fn namespace_path(&self) -> String {
        self.namespace.join("/")
    }

    /// True iff `expires_at_ms` is set and has passed. Callers use this
    /// to filter expired records out of reads (`get_record`,
    /// `search_records`, hydration) — V2.4 GC will physically remove
    /// expired rows from the KV store.
    pub fn is_expired(&self) -> bool {
        match self.expires_at_ms {
            Some(exp) => super::now_ms() >= exp,
            None => false,
        }
    }
}
