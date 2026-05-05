//! Wire DTOs for the V2.0 HTTP routes.
//!
//! These are deliberately separate from the runtime types: HTTP path params
//! already carry `tenant_id`, `agent_id`, `session_id`, so the request bodies
//! avoid repeating them. Responses serialize the runtime types directly (no
//! shape change needed there) — only the request shapes need adapting.

use serde::{Deserialize, Serialize};

use agentdb_core::record::{IndexingPolicy, SearchableRecord};
use agentdb_core::{AgentSkillBinding, ArtifactRef, SourceRef};

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateTenantBody {
    pub external_key: String,
    #[serde(default = "default_policy")]
    pub policy: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateAgentBody {
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default = "default_policy")]
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateSessionBody {
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TransitionSessionLifecycleBody {
    pub target: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RestoreSessionStorageBody {
    #[serde(default = "default_restore_target")]
    pub target: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AppendBody {
    pub agent_id: String,
    pub kind: String,
    #[serde(default = "default_policy")]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub source_refs: Vec<SourceRef>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AppendAndFetchBody {
    pub agent_id: String,
    pub kind: String,
    #[serde(default = "default_policy")]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub source_refs: Vec<SourceRef>,
    #[serde(default = "default_last")]
    pub last: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ForkSessionBody {
    pub at_seq: u64,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FetchQuery {
    #[serde(default)]
    pub from_seq: u64,
    #[serde(default = "default_fetch_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayQuery {
    #[serde(default)]
    pub from_seq: Option<u64>,
    #[serde(default = "default_fetch_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WatchQuery {
    #[serde(default = "default_watch_scope")]
    pub scope: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub from_cursor: Option<String>,
    #[serde(default = "default_fetch_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IncludeDeletedQuery {
    #[serde(default)]
    pub include_deleted: bool,
}

fn default_policy() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

fn default_last() -> usize {
    20
}

fn default_max_bytes() -> usize {
    1_000_000
}

fn default_fetch_limit() -> usize {
    100
}

fn default_restore_target() -> String {
    "cold".into()
}

fn default_watch_scope() -> String {
    "tenant".into()
}

/// Request body for `POST /api/tenants/:tenant_id/records`.
///
/// `tenant_id` comes from the path; the body has every other field that
/// `agentdb_core::NewRecord` carries. All optional fields default sensibly via
/// serde so a minimally-shaped body — `{"namespace": ["..."]}` — works.
#[derive(Debug, Deserialize, Serialize)]
pub struct CreateRecordBody {
    #[serde(default)]
    pub namespace: Vec<String>,
    #[serde(default = "default_policy")]
    pub scope: serde_json::Value,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default = "default_policy")]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub source: Option<SourceRef>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    /// Caller-supplied indexing policy. If omitted, falls back to
    /// `IndexingPolicy::default()` (metadata-only, no vector).
    #[serde(default)]
    pub policy: Option<IndexingPolicy>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
}

/// Query params for `GET /api/tenants/:tenant_id/records/search`.
///
/// `namespace` and `tag` are repeatable. GET is for text/list searches; vector
/// search uses the JSON `POST /records/search` shape below so embeddings do not
/// have to be encoded into a URL.
#[derive(Debug, Deserialize, Serialize)]
pub struct SearchRecordsQuery {
    #[serde(default)]
    pub text: Option<String>,
    /// Repeatable: `?namespace=agent&namespace=support` →
    /// `["agent", "support"]`. Empty list means "search at tenant root".
    #[serde(default, rename = "namespace")]
    pub namespace: Vec<String>,
    /// Optional placement key. Present means use owner-local LSI-like index;
    /// absent means use GSI-like global discovery.
    #[serde(default)]
    pub sharding_key: Option<String>,
    /// Repeatable: `?tag=foo&tag=bar` → `["foo", "bar"]`. A record must
    /// carry every requested tag to match.
    #[serde(default, rename = "tag")]
    pub tags: Vec<String>,
    #[serde(default)]
    pub source_session: Option<String>,
    #[serde(default)]
    pub source_seq: Option<u64>,
    #[serde(default)]
    pub source_artifact: Option<String>,
    #[serde(default)]
    pub source_record: Option<String>,
    #[serde(default)]
    pub source_skill: Option<String>,
    #[serde(default)]
    pub source_event: Option<String>,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    #[serde(default)]
    pub min_visible_record_commit_seq: Option<u64>,
}

fn default_search_limit() -> usize {
    20
}

/// Request body for `POST /api/tenants/:tenant_id/records/search`.
///
/// Supports the full V2.2 search surface, including caller-supplied vector
/// embeddings. `source` is a partial [`SourceRef`]: every populated field must
/// match the candidate record's source.
#[derive(Debug, Deserialize, Serialize)]
pub struct SearchRecordsBody {
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub namespace: Vec<String>,
    #[serde(default)]
    pub sharding_key: Option<String>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub source: Option<SourceRef>,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    #[serde(default)]
    pub min_visible_record_commit_seq: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ContextBody {
    #[serde(default = "default_context_max_events")]
    pub max_events: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_context_include")]
    pub include: Vec<String>,
    #[serde(default)]
    pub record_query: Option<SearchRecordsBody>,
}

fn default_context_max_events() -> usize {
    20
}

fn default_context_include() -> Vec<String> {
    vec!["recent_events".into()]
}

/// Response body for `GET` or `POST /api/tenants/:tenant_id/records/search`.
///
/// Wrapped in an object for forward-compat — pagination cursors land here in
/// a later milestone without breaking the wire shape.
#[derive(Debug, Serialize)]
pub struct SearchRecordsResponse {
    pub records: Vec<SearchableRecord>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateSkillBody {
    pub name: String,
    pub version: String,
    #[serde(default = "default_policy")]
    pub manifest: serde_json::Value,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BindSkillBody {
    pub skill_id: String,
}

#[derive(Debug, Serialize)]
pub struct ListAgentSkillsResponse {
    pub skills: Vec<AgentSkillBinding>,
}

#[derive(Debug, Deserialize)]
pub struct SweepBody {
    #[serde(default)]
    pub now_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct PolicyWorkerRunBody {
    #[serde(default)]
    pub now_ms: Option<u64>,
    #[serde(default = "default_policy_worker_max_tenants")]
    pub max_tenants: usize,
}

fn default_policy_worker_max_tenants() -> usize {
    64
}
