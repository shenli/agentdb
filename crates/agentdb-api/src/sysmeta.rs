//! Self-hosted tenant-scale metadata (metadata self-hosting stages 2–3).
//!
//! Admin/tenant bearer tokens, session locators, and the cluster tenant
//! registry live as records under the reserved SYSTEM TENANT inside
//! AgentDB itself — replicated through the same raft groups, covered by
//! the same recovery manifests, cached and fallback-read by the same
//! KV-authoritative machinery as user data. The external SQL database
//! keeps only the bootstrap root (nodes, cells, tablets, placement).
//!
//! Record namespaces (`["sys", *]`) are an internal contract: the public
//! HTTP API rejects every request that targets the system tenant.

use std::sync::Arc;

use agentdb_core::record::{IndexingMode, IndexingPolicy, NewRecord};
use agentdb_core::runtime::{RecordUpdate, Runtime};
use agentdb_core::{Id, SYSTEM_TENANT_ID};
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL};
use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};

pub const TOKEN_PREFIX: &str = "agentdb_";
const TOKEN_RAW_BYTES: usize = 32;

type Result<T> = std::result::Result<T, agentdb_core::Error>;

fn system_tenant_id() -> Id {
    Id::parse_kind(SYSTEM_TENANT_ID, agentdb_core::id::IdKind::Tenant)
        .expect("SYSTEM_TENANT_ID parses")
}

fn no_indexing() -> IndexingPolicy {
    IndexingPolicy {
        mode: IndexingMode::NoIndexing,
        fields: vec![],
        vector_allowed: false,
    }
}

fn internal(context: &str, err: impl std::fmt::Debug) -> agentdb_core::Error {
    agentdb_core::Error::new(
        agentdb_core::ErrorCode::Internal,
        format!("{context}: {err:?}"),
    )
}

// ---------------------------------------------------------------------------
// Bearer tokens: namespace ["sys","token"], external_id = sha256(plaintext)
// so verification is one point lookup through the external-id map.
// ---------------------------------------------------------------------------

/// Scope of a bearer token — what the holder is allowed to do.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AdminScope {
    /// Full access: admin plane and data plane, any method.
    #[default]
    Admin,
    /// Read-only: GET/HEAD anywhere.
    Readonly,
    /// Data-plane access to exactly one tenant.
    Tenant,
}

impl AdminScope {
    pub fn as_str(self) -> &'static str {
        match self {
            AdminScope::Admin => "admin",
            AdminScope::Readonly => "readonly",
            AdminScope::Tenant => "tenant",
        }
    }

    /// Parse from the persisted form. Unknown values fail closed to
    /// read-only so forward-version metadata never grants write access.
    pub fn from_stored(raw: &str) -> Self {
        match raw {
            "admin" => AdminScope::Admin,
            "tenant" => AdminScope::Tenant,
            _ => AdminScope::Readonly,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TokenSummary {
    pub token_id: String,
    pub description: String,
    pub created_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub revoked_at_ms: Option<i64>,
    pub scope: AdminScope,
}

#[derive(Debug)]
pub struct MintedToken {
    pub token_id: String,
    pub plaintext: String,
    pub summary: TokenSummary,
}

#[derive(Debug, Clone)]
pub struct VerifiedToken {
    pub token_id: String,
    pub scope: AdminScope,
    pub tenant_id: Option<String>,
}

/// Stable cache key for a presented token (same form the record's
/// external id uses), so callers can memoize verify() results without
/// holding plaintext credentials.
pub fn fingerprint(presented: &str) -> String {
    B64.encode(Sha256::digest(presented.as_bytes()))
}

pub async fn mint_token(
    runtime: &Arc<Runtime>,
    description: &str,
    scope: AdminScope,
    tenant_id: Option<&str>,
    now_ms: i64,
) -> Result<MintedToken> {
    match (scope, tenant_id) {
        (AdminScope::Tenant, None) => {
            return Err(agentdb_core::Error::new(
                agentdb_core::ErrorCode::PolicyDenied,
                "tenant-scoped tokens require a tenant_id",
            ))
        }
        (AdminScope::Admin | AdminScope::Readonly, Some(_)) => {
            return Err(agentdb_core::Error::new(
                agentdb_core::ErrorCode::PolicyDenied,
                "tenant_id is only valid for tenant-scoped tokens",
            ))
        }
        _ => {}
    }
    runtime.ensure_system_tenant().await?;
    let mut raw = [0u8; TOKEN_RAW_BYTES];
    rand::thread_rng().fill_bytes(&mut raw);
    let plaintext = format!("{TOKEN_PREFIX}{}", B64URL.encode(raw));
    let hash_b64 = fingerprint(&plaintext);
    let token_id = format!("tok_{}", B64URL.encode(&raw[..12]));
    runtime
        .upsert_record_with_commit(NewRecord {
            tenant_id: system_tenant_id(),
            external_id: Some(hash_b64),
            namespace: vec!["sys".into(), "token".into()],
            scope: serde_json::json!({}),
            text: None,
            metadata: serde_json::json!({
                "token_id": token_id,
                "description": description,
                "scope": scope.as_str(),
                "tenant_id": tenant_id,
                "created_at_ms": now_ms,
                "revoked_at_ms": null,
            }),
            tags: vec![],
            source: None,
            embedding: None,
            policy: no_indexing(),
            expires_at_ms: None,
        })
        .await?;
    Ok(MintedToken {
        token_id: token_id.clone(),
        plaintext,
        summary: TokenSummary {
            token_id,
            description: description.to_string(),
            created_at_ms: now_ms,
            tenant_id: tenant_id.map(str::to_string),
            revoked_at_ms: None,
            scope,
        },
    })
}

pub async fn verify_token(
    runtime: &Arc<Runtime>,
    presented: &str,
) -> Result<Option<VerifiedToken>> {
    let system = system_tenant_id();
    let Some(record) = runtime
        .get_record_by_external_id(&system, &fingerprint(presented))
        .await?
    else {
        return Ok(None);
    };
    let meta = &record.metadata;
    if meta.get("revoked_at_ms").is_some_and(|v| !v.is_null()) {
        return Ok(None);
    }
    let scope = AdminScope::from_stored(meta.get("scope").and_then(|v| v.as_str()).unwrap_or(""));
    let tenant_id = meta
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    // Fail closed: a tenant-scoped row without its binding must never
    // authenticate as anything.
    if scope == AdminScope::Tenant && tenant_id.is_none() {
        return Ok(None);
    }
    let token_id = meta
        .get("token_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    Ok(Some(VerifiedToken {
        token_id,
        scope,
        tenant_id,
    }))
}

async fn token_records(
    runtime: &Arc<Runtime>,
) -> Result<Vec<agentdb_core::record::SearchableRecord>> {
    let system = system_tenant_id();
    Ok(runtime
        .list_records(&system)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.namespace == ["sys", "token"])
        .collect())
}

fn summary_from_record(record: &agentdb_core::record::SearchableRecord) -> TokenSummary {
    let meta = &record.metadata;
    TokenSummary {
        token_id: meta
            .get("token_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        description: meta
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        created_at_ms: meta
            .get("created_at_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or_default(),
        tenant_id: meta
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        revoked_at_ms: meta.get("revoked_at_ms").and_then(|v| v.as_i64()),
        scope: AdminScope::from_stored(meta.get("scope").and_then(|v| v.as_str()).unwrap_or("")),
    }
}

pub async fn list_active_tokens(runtime: &Arc<Runtime>) -> Result<Vec<TokenSummary>> {
    let mut out: Vec<TokenSummary> = token_records(runtime)
        .await?
        .iter()
        .map(summary_from_record)
        .filter(|summary| summary.revoked_at_ms.is_none())
        .collect();
    out.sort_by(|a, b| {
        (a.created_at_ms, a.token_id.as_str()).cmp(&(b.created_at_ms, b.token_id.as_str()))
    });
    Ok(out)
}

/// Total tokens ever minted (revoked included) — the bootstrap gate:
/// the instance stays open only until the first token exists.
pub async fn count_all_tokens(runtime: &Arc<Runtime>) -> Result<usize> {
    Ok(token_records(runtime).await?.len())
}

/// Mark a token revoked. Idempotent; returns false for unknown or
/// already-revoked ids. The record stays (auditability) but never
/// verifies again.
pub async fn revoke_token(runtime: &Arc<Runtime>, token_id: &str, now_ms: i64) -> Result<bool> {
    let system = system_tenant_id();
    let Some(record) = token_records(runtime)
        .await?
        .into_iter()
        .find(|r| r.metadata.get("token_id").and_then(|v| v.as_str()) == Some(token_id))
    else {
        return Ok(false);
    };
    if record
        .metadata
        .get("revoked_at_ms")
        .is_some_and(|v| !v.is_null())
    {
        return Ok(false);
    }
    let mut metadata = record.metadata.clone();
    metadata["revoked_at_ms"] = serde_json::json!(now_ms);
    runtime
        .update_record_with_commit(RecordUpdate {
            tenant_id: system,
            record_id: record.record_id.clone(),
            expected_version: None,
            namespace: None,
            scope: None,
            text: None,
            metadata: Some(metadata),
            tags: None,
            source: None,
            embedding: None,
            policy: None,
            status: None,
            expires_at_ms: None,
        })
        .await?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Session locators: namespace ["sys","locator"], external_id = session_id.
// ---------------------------------------------------------------------------

pub use agentdb_meta::routing::SessionLocator;

fn locator_from_record(record: &agentdb_core::record::SearchableRecord) -> Option<SessionLocator> {
    serde_json::from_value(record.metadata.clone()).ok()
}

/// Insert-if-absent (the existing locator's tablet is the authoritative
/// current placement; handoff paths retarget explicitly). Returns true
/// when this call registered a new locator.
pub async fn upsert_locator(
    runtime: &Arc<Runtime>,
    session_id: &str,
    tenant_external_id: &str,
    tablet_id: Option<i64>,
    shard_id: i32,
    now_ms: i64,
) -> Result<bool> {
    runtime.ensure_system_tenant().await?;
    let system = system_tenant_id();
    if runtime
        .get_record_by_external_id(&system, session_id)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    let locator = SessionLocator {
        session_id: session_id.to_string(),
        tenant_internal_id: 0,
        tenant_external_id: Some(tenant_external_id.to_string()),
        tablet_id,
        shard_id,
        created_at_ms: now_ms,
    };
    runtime
        .upsert_record_with_commit(NewRecord {
            tenant_id: system,
            external_id: Some(session_id.to_string()),
            namespace: vec!["sys".into(), "locator".into()],
            scope: serde_json::json!({}),
            text: None,
            metadata: serde_json::to_value(&locator).map_err(|e| internal("locator encode", e))?,
            tags: vec![],
            source: None,
            embedding: None,
            policy: no_indexing(),
            expires_at_ms: None,
        })
        .await?;
    Ok(true)
}

pub async fn get_locator(
    runtime: &Arc<Runtime>,
    session_id: &str,
) -> Result<Option<SessionLocator>> {
    let system = system_tenant_id();
    Ok(runtime
        .get_record_by_external_id(&system, session_id)
        .await?
        .as_ref()
        .and_then(locator_from_record))
}

pub async fn delete_locator(runtime: &Arc<Runtime>, session_id: &str) -> Result<bool> {
    let system = system_tenant_id();
    let Some(record) = runtime
        .get_record_by_external_id(&system, session_id)
        .await?
    else {
        return Ok(false);
    };
    runtime.delete_record(&system, &record.record_id).await?;
    Ok(true)
}

pub async fn delete_locators_by_tenant_external(
    runtime: &Arc<Runtime>,
    tenant_external_id: &str,
) -> Result<u64> {
    let system = system_tenant_id();
    let mut deleted = 0u64;
    for record in runtime.list_records(&system).await.unwrap_or_default() {
        if record.namespace != ["sys", "locator"] {
            continue;
        }
        let Some(locator) = locator_from_record(&record) else {
            continue;
        };
        if locator.tenant_external_id.as_deref() == Some(tenant_external_id) {
            runtime.delete_record(&system, &record.record_id).await?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

pub async fn list_all_locators(runtime: &Arc<Runtime>) -> Result<Vec<SessionLocator>> {
    let system = system_tenant_id();
    Ok(runtime
        .list_records(&system)
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| r.namespace == ["sys", "locator"])
        .filter_map(locator_from_record)
        .collect())
}

// ---------------------------------------------------------------------------
// Cluster tenant registry: namespace ["sys","tenant"], external_id = the
// externally-supplied tenant identity.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredTenant {
    /// Retired (always 0); kept for wire compatibility with locator rows.
    pub internal_id: i64,
    pub external_id: String,
    pub created_at_ms: i64,
}

pub async fn get_or_create_registered_tenant(
    runtime: &Arc<Runtime>,
    external_id: &str,
    now_ms: i64,
) -> Result<RegisteredTenant> {
    runtime.ensure_system_tenant().await?;
    let system = system_tenant_id();
    let key = format!("tenant:{external_id}");
    if let Some(record) = runtime.get_record_by_external_id(&system, &key).await? {
        return Ok(RegisteredTenant {
            internal_id: 0,
            external_id: external_id.to_string(),
            created_at_ms: record
                .metadata
                .get("created_at_ms")
                .and_then(|v| v.as_i64())
                .unwrap_or(now_ms),
        });
    }
    // Upsert-by-external-id is idempotent: a concurrent caller converges
    // on the same record.
    runtime
        .upsert_record_with_commit(NewRecord {
            tenant_id: system,
            external_id: Some(key),
            namespace: vec!["sys".into(), "tenant".into()],
            scope: serde_json::json!({}),
            text: None,
            metadata: serde_json::json!({
                "external_id": external_id,
                "created_at_ms": now_ms,
            }),
            tags: vec![],
            source: None,
            embedding: None,
            policy: no_indexing(),
            expires_at_ms: None,
        })
        .await?;
    Ok(RegisteredTenant {
        internal_id: 0,
        external_id: external_id.to_string(),
        created_at_ms: now_ms,
    })
}
