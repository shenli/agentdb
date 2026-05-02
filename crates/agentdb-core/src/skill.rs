//! V2.3 skill registry types.
//!
//! Skills are versioned runtime objects that agents can bind to. A binding
//! records the exact skill id, name, and version so replay can reconstruct the
//! version used even after newer versions are registered.

use serde::{Deserialize, Serialize};

use crate::event::ArtifactRef;
use crate::id::{Id, IdKind};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Skill {
    pub skill_id: Id,
    pub tenant_id: Id,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub manifest: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<ArtifactRef>,
    pub runtime_version: String,
    pub created_at_ms: u64,
    pub deleted_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct NewSkill {
    pub tenant_id: Id,
    pub name: String,
    pub version: String,
    pub manifest: serde_json::Value,
    pub artifact_refs: Vec<ArtifactRef>,
}

impl Skill {
    pub fn create(req: NewSkill) -> Self {
        Self {
            skill_id: Id::new(IdKind::Skill),
            tenant_id: req.tenant_id,
            name: req.name,
            version: req.version,
            manifest: req.manifest,
            artifact_refs: req.artifact_refs,
            runtime_version: crate::RUNTIME_VERSION.to_string(),
            created_at_ms: crate::now_ms(),
            deleted_at_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentSkillBinding {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub skill_id: Id,
    pub name: String,
    pub version: String,
    pub bound_at_ms: u64,
    pub deleted_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct BindSkillRequest {
    pub tenant_id: Id,
    pub agent_id: Id,
    pub skill_id: Id,
    pub idempotency_key: Option<String>,
}

impl AgentSkillBinding {
    pub fn create(tenant_id: Id, agent_id: Id, skill: &Skill) -> Self {
        Self {
            tenant_id,
            agent_id,
            skill_id: skill.skill_id.clone(),
            name: skill.name.clone(),
            version: skill.version.clone(),
            bound_at_ms: crate::now_ms(),
            deleted_at_ms: None,
        }
    }
}
