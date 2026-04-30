//! Minimal V2.4 hot-path policy model.
//!
//! The first V2.4 slice resolves policy from the existing tenant `policy`
//! JSON and optional agent `config.policy` override. This keeps policy
//! enforcement local to the runtime while leaving room for separate policy
//! records/templates in the control plane.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

impl Default for Priority {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaPolicy {
    #[serde(default)]
    pub max_events_per_session: Option<u64>,
    #[serde(default)]
    pub max_records: Option<usize>,
}

impl Default for QuotaPolicy {
    fn default() -> Self {
        Self {
            max_events_per_session: None,
            max_records: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactSizePolicy {
    pub max_inline_event_bytes: usize,
    pub max_artifact_bytes: u64,
}

impl Default for ArtifactSizePolicy {
    fn default() -> Self {
        Self {
            max_inline_event_bytes: 64 * 1024,
            max_artifact_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub retention_ms: u64,
    #[serde(default)]
    pub legal_hold: bool,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retention_ms: 24 * 60 * 60 * 1000,
            legal_hold: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecyclePolicy {
    #[serde(default)]
    pub cold_after_ms: Option<u64>,
    #[serde(default)]
    pub archive_after_ms: Option<u64>,
    #[serde(default)]
    pub derivative_only_after_ms: Option<u64>,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            cold_after_ms: None,
            archive_after_ms: None,
            derivative_only_after_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePolicy {
    #[serde(default = "default_policy_version")]
    pub version: u64,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub quota: QuotaPolicy,
    #[serde(default)]
    pub artifact_size: ArtifactSizePolicy,
    #[serde(default)]
    pub retention: RetentionPolicy,
    #[serde(default)]
    pub lifecycle: LifecyclePolicy,
}

fn default_policy_version() -> u64 {
    1
}

impl Default for RuntimePolicy {
    fn default() -> Self {
        Self {
            version: 1,
            priority: Priority::Normal,
            quota: QuotaPolicy::default(),
            artifact_size: ArtifactSizePolicy::default(),
            retention: RetentionPolicy::default(),
            lifecycle: LifecyclePolicy::default(),
        }
    }
}

impl RuntimePolicy {
    pub fn from_json(v: &serde_json::Value) -> Self {
        if v.is_null() {
            return Self::default();
        }
        serde_json::from_value(v.clone()).unwrap_or_default()
    }

    pub fn merge_override(mut self, override_json: &serde_json::Value) -> Self {
        if override_json.is_null() {
            return self;
        }
        let mut base = serde_json::to_value(&self).expect("policy serializes");
        merge_json(&mut base, override_json);
        self = serde_json::from_value(base).unwrap_or(self);
        self
    }
}

fn merge_json(base: &mut serde_json::Value, overlay: &serde_json::Value) {
    match (base, overlay) {
        (serde_json::Value::Object(base), serde_json::Value::Object(overlay)) => {
            for (k, v) in overlay {
                merge_json(base.entry(k.clone()).or_insert(serde_json::Value::Null), v);
            }
        }
        (base, overlay) => *base = overlay.clone(),
    }
}
