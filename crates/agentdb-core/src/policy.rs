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
    pub soft_max_events_per_session: Option<u64>,
    #[serde(default)]
    pub max_events_per_session: Option<u64>,
    #[serde(default)]
    pub soft_max_active_sessions: Option<usize>,
    #[serde(default)]
    pub max_active_sessions: Option<usize>,
    #[serde(default)]
    pub soft_max_records: Option<usize>,
    #[serde(default)]
    pub max_records: Option<usize>,
    #[serde(default)]
    pub soft_max_artifact_storage_bytes: Option<u64>,
    #[serde(default)]
    pub max_artifact_storage_bytes: Option<u64>,
}

impl Default for QuotaPolicy {
    fn default() -> Self {
        Self {
            soft_max_events_per_session: None,
            max_events_per_session: None,
            soft_max_active_sessions: None,
            max_active_sessions: None,
            soft_max_records: None,
            max_records: None,
            soft_max_artifact_storage_bytes: None,
            max_artifact_storage_bytes: None,
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
    #[serde(default)]
    pub tier: Option<String>,
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
            tier: None,
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
        let mut base = v
            .get("tier")
            .and_then(|tier| tier.as_str())
            .map(tier_template)
            .unwrap_or_default();
        base = base.merge_override(v);
        base
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

fn tier_template(name: &str) -> RuntimePolicy {
    match name {
        "free" => RuntimePolicy {
            tier: Some("free".into()),
            version: 10_001,
            priority: Priority::Low,
            quota: QuotaPolicy {
                soft_max_events_per_session: Some(80),
                max_events_per_session: Some(100),
                soft_max_active_sessions: Some(8),
                max_active_sessions: Some(10),
                soft_max_records: Some(80),
                max_records: Some(100),
                soft_max_artifact_storage_bytes: Some(8 * 1024 * 1024),
                max_artifact_storage_bytes: Some(10 * 1024 * 1024),
            },
            artifact_size: ArtifactSizePolicy {
                max_inline_event_bytes: 32 * 1024,
                max_artifact_bytes: 2 * 1024 * 1024,
            },
            retention: RetentionPolicy::default(),
            lifecycle: LifecyclePolicy {
                cold_after_ms: Some(24 * 60 * 60 * 1000),
                archive_after_ms: Some(7 * 24 * 60 * 60 * 1000),
                derivative_only_after_ms: Some(30 * 24 * 60 * 60 * 1000),
            },
        },
        "pro" => RuntimePolicy {
            tier: Some("pro".into()),
            version: 10_002,
            priority: Priority::Normal,
            quota: QuotaPolicy {
                soft_max_events_per_session: Some(8_000),
                max_events_per_session: Some(10_000),
                soft_max_active_sessions: Some(800),
                max_active_sessions: Some(1_000),
                soft_max_records: Some(8_000),
                max_records: Some(10_000),
                soft_max_artifact_storage_bytes: Some(8 * 1024 * 1024 * 1024),
                max_artifact_storage_bytes: Some(10 * 1024 * 1024 * 1024),
            },
            artifact_size: ArtifactSizePolicy::default(),
            retention: RetentionPolicy::default(),
            lifecycle: LifecyclePolicy {
                cold_after_ms: Some(7 * 24 * 60 * 60 * 1000),
                archive_after_ms: Some(30 * 24 * 60 * 60 * 1000),
                derivative_only_after_ms: Some(90 * 24 * 60 * 60 * 1000),
            },
        },
        "enterprise" => RuntimePolicy {
            tier: Some("enterprise".into()),
            version: 10_003,
            priority: Priority::High,
            quota: QuotaPolicy {
                soft_max_events_per_session: None,
                max_events_per_session: None,
                soft_max_active_sessions: None,
                max_active_sessions: None,
                soft_max_records: None,
                max_records: None,
                soft_max_artifact_storage_bytes: None,
                max_artifact_storage_bytes: None,
            },
            artifact_size: ArtifactSizePolicy {
                max_inline_event_bytes: 256 * 1024,
                max_artifact_bytes: 128 * 1024 * 1024,
            },
            retention: RetentionPolicy::default(),
            lifecycle: LifecyclePolicy::default(),
        },
        _ => RuntimePolicy::default(),
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
