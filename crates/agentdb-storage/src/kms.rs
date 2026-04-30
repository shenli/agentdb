//! Provider-neutral KMS/key availability hook.
//!
//! AgentDB does not implement encryption in the local prototype, but runtime
//! storage boundaries still need a stable place to ask whether a tenant key is
//! usable before durable reads/writes. Production deployments can replace the
//! default resolver with a KMS-backed implementation without changing the
//! runtime API shape.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOperation {
    MetadataRead,
    MetadataWrite,
    ContentRead,
    ContentWrite,
}

impl fmt::Display for KeyOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            KeyOperation::MetadataRead => "metadata_read",
            KeyOperation::MetadataWrite => "metadata_write",
            KeyOperation::ContentRead => "content_read",
            KeyOperation::ContentWrite => "content_write",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyResolution {
    pub key_id: String,
    pub key_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KmsError {
    KeyUnavailable {
        tenant_id: String,
        operation: KeyOperation,
        reason: String,
    },
    Other(String),
}

impl fmt::Display for KmsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KmsError::KeyUnavailable {
                tenant_id,
                operation,
                reason,
            } => write!(
                f,
                "tenant {tenant_id} key unavailable for {operation}: {reason}"
            ),
            KmsError::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for KmsError {}

pub type KmsResult<T> = std::result::Result<T, KmsError>;

pub trait KmsKeyResolver: Send + Sync {
    fn resolve_tenant_key(
        &self,
        tenant_id: &str,
        operation: KeyOperation,
    ) -> KmsResult<KeyResolution>;
}

#[derive(Debug, Default)]
pub struct AllowAllKmsKeyResolver;

impl KmsKeyResolver for AllowAllKmsKeyResolver {
    fn resolve_tenant_key(
        &self,
        tenant_id: &str,
        _operation: KeyOperation,
    ) -> KmsResult<KeyResolution> {
        Ok(KeyResolution {
            key_id: format!("tenant:{tenant_id}:default"),
            key_version: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaticKeyState {
    Available {
        key_id: String,
        key_version: Option<String>,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(Debug, Default)]
pub struct StaticKmsKeyResolver {
    states: Mutex<HashMap<String, StaticKeyState>>,
}

impl StaticKmsKeyResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_state(&self, tenant_id: impl Into<String>, state: StaticKeyState) {
        self.states
            .lock()
            .expect("kms state mutex poisoned")
            .insert(tenant_id.into(), state);
    }
}

impl KmsKeyResolver for StaticKmsKeyResolver {
    fn resolve_tenant_key(
        &self,
        tenant_id: &str,
        operation: KeyOperation,
    ) -> KmsResult<KeyResolution> {
        match self
            .states
            .lock()
            .expect("kms state mutex poisoned")
            .get(tenant_id)
            .cloned()
        {
            Some(StaticKeyState::Available {
                key_id,
                key_version,
            }) => Ok(KeyResolution {
                key_id,
                key_version,
            }),
            Some(StaticKeyState::Unavailable { reason }) => Err(KmsError::KeyUnavailable {
                tenant_id: tenant_id.to_string(),
                operation,
                reason,
            }),
            None => Ok(KeyResolution {
                key_id: format!("tenant:{tenant_id}:default"),
                key_version: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_all_resolves_a_default_key() {
        let resolved = AllowAllKmsKeyResolver
            .resolve_tenant_key("ten_1", KeyOperation::ContentRead)
            .unwrap();
        assert_eq!(resolved.key_id, "tenant:ten_1:default");
    }

    #[test]
    fn static_resolver_reports_unavailable_key() {
        let resolver = StaticKmsKeyResolver::new();
        resolver.set_state(
            "ten_1",
            StaticKeyState::Unavailable {
                reason: "disabled".into(),
            },
        );
        let err = resolver
            .resolve_tenant_key("ten_1", KeyOperation::MetadataWrite)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "tenant ten_1 key unavailable for metadata_write: disabled"
        );
    }
}
