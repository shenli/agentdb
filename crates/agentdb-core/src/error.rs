use thiserror::Error;

use crate::admission::Scope;
use crate::box_runtime::FaultKind;
use agentdb_storage::BoxId;

/// Core error type. Each variant is serialized to a stable REST error
/// code in `agentdb-api`; the Mongo wire layer maps these to protocol
/// error codes independently.
#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] agentdb_storage::Error),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("{resource:?} {id} not found")]
    NotFound { resource: Resource, id: String },

    #[error("{}", message)]
    Invalid {
        /// Name of the invalid field, if the error is field-level.
        /// `None` for general validation failures.
        field: Option<String>,
        message: String,
    },

    #[error("rate limit exceeded ({scope:?})")]
    TooManyRequests { scope: Scope, retry_after_ms: u32 },

    #[error("box {box_id}: fault '{fault_kind:?}' injected")]
    FaultInjected {
        box_id: BoxId,
        fault_kind: FaultKind,
    },
}

/// Resource kinds referenced by `Error::NotFound`. Keep in sync with
/// the REST `resource` field (serialized lowercase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resource {
    Tenant,
    Session,
    Box_,
}

impl Resource {
    pub fn as_str(self) -> &'static str {
        match self {
            Resource::Tenant => "tenant",
            Resource::Session => "session",
            Resource::Box_ => "box",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_converts_via_from() {
        let storage_err = agentdb_storage::Error::Other("boom".into());
        let err: Error = storage_err.into();
        assert!(matches!(err, Error::Storage(_)));
    }

    #[test]
    fn not_found_renders_resource_and_id() {
        let err = Error::NotFound {
            resource: Resource::Session,
            id: "s-9".into(),
        };
        let rendered = err.to_string();
        assert!(rendered.contains("Session"));
        assert!(rendered.contains("s-9"));
    }

    #[test]
    fn too_many_requests_carries_retry_after() {
        let err = Error::TooManyRequests {
            scope: Scope::Tenant,
            retry_after_ms: 1200,
        };
        match err {
            Error::TooManyRequests {
                scope,
                retry_after_ms,
            } => {
                assert_eq!(scope, Scope::Tenant);
                assert_eq!(retry_after_ms, 1200);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn invalid_with_field_renders_message() {
        let err = Error::Invalid {
            field: Some("max_qps".into()),
            message: "must be positive".into(),
        };
        assert_eq!(err.to_string(), "must be positive");
    }

    #[test]
    fn resource_as_str_is_lowercase_singular() {
        assert_eq!(Resource::Tenant.as_str(), "tenant");
        assert_eq!(Resource::Session.as_str(), "session");
        assert_eq!(Resource::Box_.as_str(), "box");
    }
}
