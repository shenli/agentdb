use std::collections::HashMap;
use std::sync::Arc;

use agentdb_core::{BoxRegistry, SessionRegistry, TenantRegistry};
use parking_lot::Mutex;

use crate::sasl::SaslSession;

#[derive(Clone)]
pub struct WireState {
    pub registry: Arc<BoxRegistry>,
    pub tenants: Arc<TenantRegistry>,
    pub sessions: Arc<SessionRegistry>,
    /// M19 auth. `None` → no auth advertised or required
    /// (existing behavior for tests and pre-auth deployments).
    /// `Some` → SCRAM-SHA-256 is advertised via `hello`; if
    /// `require_auth` is true, non-SASL commands return
    /// `code: 18 AuthenticationFailed` until the client
    /// completes a successful handshake.
    pub auth: Option<WireAuthConfig>,
    /// Per-connection SASL state. Keyed by `connection_id`.
    /// Populated by `saslStart`, advanced by `saslContinue`,
    /// inspected by every other command to check
    /// `Authenticated` status.
    pub sasl_sessions: Arc<Mutex<HashMap<i64, SaslSession>>>,
}

#[derive(Clone)]
pub struct WireAuthConfig {
    pub meta: Arc<agentdb_meta::MetaStore>,
    /// When true, non-SASL commands before a successful handshake
    /// return AuthenticationFailed. When false, SCRAM is
    /// advertised but not enforced — useful for a rolling
    /// rollout where operators want to test auth without
    /// locking out unauthenticated clients.
    pub require_auth: bool,
}

impl WireState {
    /// Convenience constructor for the no-auth path (tests, dev
    /// mode). Matches the pre-M19 struct shape.
    pub fn new(
        registry: Arc<BoxRegistry>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
    ) -> Self {
        Self {
            registry,
            tenants,
            sessions,
            auth: None,
            sasl_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Constructor that attaches auth. The server binary uses
    /// this when `AGENTDB_REQUIRE_AUTH` is truthy or when any
    /// users exist in the meta store.
    pub fn with_auth(
        registry: Arc<BoxRegistry>,
        tenants: Arc<TenantRegistry>,
        sessions: Arc<SessionRegistry>,
        auth: WireAuthConfig,
    ) -> Self {
        Self {
            registry,
            tenants,
            sessions,
            auth: Some(auth),
            sasl_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check whether this connection has completed auth. Returns
    /// `Some(tenant_external)` when authenticated (so the
    /// command handler can clamp access to that tenant).
    pub fn auth_status(&self, connection_id: i64) -> AuthStatus {
        if self.auth.is_none() {
            return AuthStatus::AuthNotRequired;
        }
        let sessions = self.sasl_sessions.lock();
        match sessions.get(&connection_id) {
            Some(SaslSession::Authenticated {
                tenant_external, ..
            }) => AuthStatus::Authenticated {
                tenant_external: tenant_external.clone(),
            },
            _ => {
                let enforced = self
                    .auth
                    .as_ref()
                    .map(|a| a.require_auth)
                    .unwrap_or(false);
                if enforced {
                    AuthStatus::Required
                } else {
                    AuthStatus::AuthNotRequired
                }
            }
        }
    }
}

/// Outcome of `WireState::auth_status`. `AuthNotRequired`
/// collapses two paths (no auth configured, or auth configured
/// but `require_auth == false`) because callers treat them
/// identically — neither blocks the command.
#[derive(Debug, Clone)]
pub enum AuthStatus {
    /// Auth not configured or not enforced; let the command run.
    AuthNotRequired,
    /// Auth required but the connection hasn't completed SASL.
    /// Command handler must reject with code 18.
    Required,
    /// Connection is authenticated as this tenant. Command
    /// handler can cross-check `$db` against `tenant_external`.
    Authenticated { tenant_external: String },
}
