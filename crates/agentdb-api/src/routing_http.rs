//! HTTP-facing contract for multi-node route decisions.
//!
//! V2.10 added the pure metadata planner in `agentdb-meta`. This module is the
//! API boundary that converts planner output into the concrete action the HTTP
//! data path can take: serve locally, forward to an owner node, or reject with a
//! structured error response.

use std::sync::Arc;

use axum::{
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use agentdb_meta::{
    nodes::{self, Node},
    routing::{plan_route, ResidencyPolicy, RouteAction, RouteDecision, RouteReason, RouteRequest},
    session_locators::{self, SessionLocator},
    shards::{self, ShardRow},
    tablets::{self, Tablet},
    MetaStore,
};

pub const HDR_ROUTE_ACTION: &str = "x-agentdb-route-action";
pub const HDR_OWNER_NODE: &str = "x-agentdb-owner-node";
pub const HDR_OWNER_ADDRESS: &str = "x-agentdb-owner-address";
pub const HDR_TABLET_ID: &str = "x-agentdb-tablet-id";
pub const HDR_TABLET_EPOCH: &str = "x-agentdb-tablet-epoch";
pub const HDR_ROUTE_REASON: &str = "x-agentdb-route-reason";
pub const HDR_FORWARDED_BY: &str = "x-agentdb-forwarded-by";
pub const HDR_FORWARDED_FOR: &str = "x-agentdb-forwarded-for";

const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteHttpOutcome {
    ServeLocal(RouteTarget),
    Forward(RouteTarget),
    Reject(RouteRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteGuardOutcome {
    ServeLocal(HeaderMap),
    Forward(ForwardRequest),
    Reject(RouteRejection),
}

#[derive(Debug, Clone)]
pub struct RouteRuntime {
    local_node_id: Arc<str>,
    mode: RouteRuntimeMode,
}

#[derive(Debug, Clone)]
enum RouteRuntimeMode {
    Disabled,
    Fixed(RouteDecision),
    Snapshot(RouteSnapshot),
}

#[derive(Debug, Clone)]
pub struct RouteSnapshot {
    pub shard_count: i64,
    pub request_location: Option<String>,
    pub policy: ResidencyPolicy,
    pub shards: Arc<[ShardRow]>,
    pub tablets: Arc<[Tablet]>,
    pub nodes: Arc<[Node]>,
    pub session_locators: Arc<[SessionLocator]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub tablet_id: i64,
    pub owner_node_id: String,
    pub owner_address: String,
    pub epoch: i64,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRejection {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest {
    pub uri: String,
    pub headers: HeaderMap,
}

impl RouteRuntime {
    pub fn disabled() -> Self {
        Self {
            local_node_id: Arc::from("single-node"),
            mode: RouteRuntimeMode::Disabled,
        }
    }

    pub fn fixed(local_node_id: impl Into<Arc<str>>, decision: RouteDecision) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            mode: RouteRuntimeMode::Fixed(decision),
        }
    }

    pub fn snapshot(local_node_id: impl Into<Arc<str>>, snapshot: RouteSnapshot) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            mode: RouteRuntimeMode::Snapshot(snapshot),
        }
    }

    pub fn guard(
        &self,
        original_path_and_query: &str,
        incoming_headers: &HeaderMap,
    ) -> RouteGuardOutcome {
        let RouteRuntimeMode::Fixed(decision) = &self.mode else {
            return RouteGuardOutcome::ServeLocal(HeaderMap::new());
        };
        match route_http_outcome(decision) {
            RouteHttpOutcome::ServeLocal(target) => RouteGuardOutcome::ServeLocal(target.headers),
            RouteHttpOutcome::Forward(target) => {
                match prepare_forward_request(
                    &target,
                    original_path_and_query,
                    incoming_headers,
                    &self.local_node_id,
                ) {
                    Ok(req) => RouteGuardOutcome::Forward(req),
                    Err(rejection) => RouteGuardOutcome::Reject(rejection),
                }
            }
            RouteHttpOutcome::Reject(rejection) => RouteGuardOutcome::Reject(rejection),
        }
    }

    pub fn guard_session(
        &self,
        tenant_id: &str,
        session_locator: Option<&SessionLocator>,
        original_path_and_query: &str,
        incoming_headers: &HeaderMap,
    ) -> RouteGuardOutcome {
        let RouteRuntimeMode::Snapshot(snapshot) = &self.mode else {
            return self.guard(original_path_and_query, incoming_headers);
        };
        let decision = plan_route(
            RouteRequest {
                tenant_id,
                session_locator,
                request_location: snapshot.request_location.as_deref(),
                local_node_id: Some(&self.local_node_id),
                shard_count: snapshot.shard_count,
                now_ms: now_ms(),
                policy: snapshot.policy.clone(),
            },
            &snapshot.shards,
            &snapshot.tablets,
            &snapshot.nodes,
        );
        match route_http_outcome(&decision) {
            RouteHttpOutcome::ServeLocal(target) => RouteGuardOutcome::ServeLocal(target.headers),
            RouteHttpOutcome::Forward(target) => {
                match prepare_forward_request(
                    &target,
                    original_path_and_query,
                    incoming_headers,
                    &self.local_node_id,
                ) {
                    Ok(req) => RouteGuardOutcome::Forward(req),
                    Err(rejection) => RouteGuardOutcome::Reject(rejection),
                }
            }
            RouteHttpOutcome::Reject(rejection) => RouteGuardOutcome::Reject(rejection),
        }
    }

    pub fn guard_session_id(
        &self,
        tenant_id: &str,
        session_id: &str,
        original_path_and_query: &str,
        incoming_headers: &HeaderMap,
    ) -> RouteGuardOutcome {
        let RouteRuntimeMode::Snapshot(snapshot) = &self.mode else {
            return self.guard(original_path_and_query, incoming_headers);
        };
        let session_locator = snapshot.session_locator(tenant_id, session_id);
        self.guard_session(
            tenant_id,
            session_locator,
            original_path_and_query,
            incoming_headers,
        )
    }
}

impl RouteSnapshot {
    pub fn new(shard_count: i64) -> Self {
        Self {
            shard_count,
            request_location: None,
            policy: ResidencyPolicy::default(),
            shards: Arc::from([]),
            tablets: Arc::from([]),
            nodes: Arc::from([]),
            session_locators: Arc::from([]),
        }
    }

    pub async fn load_from_meta(
        store: &MetaStore,
        shard_count: i64,
        request_location: Option<String>,
        policy: ResidencyPolicy,
    ) -> agentdb_meta::MetaResult<Self> {
        let pool = store.pool();
        Ok(Self {
            shard_count,
            request_location,
            policy,
            shards: Arc::from(shards::list_all(pool).await?),
            tablets: Arc::from(tablets::list_all(pool).await?),
            nodes: Arc::from(nodes::list_all(pool).await?),
            session_locators: Arc::from(session_locators::list_all(pool).await?),
        })
    }

    pub fn with_request_location(mut self, request_location: impl Into<String>) -> Self {
        self.request_location = Some(request_location.into());
        self
    }

    pub fn with_policy(mut self, policy: ResidencyPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn with_shards(mut self, shards: Vec<ShardRow>) -> Self {
        self.shards = Arc::from(shards);
        self
    }

    pub fn with_tablets(mut self, tablets: Vec<Tablet>) -> Self {
        self.tablets = Arc::from(tablets);
        self
    }

    pub fn with_nodes(mut self, nodes: Vec<Node>) -> Self {
        self.nodes = Arc::from(nodes);
        self
    }

    pub fn with_session_locators(mut self, session_locators: Vec<SessionLocator>) -> Self {
        self.session_locators = Arc::from(session_locators);
        self
    }

    fn session_locator(&self, tenant_id: &str, session_id: &str) -> Option<&SessionLocator> {
        self.session_locators.iter().find(|locator| {
            locator.session_id == session_id
                && locator
                    .tenant_external_id
                    .as_deref()
                    .is_none_or(|locator_tenant| locator_tenant == tenant_id)
        })
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Debug, Serialize)]
struct ErrorDetail<'a> {
    code: &'a str,
    message: &'a str,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    suggestion: Option<&'a str>,
}

pub fn route_http_outcome(decision: &RouteDecision) -> RouteHttpOutcome {
    match decision.action {
        RouteAction::Local => RouteHttpOutcome::ServeLocal(route_target(decision, "local")),
        RouteAction::Forward => RouteHttpOutcome::Forward(route_target(decision, "forward")),
        RouteAction::Reject | RouteAction::Unassigned => {
            RouteHttpOutcome::Reject(route_rejection(decision))
        }
    }
}

pub fn route_rejection_response(rejection: &RouteRejection) -> Response {
    let suggestion = match rejection.code {
        "ROUTE_WRONG_LOCATION" => {
            Some("send the request to the owner region or enable cross-location routing")
        }
        "ROUTE_RESIDENCY_DENIED" => Some("move the session to an allowed location before retrying"),
        "ROUTE_UNASSIGNED" => Some("retry after placement or failover assigns the tablet"),
        "ROUTE_FORWARD_LOOP" => {
            Some("retry against the owner node advertised in the routing headers")
        }
        _ => None,
    };
    let body = ErrorBody {
        error: ErrorDetail {
            code: rejection.code,
            message: &rejection.message,
            retryable: rejection.retryable,
            suggestion,
        },
    };
    let mut response = (rejection.status, Json(body)).into_response();
    response.headers_mut().extend(rejection.headers.clone());
    response
}

pub fn prepare_forward_request(
    target: &RouteTarget,
    original_path_and_query: &str,
    incoming_headers: &HeaderMap,
    local_node_id: &str,
) -> Result<ForwardRequest, RouteRejection> {
    if incoming_headers.contains_key(HDR_FORWARDED_BY) {
        return Err(forward_loop_rejection(target));
    }
    let uri = format!(
        "{}{}",
        target.owner_address.trim_end_matches('/'),
        original_path_and_query
    );
    let mut headers = sanitize_forward_headers(incoming_headers);
    headers.extend(target.headers.clone());
    insert_header(&mut headers, HDR_FORWARDED_BY, local_node_id);
    let forwarded_for = match incoming_headers.get(HDR_FORWARDED_FOR) {
        Some(existing) => format!(
            "{},{}",
            existing.to_str().unwrap_or("<invalid>"),
            local_node_id
        ),
        None => local_node_id.to_string(),
    };
    insert_header(&mut headers, HDR_FORWARDED_FOR, &forwarded_for);
    Ok(ForwardRequest { uri, headers })
}

fn route_target(decision: &RouteDecision, action: &'static str) -> RouteTarget {
    let owner_node_id = decision
        .owner_node_id
        .clone()
        .expect("routable decision must carry owner_node_id");
    let owner_address = decision
        .owner_address
        .clone()
        .expect("routable decision must carry owner_address");
    let epoch = decision.epoch.expect("routable decision must carry epoch");
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, HDR_ROUTE_ACTION, action);
    insert_header(&mut headers, HDR_OWNER_NODE, &owner_node_id);
    insert_header(&mut headers, HDR_OWNER_ADDRESS, &owner_address);
    insert_header(&mut headers, HDR_TABLET_ID, &decision.tablet_id.to_string());
    insert_header(&mut headers, HDR_TABLET_EPOCH, &epoch.to_string());
    RouteTarget {
        tablet_id: decision.tablet_id,
        owner_node_id,
        owner_address,
        epoch,
        headers,
    }
}

fn route_rejection(decision: &RouteDecision) -> RouteRejection {
    let reason = decision.reason.unwrap_or(RouteReason::MissingOwner);
    let (status, code, retryable, message) = match reason {
        RouteReason::WrongLocation => (
            StatusCode::MISDIRECTED_REQUEST,
            "ROUTE_WRONG_LOCATION",
            false,
            "request arrived in the wrong AgentDB location",
        ),
        RouteReason::ResidencyDenied => (
            StatusCode::FORBIDDEN,
            "ROUTE_RESIDENCY_DENIED",
            false,
            "tenant residency policy denies routing to the owner location",
        ),
        RouteReason::MissingTablet => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ROUTE_MISSING_TABLET",
            true,
            "no tablet metadata exists for the target shard",
        ),
        RouteReason::MissingOwner => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ROUTE_UNASSIGNED",
            true,
            "target tablet has no owner",
        ),
        RouteReason::ExpiredLease => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ROUTE_EXPIRED_LEASE",
            true,
            "target tablet owner lease is expired",
        ),
        RouteReason::MissingNode => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ROUTE_MISSING_NODE",
            true,
            "target tablet owner node is missing",
        ),
        RouteReason::NodeUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "ROUTE_NODE_UNAVAILABLE",
            true,
            "target tablet owner node is not alive",
        ),
    };
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, HDR_ROUTE_ACTION, "reject");
    insert_header(&mut headers, HDR_ROUTE_REASON, code);
    insert_header(&mut headers, HDR_TABLET_ID, &decision.tablet_id.to_string());
    if let Some(epoch) = decision.epoch {
        insert_header(&mut headers, HDR_TABLET_EPOCH, &epoch.to_string());
    }
    if let Some(owner_node_id) = &decision.owner_node_id {
        insert_header(&mut headers, HDR_OWNER_NODE, owner_node_id);
    }
    if let Some(owner_address) = &decision.owner_address {
        insert_header(&mut headers, HDR_OWNER_ADDRESS, owner_address);
    }
    RouteRejection {
        status,
        code,
        message: message.into(),
        retryable,
        headers,
    }
}

fn forward_loop_rejection(target: &RouteTarget) -> RouteRejection {
    let mut headers = target.headers.clone();
    insert_header(&mut headers, HDR_ROUTE_ACTION, "reject");
    insert_header(&mut headers, HDR_ROUTE_REASON, "ROUTE_FORWARD_LOOP");
    RouteRejection {
        status: StatusCode::MISDIRECTED_REQUEST,
        code: "ROUTE_FORWARD_LOOP",
        message: "request has already been forwarded once".into(),
        retryable: false,
        headers,
    }
}

fn sanitize_forward_headers(incoming: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in incoming {
        let name_s = name.as_str();
        if HOP_BY_HOP_HEADERS
            .iter()
            .any(|blocked| name_s.eq_ignore_ascii_case(blocked))
            || name_s.starts_with("x-agentdb-route-")
            || name_s == HDR_OWNER_NODE
            || name_s == HDR_OWNER_ADDRESS
            || name_s == HDR_TABLET_ID
            || name_s == HDR_TABLET_EPOCH
        {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &str) {
    let value = HeaderValue::from_str(value).expect("route header value must be valid ASCII");
    headers.insert(name, value);
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_meta::routing::{RouteAction, RouteDecision, RouteReason};

    fn node(id: &str, location: &str) -> Node {
        Node {
            node_id: id.into(),
            address: format!("http://{id}:8080"),
            mongo_address: None,
            state: "alive".into(),
            last_heartbeat_ms: 100,
            joined_at_ms: 100,
            availability_zone: Some(location.into()),
        }
    }

    fn tablet(id: i64, owner: Option<&str>, lease_expires_at_ms: i64) -> Tablet {
        Tablet {
            tablet_id: id,
            owner_node_id: owner.map(str::to_string),
            epoch: 3,
            lease_expires_at_ms,
            created_at_ms: 0,
            handoff_target_node_id: None,
        }
    }

    fn locator(session_id: &str, shard_id: i32, tablet_id: Option<i64>) -> SessionLocator {
        SessionLocator {
            session_id: session_id.into(),
            tenant_internal_id: 1,
            tenant_external_id: Some("ten_1".into()),
            tablet_id,
            shard_id,
            created_at_ms: 0,
        }
    }

    fn decision(action: RouteAction, reason: Option<RouteReason>) -> RouteDecision {
        RouteDecision {
            action,
            shard_id: 3,
            tablet_id: 7,
            owner_node_id: Some("node-a".into()),
            owner_address: Some("http://node-a:8080".into()),
            owner_location: Some("us-west-2a".into()),
            epoch: Some(11),
            reason,
        }
    }

    #[test]
    fn local_decision_becomes_serve_local_target_with_headers() {
        let outcome = route_http_outcome(&decision(RouteAction::Local, None));
        let RouteHttpOutcome::ServeLocal(target) = outcome else {
            panic!("expected serve local");
        };
        assert_eq!(target.tablet_id, 7);
        assert_eq!(target.epoch, 11);
        assert_eq!(target.headers[HDR_ROUTE_ACTION].to_str().unwrap(), "local");
        assert_eq!(
            target.headers[HDR_OWNER_ADDRESS].to_str().unwrap(),
            "http://node-a:8080"
        );
    }

    #[test]
    fn forward_decision_preserves_owner_target() {
        let outcome = route_http_outcome(&decision(RouteAction::Forward, None));
        let RouteHttpOutcome::Forward(target) = outcome else {
            panic!("expected forward");
        };
        assert_eq!(target.owner_node_id, "node-a");
        assert_eq!(
            target.headers[HDR_ROUTE_ACTION].to_str().unwrap(),
            "forward"
        );
    }

    #[test]
    fn wrong_location_rejection_is_421_for_client_misdirected_mapping() {
        let outcome = route_http_outcome(&decision(
            RouteAction::Reject,
            Some(RouteReason::WrongLocation),
        ));
        let RouteHttpOutcome::Reject(rejection) = outcome else {
            panic!("expected rejection");
        };
        assert_eq!(rejection.status, StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(rejection.code, "ROUTE_WRONG_LOCATION");
        assert!(!rejection.retryable);
        let response = route_rejection_response(&rejection);
        assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(
            response.headers()[HDR_ROUTE_REASON].to_str().unwrap(),
            "ROUTE_WRONG_LOCATION"
        );
    }

    #[test]
    fn unassigned_rejection_is_retryable_503() {
        let mut d = decision(RouteAction::Unassigned, Some(RouteReason::ExpiredLease));
        d.owner_node_id = None;
        d.owner_address = None;
        d.epoch = None;
        let outcome = route_http_outcome(&d);
        let RouteHttpOutcome::Reject(rejection) = outcome else {
            panic!("expected rejection");
        };
        assert_eq!(rejection.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(rejection.code, "ROUTE_EXPIRED_LEASE");
        assert!(rejection.retryable);
        assert_eq!(
            rejection.headers[HDR_ROUTE_ACTION].to_str().unwrap(),
            "reject"
        );
    }

    #[test]
    fn prepare_forward_request_builds_owner_uri_and_sanitized_headers() {
        let outcome = route_http_outcome(&decision(RouteAction::Forward, None));
        let RouteHttpOutcome::Forward(target) = outcome else {
            panic!("expected forward");
        };
        let mut incoming = HeaderMap::new();
        insert_header(&mut incoming, "authorization", "Bearer token");
        insert_header(&mut incoming, "content-type", "application/json");
        insert_header(&mut incoming, "host", "wrong-node");
        insert_header(&mut incoming, "connection", "keep-alive");
        insert_header(&mut incoming, HDR_ROUTE_ACTION, "stale");

        let req = prepare_forward_request(
            &target,
            "/api/tenants/ten_1/sessions/ses_1/events?from_seq=0",
            &incoming,
            "node-local",
        )
        .unwrap();

        assert_eq!(
            req.uri,
            "http://node-a:8080/api/tenants/ten_1/sessions/ses_1/events?from_seq=0"
        );
        assert_eq!(
            req.headers["authorization"].to_str().unwrap(),
            "Bearer token"
        );
        assert_eq!(
            req.headers["content-type"].to_str().unwrap(),
            "application/json"
        );
        assert!(!req.headers.contains_key("host"));
        assert!(!req.headers.contains_key("connection"));
        assert_eq!(req.headers[HDR_ROUTE_ACTION].to_str().unwrap(), "forward");
        assert_eq!(
            req.headers[HDR_FORWARDED_BY].to_str().unwrap(),
            "node-local"
        );
        assert_eq!(
            req.headers[HDR_FORWARDED_FOR].to_str().unwrap(),
            "node-local"
        );
    }

    #[test]
    fn prepare_forward_request_appends_forwarded_for_chain() {
        let outcome = route_http_outcome(&decision(RouteAction::Forward, None));
        let RouteHttpOutcome::Forward(target) = outcome else {
            panic!("expected forward");
        };
        let mut incoming = HeaderMap::new();
        insert_header(&mut incoming, HDR_FORWARDED_FOR, "edge-a");

        let req = prepare_forward_request(&target, "/api/tenants/ten_1/watch", &incoming, "node-b")
            .unwrap();

        assert_eq!(
            req.headers[HDR_FORWARDED_FOR].to_str().unwrap(),
            "edge-a,node-b"
        );
    }

    #[test]
    fn prepare_forward_request_rejects_already_forwarded_request() {
        let outcome = route_http_outcome(&decision(RouteAction::Forward, None));
        let RouteHttpOutcome::Forward(target) = outcome else {
            panic!("expected forward");
        };
        let mut incoming = HeaderMap::new();
        insert_header(&mut incoming, HDR_FORWARDED_BY, "node-a");

        let err = prepare_forward_request(&target, "/api/tenants/ten_1/watch", &incoming, "node-b")
            .unwrap_err();

        assert_eq!(err.status, StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(err.code, "ROUTE_FORWARD_LOOP");
        assert_eq!(
            err.headers[HDR_ROUTE_REASON].to_str().unwrap(),
            "ROUTE_FORWARD_LOOP"
        );
    }

    #[test]
    fn route_runtime_disabled_serves_local_without_headers() {
        let runtime = RouteRuntime::disabled();
        let outcome = runtime.guard("/api/tenants/ten_1/watch", &HeaderMap::new());

        let RouteGuardOutcome::ServeLocal(headers) = outcome else {
            panic!("expected local");
        };
        assert!(headers.is_empty());
    }

    #[test]
    fn route_runtime_fixed_forward_prepares_request() {
        let runtime = RouteRuntime::fixed("node-local", decision(RouteAction::Forward, None));
        let outcome = runtime.guard("/api/tenants/ten_1/watch", &HeaderMap::new());

        let RouteGuardOutcome::Forward(req) = outcome else {
            panic!("expected forward");
        };
        assert_eq!(req.uri, "http://node-a:8080/api/tenants/ten_1/watch");
        assert_eq!(
            req.headers[HDR_FORWARDED_BY].to_str().unwrap(),
            "node-local"
        );
    }

    #[test]
    fn route_runtime_fixed_reject_returns_rejection() {
        let runtime = RouteRuntime::fixed(
            "node-local",
            decision(RouteAction::Reject, Some(RouteReason::WrongLocation)),
        );
        let outcome = runtime.guard("/api/tenants/ten_1/watch", &HeaderMap::new());

        let RouteGuardOutcome::Reject(rejection) = outcome else {
            panic!("expected rejection");
        };
        assert_eq!(rejection.code, "ROUTE_WRONG_LOCATION");
    }

    #[test]
    fn route_runtime_snapshot_serves_local_owner() {
        let loc = locator("ses_1", 1, Some(7));
        let snapshot = RouteSnapshot::new(16)
            .with_request_location("us-west-2a")
            .with_tablets(vec![tablet(7, Some("node-a"), i64::MAX)])
            .with_nodes(vec![node("node-a", "us-west-2a")]);
        let runtime = RouteRuntime::snapshot("node-a", snapshot);

        let outcome = runtime.guard_session("ten_1", Some(&loc), "/api/test", &HeaderMap::new());

        let RouteGuardOutcome::ServeLocal(headers) = outcome else {
            panic!("expected local");
        };
        assert_eq!(headers[HDR_ROUTE_ACTION].to_str().unwrap(), "local");
        assert_eq!(headers[HDR_TABLET_ID].to_str().unwrap(), "7");
    }

    #[test]
    fn route_runtime_snapshot_forwards_remote_owner() {
        let loc = locator("ses_1", 1, Some(7));
        let snapshot = RouteSnapshot::new(16)
            .with_request_location("us-west-2a")
            .with_tablets(vec![tablet(7, Some("node-b"), i64::MAX)])
            .with_nodes(vec![node("node-b", "us-west-2a")]);
        let runtime = RouteRuntime::snapshot("node-a", snapshot);

        let outcome = runtime.guard_session(
            "ten_1",
            Some(&loc),
            "/api/tenants/ten_1/sessions/ses_1/events",
            &HeaderMap::new(),
        );

        let RouteGuardOutcome::Forward(req) = outcome else {
            panic!("expected forward");
        };
        assert_eq!(
            req.uri,
            "http://node-b:8080/api/tenants/ten_1/sessions/ses_1/events"
        );
        assert_eq!(req.headers[HDR_FORWARDED_BY].to_str().unwrap(), "node-a");
    }

    #[test]
    fn route_runtime_snapshot_uses_shard_override_without_locator() {
        let snapshot = RouteSnapshot::new(1)
            .with_shards(vec![ShardRow {
                shard_id: 0,
                tablet_id: Some(42),
                updated_at_ms: 10,
            }])
            .with_tablets(vec![tablet(42, Some("node-a"), i64::MAX)])
            .with_nodes(vec![node("node-a", "us-west-2a")]);
        let runtime = RouteRuntime::snapshot("node-a", snapshot);

        let outcome = runtime.guard_session("ten_1", None, "/api/test", &HeaderMap::new());

        let RouteGuardOutcome::ServeLocal(headers) = outcome else {
            panic!("expected local");
        };
        assert_eq!(headers[HDR_ROUTE_ACTION].to_str().unwrap(), "local");
        assert_eq!(headers[HDR_TABLET_ID].to_str().unwrap(), "42");
    }

    #[test]
    fn route_runtime_snapshot_guard_session_id_uses_locator() {
        let snapshot = RouteSnapshot::new(1)
            .with_tablets(vec![
                tablet(0, Some("node-b"), i64::MAX),
                tablet(7, Some("node-a"), i64::MAX),
            ])
            .with_nodes(vec![
                node("node-a", "us-west-2a"),
                node("node-b", "us-west-2a"),
            ])
            .with_session_locators(vec![locator("ses_1", 0, Some(7))]);
        let runtime = RouteRuntime::snapshot("node-a", snapshot);

        let outcome = runtime.guard_session_id("ten_1", "ses_1", "/api/test", &HeaderMap::new());

        let RouteGuardOutcome::ServeLocal(headers) = outcome else {
            panic!("expected local");
        };
        assert_eq!(headers[HDR_ROUTE_ACTION].to_str().unwrap(), "local");
        assert_eq!(headers[HDR_TABLET_ID].to_str().unwrap(), "7");
    }

    #[tokio::test]
    async fn route_snapshot_loads_from_meta_store() {
        let store = MetaStore::open_sqlite(":memory:").await.unwrap();
        let pool = store.pool();
        agentdb_meta::shards::seed_shards(pool, 1, 10)
            .await
            .unwrap();
        agentdb_meta::shards::set_assignment(pool, 0, Some(7), 11)
            .await
            .unwrap();
        agentdb_meta::nodes::upsert(
            pool,
            "node-a",
            "http://node-a:8080",
            None,
            "alive",
            100,
            Some("us-west-2a"),
        )
        .await
        .unwrap();
        agentdb_meta::tablets::get_or_create(pool, 7, 100)
            .await
            .unwrap();
        agentdb_meta::tablets::assume_ownership(pool, 7, 0, "node-a", i64::MAX)
            .await
            .unwrap();
        agentdb_meta::session_locators::upsert(pool, "ses_1", 1, "ten_1", Some(7), 0, 100)
            .await
            .unwrap();

        let snapshot =
            RouteSnapshot::load_from_meta(&store, 1, Some("us-west-2a".into()), Default::default())
                .await
                .unwrap();

        assert_eq!(snapshot.shards.len(), 1);
        assert_eq!(snapshot.tablets.len(), 1);
        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(snapshot.session_locators.len(), 1);
        assert_eq!(snapshot.session_locators[0].session_id, "ses_1");
    }

    #[test]
    fn route_runtime_snapshot_rejects_unassigned_tablet() {
        let loc = locator("ses_1", 1, Some(7));
        let snapshot = RouteSnapshot::new(16)
            .with_tablets(vec![tablet(7, None, i64::MAX)])
            .with_nodes(vec![node("node-b", "us-west-2a")]);
        let runtime = RouteRuntime::snapshot("node-a", snapshot);

        let outcome = runtime.guard_session("ten_1", Some(&loc), "/api/test", &HeaderMap::new());

        let RouteGuardOutcome::Reject(rejection) = outcome else {
            panic!("expected reject");
        };
        assert_eq!(rejection.code, "ROUTE_UNASSIGNED");
        assert_eq!(rejection.status, StatusCode::SERVICE_UNAVAILABLE);
    }
}
