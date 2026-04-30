//! HTTP-facing contract for multi-node route decisions.
//!
//! V2.10 added the pure metadata planner in `agentdb-meta`. This module is the
//! API boundary that converts planner output into the concrete action the HTTP
//! data path can take: serve locally, forward to an owner node, or reject with a
//! structured error response.

use axum::{
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use agentdb_meta::routing::{RouteAction, RouteDecision, RouteReason};

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

#[cfg(test)]
mod tests {
    use super::*;
    use agentdb_meta::routing::{RouteAction, RouteDecision, RouteReason};

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
}
