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
}
