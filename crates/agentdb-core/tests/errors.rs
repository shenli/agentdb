use agentdb_core::errors::{Error, ErrorCode};

#[test]
fn error_serializes_with_required_fields() {
    let e = Error::new(ErrorCode::EventTooLarge, "payload exceeds 64KB")
        .with_suggestion("store as artifact and reference via artifact_refs");
    let v = serde_json::to_value(&e).unwrap();
    assert_eq!(v["code"], "EVENT_TOO_LARGE");
    assert_eq!(v["retryable"], false);
    assert!(v["suggestion"].as_str().unwrap().contains("artifact"));
    assert!(v["message"].as_str().unwrap().contains("64KB"));
}

#[test]
fn rate_limited_is_retryable() {
    let e = Error::new(ErrorCode::RateLimited, "tenant over qps");
    let v = serde_json::to_value(&e).unwrap();
    assert_eq!(v["retryable"], true);
}
