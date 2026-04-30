#[test]
fn dashboard_targets_v2_session_surface() {
    let index = include_str!("../static/index.html");
    let app = include_str!("../static/app.js");

    assert!(index.contains("Session timeline"));
    assert!(app.contains("/events"));
    assert!(app.contains("/replay"));
    assert!(app.contains("/subscribe"));
    assert!(app.contains("/records/search"));
    assert!(app.contains("/artifacts/"));
    assert!(app.contains("/skills"));
}

#[test]
fn dashboard_does_not_call_legacy_admin_surface() {
    let index = include_str!("../static/index.html");
    let app = include_str!("../static/app.js");

    for legacy in [
        "/api/admin",
        "/api/stream",
        "demoFork",
        "demoFault",
        "demoCold",
    ] {
        assert!(!index.contains(legacy), "{legacy} remains in index.html");
        assert!(!app.contains(legacy), "{legacy} remains in app.js");
    }
}
