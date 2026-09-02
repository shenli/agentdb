#[test]
fn dashboard_targets_v2_session_surface() {
    let index = include_str!("../static/index.html");
    let app = include_str!("../static/app.js");

    assert!(index.contains("Runtime dashboard"));
    assert!(index.contains("Session explorer"));
    assert!(app.contains("/events"));
    assert!(app.contains("/replay"));
    assert!(app.contains("/subscribe"));
    assert!(app.contains("/records/search"));
    assert!(app.contains("/artifacts/"));
    assert!(app.contains("/storage"));
    assert!(app.contains("/api/storage/summary"));
    assert!(app.contains("/lifecycle"));
    assert!(app.contains("/restore"));
    assert!(index.contains("Storage detail"));
    assert!(index.contains("Warm"));
    assert!(index.contains("Cold"));
    assert!(index.contains("Archive"));
    assert!(index.contains("Restore cold"));
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

#[test]
fn dashboard_never_persists_or_links_bearer_tokens() {
    let app = include_str!("../static/app.js");

    assert!(app.contains("delete persisted.token"));
    assert!(app.contains("delete stored.token"));
    assert!(app.contains("url.searchParams.delete(\"token\")"));
    assert!(!app.contains("mapQueryParam(params, cfg, \"token\""));
    assert!(!app.contains("setQueryValue(url.searchParams, \"token\""));
}
