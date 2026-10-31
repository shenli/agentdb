//! Route stability enforcement (product-simplification proposal, "Stability
//! Labels"): every Core route is covered by both SDKs, and every Labs route
//! stays out of the quickstart — the beginner path. Labels live in
//! `routes::route_catalog()`.

use agentdb_api::routes::{route_catalog, Stability};

/// The distinctive trailing path token for a route: the last non-`:param`
/// segment, e.g. `/events`, `/replay-prefix`, or (for `/records/search`)
/// `/search`. A robust probe against SDK source and docs without reproducing
/// each SDK's path templating.
fn distinctive_suffix(path: &str) -> String {
    let last = path
        .split('/')
        .rfind(|s| !s.is_empty() && !s.starts_with(':'))
        .expect("every route path has at least one literal segment");
    format!("/{last}")
}

fn read(rel: &str) -> String {
    let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

#[test]
fn core_routes_are_covered_by_both_sdks() {
    let py = read("../../clients/python/agentdb/client.py");
    let ts = read("../../clients/typescript/src/client.ts");
    for (stability, path) in route_catalog() {
        if *stability != Stability::Core {
            continue;
        }
        let probe = distinctive_suffix(path);
        assert!(
            py.contains(&probe),
            "Core route {path} ({probe}) is not covered by the Python SDK",
        );
        assert!(
            ts.contains(&probe),
            "Core route {path} ({probe}) is not covered by the TypeScript SDK",
        );
    }
}

#[test]
fn labs_routes_are_absent_from_the_quickstart() {
    let quickstart = read("../../docs/quickstart.md");
    for (stability, path) in route_catalog() {
        if *stability != Stability::Labs {
            continue;
        }
        let probe = distinctive_suffix(path);
        assert!(
            !quickstart.contains(&probe),
            "Labs route {path} ({probe}) must not appear in the quickstart (beginner path)",
        );
    }
}

#[test]
fn catalog_covers_all_three_labels() {
    let has = |want: Stability| route_catalog().iter().any(|(s, _)| *s == want);
    assert!(has(Stability::Core), "catalog must classify Core routes");
    assert!(has(Stability::Labs), "catalog must classify Labs routes");
    assert!(
        has(Stability::Internal),
        "catalog must classify Internal routes"
    );
}
