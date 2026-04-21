//! Lock the generated OpenAPI spec against a checked-in snapshot.
//! Any change to the REST surface updates this file; reviewers see
//! the diff in the PR.

use utoipa::OpenApi;

const SNAPSHOT: &str = include_str!("snapshots/openapi.json");

#[test]
fn openapi_snapshot_matches() {
    let spec = agentdb_api::ApiDoc::openapi();
    let generated = serde_json::to_string_pretty(&spec).expect("serialize OpenAPI");
    if generated.trim() != SNAPSHOT.trim() {
        // Print the generated spec so the committer can diff or
        // copy-paste into the snapshot file.
        eprintln!("\nOpenAPI snapshot drift detected.");
        eprintln!("To update the snapshot:");
        eprintln!("  1. Inspect the diff below.");
        eprintln!("  2. If intentional, write the generated JSON to");
        eprintln!("     crates/agentdb-api/tests/snapshots/openapi.json");
        eprintln!("---GENERATED---");
        eprintln!("{generated}");
        eprintln!("---END GENERATED---");
        panic!("OpenAPI snapshot mismatch");
    }
}
