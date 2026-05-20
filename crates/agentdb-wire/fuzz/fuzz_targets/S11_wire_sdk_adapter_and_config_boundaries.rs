// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S11 wire_sdk_adapter_and_config_boundaries_reject_malformed_or_unsupported_states
// Falsifies: C14, C18, C20, C21
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

#![no_main]

use bson::Document;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Workload: feed arbitrary bytes to BSON and Mongo command-shaped decoding.
    // Faults: malformed bytes, truncated UTF-8, oversized or unknown command bodies.
    // Oracle: parser must not panic or hang; invalid input must reject as an error.
    let _ = bson::from_slice::<Document>(data);

    if let Ok(text) = std::str::from_utf8(data) {
        let _ = serde_json::from_str::<serde_json::Value>(text);
    }
});
