// AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
// Scenario: S11 wire_sdk_adapter_and_config_boundaries_reject_malformed_or_unsupported_states
// Falsifies: C14, C18, C20, C21
//
// REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

#![no_main]

use agentdb_wire::command::command_name;
use agentdb_wire::opmsg::{parse_message_frame, OP_MSG, OP_QUERY};
use bson::Document;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Workload: feed arbitrary bytes to BSON, command-name extraction, and
    // production OP_MSG / legacy OP_QUERY frame decoding.
    // Faults: malformed bytes, truncated UTF-8, invalid lengths, unsupported
    // opcodes, malformed sections, unknown commands, and command-shaped values.
    // Oracle: parser/dispatcher boundary must not panic or hang; invalid input
    // must reject as an error.
    if data.len() <= 4096 {
        let _ = parse_message_frame(data);
    }

    if let Ok(doc) = bson::from_slice::<Document>(data) {
        let _ = command_name(&doc);
        let _ = parse_message_frame(&op_msg_frame(&doc));
        let _ = parse_message_frame(&op_query_frame("agentdb.$cmd", &doc));
    }

    if let Ok(text) = std::str::from_utf8(data) {
        let _ = serde_json::from_str::<serde_json::Value>(text);
    }
});

fn op_msg_frame(doc: &Document) -> Vec<u8> {
    let mut body = Vec::new();
    if doc.to_writer(&mut body).is_err() {
        return Vec::new();
    }
    let message_len = 16 + 4 + 1 + body.len();
    let mut frame = Vec::with_capacity(message_len);
    frame.extend_from_slice(&(message_len as i32).to_le_bytes());
    frame.extend_from_slice(&1_i32.to_le_bytes());
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(&OP_MSG.to_le_bytes());
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.push(0);
    frame.extend_from_slice(&body);
    frame
}

fn op_query_frame(namespace: &str, doc: &Document) -> Vec<u8> {
    let mut body = Vec::new();
    if doc.to_writer(&mut body).is_err() {
        return Vec::new();
    }
    let message_len = 16 + 4 + namespace.len() + 1 + 4 + 4 + body.len();
    let mut frame = Vec::with_capacity(message_len);
    frame.extend_from_slice(&(message_len as i32).to_le_bytes());
    frame.extend_from_slice(&2_i32.to_le_bytes());
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(&OP_QUERY.to_le_bytes());
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(namespace.as_bytes());
    frame.push(0);
    frame.extend_from_slice(&0_i32.to_le_bytes());
    frame.extend_from_slice(&1_i32.to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}
