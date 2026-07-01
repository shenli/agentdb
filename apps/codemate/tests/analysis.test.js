import test from "node:test";
import assert from "node:assert/strict";

import { buildReviewModel, generatePacket } from "../src/analysis.js";

test("builds issues and packet from failed then successful verification", () => {
  const session = {
    host: "codex",
    displayPath: "~/session.jsonl",
    latestResetIndex: -1,
    meta: { repo: "agentdb", branch: "feature" },
    events: [
      { seq: 0, category: "prompt", title: "User prompt", text: "Fix the bug", timestamp: "t0" },
      { seq: 1, category: "tool_call", title: "Command", command: "cargo test -q -p agentdbctl", callId: "1", text: "", timestamp: "t1" },
      { seq: 2, category: "failure", title: "Tool result", callId: "1", text: "Process exited with code 101", exitCode: 101, timestamp: "t2" },
      { seq: 3, category: "tool_call", title: "Command", command: "cargo test -q -p agentdbctl", callId: "2", text: "", timestamp: "t3" },
      { seq: 4, category: "tool_result", title: "Tool result", callId: "2", text: "Process exited with code 0", exitCode: 0, timestamp: "t4" },
    ],
  };
  const model = buildReviewModel(session);
  assert.equal(model.commandPairs.length, 2);
  assert.equal(model.issues.some((issue) => issue.id === "failed-commands"), true);
  assert.equal(model.suggestions.some((suggestion) => suggestion.title.includes("verification")), true);
  const packet = generatePacket(model);
  assert.match(packet, /PASS `cargo test -q -p agentdbctl`/);
  assert.match(packet, /FAIL `cargo test -q -p agentdbctl` exited 101/);
});
