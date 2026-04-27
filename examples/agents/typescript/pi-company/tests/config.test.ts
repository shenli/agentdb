import { test } from "node:test";
import assert from "node:assert/strict";
import { mergePatch, reduceConfigEvents, CODE_DEFAULTS } from "../src/config.ts";
import type { AgentConfig, ConfigPatch } from "../src/types.ts";

test("CODE_DEFAULTS has entries for all 5 employee/contractor roles", () => {
  for (const role of ["cto", "engineer", "marketing", "sales", "contractor"] as const) {
    assert.ok(CODE_DEFAULTS[role], `missing defaults for ${role}`);
    assert.ok(CODE_DEFAULTS[role].systemPrompt.length > 50, `${role} prompt too short`);
    assert.ok(CODE_DEFAULTS[role].tools.length > 0, `${role} has no tools`);
    assert.equal(CODE_DEFAULTS[role].role, role, `${role} mismatched role field`);
  }
});

test("mergePatch shallow-merges a patch onto a config", () => {
  const base: AgentConfig = {
    role: "engineer",
    systemPrompt: "default prompt",
    tools: ["a", "b"],
    model: "claude-haiku-4-5",
  };
  const patch: ConfigPatch = { systemPromptSuffix: "be terse", model: "gpt-4o-mini" };
  const merged = mergePatch(base, patch);
  assert.equal(merged.systemPrompt, "default prompt");
  assert.equal(merged.systemPromptSuffix, "be terse");
  assert.equal(merged.model, "gpt-4o-mini");
  assert.equal(merged.role, "engineer", "role must be preserved");
  // Original unchanged.
  assert.equal(base.systemPromptSuffix, undefined);
});

test("reduceConfigEvents applies overrides in seq order, last write wins", () => {
  const events = [
    { payload: { kind: "agent_override", role: "engineer", patch: { systemPromptSuffix: "v1" } } },
    { payload: { kind: "agent_override", role: "engineer", patch: { systemPromptSuffix: "v2" } } },
    { payload: { kind: "agent_override", role: "marketing", patch: { systemPromptSuffix: "punchy" } } },
  ];
  const out = reduceConfigEvents(events as any, "engineer");
  assert.equal(out.systemPromptSuffix, "v2");
});

test("reduceConfigEvents returns empty patch when no events for role", () => {
  const out = reduceConfigEvents([], "engineer");
  assert.deepEqual(out, {});
});

test("reduceConfigEvents merges multiple distinct fields across events", () => {
  const events = [
    { payload: { kind: "agent_override", role: "engineer", patch: { systemPromptSuffix: "be terse" } } },
    { payload: { kind: "agent_override", role: "engineer", patch: { model: "gpt-4o-mini" } } },
  ];
  const out = reduceConfigEvents(events as any, "engineer");
  assert.equal(out.systemPromptSuffix, "be terse");
  assert.equal(out.model, "gpt-4o-mini");
});

test("reduceConfigEvents ignores non-override events", () => {
  const events = [
    { payload: { kind: "_noop" } },
    { payload: { kind: "directive", text: "hello" } },
    { payload: { kind: "agent_override", role: "engineer", patch: { model: "x" } } },
    { payload: { kind: "agent_override", role: "marketing" /* no patch field */ } },
  ];
  const out = reduceConfigEvents(events as any, "engineer");
  assert.equal(out.model, "x");
  assert.equal(Object.keys(out).length, 1);
});
