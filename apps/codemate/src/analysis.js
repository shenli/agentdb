const VERIFY_RE = /\b(cargo\s+(test|check|clippy)|npm\s+(test|run\s+build)|pnpm\s+(test|build)|yarn\s+(test|build)|pytest|python3?\s+-m\s+pytest|go\s+test|make\s+test)\b/i;
const CLAIM_RE = /\b(tests?\s+pass(?:ed)?|verified|green|works|fixed|complete|done)\b/i;

export function windowEvents(session, mode = "after-reset") {
  if (mode === "all") return session.events;
  const start = session.latestResetIndex >= 0 ? session.latestResetIndex + 1 : 0;
  return session.events.slice(start);
}

export function buildReviewModel(session, mode = "after-reset") {
  const events = windowEvents(session, mode);
  const pairs = pairCommands(events);
  const issues = findIssues(session, events, pairs);
  const suggestions = suggestBestPractices(session, events, issues, pairs);
  return { session, events, commandPairs: pairs, issues, suggestions, mode };
}

export function generatePacket(model) {
  const { session, events, commandPairs, issues, mode } = model;
  const meta = session.meta;
  const scope = mode === "all" || session.latestResetIndex < 0
    ? "entire imported session"
    : `after latest reset boundary (event ${session.latestResetIndex + 1}; ${session.latestResetIndex + 1} earlier events omitted)`;
  const failed = commandPairs.filter((pair) => pair.exitCode !== null && pair.exitCode !== 0);
  const passedVerify = commandPairs.filter((pair) => pair.isVerification && pair.exitCode === 0);
  const highlights = events
    .filter((event) => ["prompt", "assistant", "failure", "reset"].includes(event.category))
    .slice(0, 12);

  return [
    `# CodeMate Review Packet`,
    ``,
    `## Session`,
    ``,
    `- Repo: ${meta.repo ?? "unknown"}`,
    `- Branch: ${meta.branch ?? "unknown"}`,
    `- Source: ${session.host}`,
    `- Window: ${scope}`,
    `- Events: ${events.length}`,
    ``,
    `## Timeline Highlights`,
    ``,
    ...bulletEvents(highlights),
    ``,
    `## Verification`,
    ``,
    passedVerify.length
      ? passedVerify.map((pair) => `- PASS \`${pair.command}\`${pair.duration ? ` (${pair.duration})` : ""}`).join("\n")
      : `- No successful verification command detected in this window.`,
    ``,
    `## Failed Commands`,
    ``,
    failed.length
      ? failed.map((pair) => `- FAIL \`${pair.command}\` exited ${pair.exitCode}`).join("\n")
      : `- No failed commands detected in this window.`,
    ``,
    `## Review Notes`,
    ``,
    issues.length
      ? issues.map((issue) => `- ${issue.severity.toUpperCase()}: ${issue.message}`).join("\n")
      : `- No obvious review issues detected by the MVP analyzer.`,
    ``,
    `## Replay`,
    ``,
    `- Imported session path: \`${session.displayPath}\``,
    `- Use CodeMate details view for raw event JSON and AgentDB replay instructions after import.`,
    ``,
  ].join("\n");
}

export function findIssues(session, events, commandPairs = pairCommands(events)) {
  const issues = [];
  const verification = commandPairs.filter((pair) => pair.isVerification);
  const successfulVerification = verification.some((pair) => pair.exitCode === 0);
  const failed = commandPairs.filter((pair) => pair.exitCode !== null && pair.exitCode !== 0);
  const claims = events.filter((event) => event.category === "assistant" && CLAIM_RE.test(event.text));

  if (!successfulVerification) {
    issues.push({
      id: "missing-successful-verification",
      severity: "high",
      message: "No successful verification command was detected in the selected review window.",
    });
  }
  if (failed.length) {
    issues.push({
      id: "failed-commands",
      severity: "medium",
      message: `${failed.length} failed command${failed.length === 1 ? "" : "s"} detected before the final packet.`,
    });
  }
  if (claims.length && !successfulVerification) {
    issues.push({
      id: "unbacked-success-claim",
      severity: "high",
      message: "Assistant made completion or verification claims without a matching successful verification command.",
    });
  }
  if (session.latestResetIndex >= 0 && events.length < session.events.length) {
    issues.push({
      id: "reset-scope",
      severity: "info",
      message: "The default packet omits events before the latest reset boundary; widen the window if earlier context matters.",
    });
  }
  return issues;
}

export function suggestBestPractices(session, events, issues, commandPairs = pairCommands(events)) {
  const suggestions = [];
  const verificationCommands = commandPairs.filter((pair) => pair.isVerification).map((pair) => pair.command);
  if (issues.some((issue) => issue.id === "missing-successful-verification")) {
    suggestions.push({
      title: "Require explicit verification before claiming completion",
      target: "AGENTS.md or review checklist",
      body: "When an agent claims a task is complete, include the exact verification command and result in the handoff.",
    });
  }
  if (verificationCommands.length) {
    suggestions.push({
      title: "Document the relevant verification command",
      target: "AGENTS.md",
      body: `For similar work, run \`${verificationCommands[verificationCommands.length - 1]}\` before final handoff.`,
    });
  }
  const correction = events.find((event) => event.category === "prompt" && /\b(must|should|need|please|don't|do not)\b/i.test(event.text));
  if (correction) {
    suggestions.push({
      title: "Promote recurring human correction into guidance",
      target: "repo guidance or skill",
      body: `Consider adding this correction to durable guidance: "${firstLine(correction.text, 180)}"`,
    });
  }
  return suggestions;
}

export function pairCommands(events) {
  const calls = [];
  const byCallId = new Map();
  for (const event of events) {
    if (event.category === "tool_call" && event.command) {
      const pair = {
        callId: event.callId,
        command: event.command,
        startedAt: event.timestamp,
        exitCode: null,
        output: "",
        isVerification: VERIFY_RE.test(event.command),
      };
      calls.push(pair);
      if (event.callId) byCallId.set(event.callId, pair);
    } else if ((event.category === "tool_result" || event.category === "failure") && event.callId && byCallId.has(event.callId)) {
      const pair = byCallId.get(event.callId);
      pair.output = event.text;
      pair.exitCode = event.exitCode;
      pair.finishedAt = event.timestamp;
    }
  }
  return calls;
}

function bulletEvents(events) {
  if (!events.length) return ["- No timeline highlights detected."];
  return events.map((event) => `- ${event.timestamp ?? "unknown time"} [${event.category}] ${event.title}${event.text ? ` - ${firstLine(event.text, 160)}` : ""}`);
}

function firstLine(text, max = 120) {
  const line = String(text ?? "").replace(/\s+/g, " ").trim();
  return line.length > max ? `${line.slice(0, max - 1)}...` : line;
}
