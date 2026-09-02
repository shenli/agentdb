/**
 * Advisory session event conventions.
 *
 * The runtime accepts arbitrary event kinds and payloads. These helpers provide
 * a shared vocabulary for SDK examples and framework adapters without making
 * the server enforce a closed schema.
 */

export const SessionEventKind = {
  UserMessage: "user_message",
  ModelMessage: "model_message",
  ModelThought: "model_thought",
  ToolCall: "tool_call",
  ToolResult: "tool_result",
  ToolError: "tool_error",
  ArtifactCreated: "artifact_created",
  ApprovalRequested: "approval_requested",
  ApprovalResult: "approval_result",
  ContextPackBuilt: "context_pack_built",
  AgentStatus: "agent_status",
} as const;

export type SessionEventKind = typeof SessionEventKind[keyof typeof SessionEventKind];
export type Payload = Record<string, unknown>;

export const ADAPTER_READINESS_CHECKS = [
  "agentdb_ordering",
  "tool_call_pairing",
  "artifact_large_outputs",
  "resume_cursor",
  "context_provenance",
  "source_refs",
] as const;

export type AdapterReadinessCheck = typeof ADAPTER_READINESS_CHECKS[number];

export function userMessage(
  text: string,
  opts: { role?: string; userId?: string; channel?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    role: opts.role ?? "user",
    text,
    user_id: opts.userId,
    channel: opts.channel,
    metadata: opts.metadata,
  });
}

export function modelMessage(
  text: string,
  opts: {
    role?: string;
    model?: string;
    provider?: string;
    finishReason?: string;
    usage?: Payload;
    contextPackId?: string;
    metadata?: Payload;
  } = {}
): Payload {
  return compact({
    role: opts.role ?? "assistant",
    text,
    model: opts.model,
    provider: opts.provider,
    finish_reason: opts.finishReason,
    usage: opts.usage,
    context_pack_id: opts.contextPackId,
    metadata: opts.metadata,
  });
}

export function modelThought(
  summary: string,
  opts: { text?: string; visibility?: string; model?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    summary,
    text: opts.text,
    visibility: opts.visibility,
    model: opts.model,
    metadata: opts.metadata,
  });
}

export function toolCall(
  toolCallId: string,
  tool: string,
  input: Payload,
  opts: { handId?: string; timeoutMs?: number; idempotencyKey?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    tool_call_id: toolCallId,
    tool,
    input,
    hand_id: opts.handId,
    timeout_ms: opts.timeoutMs,
    idempotency_key: opts.idempotencyKey,
    metadata: opts.metadata,
  });
}

export function toolResult(
  toolCallId: string,
  summary: string,
  opts: { status?: string; output?: Payload; durationMs?: number; exitCode?: number; metadata?: Payload } = {}
): Payload {
  return compact({
    tool_call_id: toolCallId,
    status: opts.status ?? "ok",
    summary: sanitizeToolOutput(summary),
    output: opts.output === undefined ? undefined : sanitizeToolOutput(opts.output),
    duration_ms: opts.durationMs,
    exit_code: opts.exitCode,
    metadata: opts.metadata === undefined ? undefined : sanitizeToolOutput(opts.metadata),
  });
}

const SECRET_ASSIGNMENT_RE = /\b(api[_-]?key|access[_-]?token|auth[_-]?token|client[_-]?secret|password|passwd|private[_-]?key|refresh[_-]?token|secret[_-]?access[_-]?key)\b(\s*[:=]\s*)([^\s,;]+)/gi;
const AUTHORIZATION_RE = /\b(authorization\s*[:=]\s*(?:Bearer\s+)?)([^\s,;]+)/gi;
const BEARER_RE = /\b(Bearer\s+)[A-Za-z0-9._~+/=-]{20,}\b/gi;
const OPENAI_KEY_RE = /\bsk-[A-Za-z0-9_-]{16,}\b/g;
const GITHUB_TOKEN_RE = /\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b/g;
const AWS_ACCESS_KEY_RE = /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g;

function sanitizeToolOutput(value: unknown): unknown {
  return sanitizeToolOutputValue(value, new WeakSet<object>());
}

function sanitizeToolOutputValue(value: unknown, seen: WeakSet<object>): unknown {
  if (typeof value === "string") return sanitizeSecretText(value);
  if (Array.isArray(value)) {
    if (seen.has(value)) return value;
    seen.add(value);
    return value.map((child) => sanitizeToolOutputValue(child, seen));
  }
  if (value === null || typeof value !== "object") return value;
  if (seen.has(value)) return value;
  seen.add(value);
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, child]) => [
      key,
      sanitizeToolOutputValue(child, seen),
    ])
  );
}

function sanitizeSecretText(text: string): string {
  return text
    .replace(AUTHORIZATION_RE, "$1[REDACTED]")
    .replace(BEARER_RE, "$1[REDACTED]")
    .replace(SECRET_ASSIGNMENT_RE, "$1$2[REDACTED]")
    .replace(OPENAI_KEY_RE, "[REDACTED]")
    .replace(GITHUB_TOKEN_RE, "[REDACTED]")
    .replace(AWS_ACCESS_KEY_RE, "[REDACTED]");
}

export function toolError(
  toolCallId: string,
  summary: string,
  opts: {
    code: string;
    message: string;
    retryable: boolean;
    durationMs?: number;
    exitCode?: number;
    metadata?: Payload;
  }
): Payload {
  return compact({
    tool_call_id: toolCallId,
    retryable: opts.retryable,
    summary,
    error: { code: opts.code, message: opts.message },
    duration_ms: opts.durationMs,
    exit_code: opts.exitCode,
    metadata: opts.metadata,
  });
}

export function artifactCreated(
  artifactId: string,
  summary: string,
  opts: {
    name?: string;
    contentType?: string;
    bytes?: number;
    sha256?: string;
    purpose?: string;
    metadata?: Payload;
  } = {}
): Payload {
  return compact({
    artifact_id: artifactId,
    summary,
    name: opts.name,
    content_type: opts.contentType,
    bytes: opts.bytes,
    sha256: opts.sha256,
    purpose: opts.purpose,
    metadata: opts.metadata,
  });
}

export function approvalRequested(
  approvalId: string,
  action: string,
  summary: string,
  opts: { risk?: string; expiresAtMs?: number; metadata?: Payload } = {}
): Payload {
  return compact({
    approval_id: approvalId,
    action,
    summary,
    risk: opts.risk,
    expires_at_ms: opts.expiresAtMs,
    metadata: opts.metadata,
  });
}

export function approvalResult(
  approvalId: string,
  decision: string,
  actor: Payload,
  opts: { reason?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    approval_id: approvalId,
    decision,
    actor,
    reason: opts.reason,
    metadata: opts.metadata,
  });
}

export function contextPackBuilt(
  contextPackId: string,
  include: string[],
  opts: {
    maxEvents?: number;
    maxBytes?: number;
    tokenBudget?: number;
    selectedEventCount?: number;
    selectedRecordCount?: number;
    promptCacheKey?: string;
    metadata?: Payload;
  } = {}
): Payload {
  return compact({
    context_pack_id: contextPackId,
    include,
    max_events: opts.maxEvents,
    max_bytes: opts.maxBytes,
    token_budget: opts.tokenBudget,
    selected_event_count: opts.selectedEventCount,
    selected_record_count: opts.selectedRecordCount,
    prompt_cache_key: opts.promptCacheKey,
    metadata: opts.metadata,
  });
}

export function contextSourceRefs(
  context: Payload,
  opts: { maxRefs?: number } = {}
): Payload[] {
  const refs: Payload[] = [];
  for (const event of arrayField(context.events)) {
    const eventId = field(event, "event_id", "eventId");
    if (eventId !== undefined) refs.push({ event_id: eventId });
  }
  for (const record of arrayField(context.records)) {
    const recordId = field(record, "record_id", "recordId");
    if (recordId !== undefined) refs.push({ record_id: recordId });
  }
  return opts.maxRefs === undefined ? refs : refs.slice(0, opts.maxRefs);
}

export function agentStatus(
  status: string,
  opts: { summary?: string; lastSeenSeq?: number; reason?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    status,
    summary: opts.summary,
    last_seen_seq: opts.lastSeenSeq,
    reason: opts.reason,
    metadata: opts.metadata,
  });
}

export function validateAdapterEvents(
  events: Payload[],
  opts: { largeOutputChars?: number } = {}
): string[] {
  const largeOutputChars = opts.largeOutputChars ?? 4096;
  const warnings: string[] = [];
  const toolCalls = new Map<string, unknown>();
  const pairedToolCalls = new Set<string>();
  let hasContextPack = false;
  let hasModelMessage = false;
  let hasRecoveryStatus = false;
  let sawSeq = false;

  for (const event of events) {
    const kind = field(event, "kind");
    const payload = payloadField(event);
    const eventId = field(event, "event_id", "eventId");
    const seq = field(event, "seq");
    if (seq !== undefined) sawSeq = true;

    if (kind === SessionEventKind.ToolCall) {
      const callId = stringField(payload, "tool_call_id", "toolCallId");
      if (callId === undefined) warnings.push("tool_call_missing_tool_call_id");
      else toolCalls.set(callId, eventId ?? callId);
    }

    if (kind === SessionEventKind.ToolResult || kind === SessionEventKind.ToolError) {
      const callId = stringField(payload, "tool_call_id", "toolCallId");
      if (callId === undefined) warnings.push(`${kind}_missing_tool_call_id`);
      else if (!toolCalls.has(callId)) warnings.push(`${kind}_without_prior_tool_call:${callId}`);
      else pairedToolCalls.add(callId);
      const output = field(payload, "output");
      if (typeof output === "string" && output.length > largeOutputChars) {
        if (arrayField(field(event, "artifact_refs", "artifactRefs")).length === 0) {
          warnings.push(`${kind}_large_output_without_artifact:${callId ?? ""}`);
        }
      }
    }

    if (kind === SessionEventKind.ContextPackBuilt) {
      hasContextPack = true;
      if (arrayField(field(event, "source_refs", "sourceRefs")).length === 0) {
        warnings.push("context_pack_built_without_source_refs");
      }
    }

    if (kind === SessionEventKind.ModelMessage) {
      hasModelMessage = true;
      const contextPackId = field(payload, "context_pack_id", "contextPackId");
      if (contextPackId !== undefined && arrayField(field(event, "source_refs", "sourceRefs")).length === 0) {
        warnings.push("model_message_with_context_pack_without_source_refs");
      }
    }

    if (kind === SessionEventKind.AgentStatus && field(payload, "status") === "recovered") {
      hasRecoveryStatus = true;
      if (field(payload, "last_seen_seq", "lastSeenSeq") === undefined) {
        warnings.push("recovered_status_without_last_seen_seq");
      }
    }
  }

  for (const callId of toolCalls.keys()) {
    if (!pairedToolCalls.has(callId)) warnings.push(`tool_call_without_result_or_error:${callId}`);
  }
  if (hasModelMessage && !hasContextPack) warnings.push("model_message_without_context_pack_built");
  if (!hasRecoveryStatus) warnings.push("missing_recovered_agent_status");
  if (!sawSeq) warnings.push("events_missing_agentdb_seq");
  return warnings;
}

function compact(input: Payload): Payload {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined));
}

function arrayField(value: unknown): Payload[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is Payload => item !== null && typeof item === "object" && !Array.isArray(item));
}

function field(value: Payload, ...names: string[]): unknown {
  for (const name of names) {
    if (Object.prototype.hasOwnProperty.call(value, name)) return value[name];
  }
  return undefined;
}

function payloadField(event: Payload): Payload {
  const payload = field(event, "payload");
  if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) return payload as Payload;
  return {};
}

function stringField(value: Payload, ...names: string[]): string | undefined {
  const found = field(value, ...names);
  return typeof found === "string" ? found : undefined;
}
