import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const MAX_SESSIONS = 200;
const MAX_EVENT_TEXT = 4000;

export function codexSessionRoots(env = process.env) {
  const roots = [];
  if (env.CODEX_HOME) roots.push(path.join(env.CODEX_HOME, "sessions"));
  roots.push(path.join(os.homedir(), ".codex", "sessions"));
  return [...new Set(roots.map((root) => path.resolve(root)))];
}

export function listCodexSessions({ roots = codexSessionRoots(), limit = MAX_SESSIONS } = {}) {
  const files = [];
  for (const root of roots) {
    if (!safeExists(root)) continue;
    walkJsonl(root, files);
  }
  return files
    .map((file) => {
      const stat = fs.statSync(file);
      const quick = quickSessionInfo(file);
      return {
        id: Buffer.from(file).toString("base64url"),
        host: "codex",
        path: file,
        displayPath: displayPath(file),
        title: quick.title,
        repo: quick.repo,
        branch: quick.branch,
        startedAt: quick.startedAt ?? stat.birthtime.toISOString(),
        updatedAt: stat.mtime.toISOString(),
        eventCount: quick.eventCount,
        resetCount: quick.resetCount,
      };
    })
    .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
    .slice(0, limit);
}

export function parseCodexSession(file) {
  const events = [];
  let meta = {};
  const lines = fs.readFileSync(file, "utf8").split(/\r?\n/).filter(Boolean);
  let seq = 0;
  for (const line of lines) {
    let record;
    try {
      record = JSON.parse(line);
    } catch {
      continue;
    }
    if (record.type === "session_meta") {
      meta = { ...meta, ...normalizeMeta(record.payload ?? {}) };
    }
    const event = normalizeRecord(record, seq++);
    if (event) events.push(event);
  }
  const resetIndexes = events
    .map((event, index) => (event.category === "reset" ? index : -1))
    .filter((index) => index >= 0);
  const latestResetIndex = resetIndexes.length ? resetIndexes[resetIndexes.length - 1] : -1;
  return {
    id: Buffer.from(file).toString("base64url"),
    host: "codex",
    path: file,
    displayPath: displayPath(file),
    meta: {
      ...meta,
      title: meta.title || firstTitle(events) || path.basename(file),
      repo: meta.repo || repoFromCwd(meta.cwd),
      branch: meta.branch || "unknown",
    },
    events,
    resetIndexes,
    latestResetIndex,
    stats: summarizeEvents(events),
  };
}

export function resolveSessionPath(id, roots = codexSessionRoots()) {
  let decoded;
  try {
    decoded = Buffer.from(id, "base64url").toString("utf8");
  } catch {
    throw new Error("invalid session id");
  }
  const resolved = path.resolve(decoded);
  const allowed = roots.some((root) => resolved.startsWith(path.resolve(root) + path.sep));
  if (!allowed) throw new Error("session path is outside configured Codex session roots");
  if (!resolved.endsWith(".jsonl")) throw new Error("session path is not a JSONL file");
  return resolved;
}

function quickSessionInfo(file) {
  const info = { title: path.basename(file), repo: "unknown", branch: "unknown", eventCount: 0, resetCount: 0 };
  const fd = fs.openSync(file, "r");
  try {
    const text = fs.readFileSync(fd, "utf8");
    for (const line of text.split(/\r?\n/)) {
      if (!line.trim()) continue;
      info.eventCount += 1;
      let record;
      try {
        record = JSON.parse(line);
      } catch {
        continue;
      }
      if (!info.startedAt && record.timestamp) info.startedAt = record.timestamp;
      if (record.type === "session_meta") {
        const meta = normalizeMeta(record.payload ?? {});
        info.repo = meta.repo ?? info.repo;
        info.branch = meta.branch ?? info.branch;
      }
      const event = normalizeRecord(record, info.eventCount);
      if (event?.category === "reset") info.resetCount += 1;
      if (info.title === path.basename(file) && event?.category === "prompt" && event.text) {
        info.title = firstLine(event.text, 80);
      }
    }
  } finally {
    fs.closeSync(fd);
  }
  return info;
}

function normalizeMeta(payload) {
  const git = payload.git && typeof payload.git === "object" ? payload.git : {};
  return {
    id: payload.id,
    startedAt: payload.timestamp,
    cwd: payload.cwd,
    repo: repoFromCwd(payload.cwd),
    branch: git.branch || git.ref || git.head || "unknown",
    commit: git.commit || git.sha || git.head_sha,
    cliVersion: payload.cli_version,
    source: payload.source,
    modelProvider: payload.model_provider,
  };
}

function normalizeRecord(record, seq) {
  const payload = record.payload ?? {};
  const type = payload.type ?? record.type;
  const timestamp = record.timestamp ?? payload.started_at ?? payload.completed_at;
  const base = {
    seq,
    timestamp,
    recordType: record.type,
    type,
    category: "other",
    title: type,
    text: "",
    callId: payload.call_id,
    command: null,
    exitCode: null,
    success: payload.success,
    raw: compactRaw(payload),
  };

  if (record.type === "session_meta") return { ...base, category: "meta", title: "Session started", text: payload.cwd ?? "" };
  if (record.type === "turn_context") {
    return { ...base, category: "context", title: "Turn context", text: payload.cwd ?? "" };
  }
  if (record.type === "compacted" || type === "context_compacted" || type === "thread_rolled_back") {
    return { ...base, category: "reset", title: resetTitle(type), text: payload.message ?? payload.reason ?? "" };
  }
  if (type === "task_started") return { ...base, category: "task", title: "Task started" };
  if (type === "task_complete") {
    return { ...base, category: "task", title: "Task complete", text: payload.last_agent_message ?? "" };
  }
  if (type === "turn_aborted") return { ...base, category: "failure", title: "Turn aborted", text: payload.reason ?? "" };
  if (type === "user_message") {
    return { ...base, category: "prompt", title: "User prompt", text: cleanText(payload.message) };
  }
  if (type === "agent_message") {
    return { ...base, category: "assistant", title: "Assistant message", text: cleanText(payload.message) };
  }
  if (type === "message") {
    const role = payload.role ?? "message";
    const text = cleanText(extractContent(payload.content));
    return {
      ...base,
      category: role === "user" ? "prompt" : role === "assistant" ? "assistant" : "message",
      title: `${capitalize(role)} message`,
      text,
    };
  }
  if (type === "function_call" || type === "custom_tool_call" || type === "web_search_call" || type === "tool_search_call") {
    const name = payload.name || payload.action?.type || type;
    const args = parseMaybeJson(payload.arguments ?? payload.input ?? payload.action);
    const command = commandFromTool(name, args);
    return {
      ...base,
      category: "tool_call",
      title: command ? `Command: ${firstLine(command, 100)}` : `Tool call: ${name}`,
      text: summarizeToolInput(name, args),
      command,
      callId: payload.call_id,
      raw: compactRaw({ ...payload, arguments: args }),
    };
  }
  if (type === "function_call_output" || type === "custom_tool_call_output") {
    const output = cleanText(payload.output);
    return {
      ...base,
      category: outputLooksFailed(output) ? "failure" : "tool_result",
      title: "Tool result",
      text: output,
      exitCode: exitCodeFromText(output),
      success: exitCodeFromText(output) === null ? undefined : exitCodeFromText(output) === 0,
    };
  }
  if (type === "patch_apply_end") {
    return {
      ...base,
      category: payload.success === false ? "failure" : "tool_result",
      title: payload.success === false ? "Patch failed" : "Patch applied",
      text: cleanText(payload.stdout || payload.stderr || payload.status || ""),
      success: payload.success,
    };
  }
  if (type === "mcp_tool_call_end") {
    const success = !String(payload.result?.isError ?? "").includes("true");
    return {
      ...base,
      category: success ? "tool_result" : "failure",
      title: "MCP tool result",
      text: cleanText(JSON.stringify(payload.result ?? {})),
      success,
    };
  }
  return { ...base, text: cleanText(payload.message ?? payload.summary ?? "") };
}

function summarizeEvents(events) {
  return {
    total: events.length,
    prompts: events.filter((event) => event.category === "prompt").length,
    toolCalls: events.filter((event) => event.category === "tool_call").length,
    failures: events.filter((event) => event.category === "failure" || event.success === false || (event.exitCode ?? 0) !== 0).length,
    resets: events.filter((event) => event.category === "reset").length,
  };
}

function firstTitle(events) {
  return events.find((event) => event.category === "prompt" && event.text)?.text?.split(/\n/)[0]?.slice(0, 80);
}

function walkJsonl(dir, out) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walkJsonl(p, out);
    else if (ent.name.endsWith(".jsonl")) out.push(p);
  }
}

function safeExists(p) {
  try {
    return fs.existsSync(p);
  } catch {
    return false;
  }
}

function displayPath(file) {
  return file.startsWith(os.homedir()) ? `~${file.slice(os.homedir().length)}` : file;
}

function repoFromCwd(cwd) {
  if (!cwd) return "unknown";
  return path.basename(cwd);
}

function firstLine(text, max = 120) {
  const line = String(text ?? "").replace(/\s+/g, " ").trim();
  return line.length > max ? `${line.slice(0, max - 1)}...` : line;
}

function cleanText(value) {
  if (value === undefined || value === null) return "";
  const text = typeof value === "string" ? value : JSON.stringify(value);
  return text.length > MAX_EVENT_TEXT ? `${text.slice(0, MAX_EVENT_TEXT)}\n...[truncated]` : text;
}

function extractContent(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return content ?? "";
  return content
    .map((part) => {
      if (typeof part === "string") return part;
      if (part?.type === "input_text" || part?.type === "output_text" || part?.type === "text") return part.text ?? "";
      return "";
    })
    .filter(Boolean)
    .join("\n");
}

function parseMaybeJson(value) {
  if (typeof value !== "string") return value ?? {};
  try {
    return JSON.parse(value);
  } catch {
    return { value };
  }
}

function commandFromTool(name, args) {
  if (name === "exec_command" || name === "Bash") return args?.cmd ?? args?.command ?? null;
  return null;
}

function summarizeToolInput(name, args) {
  if (!args || typeof args !== "object") return String(args ?? "");
  if (name === "exec_command" && args.cmd) return args.cmd;
  const keys = Object.keys(args).slice(0, 6);
  return keys.map((key) => `${key}: ${firstLine(args[key], 120)}`).join("\n");
}

function exitCodeFromText(text) {
  const match = String(text ?? "").match(/Process exited with code\s+(-?\d+)/);
  return match ? Number(match[1]) : null;
}

function outputLooksFailed(text) {
  const exit = exitCodeFromText(text);
  return exit !== null && exit !== 0;
}

function resetTitle(type) {
  if (type === "context_compacted" || type === "compacted") return "Context compacted";
  if (type === "thread_rolled_back") return "Thread rolled back";
  return "Reset boundary";
}

function compactRaw(value) {
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [key, val] of Object.entries(value)) {
    if (key === "encrypted_content") continue;
    if (typeof val === "string" && val.length > MAX_EVENT_TEXT) out[key] = `${val.slice(0, MAX_EVENT_TEXT)}\n...[truncated]`;
    else out[key] = val;
  }
  return out;
}

function capitalize(value) {
  return value ? `${value[0].toUpperCase()}${value.slice(1)}` : value;
}
