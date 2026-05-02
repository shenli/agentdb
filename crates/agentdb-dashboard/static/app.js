const storeKey = "agentdb.v28.dashboard";

const state = {
  events: [],
  forks: [],
  tenants: [],
  sessions: [],
};

const $ = (id) => document.getElementById(id);

document.addEventListener("DOMContentLoaded", () => {
  restoreInputs();
  document.querySelectorAll("[data-action]").forEach((button) => {
    button.addEventListener("click", () => dispatch(button.dataset.action));
  });
  renderTimeline();
  renderForkTree();
  renderTenantBrowser();
  renderSessionBrowser();
  loadTenants();
  maybeAutoloadFromUrl();
});

function dispatch(action) {
  const actions = {
    tenants: loadTenants,
    sessions: loadSessions,
    load: loadTimeline,
    replay: replaySession,
    subscribe: subscribeSession,
    watch: watchScope,
    context: buildContext,
    fork: forkSession,
    artifact: loadArtifact,
    records: searchRecords,
    skills: loadSkills,
    clear: () => setRaw(""),
  };
  return actions[action] && actions[action]();
}

function readConfig(requireIds = true) {
  const cfg = {
    tenantId: $("tenantId").value.trim(),
    sessionId: $("sessionId").value.trim(),
    agentId: $("agentId").value.trim(),
    fromSeq: Number($("fromSeq").value || 0),
    limit: Number($("limit").value || 100),
    token: $("token").value.trim(),
  };
  localStorage.setItem(storeKey, JSON.stringify(cfg));
  syncUrl(cfg);
  if (requireIds) {
    if (!cfg.tenantId) throw new Error("tenant_id is required");
    if (!cfg.sessionId) throw new Error("session_id is required");
  }
  return cfg;
}

function restoreInputs() {
  try {
    const cfg = {
      ...JSON.parse(localStorage.getItem(storeKey) || "{}"),
      ...configFromQuery(),
    };
    for (const [key, value] of Object.entries(cfg)) {
      if ($(key) && value !== undefined) $(key).value = value;
    }
  } catch (_) {}
}

function configFromQuery() {
  const params = new URLSearchParams(window.location.search);
  const cfg = {};
  mapQueryParam(params, cfg, "tenantId", ["tenantId", "tenant_id"]);
  mapQueryParam(params, cfg, "sessionId", ["sessionId", "session_id"]);
  mapQueryParam(params, cfg, "agentId", ["agentId", "agent_id"]);
  mapQueryParam(params, cfg, "fromSeq", ["fromSeq", "from_seq"]);
  mapQueryParam(params, cfg, "limit", ["limit"]);
  mapQueryParam(params, cfg, "token", ["token"]);
  return cfg;
}

function mapQueryParam(params, cfg, key, names) {
  for (const name of names) {
    const value = params.get(name);
    if (value) {
      cfg[key] = value;
      return;
    }
  }
}

function syncUrl(cfg) {
  const url = new URL(window.location.href);
  setQueryValue(url.searchParams, "tenant_id", cfg.tenantId);
  setQueryValue(url.searchParams, "session_id", cfg.sessionId);
  setQueryValue(url.searchParams, "agent_id", cfg.agentId);
  setQueryValue(url.searchParams, "from_seq", String(cfg.fromSeq || 0));
  setQueryValue(url.searchParams, "limit", String(cfg.limit || 100));
  if (cfg.token) {
    setQueryValue(url.searchParams, "token", cfg.token);
  } else {
    url.searchParams.delete("token");
  }
  window.history.replaceState({}, "", url);
}

function setQueryValue(params, key, value) {
  if (value && value !== "0" && value !== "100") {
    params.set(key, value);
    return;
  }
  if ((key === "from_seq" && value === "0") || (key === "limit" && value === "100")) {
    params.delete(key);
    return;
  }
  if (value) {
    params.set(key, value);
  } else {
    params.delete(key);
  }
}

function maybeAutoloadFromUrl() {
  const cfg = configFromQuery();
  if (cfg.tenantId && cfg.sessionId) {
    loadTimeline();
  }
}

async function request(path, options = {}) {
  const cfg = readConfig(options.requireIds !== false);
  const headers = new Headers(options.headers || {});
  applyAuth(headers, cfg);
  if (options.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const resp = await fetch(path, { ...options, headers });
  const contentType = resp.headers.get("content-type") || "";
  const body = contentType.includes("application/json") ? await resp.json() : await resp.text();
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}: ${stringify(body)}`);
  setConnection("ok");
  setRaw(body);
  return body;
}

async function loadTenants() {
  try {
    const data = await request("/api/tenants", { requireIds: false });
    state.tenants = data.tenants || [];
    renderTenantBrowser();
    if ($("tenantId").value.trim()) {
      loadSessions();
    }
  } catch (err) {
    fail(err);
  }
}

async function loadSessions() {
  try {
    const cfg = readConfig(false);
    if (!cfg.tenantId) {
      state.sessions = [];
      renderSessionBrowser();
      return;
    }
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions`, { requireIds: false });
    state.sessions = data.sessions || [];
    renderSessionBrowser();
  } catch (err) {
    fail(err);
  }
}

async function loadTimeline() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ from_seq: String(cfg.fromSeq), limit: String(cfg.limit) });
    state.events = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/events?${params}`);
    renderTimeline();
  } catch (err) {
    fail(err);
  }
}

async function replaySession() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ from_seq: String(cfg.fromSeq), limit: String(cfg.limit) });
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/replay?${params}`);
    state.events = data.events || [];
    renderTimeline();
    if (data.lineage) {
      state.forks = data.lineage;
      renderForkTree();
    }
  } catch (err) {
    fail(err);
  }
}

async function subscribeSession() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ from_seq: String(cfg.fromSeq), limit: String(cfg.limit) });
    const url = `/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/subscribe?${params}`;
    const headers = new Headers({ Accept: "text/event-stream" });
    applyAuth(headers, cfg);
    const resp = await fetch(url, { headers });
    const text = await resp.text();
    if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}: ${text.slice(0, 500)}`);
    state.events = parseSse(text);
    setConnection("sse snapshot");
    setRaw(state.events);
    renderTimeline();
  } catch (err) {
    fail(err);
  }
}

async function watchScope() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ scope: "session", session_id: cfg.sessionId, limit: String(cfg.limit) });
    if (cfg.agentId) params.set("agent_id", cfg.agentId);
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/watch?${params}`);
    state.events = data.map((item) => item.event || item);
    renderTimeline();
  } catch (err) {
    fail(err);
  }
}

async function buildContext() {
  try {
    const cfg = readConfig();
    await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/context`, {
      method: "POST",
      body: JSON.stringify({
        max_events: 20,
        max_bytes: 1000000,
        include: ["recent_events", "records", "skills", "policy"],
        record_query: recordQuery(),
      }),
    });
  } catch (err) {
    fail(err);
  }
}

async function forkSession() {
  try {
    const cfg = readConfig();
    const body = {
      at_seq: Number($("forkSeq").value || 0),
      alias: $("forkAlias").value.trim() || null,
    };
    const child = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/fork`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    state.forks.push({ parent_session_id: cfg.sessionId, child_session_id: child.session_id, at_seq: body.at_seq });
    renderForkTree();
  } catch (err) {
    fail(err);
  }
}

async function loadArtifact() {
  try {
    const cfg = readConfig();
    const artifactId = $("artifactId").value.trim();
    if (!artifactId) throw new Error("artifact_id is required");
    const meta = await request(`/api/tenants/${enc(cfg.tenantId)}/artifacts/${enc(artifactId)}`);
    $("artifactView").textContent = stringify(meta);
  } catch (err) {
    fail(err);
  }
}

async function searchRecords() {
  try {
    const cfg = readConfig();
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/records/search`, {
      method: "POST",
      body: JSON.stringify(recordQuery()),
    });
    renderRows("records", data.records || []);
  } catch (err) {
    fail(err);
  }
}

async function loadSkills() {
  try {
    const cfg = readConfig();
    if (!cfg.agentId) throw new Error("agent_id is required");
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/agents/${enc(cfg.agentId)}/skills`);
    renderRows("skills", data.skills || []);
  } catch (err) {
    fail(err);
  }
}

function recordQuery() {
  const sourceSession = $("recordSourceSession").value.trim();
  return {
    text: $("recordText").value.trim() || null,
    namespace: csv($("recordNamespace").value),
    tags: csv($("recordTags").value),
    source: sourceSession ? { session_id: sourceSession } : null,
    limit: Number($("limit").value || 100),
  };
}

function renderTimeline() {
  $("eventCount").textContent = `${state.events.length} events`;
  const root = $("timeline");
  root.textContent = "";
  if (!state.events.length) {
    root.append(empty("No events loaded"));
    return;
  }
  state.events
    .slice()
    .sort((a, b) => (a.seq || 0) - (b.seq || 0))
    .forEach((event) => {
      const item = document.createElement("article");
      item.className = "event";
      item.innerHTML = `
        <div class="event-meta">
          <span class="seq">#${event.seq ?? "-"}</span>
          <span>${escapeHtml(event.kind || "event")}</span>
          <time>${formatTime(event.created_at_ms)}</time>
        </div>
        <pre class="summary">${escapeHtml(summary(event))}</pre>
        <div class="refs">${artifactRefs(event)}</div>
      `;
      item.addEventListener("click", () => setRaw(event));
      item.querySelectorAll("[data-artifact]").forEach((button) => {
        button.addEventListener("click", (clickEvent) => {
          clickEvent.stopPropagation();
          const artifactId = button.dataset.artifact || "";
          $("artifactId").value = artifactId;
          loadArtifact();
        });
      });
      root.append(item);
    });
}

function renderForkTree() {
  const root = $("forkTree");
  root.textContent = "";
  if (!state.forks.length) {
    root.append(empty("No forks recorded in this browser"));
    return;
  }
  state.forks.forEach((edge) => {
    const row = document.createElement("div");
    row.className = "edge";
    row.textContent = `${edge.parent_session_id || edge.parent || "parent"} -> ${edge.child_session_id || edge.child || "child"} @ ${edge.at_seq ?? "-"}`;
    root.append(row);
  });
}

function renderTenantBrowser() {
  $("tenantCount").textContent = `${state.tenants.length} tenants`;
  const root = $("tenants");
  root.textContent = "";
  if (!state.tenants.length) {
    root.append(empty("No tenants"));
    return;
  }
  state.tenants.forEach((tenant) => {
    const node = document.createElement("button");
    node.className = "row browser-row";
    node.type = "button";
    node.innerHTML = `
      <strong>${escapeHtml(tenant.external_key || "(no external key)")}</strong>
      <span>${escapeHtml(tenant.tenant_id || "")}</span>
    `;
    node.addEventListener("click", () => {
      $("tenantId").value = tenant.tenant_id || "";
      $("sessionId").value = "";
      $("agentId").value = "";
      state.sessions = [];
      renderSessionBrowser();
      readConfig(false);
      loadSessions();
    });
    root.append(node);
  });
}

function renderSessionBrowser() {
  $("sessionCount").textContent = `${state.sessions.length} sessions`;
  const root = $("sessions");
  root.textContent = "";
  if (!state.sessions.length) {
    root.append(empty("No sessions for selected tenant"));
    return;
  }
  state.sessions.forEach((session) => {
    const node = document.createElement("button");
    node.className = "row browser-row";
    node.type = "button";
    node.innerHTML = `
      <strong>${escapeHtml(session.alias || "(no alias)")}</strong>
      <span>${escapeHtml(session.session_id || "")}</span>
      <span>agent ${escapeHtml(session.agent_id || "")}</span>
    `;
    node.addEventListener("click", () => {
      $("sessionId").value = session.session_id || "";
      $("agentId").value = session.agent_id || "";
      readConfig(false);
      loadTimeline();
    });
    root.append(node);
  });
}

function renderRows(id, rows) {
  const root = $(id);
  root.textContent = "";
  if (!rows.length) {
    root.append(empty("No rows"));
    return;
  }
  rows.forEach((row) => {
    const node = document.createElement("button");
    node.className = "row";
    node.type = "button";
    node.textContent = row.record_id || row.skill_id || row.id || summary(row);
    node.addEventListener("click", () => setRaw(row));
    root.append(node);
  });
}

function upsertEvent(event) {
  const idx = state.events.findIndex((item) => item.event_id === event.event_id);
  if (idx >= 0) state.events[idx] = event;
  else state.events.push(event);
}

function parseSse(text) {
  const out = [];
  let data = "";
  text.split(/\r?\n/).forEach((line) => {
    if (line.startsWith("data:")) data += line.slice(5).trimStart();
    if (line === "" && data) {
      out.push(JSON.parse(data));
      data = "";
    }
  });
  if (data) out.push(JSON.parse(data));
  return out;
}

function applyAuth(headers, cfg) {
  if (cfg.token) headers.set("Authorization", `Bearer ${cfg.token}`);
}

function artifactRefs(event) {
  const refs = event.artifact_refs || [];
  if (!refs.length) return "";
  return refs.map((ref) => `<button class="ref" data-artifact="${escapeHtml(ref.artifact_id || ref)}">${escapeHtml(ref.artifact_id || ref)}</button>`).join("");
}

function empty(text) {
  const node = document.createElement("p");
  node.className = "empty";
  node.textContent = text;
  return node;
}

function fail(err) {
  setConnection("error");
  setRaw(String(err && err.message ? err.message : err));
}

function setConnection(text) {
  $("connection").textContent = text;
}

function setRaw(value) {
  $("raw").textContent = typeof value === "string" ? value : stringify(value);
}

function summary(event) {
  const payload = event && event.payload ? event.payload : {};
  const text = eventSummary(event.kind, payload);
  return text.length > 220 ? `${text.slice(0, 220)}...` : text;
}

function eventSummary(kind, payload) {
  const lines = [];
  if (kind === "user_message" || kind === "model_message") {
    if (payload.role) lines.push(`role: ${payload.role}`);
    if (payload.text) lines.push(payload.text);
  } else if (kind === "tool_call") {
    if (payload.tool) lines.push(`tool: ${payload.tool}`);
    if (payload.tool_call_id) lines.push(`call: ${payload.tool_call_id}`);
    if (payload.input && payload.input.cmd) lines.push(`cmd: ${payload.input.cmd}`);
    if (payload.timeout_ms !== undefined) lines.push(`timeout: ${payload.timeout_ms}ms`);
  } else if (kind === "tool_result") {
    if (payload.status) lines.push(`status: ${payload.status}`);
    if (payload.summary) lines.push(payload.summary);
    if (payload.output && payload.output.preview) lines.push(`preview: ${payload.output.preview}`);
    if (payload.duration_ms !== undefined) lines.push(`duration: ${payload.duration_ms}ms`);
  } else if (kind === "context_pack_built") {
    if (payload.context_pack_id) lines.push(`context pack: ${payload.context_pack_id}`);
    if (payload.include) lines.push(`include: ${payload.include.join(", ")}`);
    if (payload.selected_event_count !== undefined) {
      lines.push(`selected events: ${payload.selected_event_count}`);
    }
  } else if (kind === "agent_status") {
    if (payload.status) lines.push(`status: ${payload.status}`);
    if (payload.summary) lines.push(payload.summary);
    if (payload.last_seen_seq !== undefined) lines.push(`last seen seq: ${payload.last_seen_seq}`);
  }
  if (!lines.length) return stringify(payload);
  return lines.join("\n");
}

function stringify(value) {
  return JSON.stringify(value, null, 2);
}

function csv(value) {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function enc(value) {
  return encodeURIComponent(value);
}

function formatTime(ms) {
  return ms ? new Date(ms).toLocaleString() : "";
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
}
