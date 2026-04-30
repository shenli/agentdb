const storeKey = "agentdb.v28.dashboard";

const state = {
  events: [],
  forks: [],
};

const $ = (id) => document.getElementById(id);

document.addEventListener("DOMContentLoaded", () => {
  restoreInputs();
  document.querySelectorAll("[data-action]").forEach((button) => {
    button.addEventListener("click", () => dispatch(button.dataset.action));
  });
  renderTimeline();
  renderForkTree();
});

function dispatch(action) {
  const actions = {
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

function readConfig() {
  const cfg = {
    tenantId: $("tenantId").value.trim(),
    sessionId: $("sessionId").value.trim(),
    agentId: $("agentId").value.trim(),
    fromSeq: Number($("fromSeq").value || 0),
    limit: Number($("limit").value || 100),
    token: $("token").value.trim(),
  };
  localStorage.setItem(storeKey, JSON.stringify(cfg));
  if (!cfg.tenantId) throw new Error("tenant_id is required");
  if (!cfg.sessionId) throw new Error("session_id is required");
  return cfg;
}

function restoreInputs() {
  try {
    const cfg = JSON.parse(localStorage.getItem(storeKey) || "{}");
    for (const [key, value] of Object.entries(cfg)) {
      if ($(key) && value !== undefined) $(key).value = value;
    }
  } catch (_) {}
}

async function request(path, options = {}) {
  const cfg = readConfig();
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
        <div class="summary">${escapeHtml(summary(event.payload))}</div>
        <div class="refs">${artifactRefs(event)}</div>
      `;
      item.addEventListener("click", () => setRaw(event));
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

function summary(value) {
  const text = stringify(value);
  return text.length > 220 ? `${text.slice(0, 220)}...` : text;
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
