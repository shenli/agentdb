export function renderApp() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CodeMate</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f7f4;
      --panel: #ffffff;
      --ink: #1f2933;
      --muted: #687382;
      --line: #d9ddd4;
      --accent: #1f7a5a;
      --accent-ink: #ffffff;
      --warn: #a45700;
      --bad: #b42318;
      --good: #16794c;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: var(--bg);
    }
    header {
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 20px;
      border-bottom: 1px solid var(--line);
      background: #fbfbf8;
    }
    h1 { margin: 0; font-size: 18px; letter-spacing: 0; }
    main {
      display: grid;
      grid-template-columns: 360px 1fr;
      min-height: calc(100vh - 56px);
    }
    aside {
      border-right: 1px solid var(--line);
      padding: 16px;
      overflow: auto;
      background: #fbfbf8;
    }
    section {
      padding: 18px;
      overflow: auto;
    }
    button, select {
      font: inherit;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--ink);
      border-radius: 6px;
      padding: 8px 10px;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: var(--accent-ink);
    }
    button:disabled { opacity: 0.5; }
    .toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .list { display: grid; gap: 8px; margin-top: 12px; }
    .session {
      text-align: left;
      padding: 10px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      width: 100%;
      cursor: pointer;
    }
    .session.active { border-color: var(--accent); box-shadow: inset 3px 0 0 var(--accent); }
    .session strong { display: block; font-size: 13px; line-height: 1.35; }
    .meta, .muted { color: var(--muted); font-size: 12px; line-height: 1.45; }
    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(360px, 0.9fr);
      gap: 16px;
      align-items: start;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    .panel h2 {
      margin: 0;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
    }
    .panel-body { padding: 12px 14px; }
    .timeline { display: grid; gap: 8px; max-height: 58vh; overflow: auto; }
    .event {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #fff;
    }
    .event .kind { font-size: 11px; text-transform: uppercase; color: var(--muted); }
    .event pre, .packet {
      white-space: pre-wrap;
      word-break: break-word;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
      line-height: 1.45;
    }
    .tabs { display: flex; gap: 6px; margin-bottom: 12px; flex-wrap: wrap; }
    .tab.active { border-color: var(--accent); color: var(--accent); }
    .badge { display: inline-block; border: 1px solid var(--line); border-radius: 999px; padding: 2px 7px; font-size: 12px; color: var(--muted); }
    .issue.high { color: var(--bad); }
    .issue.medium { color: var(--warn); }
    .issue.info { color: var(--muted); }
    @media (max-width: 900px) {
      main, .grid { grid-template-columns: 1fr; }
      aside { border-right: 0; border-bottom: 1px solid var(--line); }
    }
  </style>
</head>
<body>
  <header>
    <h1>CodeMate</h1>
    <div class="toolbar">
      <span class="muted" id="status">Local session review</span>
      <button id="refresh">Refresh</button>
    </div>
  </header>
  <main>
    <aside>
      <div class="toolbar">
        <strong>Sessions</strong>
        <span class="badge" id="count">0</span>
      </div>
      <div class="list" id="sessions"></div>
    </aside>
    <section>
      <div class="toolbar" style="margin-bottom:12px">
        <select id="windowMode">
          <option value="after-reset">After latest reset</option>
          <option value="all">Entire session</option>
        </select>
        <button class="primary" id="copyPacket" disabled>Copy packet</button>
        <button id="importAgentdb" disabled>Import to AgentDB</button>
      </div>
      <div class="grid">
        <div class="panel">
          <h2>Timeline</h2>
          <div class="panel-body">
            <div class="timeline" id="timeline"><p class="muted">Select a session.</p></div>
          </div>
        </div>
        <div class="panel">
          <h2>Outputs</h2>
          <div class="panel-body">
            <div class="tabs">
              <button class="tab active" data-tab="packet">Packet</button>
              <button class="tab" data-tab="issues">Issues</button>
              <button class="tab" data-tab="suggestions">Best practices</button>
            </div>
            <div id="output"></div>
          </div>
        </div>
      </div>
    </section>
  </main>
  <script>
    let sessions = [];
    let selected = null;
    let detail = null;
    let analysis = null;
    let activeTab = 'packet';

    const el = (id) => document.getElementById(id);
    const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c]));

    async function loadSessions() {
      el('status').textContent = 'Loading sessions...';
      const res = await fetch('/api/sessions');
      const data = await res.json();
      sessions = data.sessions || [];
      el('count').textContent = sessions.length;
      el('sessions').innerHTML = sessions.map(s => \`
        <button class="session \${selected?.id === s.id ? 'active' : ''}" data-id="\${esc(s.id)}">
          <strong>\${esc(s.title || 'Untitled session')}</strong>
          <span class="meta">\${esc(s.repo)} · \${esc(s.branch)} · \${new Date(s.updatedAt).toLocaleString()}</span><br/>
          <span class="meta">\${s.eventCount} events · \${s.resetCount} resets</span>
        </button>\`).join('') || '<p class="muted">No Codex sessions found.</p>';
      document.querySelectorAll('.session').forEach(btn => btn.addEventListener('click', () => selectSession(btn.dataset.id)));
      el('status').textContent = sessions.length ? 'Ready' : 'No sessions found';
    }

    async function selectSession(id) {
      selected = sessions.find(s => s.id === id);
      detail = null;
      analysis = null;
      renderSessions();
      el('status').textContent = 'Parsing session...';
      const res = await fetch('/api/session/' + encodeURIComponent(id));
      detail = await res.json();
      await refreshAnalysis();
      el('copyPacket').disabled = false;
      el('importAgentdb').disabled = false;
      el('status').textContent = 'Session loaded';
    }

    function renderSessions() {
      document.querySelectorAll('.session').forEach(btn => btn.classList.toggle('active', btn.dataset.id === selected?.id));
    }

    async function refreshAnalysis() {
      if (!selected) return;
      const mode = el('windowMode').value;
      const res = await fetch('/api/analyze/' + encodeURIComponent(selected.id) + '?mode=' + encodeURIComponent(mode));
      analysis = await res.json();
      renderTimeline();
      renderOutput();
    }

    function renderTimeline() {
      const events = analysis?.events || [];
      el('timeline').innerHTML = events.map(ev => \`
        <div class="event">
          <div class="kind">\${esc(ev.category)} · \${esc(ev.timestamp || '')}</div>
          <strong>\${esc(ev.title)}</strong>
          \${ev.text ? '<pre>' + esc(ev.text.slice(0, 900)) + '</pre>' : ''}
        </div>\`).join('') || '<p class="muted">No events in this window.</p>';
    }

    function renderOutput() {
      if (!analysis) return;
      if (activeTab === 'packet') {
        el('output').innerHTML = '<pre class="packet">' + esc(analysis.packet) + '</pre>';
      } else if (activeTab === 'issues') {
        el('output').innerHTML = (analysis.issues || []).map(i => \`
          <p class="issue \${esc(i.severity)}"><strong>\${esc(i.severity.toUpperCase())}</strong> \${esc(i.message)}</p>\`).join('') || '<p class="muted">No obvious issues detected.</p>';
      } else {
        el('output').innerHTML = (analysis.suggestions || []).map(s => \`
          <div class="event"><strong>\${esc(s.title)}</strong><p class="meta">\${esc(s.target)}</p><pre>\${esc(s.body)}</pre></div>\`).join('') || '<p class="muted">No best-practice suggestions yet.</p>';
      }
    }

    document.querySelectorAll('.tab').forEach(btn => btn.addEventListener('click', () => {
      activeTab = btn.dataset.tab;
      document.querySelectorAll('.tab').forEach(b => b.classList.toggle('active', b === btn));
      renderOutput();
    }));
    el('refresh').addEventListener('click', loadSessions);
    el('windowMode').addEventListener('change', refreshAnalysis);
    el('copyPacket').addEventListener('click', async () => {
      await navigator.clipboard.writeText(analysis?.packet || '');
      el('status').textContent = 'Packet copied';
    });
    el('importAgentdb').addEventListener('click', async () => {
      el('status').textContent = 'Importing to AgentDB...';
      const res = await fetch('/api/import-agentdb/' + encodeURIComponent(selected.id) + '?mode=' + encodeURIComponent(el('windowMode').value), { method: 'POST' });
      const data = await res.json();
      el('status').textContent = data.ok ? \`Imported \${data.result.appended} events\` : data.error;
    });

    loadSessions().catch(err => { el('status').textContent = err.message; });
  </script>
</body>
</html>`;
}
