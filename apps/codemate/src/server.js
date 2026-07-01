#!/usr/bin/env node
import http from "node:http";
import { URL } from "node:url";

import { buildReviewModel, generatePacket } from "./analysis.js";
import { importReviewModel } from "./agentdb.js";
import { listCodexSessions, parseCodexSession, resolveSessionPath } from "./codex.js";
import { renderApp } from "./ui.js";

const DEFAULT_PORT = 3737;

export function createServer() {
  return http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, `http://${req.headers.host || "127.0.0.1"}`);
      if (req.method === "GET" && url.pathname === "/") {
        return sendHtml(res, renderApp());
      }
      if (req.method === "GET" && url.pathname === "/api/sessions") {
        return sendJson(res, { sessions: listCodexSessions() });
      }
      const sessionMatch = url.pathname.match(/^\/api\/session\/([^/]+)$/);
      if (req.method === "GET" && sessionMatch) {
        const session = loadSession(sessionMatch[1]);
        return sendJson(res, session);
      }
      const analysisMatch = url.pathname.match(/^\/api\/analyze\/([^/]+)$/);
      if (req.method === "GET" && analysisMatch) {
        const session = loadSession(analysisMatch[1]);
        const model = buildReviewModel(session, url.searchParams.get("mode") || "after-reset");
        return sendJson(res, publicAnalysis(model));
      }
      const importMatch = url.pathname.match(/^\/api\/import-agentdb\/([^/]+)$/);
      if (req.method === "POST" && importMatch) {
        const session = loadSession(importMatch[1]);
        const model = buildReviewModel(session, url.searchParams.get("mode") || "after-reset");
        const result = await importReviewModel(model);
        return sendJson(res, { ok: true, result });
      }
      sendJson(res, { error: "not found" }, 404);
    } catch (error) {
      sendJson(res, { ok: false, error: error.message }, 500);
    }
  });
}

function loadSession(id) {
  return parseCodexSession(resolveSessionPath(id));
}

function publicAnalysis(model) {
  return {
    session: {
      id: model.session.id,
      host: model.session.host,
      displayPath: model.session.displayPath,
      meta: model.session.meta,
      stats: model.session.stats,
      latestResetIndex: model.session.latestResetIndex,
    },
    mode: model.mode,
    events: model.events.map((event) => ({
      seq: event.seq,
      timestamp: event.timestamp,
      category: event.category,
      title: event.title,
      text: event.text,
      command: event.command,
      exitCode: event.exitCode,
      success: event.success,
    })),
    commandPairs: model.commandPairs,
    issues: model.issues,
    suggestions: model.suggestions,
    packet: generatePacket(model),
  };
}

function sendHtml(res, html) {
  res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
  res.end(html);
}

function sendJson(res, body, status = 200) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const port = Number(process.env.PORT || DEFAULT_PORT);
  const server = createServer();
  server.listen(port, "127.0.0.1", () => {
    console.log(`CodeMate running at http://127.0.0.1:${port}`);
  });
}
