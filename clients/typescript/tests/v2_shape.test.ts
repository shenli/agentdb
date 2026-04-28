/**
 * Compile-time and runtime assertions for the v2 Client shape.
 *
 * Verifies:
 * - `tenant` is no longer a valid constructor arg (compile-time, via @ts-expect-error)
 * - URLs use v2 shapes (/api/sessions/{s}/..., /api/subscribe, /api/channels/{c}/...)
 * - No /api/tenants/ prefix appears in any URL
 *
 * Uses the same node:test runner as the rest of the SDK test suite.
 * The `fetch` injection in ClientOptions lets us assert URL shapes
 * without spinning up a server.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { Client } from "../src/index.js";

test("constructor takes baseUrl + auth, no tenant field", () => {
  // @ts-expect-error — `tenant` must not be a valid constructor option
  const _bad = new Client({ baseUrl: "http://localhost", tenant: "x" });
  // Suppress unused-variable warning at runtime
  void _bad;
});

test("constructor accepts auth: { user, password }", () => {
  const c = new Client({ baseUrl: "http://localhost", auth: { user: "u", password: "p" } });
  assert.ok(c);
  assert.equal(c.authenticated, true);
});

test("constructor accepts token (JWT) for channel routes", () => {
  const c = new Client({ baseUrl: "http://localhost", token: "jwt.tok.en" });
  assert.ok(c);
  assert.equal(c.authenticated, false); // Basic auth absent
});

test("append posts to /api/sessions/{s}/append_and_fetch", async () => {
  const calls: string[] = [];
  const mockFetch = async (url: string | URL) => {
    calls.push(String(url));
    return new Response(JSON.stringify({ seq: 1, recent: [] }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  const c = new Client({
    baseUrl: "http://x",
    auth: { user: "u", password: "p" },
    fetch: mockFetch as typeof globalThis.fetch,
  });
  await c.session("s1").append({ payload: { hello: "world" } });
  assert.equal(calls.length, 1);
  assert.ok(calls[0]!.includes("/api/sessions/s1/append_and_fetch"), `unexpected URL: ${calls[0]}`);
  assert.ok(!calls[0]!.includes("/api/tenants/"), `URL must not contain /api/tenants/: ${calls[0]}`);
});

test("fetchRecent posts to /api/sessions/{s}/fetch_recent", async () => {
  const calls: string[] = [];
  const mockFetch = async (url: string | URL) => {
    calls.push(String(url));
    return new Response(JSON.stringify({ recent: [] }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  const c = new Client({
    baseUrl: "http://x",
    auth: { user: "u", password: "p" },
    fetch: mockFetch as typeof globalThis.fetch,
  });
  await c.session("sess-abc").fetchRecent(5);
  assert.equal(calls.length, 1);
  assert.ok(calls[0]!.includes("/api/sessions/sess-abc/fetch_recent"), `unexpected URL: ${calls[0]}`);
  assert.ok(!calls[0]!.includes("/api/tenants/"), `URL must not contain /api/tenants/: ${calls[0]}`);
});

test("session.subscribe targets /api/sessions/{s}/subscribe", async () => {
  // We verify the URL by consuming one iteration of the subscribe generator
  // against a mock fetch that returns an immediately-closing SSE stream.
  const calls: string[] = [];
  const mockFetch = async (url: string | URL) => {
    calls.push(String(url));
    // Return a minimal SSE stream that closes immediately (no frames).
    const body = new ReadableStream({
      start(controller) {
        controller.close();
      },
    });
    return new Response(body, {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    });
  };
  const c = new Client({
    baseUrl: "http://x",
    auth: { user: "u", password: "p" },
    fetch: mockFetch as typeof globalThis.fetch,
  });
  // Consume the subscribe iterable — the mock stream closes immediately so
  // the for-await loop exits after one fetch round trip.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _evt of c.session("my-session").subscribe()) {
    // no events since the stream closes immediately
  }
  assert.equal(calls.length, 1);
  assert.ok(calls[0]!.includes("/api/sessions/my-session/subscribe"), `unexpected URL: ${calls[0]}`);
  assert.ok(!calls[0]!.includes("/api/tenants/"), `URL must not contain /api/tenants/: ${calls[0]}`);
});

test("client.subscribe targets /api/subscribe (tenant-wide)", async () => {
  const calls: string[] = [];
  const mockFetch = async (url: string | URL) => {
    calls.push(String(url));
    const body = new ReadableStream({
      start(controller) {
        controller.close();
      },
    });
    return new Response(body, {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    });
  };
  const c = new Client({
    baseUrl: "http://x",
    auth: { user: "u", password: "p" },
    fetch: mockFetch as typeof globalThis.fetch,
  });
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _evt of c.subscribe()) {
    // no events since the stream closes immediately
  }
  assert.equal(calls.length, 1);
  assert.ok(calls[0]!.includes("/api/subscribe"), `unexpected URL: ${calls[0]}`);
  assert.ok(!calls[0]!.includes("/api/tenants/"), `URL must not contain /api/tenants/: ${calls[0]}`);
});

test("broadcast posts to /api/channels/{c}/publish", async () => {
  const calls: string[] = [];
  const mockFetch = async (url: string | URL) => {
    calls.push(String(url));
    return new Response(null, { status: 204 });
  };
  const c = new Client({
    baseUrl: "http://x",
    token: "jwt",
    fetch: mockFetch as typeof globalThis.fetch,
  });
  await c.broadcast({ channel: "my-chan", payload: { x: 1 } });
  assert.equal(calls.length, 1);
  assert.ok(calls[0]!.includes("/api/channels/my-chan/publish"), `unexpected URL: ${calls[0]}`);
  assert.ok(!calls[0]!.includes("/api/tenants/"), `URL must not contain /api/tenants/: ${calls[0]}`);
});
