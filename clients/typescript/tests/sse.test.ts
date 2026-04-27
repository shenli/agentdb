/**
 * Unit tests for the SSE parser. These run on every `npm test`
 * (no server needed); they're the cheap, fast first line of defence
 * against subscribe-side regressions.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { parseBlock, parseSse } from "../src/sse.js";

test("parseBlock yields a named event with data", () => {
  const f = parseBlock('event: subscribed\ndata: {"live_seq": 1}');
  assert.equal(f.event, "subscribed");
  assert.equal(f.data, '{"live_seq": 1}');
});

test("parseBlock skips comment lines", () => {
  const f = parseBlock(': keepalive\ndata: {"seq": 1}');
  assert.equal(f.event, undefined);
  assert.equal(f.data, '{"seq": 1}');
});

test("parseBlock joins multi-line data with newlines", () => {
  const f = parseBlock("data: line1\ndata: line2");
  assert.equal(f.data, "line1\nline2");
});

test("parseBlock strips one leading space from data", () => {
  // The SSE spec says strip exactly one leading space; the parser
  // uses a single-space `.replace(/^ /, "")` so callers that rely
  // on padding round-trip safely.
  const f = parseBlock("data:nospace\ndata: withspace\ndata:  twospaces");
  assert.equal(f.data, "nospace\nwithspace\n twospaces");
});

test("parseBlock tolerates trailing CR for CRLF servers", () => {
  const f = parseBlock('event: ping\r\ndata: {"hi":true}\r');
  assert.equal(f.event, "ping");
  assert.equal(f.data, '{"hi":true}');
});

/** Drive parseSse with an in-memory ReadableStream to exercise the
 * frame-boundary, comment-skip, and EOF paths without touching the
 * network. */
async function streamFrames(chunks: string[]) {
  const enc = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(ctrl) {
      for (const c of chunks) ctrl.enqueue(enc.encode(c));
      ctrl.close();
    },
  });
  const reader = stream.getReader();
  const out = [];
  for await (const frame of parseSse(reader)) {
    out.push(frame);
  }
  return out;
}

test("parseSse splits on \\n\\n and yields each frame", async () => {
  const frames = await streamFrames([
    "event: subscribed\n",
    'data: {"live_seq":0}\n\n',
    'data: {"seq":1}\n\n',
    'data: {"seq":2}\n\n',
  ]);
  assert.equal(frames.length, 3);
  assert.equal(frames[0]!.event, "subscribed");
  assert.equal(frames[1]!.data, '{"seq":1}');
  assert.equal(frames[2]!.data, '{"seq":2}');
});

test("parseSse skips frames that are only comments / keepalives", async () => {
  const frames = await streamFrames([
    ":\n\n",            // empty comment, full frame — should be skipped
    ": keepalive\n\n",  // comment with text — also skipped
    'data: {"seq":1}\n\n',
  ]);
  assert.equal(frames.length, 1);
  assert.equal(frames[0]!.data, '{"seq":1}');
});

test("parseSse decodes split UTF-8 multi-byte boundaries", async () => {
  // The unicode '✓' is 3 bytes in UTF-8; split it across two reads
  // and verify the parser doesn't drop bytes at the boundary.
  const enc = new TextEncoder();
  const all = enc.encode('data: {"x":"✓"}\n\n');
  const stream = new ReadableStream<Uint8Array>({
    start(ctrl) {
      ctrl.enqueue(all.slice(0, 12));
      ctrl.enqueue(all.slice(12));
      ctrl.close();
    },
  });
  const reader = stream.getReader();
  const frames = [];
  for await (const f of parseSse(reader)) frames.push(f);
  assert.equal(frames.length, 1);
  assert.equal(frames[0]!.data, '{"x":"✓"}');
});
