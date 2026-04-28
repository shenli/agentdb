import { test } from "node:test";
import assert from "node:assert/strict";
import { Channels, channelName } from "../src/channels.ts";

test("channelName composes runId and channel", () => {
  assert.equal(
    channelName({ runId: "r-1" }, "announcements"),
    "r-1:announcements",
  );
});

test("Channels.publish forwards to broadcast()", async () => {
  const calls: any[] = [];
  const fakeClient = {
    broadcast: async (args: any) => { calls.push(args); },
  };
  const ch = new Channels(fakeClient as any, { runId: "r-1" });
  await ch.publish("announcements", { kind: "ping" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].channel, "r-1:announcements");
  assert.equal((calls[0].payload as any).kind, "ping");
});

test("Channels.subscribe yields broadcasts via the SDK's AsyncIterable", async () => {
  async function* fakeSub(_args: any): AsyncIterable<any> {
    yield { tenant: "pi-company", channel: "r-1:announcements", ts_ms: 1, payload: { kind: "a" } };
    yield { tenant: "pi-company", channel: "r-1:announcements", ts_ms: 2, payload: { kind: "b" } };
  }
  const fakeClient = {
    subscribeChannel: (args: any) => fakeSub(args),
  };
  const ch = new Channels(fakeClient as any, { runId: "r-1" });
  const got: any[] = [];
  for await (const msg of ch.subscribe("announcements")) {
    got.push((msg as any).payload.kind);
    if (got.length >= 2) break;
  }
  assert.deepEqual(got, ["a", "b"]);
});
