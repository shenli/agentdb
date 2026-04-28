import type { Client, BroadcastMessage } from "agentdb";
import type { RunIdentity } from "./types.ts";

export type { BroadcastMessage } from "agentdb";

/** Compose `{runId}:{name}` so concurrent runs don't cross-talk. */
export function channelName(run: RunIdentity, name: string): string {
  return `${run.runId}:${name}`;
}

/**
 * Run-scoped wrapper over AgentDB Phase 2 broadcast (ephemeral pub/sub).
 *
 * One instance binds a {@link RunIdentity} and prefixes every channel
 * with its `runId`. Subscribers consume the SDK's `AsyncIterable` —
 * the consumer drives cancellation by breaking out of the loop.
 */
export class Channels {
  readonly run: RunIdentity;
  private readonly _client: Client;

  constructor(client: Client, run: RunIdentity) {
    this._client = client;
    this.run = run;
  }

  /** Publish an ephemeral broadcast. No durability, no replay. */
  async publish(name: string, payload: unknown): Promise<void> {
    await this._client.broadcast({
      channel: channelName(this.run, name),
      payload,
    });
  }

  /**
   * Stream broadcasts on `name`. Returns the SDK's `AsyncIterable<BroadcastMessage>`;
   * consumer breaks out of `for await` to cancel.
   */
  subscribe(name: string): AsyncIterable<BroadcastMessage> {
    return this._client.subscribeChannel({
      channel: channelName(this.run, name),
    });
  }
}
