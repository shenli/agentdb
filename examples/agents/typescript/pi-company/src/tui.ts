import * as readline from "node:readline";
import chalk from "chalk";
import type { Channels, BroadcastMessage } from "./channels.ts";

/**
 * Per-role color scheme for the chronological log. Falls back to white when
 * a payload has no recognizable `from` field.
 */
const ROLE_COLORS: Record<string, (s: string) => string> = {
  ceo: chalk.bold.cyan,
  cto: chalk.green,
  engineer: chalk.yellow,
  marketing: chalk.magenta,
  sales: chalk.blue,
  contractor: chalk.gray,
};

/** Channels the TUI tails. Mirrors the orchestrator's three-channel topology. */
const TUI_CHANNELS = ["announcements", "engineering", "gtm"] as const;

export interface TUIDeps {
  channels: Channels;
  /** Called when the user submits a CEO directive at the prompt. */
  onCeoInput: (text: string) => Promise<void>;
}

/**
 * Minimal terminal UI for the pi-company demo. Renders a chronological
 * color-coded log of all 3 channel feeds and exposes a CEO prompt for
 * typed directives.
 *
 * We deliberately use `chalk` + `readline` rather than pi-tui's component
 * stack. pi-tui's `Editor` requires constructing a full TUI runtime
 * (terminal handle, theme, autocomplete provider, focus plumbing) — heavy
 * boilerplate for a demo whose UI quality is not load-bearing. The plan
 * explicitly accepts this trade-off.
 *
 * Subscription loops outlive `start()` returning; the TUI stays live until
 * `/quit` triggers `process.exit(0)` or the channel sources close.
 */
export class CompanyTUI {
  private rl?: readline.Interface;
  private readonly deps: TUIDeps;
  private subscriptions: Promise<void>[] = [];
  private stopped = false;

  constructor(deps: TUIDeps) {
    this.deps = deps;
  }

  async start(): Promise<void> {
    this.rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: true,
    });
    this.rl.setPrompt(chalk.bold.cyan("> CEO: "));

    // Print a banner so the user sees the TUI is alive even before any
    // channel events arrive.
    console.log(chalk.dim("--- pi-company TUI (type a directive, /quit to exit) ---"));
    this.rl.prompt();

    this.rl.on("line", (line) => {
      void this.handleLine(line);
    });

    this.rl.on("close", () => {
      this.stopped = true;
    });

    for (const ch of TUI_CHANNELS) {
      this.subscriptions.push(this.subscribeAndRender(ch));
    }
  }

  /** Wait for all channel subscription loops to finish. */
  async wait(): Promise<void> {
    await Promise.allSettled(this.subscriptions);
  }

  private async handleLine(line: string): Promise<void> {
    const text = line.trim();
    if (!text) {
      this.rl?.prompt();
      return;
    }
    if (text === "/quit") {
      this.stopped = true;
      this.rl?.close();
      process.exit(0);
    }
    try {
      await this.deps.onCeoInput(text);
    } catch (err) {
      this.writeLine(chalk.red(`(error: ${err instanceof Error ? err.message : String(err)})`));
    }
    this.rl?.prompt();
  }

  private async subscribeAndRender(channel: string): Promise<void> {
    try {
      for await (const msg of this.deps.channels.subscribe(channel)) {
        if (this.stopped) break;
        this.renderEvent(channel, msg as BroadcastMessage);
      }
    } catch (err) {
      this.writeLine(chalk.red(`(channel ${channel} closed: ${err instanceof Error ? err.message : String(err)})`));
    }
  }

  private renderEvent(channel: string, msg: BroadcastMessage): void {
    const payload = msg.payload as Record<string, unknown> | string | null | undefined;
    const role = this.extractRole(payload);
    const color = ROLE_COLORS[role] ?? chalk.white;
    const summary = this.summarize(payload);
    const line = `${color(`[${role.toUpperCase()}]`)} ${chalk.dim(`(${channel})`)} ${summary}`;
    this.writeLine(line);
  }

  private extractRole(payload: unknown): string {
    if (payload && typeof payload === "object" && "from" in payload) {
      const from = (payload as { from?: unknown }).from;
      if (typeof from === "string" && from.length > 0) return from;
    }
    return "unknown";
  }

  private summarize(payload: unknown): string {
    if (typeof payload === "string") return payload.slice(0, 200);
    if (payload === null || payload === undefined) return "";
    if (typeof payload !== "object") return String(payload);
    const p = payload as Record<string, unknown>;
    if (typeof p.text === "string") return p.text.slice(0, 200);
    if (typeof p.kind === "string") {
      const title = typeof p.title === "string" ? `: ${p.title}` : "";
      return `${p.kind}${title}`;
    }
    try {
      return JSON.stringify(p).slice(0, 200);
    } catch {
      return "[unserializable payload]";
    }
  }

  /**
   * Concurrent stdout writes from multiple subscription loops can interleave
   * with the readline prompt. We clear the current line, write the event,
   * and then redraw the prompt with whatever the user has typed so far.
   */
  private writeLine(line: string): void {
    if (this.rl) {
      // Clear current line + carriage return.
      process.stdout.write("\r\x1b[K");
      console.log(line);
      this.rl.prompt(true);
    } else {
      console.log(line);
    }
  }
}
