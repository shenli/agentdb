import type { Client } from "agentdb";
import type { CompanyTool, ToolContext } from "./agent.ts";
import type { RunIdentity } from "./types.ts";
import { appendObservation } from "./plan.ts";

const MAX_PAYLOAD_LEN = 240;

/**
 * Reduce a tool's params object to a JSON-stringified, length-capped form.
 * The full args remain in the agent's own session log via Agent's
 * `tool_call` audit event; the observation is the CEO-facing summary.
 */
export function summarizePayload(args: unknown): unknown {
  if (args === null || args === undefined) return args;
  if (typeof args === "string") return args.slice(0, MAX_PAYLOAD_LEN);
  try {
    const json = JSON.stringify(args);
    if (json.length <= MAX_PAYLOAD_LEN) return args;
    return { _truncated: true, preview: json.slice(0, MAX_PAYLOAD_LEN) };
  } catch {
    return { _unserializable: true };
  }
}

/**
 * Wrap a CompanyTool so its handler appends an `observation` event to
 * `{runId}:_plan` after executing. The wrapper:
 *   - delegates to the underlying handler
 *   - on success, writes an observation with the (truncated) args
 *   - re-throws any handler error UNCHANGED, after attempting to write
 *     a `kind: observation` with status:"error" so the CEO sees failed
 *     tool calls too
 *
 * The wrapper does NOT swallow handler errors — pi-mono's runtime turns
 * them into tool-result error messages, which is the contract Agent's
 * `adaptTool` relies on.
 */
export function wrapToolForObservation(
  tool: CompanyTool,
  role: string,
  client: Client,
  run: RunIdentity,
): CompanyTool {
  return {
    ...tool,
    handler: async (args, ctx) => {
      let threw: unknown;
      try {
        const result = await tool.handler(args, ctx);
        try {
          await appendObservation(
            client,
            run,
            role,
            tool.name,
            summarizePayload(args),
            (args as any)?.step_id,
          );
        } catch {
          // best-effort; CEO simply won't see this observation
        }
        return result;
      } catch (err) {
        threw = err;
        try {
          await appendObservation(
            client,
            run,
            role,
            tool.name,
            { _error: String(err), args: summarizePayload(args) },
            (args as any)?.step_id,
          );
        } catch {
          // swallow
        }
        throw threw;
      }
    },
  };
}
