/**
 * Minimal Server-Sent Events parser for the AgentDB SDK.
 *
 * Yields one [`SseFrame`] per `\n\n` block from a `fetch` response
 * body. Only the three lines AgentDB's stream uses are recognised
 * (`event:`, `data:`, `:`-comments) — keeps the parser tiny and
 * the failure modes few.
 */

export interface SseFrame {
  /** The `event:` line value, or `undefined` for data-only frames. */
  event?: string;
  /** All `data:` lines joined with `\n`, leading-space stripped. */
  data: string;
}

/**
 * Stream a `fetch` response body and yield SSE frames as they arrive.
 *
 * Caller responsibility: keep `for await`-ing this until the stream
 * closes; cancellation is handled by the caller breaking out of the
 * loop, which propagates to the underlying `ReadableStream`.
 */
export async function* parseSse(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): AsyncGenerator<SseFrame> {
  const decoder = new TextDecoder();
  let buf = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) {
      // Trailing partial frame at clean EOF — emit it so consumers
      // don't miss a final close-with-no-newline payload.
      if (buf.trim()) yield parseBlock(buf);
      return;
    }
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf("\n\n")) !== -1) {
      const block = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      const frame = parseBlock(block);
      // Skip frames that were only comments / keep-alives — they
      // carry neither an event name nor data.
      if (frame.event === undefined && frame.data === "") continue;
      yield frame;
    }
  }
}

/**
 * Parse one frame block (the bytes between two `\n\n` delimiters).
 * Exposed for unit tests; production code goes through `parseSse`.
 */
export function parseBlock(block: string): SseFrame {
  let event: string | undefined;
  const dataLines: string[] = [];
  for (const raw of block.split("\n")) {
    const line = raw.replace(/\r$/, "");
    if (line.startsWith(":")) continue;
    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
    } else if (line.startsWith("data:")) {
      dataLines.push(line.slice("data:".length).replace(/^ /, ""));
    }
  }
  return { event, data: dataLines.join("\n") };
}
