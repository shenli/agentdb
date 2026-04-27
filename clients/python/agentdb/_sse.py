"""Minimal Server-Sent Events parser used by `subscribe()`.

Yields one [`SseFrame`] per `\\n\\n` block from a stream of decoded
text lines (HTTP libraries already split the response on newlines —
we accumulate per-frame state and emit a frame when the blank
delimiter line shows up).

Tiny on purpose: AgentDB's SSE stream uses only `event:`, `data:`,
and `:`-comment lines, so the full SSE spec (id retry hints, multi-
line ids, CRLF normalisation past the trailing `\\r`) isn't needed.

Both a sync and an async iterator variant exist so the same shape
fits the sync `requests`-backed client and the async
`httpx.AsyncClient`-backed one without forking the parser.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Iterator, List, Optional


@dataclass
class SseFrame:
    """One server-sent-event frame.

    `event` is the optional `event:` line value (None when the
    frame had only `data:` lines). `data` is every `data:` line
    joined with `\\n`, with a single leading space stripped per
    line (per the SSE spec).
    """

    event: Optional[str]
    data: str


def parse_lines(lines: Iterator[str]) -> Iterator[SseFrame]:
    """Synchronous frame parser. Caller passes `requests.iter_lines(decode_unicode=True)`."""
    event: Optional[str] = None
    data_lines: List[str] = []
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data_lines or event is not None:
                yield SseFrame(event=event, data="\n".join(data_lines))
            event, data_lines = None, []
            continue
        if line.startswith(":"):
            # Comment / keepalive — skip.
            continue
        if line.startswith("event:"):
            event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].lstrip(" "))


async def parse_lines_async(lines: AsyncIterator[str]) -> AsyncIterator[SseFrame]:
    """Async counterpart of [`parse_lines`]. Caller passes
    `httpx.Response.aiter_lines()` (already decoded + newline-split).
    """
    event: Optional[str] = None
    data_lines: List[str] = []
    async for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data_lines or event is not None:
                yield SseFrame(event=event, data="\n".join(data_lines))
            event, data_lines = None, []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].lstrip(" "))
