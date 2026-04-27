package agentdb

import (
	"bufio"
	"strings"
)

// SseFrame is one server-sent-event frame parsed from the response
// body. `Event` is the optional `event:` line value (empty for
// data-only frames). `Data` is every `data:` line joined with `\n`,
// with leading-space padding stripped per the SSE spec.
type SseFrame struct {
	Event string
	Data  string
}

// scanSseFrames reads an SSE stream from a Reader and pushes each
// frame onto `out`, blocking until the underlying reader returns
// EOF / error. Skips comment (`:`-prefixed) lines and pure-comment
// frames (no event + no data) so consumers don't see keep-alives.
//
// Tightly scoped to AgentDB's stream shape: only `event:`, `data:`,
// and `:`-comments are recognised. Everything else is ignored.
func scanSseFrames(r *bufio.Reader, out chan<- SseFrame) error {
	var event string
	var dataLines []string
	emit := func() {
		if event == "" && len(dataLines) == 0 {
			return
		}
		out <- SseFrame{Event: event, Data: strings.Join(dataLines, "\n")}
		event = ""
		dataLines = dataLines[:0]
	}
	for {
		raw, err := r.ReadString('\n')
		if len(raw) > 0 {
			line := strings.TrimRight(raw, "\r\n")
			switch {
			case line == "":
				emit()
			case strings.HasPrefix(line, ":"):
				// Comment / keep-alive — skip.
			case strings.HasPrefix(line, "event:"):
				event = strings.TrimSpace(line[len("event:"):])
			case strings.HasPrefix(line, "data:"):
				// SSE spec: strip a leading space if present. We
				// strip all leading spaces (matches Python /
				// TypeScript SDKs) since the server only ever emits
				// at most one space of padding.
				dataLines = append(dataLines, strings.TrimLeft(line[len("data:"):], " "))
			}
		}
		if err != nil {
			// Flush any in-flight frame at EOF so callers don't
			// miss a final close-with-no-newline payload.
			emit()
			return err
		}
	}
}
