package agentdb

import (
	"bufio"
	"strings"
	"testing"
)

// drain runs scanSseFrames against a string and returns the frames.
func drainFrames(t *testing.T, in string) []SseFrame {
	t.Helper()
	r := bufio.NewReader(strings.NewReader(in))
	out := make(chan SseFrame, 16)
	done := make(chan struct{})
	go func() {
		_ = scanSseFrames(r, out)
		close(out)
		close(done)
	}()
	frames := []SseFrame{}
	for f := range out {
		frames = append(frames, f)
	}
	<-done
	return frames
}

func TestScanSseFramesYieldsNamedEventThenData(t *testing.T) {
	frames := drainFrames(t, "event: subscribed\ndata: {\"live_seq\": 1}\n\n")
	if got, want := len(frames), 1; got != want {
		t.Fatalf("frames len: got %d, want %d", got, want)
	}
	if frames[0].Event != "subscribed" {
		t.Errorf("event: got %q, want subscribed", frames[0].Event)
	}
	if frames[0].Data != `{"live_seq": 1}` {
		t.Errorf("data: got %q", frames[0].Data)
	}
}

func TestScanSseFramesSkipsCommentLines(t *testing.T) {
	frames := drainFrames(t, ": keepalive\n\ndata: {\"seq\": 1}\n\n")
	if got, want := len(frames), 1; got != want {
		t.Fatalf("frames len: got %d, want %d (frames=%v)", got, want, frames)
	}
	if frames[0].Event != "" {
		t.Errorf("event: got %q, want empty", frames[0].Event)
	}
	if frames[0].Data != `{"seq": 1}` {
		t.Errorf("data: got %q", frames[0].Data)
	}
}

func TestScanSseFramesJoinsMultiLineData(t *testing.T) {
	frames := drainFrames(t, "data: line1\ndata: line2\n\n")
	if got, want := len(frames), 1; got != want {
		t.Fatalf("frames len: got %d, want %d", got, want)
	}
	if got, want := frames[0].Data, "line1\nline2"; got != want {
		t.Errorf("data: got %q, want %q", got, want)
	}
}

func TestScanSseFramesTolerCRLF(t *testing.T) {
	frames := drainFrames(t, "event: ping\r\ndata: hi\r\n\r\n")
	if got, want := len(frames), 1; got != want {
		t.Fatalf("frames len: got %d, want %d (frames=%v)", got, want, frames)
	}
	if frames[0].Event != "ping" || frames[0].Data != "hi" {
		t.Errorf("got event=%q data=%q", frames[0].Event, frames[0].Data)
	}
}
