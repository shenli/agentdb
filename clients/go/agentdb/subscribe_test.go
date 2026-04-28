package agentdb

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"
)

// Validation tests (no server) ------------------------------------

func TestSubscribeBuildsV2URL(t *testing.T) {
	// v2: Subscribe no longer requires a tenant — it derives tenant
	// server-side from auth. Just verify the call compiles and can be
	// constructed without error.
	client := New("http://127.0.0.1:7878", WithBasicAuth("u", "p"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so no real connection is attempted
	events, errs := client.Subscribe(ctx, SubscribeOptions{})
	for range events {
	}
	for range errs {
	}
}

func TestSessionSubscribeBuildsV2URL(t *testing.T) {
	// v2: Session.Subscribe no longer requires a tenant.
	client := New("http://127.0.0.1:7878", WithBasicAuth("u", "p"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so no real connection is attempted
	sess := client.Session("agent-1")
	events, errs := sess.Subscribe(ctx, SessionSubscribeOptions{})
	for range events {
	}
	for range errs {
	}
}

func TestNewWithJWTAndTokenProviderPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic, got none")
		}
	}()
	_ = New("http://127.0.0.1:7878",
		WithJWT("x"),
		WithTokenProvider(func() (string, error) { return "y", nil }),
	)
}

func TestSubscriptionClosedErrorMessageMentionsCode(t *testing.T) {
	last := int64(42)
	err := error(&SubscriptionClosed{ErrorCode: "lag_too_far", LastSeq: &last})
	if !contains(err.Error(), "lag_too_far") {
		t.Errorf("expected lag_too_far in message, got %q", err.Error())
	}
	var sc *SubscriptionClosed
	if !errors.As(err, &sc) {
		t.Fatal("errors.As should match SubscriptionClosed")
	}
	if sc.LastSeq == nil || *sc.LastSeq != 42 {
		t.Errorf("last_seq should round-trip, got %v", sc.LastSeq)
	}
}

// Live-server smoke -----------------------------------------------

// TestSubscribeYieldsAppendedEvents opens a per-session subscribe
// then drives three appends and waits for them to arrive.
func TestSubscribeYieldsAppendedEvents(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	client := New(base, WithBasicAuth(smokeUser, smokePassword))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sess := client.Session("s-sub-go")
	events, errs := sess.Subscribe(ctx, SessionSubscribeOptions{})
	// Give the subscribe handshake a beat to attach.
	time.Sleep(500 * time.Millisecond)

	writer := client.Session("s-sub-go")
	for i := 1; i <= 3; i++ {
		if _, err := writer.Append(ctx, map[string]any{"i": i}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	got := []int64{}
	for len(got) < 3 {
		select {
		case env, ok := <-events:
			if !ok {
				t.Fatalf("events closed early; got %v", got)
			}
			got = append(got, env.Seq)
		case err := <-errs:
			t.Fatalf("subscribe error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout; got seqs %v", got)
		}
	}
	cancel()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := []int64{1, 2, 3}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("seqs mismatch: got %v, want %v", got, want)
			break
		}
	}
}

// TestSubscribeReplaysFromSeq pre-seeds three events, then
// subscribes with from_seq=1 and verifies the replay shape.
func TestSubscribeReplaysFromSeq(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	client := New(base, WithBasicAuth(smokeUser, smokePassword))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	writer := client.Session("s-sub-go-replay")
	for i := 0; i < 3; i++ {
		if _, err := writer.Append(ctx, map[string]any{"i": i}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	reader := client.Session("s-sub-go-replay")
	events, errs := reader.Subscribe(ctx, SessionSubscribeOptions{FromSeq: 1})
	got := []int64{}
	for len(got) < 3 {
		select {
		case env, ok := <-events:
			if !ok {
				t.Fatalf("events closed early; got %v", got)
			}
			got = append(got, env.Seq)
		case err := <-errs:
			t.Fatalf("subscribe error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout; got seqs %v", got)
		}
	}
	cancel()
	want := []int64{1, 2, 3}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("replay seqs mismatch: got %v, want %v", got, want)
			break
		}
	}
}
