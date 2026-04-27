package agentdb

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"
)

// Validation tests (no server) ------------------------------------

func TestSubscribeRequiresTenant(t *testing.T) {
	client := New("http://127.0.0.1:7878")
	events, errs := client.Subscribe(context.Background(), SubscribeOptions{})
	// First receive on errs gives the validation error; events
	// channel is closed in the same goroutine.
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !contains(err.Error(), "tenant") {
			t.Errorf("error should mention tenant, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("validation didn't fire within 2s")
	}
	// events should drain to closed.
	for range events {
	}
}

func TestSessionSubscribeRequiresTenant(t *testing.T) {
	client := New("http://127.0.0.1:7878")
	sess := client.Session("agent-1")
	events, errs := sess.Subscribe(context.Background(), SessionSubscribeOptions{})
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !contains(err.Error(), "tenant") {
			t.Errorf("error should mention tenant, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("validation didn't fire within 2s")
	}
	for range events {
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
	client := New(base, WithDefaultTenant("t-1"))
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
	client := New(base, WithDefaultTenant("t-1"))
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
