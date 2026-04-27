package agentdb

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Validation tests (no server) ------------------------------------

func TestBroadcastMessageJSONRoundTrip(t *testing.T) {
	in := BroadcastMessage{
		Tenant:  "t-1",
		Channel: "c-1",
		TsMs:    100,
		Sender:  "u-1",
		Payload: json.RawMessage(`{"k":"v"}`),
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out BroadcastMessage
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Tenant != "t-1" || out.Channel != "c-1" || out.Sender != "u-1" || out.TsMs != 100 {
		t.Fatalf("round trip lost fields: %+v", out)
	}
	var payload map[string]string
	if err := json.Unmarshal(out.Payload, &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload["k"] != "v" {
		t.Fatalf("payload mismatch: %v", payload)
	}
}

func TestBroadcastMessageOmitsEmptySender(t *testing.T) {
	in := BroadcastMessage{
		Tenant:  "t-1",
		Channel: "c-1",
		TsMs:    100,
		Payload: json.RawMessage(`{}`),
		// Sender intentionally omitted
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `"sender"`; bytes.Contains(b, []byte(want)) {
		t.Fatalf("expected sender to be omitted, got: %s", b)
	}
}

func TestBroadcastRequiresTenant(t *testing.T) {
	client := New("http://127.0.0.1:7878")
	err := client.Broadcast(context.Background(), "c-1", map[string]string{"k": "v"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "tenant") {
		t.Errorf("error should mention tenant, got: %v", err)
	}
}

func TestSubscribeChannelRequiresTenant(t *testing.T) {
	client := New("http://127.0.0.1:7878")
	msgs, errs := client.SubscribeChannel(context.Background(), "c-1")
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
	for range msgs {
	}
}

// Live-server smoke -----------------------------------------------

// TestBroadcastRoundTripsViaSubscribeChannel publishes three
// messages on a channel and verifies a concurrent SubscribeChannel
// receives them. Mirrors the Phase 1 Subscribe smoke pattern: gated
// on the server binary being built (skipped otherwise via
// startServer).
func TestBroadcastRoundTripsViaSubscribeChannel(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	client := New(base, WithDefaultTenant("t-1"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	msgs, errs := client.SubscribeChannel(ctx, "c-go-smoke")
	// Give the SSE handshake a moment so publishes don't race ahead
	// of attach.
	time.Sleep(500 * time.Millisecond)

	for i := 1; i <= 3; i++ {
		if err := client.Broadcast(ctx, "c-go-smoke", map[string]any{"i": i}); err != nil {
			t.Fatalf("broadcast %d: %v", i, err)
		}
	}

	got := []int{}
	for len(got) < 3 {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatalf("msgs closed early; got %v", got)
			}
			var p struct {
				I int `json:"i"`
			}
			if err := json.Unmarshal(msg.Payload, &p); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			if msg.Tenant != "t-1" || msg.Channel != "c-go-smoke" {
				t.Errorf("unexpected envelope: %+v", msg)
			}
			got = append(got, p.I)
		case err := <-errs:
			t.Fatalf("subscribe-channel error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout; got %v", got)
		}
	}
	cancel()
}
