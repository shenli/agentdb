// End-to-end smoke test for the Go SDK. Spawns
// `agentdb serve --dev` as a subprocess on an ephemeral port,
// waits for /health/ready, and exercises the SDK against it.
//
// Run with:
//
//	cd clients/go && go test ./...
//
// Skipped automatically if the server binary hasn't been built.
package agentdb

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// repoRoot walks up from this test file to the repo root.
//   clients/go/agentdb/smoke_test.go → 3 levels up
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine test file location")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

// startServer spawns `agentdb serve --dev` on free ports and
// returns the base URL once /health/ready returns 200. The
// returned cleanup func kills the process and waits for exit.
func startServer(t *testing.T) (baseURL string, cleanup func()) {
	t.Helper()
	root := repoRoot(t)
	bin := filepath.Join(root, "target", "debug", "agentdb")
	if _, err := os.Stat(bin); errors.Is(err, os.ErrNotExist) {
		t.Skipf("server binary not found at %s; run `cargo build -p agentdb-server` first", bin)
	}
	rest := freePort(t)
	mongo := freePort(t)
	cmd := exec.Command(bin,
		"serve", "--dev",
		"--addr", "127.0.0.1:"+strconv.Itoa(rest),
		"--mongo-addr", "127.0.0.1:"+strconv.Itoa(mongo),
	)
	cmd.Env = append(os.Environ(), "RUST_LOG=warn")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	baseURL = "http://127.0.0.1:" + strconv.Itoa(rest)

	client := New(baseURL)
	deadline := time.Now().Add(15 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := client.HealthReady(ctx)
		cancel()
		if err == nil {
			ready = true
			break
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("server exited before ready (rc=%d)", cmd.ProcessState.ExitCode())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf("server didn't become ready within 15s")
	}

	cleanup = func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
	return baseURL, cleanup
}

func TestAppendRoundTrips(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	client := New(base, WithDefaultTenant("demo"))
	sess := client.Session("agent-smoke-1")

	res, err := sess.Append(context.Background(),
		map[string]any{"role": "user", "text": "hello"},
		AppendOptions{FetchLastN: 5},
	)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if res.Seq < 1 {
		t.Errorf("expected seq >= 1, got %d", res.Seq)
	}
	if got := len(res.Recent); got != 1 {
		t.Errorf("expected 1 recent event, got %d", got)
	}
}

func TestMultipleAppendsAccumulateInOrder(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	client := New(base, WithDefaultTenant("demo"))
	sess := client.Session("agent-smoke-2")

	ctx := context.Background()
	if _, err := sess.Append(ctx, map[string]any{"i": 1}); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if _, err := sess.Append(ctx, map[string]any{"i": 2}); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	res, err := sess.Append(ctx, map[string]any{"i": 3}, AppendOptions{FetchLastN: 10})
	if err != nil {
		t.Fatalf("append 3: %v", err)
	}
	if got := len(res.Recent); got != 3 {
		t.Fatalf("expected 3 recent events, got %d", got)
	}
}

func TestForkCreatesAChildThatInheritsParentEvents(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	client := New(base, WithDefaultTenant("demo"))
	parent := client.Session("agent-smoke-fork-parent")
	ctx := context.Background()

	if _, err := parent.Append(ctx, map[string]any{"i": 1}); err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	if _, err := parent.Append(ctx, map[string]any{"i": 2}); err != nil {
		t.Fatalf("seed 2: %v", err)
	}
	child, err := parent.Fork(ctx, "agent-smoke-fork-child")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if child.ID() != "agent-smoke-fork-child" {
		t.Errorf("unexpected child id: %s", child.ID())
	}
	res, err := child.Append(ctx, map[string]any{"i": 99}, AppendOptions{FetchLastN: 10})
	if err != nil {
		t.Fatalf("child append: %v", err)
	}
	if got := len(res.Recent); got < 1 {
		t.Errorf("expected child to see events; got %d", got)
	}
}

func TestSessionWithoutTenantOrAuthErrorsBeforeRequest(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	// No tenant on client, no auth — Append must refuse to
	// construct a path rather than send a 4xx-bound request.
	client := New(base)
	sess := client.Session("agent-smoke-3")
	_, err := sess.Append(context.Background(), map[string]any{"x": 1})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "tenant") {
		t.Errorf("error should mention tenant, got: %v", err)
	}
}

func TestHealthReadyProbe(t *testing.T) {
	base, stop := startServer(t)
	defer stop()
	client := New(base)
	if err := client.HealthReady(context.Background()); err != nil {
		t.Fatalf("HealthReady: %v", err)
	}
}

func TestNewWithUnpairedCredentialsPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic, got none")
		}
	}()
	_ = New("http://127.0.0.1:7878", WithBasicAuth("alice", ""))
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
