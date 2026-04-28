package agentdb_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shenli/agentdb/clients/go/agentdb"
)

func TestV2_AppendURL(t *testing.T) {
	var capturedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"seq": 1, "recent": []}`)) //nolint
	}))
	defer srv.Close()

	c := agentdb.New(srv.URL, agentdb.WithBasicAuth("u", "p"))
	if _, err := c.Session("s1").Append(context.Background(), map[string]any{"hello": "world"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if !strings.Contains(capturedURL, "/api/sessions/s1/append_and_fetch") {
		t.Errorf("expected v2 URL, got %s", capturedURL)
	}
	if strings.Contains(capturedURL, "/api/tenants/") {
		t.Errorf("legacy URL leaked: %s", capturedURL)
	}
}

func TestV2_ForkURL(t *testing.T) {
	var capturedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := agentdb.New(srv.URL, agentdb.WithBasicAuth("u", "p"))
	if _, err := c.Session("s1").Fork(context.Background(), "s2"); err != nil {
		t.Fatalf("fork: %v", err)
	}
	if !strings.Contains(capturedURL, "/api/sessions/s1/fork") {
		t.Errorf("expected v2 fork URL, got %s", capturedURL)
	}
	if strings.Contains(capturedURL, "/api/tenants/") {
		t.Errorf("legacy URL leaked: %s", capturedURL)
	}
}

func TestV2_BroadcastURL(t *testing.T) {
	var capturedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := agentdb.New(srv.URL, agentdb.WithBearerToken("tok"))
	if err := c.Broadcast(context.Background(), "my-chan", map[string]any{"k": "v"}); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if !strings.Contains(capturedURL, "/api/channels/my-chan/publish") {
		t.Errorf("expected v2 channel URL, got %s", capturedURL)
	}
	if strings.Contains(capturedURL, "/api/tenants/") {
		t.Errorf("legacy URL leaked: %s", capturedURL)
	}
}

func TestV2_NoTenantInClient(t *testing.T) {
	// Compile-time check: Client is usable without any tenant option.
	// WithBearerToken is the alias for WithJWT.
	c := agentdb.New("http://x", agentdb.WithBasicAuth("u", "p"))
	_ = c
	// WithBearerToken should also compile.
	c2 := agentdb.New("http://x", agentdb.WithBearerToken("tok"))
	_ = c2
}
