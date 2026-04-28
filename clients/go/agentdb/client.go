package agentdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is an AgentDB connection. Constructed via [New]. Safe
// for concurrent use by multiple goroutines: the underlying
// http.Client is goroutine-safe, and the Client struct is
// otherwise read-only after construction.
type Client struct {
	baseURL    string
	user       string
	password   string
	timeout    time.Duration
	httpClient *http.Client
	// Channel-route bearer auth. jwt is the static form;
	// getToken is the refreshable form (called fresh on every
	// subscribe reconnect so a server-side jwt_expired close
	// recovers transparently).
	jwt      string
	getToken func() (string, error)
}

// Option configures a Client at construction time. Use the
// With* helpers; do not construct these directly.
type Option func(*Client)

// WithBasicAuth enables HTTP Basic auth on session and subscribe
// routes. `user` and `password` are required together. Tenant is
// derived from credentials server-side — no separate tenant option
// is needed.
func WithBasicAuth(user, password string) Option {
	return func(c *Client) {
		c.user = user
		c.password = password
	}
}

// WithBearerToken sets a static JWT used on channel routes
// (Broadcast / SubscribeChannel). Alias for [WithJWT]. Mutually
// exclusive with [WithTokenProvider].
func WithBearerToken(token string) Option {
	return func(c *Client) { c.jwt = token }
}

// WithTimeout overrides the per-request timeout. Default 30s.
// Tune down for interactive agent work where you'd rather fail
// fast and retry than wait on a slow round trip.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// WithHTTPClient lets the caller supply a custom *http.Client
// — useful for instrumented clients, custom transports, or
// tests that intercept requests.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.httpClient = h }
}

// WithJWT sets a static bearer token used by [Client.Subscribe] /
// [Session.Subscribe]. Mutually exclusive with [WithTokenProvider]
// — pass exactly one when the server requires auth on the
// subscribe path.
func WithJWT(token string) Option {
	return func(c *Client) { c.jwt = token }
}

// WithTokenProvider sets a callback that returns the current
// bearer token. Called fresh on every reconnect, which is what
// makes the SDK's `jwt_expired` auto-resume transparent — rotate
// once in the provider and the next reconnect picks it up
// without restarting the goroutine.
func WithTokenProvider(fn func() (string, error)) Option {
	return func(c *Client) { c.getToken = fn }
}

// New constructs a Client.
//
// Session/subscribe routes use Basic auth:
//
//	client := agentdb.New("https://prod.example.com",
//	    agentdb.WithBasicAuth("alice", "hunter2"))
//
// Channel routes use Bearer auth:
//
//	client := agentdb.New("https://prod.example.com",
//	    agentdb.WithBearerToken("eyJ..."))
//
// Both can be supplied simultaneously — Basic auth covers
// session/subscribe; Bearer covers channels. Tenant is always
// derived server-side from credentials; there is no client-side
// tenant option.
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		timeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: c.timeout}
	}
	if (c.user == "") != (c.password == "") {
		// Only one of the credential pair is set — programmer
		// error, surface eagerly. We can't return an error
		// from a constructor in idiomatic Go, so we panic.
		// Callers control this input directly so a panic is
		// fine — the alternative is silent auth misconfig.
		panic("agentdb: WithBasicAuth requires both user and password")
	}
	if c.jwt != "" && c.getToken != nil {
		panic("agentdb: pass WithJWT or WithTokenProvider, not both")
	}
	return c
}

// Authenticated reports whether the client was constructed
// with Basic-auth credentials.
func (c *Client) Authenticated() bool { return c.user != "" }

// HealthReady probes /health/ready. Returns nil on 200, the
// typed error otherwise. Use it while waiting on a server
// that's still bootstrapping (smoke tests, dev-mode harnesses).
func (c *Client) HealthReady(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodGet, "/health/ready", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return errorFromResponse(resp.StatusCode, body, decodeJSONOrEmpty(body))
}

// Session returns a handle to one session. Tenant is always
// derived server-side from the Basic-auth credentials supplied
// at construction via [WithBasicAuth].
func (c *Client) Session(sessionID string) *Session {
	return &Session{client: c, id: sessionID}
}

// Session is a single AgentDB session. Construct via
// [Client.Session]. Every method makes one round trip to the
// server; no client-side event caching.
type Session struct {
	client *Client
	id     string
}

// ID returns the session id this handle points at.
func (s *Session) ID() string { return s.id }

// Event is one record read back from AgentDB. The shape
// mirrors the server's EventDto.
type Event struct {
	Seq     int64           `json:"seq"`
	TsMs    int64           `json:"ts_ms"`
	Payload json.RawMessage `json:"payload"`
	ActorID *int64          `json:"actor_id,omitempty"`
}

// AppendResult is what [Session.Append] returns: the new
// event's sequence number and (when fetch_last_n > 0) the
// recent events read back in the same round trip.
type AppendResult struct {
	Seq    int64   `json:"seq"`
	Recent []Event `json:"recent"`
}

// AppendOptions tunes a single append call.
type AppendOptions struct {
	// FetchLastN reads the last N events back in the same
	// round trip. 0 (the default) skips the read-back; the
	// returned AppendResult.Recent is empty.
	FetchLastN int
	// ActorID is an optional small-int identity for multi-
	// actor sessions (user vs tool). Leave nil if unused.
	ActorID *int64
}

// Append writes one event and (optionally) reads back the last
// N. The canonical hot-path primitive — combines the write and
// the read-back into one round trip.
func (s *Session) Append(ctx context.Context, event any, opts ...AppendOptions) (*AppendResult, error) {
	var o AppendOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	body := map[string]any{
		"event":         event,
		"fetch_last_n":  o.FetchLastN,
	}
	if o.ActorID != nil {
		body["actor_id"] = *o.ActorID
	}
	path := s.appendPath()
	var out AppendResult
	if err := s.client.requestJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Fork branches this session into a child that inherits the
// parent's events at fork time, MVCC-backed. Returns a Session
// handle for the new id.
func (s *Session) Fork(ctx context.Context, childSessionID string) (*Session, error) {
	body := map[string]any{"new_session_id": childSessionID}
	path := fmt.Sprintf("/api/sessions/%s/fork", url.PathEscape(s.id))
	if err := s.client.requestJSON(ctx, http.MethodPost, path, body, nil); err != nil {
		return nil, err
	}
	return &Session{client: s.client, id: childSessionID}, nil
}

func (s *Session) appendPath() string {
	return fmt.Sprintf("/api/sessions/%s/append_and_fetch", url.PathEscape(s.id))
}

// requestJSON issues a request and decodes the response body
// into `out` (when non-nil). Centralizes auth + error handling.
// `out` may be a pointer to a struct or nil (for fire-and-
// forget calls like fork).
func (c *Client) requestJSON(ctx context.Context, method, path string, body any, out any) error {
	resp, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("agentdb: read response body: %w", err)
	}
	parsed := decodeJSONOrEmpty(raw)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("agentdb: decode response body: %w", err)
		}
		return nil
	}
	return errorFromResponse(resp.StatusCode, raw, parsed)
}

func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("agentdb: encode request body: %w", err)
		}
		bodyReader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("agentdb: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.user != "" {
		token := base64.StdEncoding.EncodeToString([]byte(c.user + ":" + c.password))
		req.Header.Set("Authorization", "Basic "+token)
	}
	return c.httpClient.Do(req)
}

func decodeJSONOrEmpty(b []byte) map[string]any {
	if len(b) == 0 {
		return map[string]any{}
	}
	var parsed map[string]any
	if err := json.Unmarshal(b, &parsed); err != nil {
		return map[string]any{}
	}
	return parsed
}
