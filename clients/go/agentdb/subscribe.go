package agentdb

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// EventEnvelope is the SSE-flavoured event the server emits on
// subscribe streams. Mirrors the [Event] shape but with the extra
// fan-out fields (`Tenant`, `Session`, `Kind`) populated.
type EventEnvelope struct {
	Tenant  string          `json:"tenant"`
	Session string          `json:"session"`
	Seq     int64           `json:"seq"`
	TsMs    int64           `json:"ts_ms"`
	Kind    string          `json:"kind,omitempty"`
	ActorID *int64          `json:"actor_id,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// SubscriptionClosed represents a server-emitted terminal `event:
// error` frame on a subscribe stream. ErrorCode is the machine
// code (e.g. `lag_too_far`, `catchup_failed`); LastSeq is the
// last delivered seq when the server reported one.
//
// Subscribe's auto-reconnect loop swallows `jwt_expired` when a
// token provider is configured. Other codes flow back to the
// caller via the error channel returned from Subscribe.
type SubscriptionClosed struct {
	ErrorCode string
	LastSeq   *int64
}

func (e *SubscriptionClosed) Error() string {
	return fmt.Sprintf("agentdb: subscription closed: %s", e.ErrorCode)
}

// SubscribeOptions tunes a tenant-wide [Client.Subscribe] call.
type SubscribeOptions struct {
	// Tenant overrides the client's default. Required when the
	// client wasn't constructed with [WithDefaultTenant].
	Tenant string
	// Session filters to one or more session ids.
	Session []string
	// Kind filters to one or more event kinds.
	Kind []string
	// FromSeq replays from this seq first, then transitions to
	// live tail. Inclusive lower bound. Ignored when 0.
	FromSeq int64
	// FromSeqPerSession is per-session catch-up cursors for the
	// tenant-wide endpoint, sent as
	// `from_seq_per_session=sid:seq,sid:seq,…`.
	FromSeqPerSession map[string]int64
}

// SessionSubscribeOptions tunes a per-session [Session.Subscribe].
type SessionSubscribeOptions struct {
	Kind    []string
	FromSeq int64
}

// Subscribe streams events for the whole tenant. Returns two
// channels: events (closed when the stream ends) and errs (sized
// 1; receives the terminal error if any, then closes).
//
// On a `jwt_expired` close frame, the client transparently
// refreshes the token (via the configured provider) and reconnects
// with `from_seq = lastSeen + 1` so consumers never see a gap. Any
// other [SubscriptionClosed], or a transport-level error, flows
// out via the errs channel.
//
// The `ctx` argument controls cancellation: cancel it to tear the
// stream down deterministically.
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions) (<-chan EventEnvelope, <-chan error) {
	tenant := opts.Tenant
	if tenant == "" {
		tenant = c.tenant
	}
	events := make(chan EventEnvelope)
	errs := make(chan error, 1)
	go func() {
		defer close(events)
		defer close(errs)
		if tenant == "" {
			errs <- errors.New(
				"agentdb: tenant must be set on the client or in SubscribeOptions",
			)
			return
		}
		path := fmt.Sprintf("/api/tenants/%s/subscribe", url.PathEscape(tenant))
		c.subscribeLoop(ctx, path, opts.Session, opts.Kind, opts.FromSeq, opts.FromSeqPerSession, events, errs)
	}()
	return events, errs
}

// Subscribe streams events for this session. Same channel shape
// as [Client.Subscribe]; see that doc for reconnect semantics.
func (s *Session) Subscribe(ctx context.Context, opts SessionSubscribeOptions) (<-chan EventEnvelope, <-chan error) {
	events := make(chan EventEnvelope)
	errs := make(chan error, 1)
	go func() {
		defer close(events)
		defer close(errs)
		if s.tenant == "" {
			errs <- errors.New(
				"agentdb: tenant must be set on the client or session for subscribe (auth-derived tenants on subscribe aren't supported yet)",
			)
			return
		}
		path := fmt.Sprintf(
			"/api/tenants/%s/sessions/%s/subscribe",
			url.PathEscape(s.tenant), url.PathEscape(s.id),
		)
		s.client.subscribeLoop(ctx, path, nil, opts.Kind, opts.FromSeq, nil, events, errs)
	}()
	return events, errs
}

// bearerToken returns the current JWT. Called fresh on every
// reconnect so a token rotation between attempts comes through
// without restarting the goroutine.
func (c *Client) bearerToken() (string, error) {
	if c.getToken != nil {
		return c.getToken()
	}
	return c.jwt, nil
}

// subscribeLoop is the connect-and-stream cycle with JWT-expiry
// auto-resume. Shared between the per-session and tenant-wide
// subscribe entry points.
func (c *Client) subscribeLoop(
	ctx context.Context,
	path string,
	sessions []string,
	kinds []string,
	fromSeq int64,
	fromSeqPerSession map[string]int64,
	events chan<- EventEnvelope,
	errs chan<- error,
) {
	cursor := fromSeq
	perSessionCursor := map[string]int64{}
	for k, v := range fromSeqPerSession {
		perSessionCursor[k] = v
	}
	for {
		var lastSeen int64
		closeErr := c.streamOnce(
			ctx, path, sessions, kinds, cursor, perSessionCursor,
			func(env EventEnvelope) {
				if env.Seq > lastSeen {
					lastSeen = env.Seq
				}
				if len(perSessionCursor) > 0 && env.Session != "" {
					perSessionCursor[env.Session] = env.Seq
				}
				select {
				case events <- env:
				case <-ctx.Done():
				}
			},
		)
		if closeErr == nil {
			// Server closed cleanly.
			return
		}
		// `jwt_expired` plus a token provider → refresh + reconnect.
		var sc *SubscriptionClosed
		if errors.As(closeErr, &sc) && sc.ErrorCode == "jwt_expired" &&
			(c.getToken != nil || c.jwt != "") {
			fallback := lastSeen
			if sc.LastSeq != nil {
				fallback = *sc.LastSeq
			}
			if fallback > 0 {
				cursor = fallback + 1
			}
			continue
		}
		// Surface to caller.
		select {
		case errs <- closeErr:
		case <-ctx.Done():
		}
		return
	}
}

// streamOnce opens one SSE connection and pushes envelopes into
// `onEvent` until the stream closes. Returns nil for clean EOF, a
// [SubscriptionClosed] for an `event: error` frame, or any other
// error for transport-level failures.
func (c *Client) streamOnce(
	ctx context.Context,
	path string,
	sessions []string,
	kinds []string,
	fromSeq int64,
	fromSeqPerSession map[string]int64,
	onEvent func(EventEnvelope),
) error {
	u, err := url.Parse(c.baseURL + path)
	if err != nil {
		return fmt.Errorf("agentdb: parse url: %w", err)
	}
	q := u.Query()
	if fromSeq > 0 {
		q.Set("from_seq", strconv.FormatInt(fromSeq, 10))
	}
	if len(fromSeqPerSession) > 0 {
		parts := make([]string, 0, len(fromSeqPerSession))
		for sid, seq := range fromSeqPerSession {
			parts = append(parts, fmt.Sprintf("%s:%d", sid, seq))
		}
		q.Set("from_seq_per_session", strings.Join(parts, ","))
	}
	for _, s := range sessions {
		q.Add("session", s)
	}
	for _, k := range kinds {
		q.Add("kind", k)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("agentdb: build subscribe request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	token, err := c.bearerToken()
	if err != nil {
		return fmt.Errorf("agentdb: fetch bearer token: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	// SSE streams are long-lived; use a dedicated client without
	// the per-request timeout so connections survive idle keep-
	// alive periods.
	httpClient := c.httpClient
	if httpClient.Timeout != 0 {
		httpClient = &http.Client{
			Transport: c.httpClient.Transport,
		}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("agentdb: open subscribe stream: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return errorFromResponse(resp.StatusCode, raw, decodeJSONOrEmpty(raw))
	}
	r := bufio.NewReader(resp.Body)
	frames := make(chan SseFrame, 4)
	done := make(chan error, 1)
	go func() {
		err := scanSseFrames(r, frames)
		close(frames)
		// EOF on a clean close maps to nil; anything else is a
		// transport-level read error.
		if errors.Is(err, io.EOF) {
			done <- nil
			return
		}
		done <- err
	}()
	for {
		select {
		case <-ctx.Done():
			// Caller cancelled — let the deferred Body.Close
			// rip the underlying connection.
			return ctx.Err()
		case frame, ok := <-frames:
			if !ok {
				// Reader goroutine exited; surface its result.
				return <-done
			}
			switch frame.Event {
			case "subscribed":
				// Acknowledgement; ignore the live_seq payload.
				continue
			case "error":
				var body struct {
					Error   string `json:"error"`
					LastSeq *int64 `json:"last_seq,omitempty"`
				}
				if frame.Data != "" {
					_ = json.Unmarshal([]byte(frame.Data), &body)
				}
				if body.Error == "" {
					body.Error = "unknown"
				}
				return &SubscriptionClosed{
					ErrorCode: body.Error,
					LastSeq:   body.LastSeq,
				}
			default:
				if frame.Data == "" {
					continue
				}
				var env EventEnvelope
				if err := json.Unmarshal([]byte(frame.Data), &env); err != nil {
					return fmt.Errorf("agentdb: decode event envelope: %w", err)
				}
				onEvent(env)
			}
		}
	}
}
