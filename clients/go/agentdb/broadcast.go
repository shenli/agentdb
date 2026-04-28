package agentdb

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// BroadcastMessage is the wire shape for ephemeral channel
// broadcasts (spec § 5.2). Tenant-scoped, no `seq`, no replay —
// structurally distinct from [EventEnvelope] on purpose. `Sender`
// is set when the publisher used JWT auth (the `sub` claim); it's
// absent for Basic-auth publishers, which have no per-user
// identity.
type BroadcastMessage struct {
	Tenant  string          `json:"tenant"`
	Channel string          `json:"channel"`
	TsMs    int64           `json:"ts_ms"`
	Sender  string          `json:"sender,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// Broadcast publishes an ephemeral message to a channel. Returns
// nil on 204; a typed error otherwise.
//
// No durability, no replay — for transient signals (typing
// indicators, cursor positions, agent-status pings). If you need
// durability or catch-up, use [Session.Append] + [Session.Subscribe]
// instead.
//
// `payload` is marshalled to JSON and sent as the request body. The
// server wraps it in a [BroadcastMessage] envelope before fan-out.
//
// Tenant is derived server-side from the Bearer JWT supplied via
// [WithBearerToken] or [WithTokenProvider].
func (c *Client) Broadcast(ctx context.Context, channel string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("agentdb: marshal broadcast payload: %w", err)
	}
	endpoint := fmt.Sprintf(
		"%s/api/channels/%s/publish",
		c.baseURL, url.PathEscape(channel),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("agentdb: build broadcast request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if err := c.applyBearerOrBasic(req); err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("agentdb: send broadcast: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	raw, _ := io.ReadAll(resp.Body)
	return errorFromResponse(resp.StatusCode, raw, decodeJSONOrEmpty(raw))
}

// SubscribeChannel opens an SSE subscription to a channel. Returns a
// receive-only channel of [BroadcastMessage] (closed when the stream
// ends) and a buffered error channel sized 1 (receives at most one
// terminal error before closing).
//
// Ephemeral, no replay, no JWT auto-reconnect on expiry. A
// `jwt_expired` close flows out as a [SubscriptionClosed] error; the
// caller decides whether to reconnect. Callers that need durability
// or transparent JWT rotation use [Session.Subscribe] on a session.
//
// `ctx` controls cancellation: cancel it to tear the stream down.
func (c *Client) SubscribeChannel(ctx context.Context, channel string) (<-chan BroadcastMessage, <-chan error) {
	msgs := make(chan BroadcastMessage)
	errs := make(chan error, 1)
	go func() {
		defer close(msgs)
		defer close(errs)
		endpoint := fmt.Sprintf(
			"%s/api/channels/%s/subscribe",
			c.baseURL, url.PathEscape(channel),
		)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			errs <- fmt.Errorf("agentdb: build subscribe-channel request: %w", err)
			return
		}
		req.Header.Set("Accept", "text/event-stream")
		if err := c.applyBearerOrBasic(req); err != nil {
			errs <- err
			return
		}

		// Mirror Subscribe(): drop the per-request timeout for the
		// long-lived stream so idle keep-alives don't trip it.
		httpClient := c.httpClient
		if httpClient.Timeout != 0 {
			httpClient = &http.Client{Transport: c.httpClient.Transport}
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			errs <- fmt.Errorf("agentdb: open subscribe-channel stream: %w", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			raw, _ := io.ReadAll(resp.Body)
			errs <- errorFromResponse(resp.StatusCode, raw, decodeJSONOrEmpty(raw))
			return
		}

		r := bufio.NewReader(resp.Body)
		frames := make(chan SseFrame, 4)
		done := make(chan error, 1)
		go func() {
			err := scanSseFrames(r, frames)
			close(frames)
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			done <- err
		}()
		for {
			select {
			case <-ctx.Done():
				// Caller cancelled — Body.Close (deferred above)
				// will rip the underlying connection.
				return
			case frame, ok := <-frames:
				if !ok {
					if err := <-done; err != nil {
						errs <- err
					}
					return
				}
				switch frame.Event {
				case "subscribed":
					// Acknowledgement; ignore.
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
					errs <- &SubscriptionClosed{
						ErrorCode: body.Error,
						LastSeq:   body.LastSeq,
					}
					return
				default:
					if frame.Data == "" {
						continue
					}
					var msg BroadcastMessage
					if err := json.Unmarshal([]byte(frame.Data), &msg); err != nil {
						errs <- fmt.Errorf("agentdb: decode broadcast envelope: %w", err)
						return
					}
					select {
					case msgs <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return msgs, errs
}

// applyBearerOrBasic sets Authorization on `req` using whichever
// credential the client was constructed with. Mirrors the auth
// resolution used by the existing subscribe path: prefer bearer
// (JWT or token provider) when configured, fall back to Basic.
// Returns an error if the token provider itself fails.
func (c *Client) applyBearerOrBasic(req *http.Request) error {
	token, err := c.bearerToken()
	if err != nil {
		return fmt.Errorf("agentdb: fetch bearer token: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	return nil
}
