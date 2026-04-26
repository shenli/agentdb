// Package agentdb is the Go client for AgentDB.
//
// This file defines the typed error hierarchy. The server emits
// structured error bodies of the form
//
//	{"error": {"code": "...", "kind": "...", "message": "..."}}
//
// which this package surfaces as one of the typed structs below.
// Status codes are also preserved so callers can switch on either
// the HTTP layer or the error code, whichever fits better.
//
// Idiomatic use:
//
//	var rl *agentdb.RateLimited
//	if errors.As(err, &rl) {
//	    time.Sleep(time.Duration(rl.RetryAfterMs) * time.Millisecond)
//	    // retry
//	}
package agentdb

import "fmt"

// Error is the base interface every typed AgentDB error
// satisfies. Use errors.As to extract the concrete subtype;
// errors.Is to compare against sentinel cases (rare).
type Error interface {
	error
	StatusCode() int
	Code() string
	Kind() string
}

// errorBase is the shared embedded type for every typed error.
// Carries the structured fields the server returned plus the
// HTTP status. Callers normally interact through the wrapping
// type (BadRequest, RateLimited, etc.) and don't construct this
// directly.
type errorBase struct {
	Status   int
	CodeStr  string
	KindStr  string
	Message  string
	RawBody  any
}

func (e *errorBase) Error() string {
	if e.CodeStr != "" {
		return fmt.Sprintf("agentdb: %s (code=%s) [HTTP %d]", e.Message, e.CodeStr, e.Status)
	}
	return fmt.Sprintf("agentdb: %s [HTTP %d]", e.Message, e.Status)
}

func (e *errorBase) StatusCode() int { return e.Status }
func (e *errorBase) Code() string    { return e.CodeStr }
func (e *errorBase) Kind() string    { return e.KindStr }

// BadRequest — 400. Malformed body, missing field, mismatch.
type BadRequest struct{ errorBase }

// Unauthorized — 401. Auth header missing or credentials invalid.
type Unauthorized struct{ errorBase }

// Forbidden — 403. Authed user can't act on the target.
type Forbidden struct{ errorBase }

// NotFound — 404. Tenant or session doesn't exist.
type NotFound struct{ errorBase }

// Conflict — 409. Resource already exists / state collision.
type Conflict struct{ errorBase }

// Misdirected — 421. Multi-node routing drift.
type Misdirected struct{ errorBase }

// RateLimited — 429. Quota exhausted; retry after RetryAfterMs.
type RateLimited struct {
	errorBase
	RetryAfterMs int64
}

// ServerError — 5xx. Forwarding failure, fault injection,
// draining. Almost always transient — sensible to retry once
// with backoff.
type ServerError struct{ errorBase }

// UnexpectedResponse covers shapes the typed cases don't —
// a 2xx with a non-JSON body, an unexpected status code, etc.
// Surfaced verbatim so the caller can see what went wrong.
type UnexpectedResponse struct{ errorBase }

// errorFromResponse builds the right typed error from an HTTP
// response body. Tolerates a missing or unstructured body —
// the server normally emits the v2 shape, but proxies / very
// early failures might not.
func errorFromResponse(status int, body []byte, parsed map[string]any) error {
	base := errorBase{Status: status, RawBody: parsed}
	if inner, ok := parsed["error"].(map[string]any); ok {
		if v, ok := inner["code"].(string); ok {
			base.CodeStr = v
		}
		if v, ok := inner["kind"].(string); ok {
			base.KindStr = v
		}
		if v, ok := inner["message"].(string); ok {
			base.Message = v
		}
		// rate-limit hints: int64 in the source, but JSON
		// numbers parse as float64 in Go's stdlib.
		var retryAfter int64
		if v, ok := inner["retry_after_ms"].(float64); ok {
			retryAfter = int64(v)
		}
		if status == 429 {
			return &RateLimited{errorBase: base, RetryAfterMs: retryAfter}
		}
	}
	if base.Message == "" {
		// Fall back to the legacy `errmsg` field, then a body
		// summary, then a default.
		if v, ok := parsed["errmsg"].(string); ok {
			base.Message = v
		} else if len(body) > 0 {
			base.Message = string(body)
		} else {
			base.Message = fmt.Sprintf("AgentDB returned HTTP %d", status)
		}
	}

	switch {
	case status == 400:
		return &BadRequest{errorBase: base}
	case status == 401:
		return &Unauthorized{errorBase: base}
	case status == 403:
		return &Forbidden{errorBase: base}
	case status == 404:
		return &NotFound{errorBase: base}
	case status == 409:
		return &Conflict{errorBase: base}
	case status == 421:
		return &Misdirected{errorBase: base}
	case status == 429:
		return &RateLimited{errorBase: base}
	case status >= 500:
		return &ServerError{errorBase: base}
	default:
		return &UnexpectedResponse{errorBase: base}
	}
}
