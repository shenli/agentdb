package persona

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

type SessionRun struct {
	SessionID string
	Persona   *Persona
	Appender  Appender
	Searcher  Searcher
}

type Appender interface {
	Append(ctx context.Context, tenantID, sessionID string, event map[string]any, fetchN int) error
}

type Searcher interface {
	Search(ctx context.Context, tenantID, query string) error
}

var ticketCounter atomic.Uint64

func NewSessionID(p *Persona) string {
	n := ticketCounter.Add(1)
	return fmt.Sprintf("ticket-%s-%d", p.TenantID, n)
}

// Run drives a single conversation to completion.
func (s *SessionRun) Run(ctx context.Context, rng *rand.Rand) {
	length := randRange(rng, s.Persona.SessionLen[0], s.Persona.SessionLen[1])
	for i := 0; i < length; i++ {
		if ctx.Err() != nil {
			return
		}
		event := map[string]any{
			"kind": "message",
			"text": fmt.Sprintf("message %d of %d on %s", i+1, length, s.SessionID),
			"i":    i,
		}
		_ = s.Appender.Append(ctx, s.Persona.TenantID, s.SessionID, event, 5)

		if s.Searcher != nil && rng.Float64() < s.Persona.MemoryRecallProb {
			_ = s.Searcher.Search(ctx, s.Persona.TenantID, "message")
		}

		sleep := time.Duration(randRange(rng, s.Persona.MessageIntervalMs[0], s.Persona.MessageIntervalMs[1])) * time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func randRange(rng *rand.Rand, lo, hi int) int {
	if hi <= lo {
		return lo
	}
	return lo + rng.Intn(hi-lo)
}
