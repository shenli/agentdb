// perf-probe fires concurrent append_and_fetch requests at agentdb-server
// and reports throughput + latency percentiles. Intended for manual perf
// runs, not for CI.
//
// Each worker opens its own session up front (POST /api/sessions/open) to
// obtain canonical tenant/agent/session IDs, then loops append_and_fetch
// against those IDs. Workers spread across `-tenants` distinct tenants.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func readN(r io.Reader, n int) (string, error) {
	buf := make([]byte, n)
	m, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return "", err
	}
	return string(buf[:m]), nil
}

// session holds the canonical IDs returned by /api/sessions/open.
type session struct {
	TenantID  string `json:"tenant_id"`
	AgentID   string `json:"agent_id"`
	SessionID string `json:"session_id"`
}

func openSession(client *http.Client, base, workspace, agent, sess string) (session, error) {
	body, _ := json.Marshal(map[string]any{
		"workspace": workspace,
		"agent":     agent,
		"session":   sess,
	})
	resp, err := client.Post(base+"/api/sessions/open", "application/json", bytes.NewReader(body))
	if err != nil {
		return session{}, err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp.StatusCode >= 400 {
		msg, _ := readN(resp.Body, 256)
		return session{}, fmt.Errorf("open status=%d body=%s", resp.StatusCode, msg)
	}
	var s session
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return session{}, err
	}
	return s, nil
}

func appendEvent(client *http.Client, base string, s session, seq int) error {
	body, _ := json.Marshal(map[string]any{
		"agent_id": s.AgentID,
		"kind":     "message",
		"payload":  map[string]any{"seq": seq},
	})
	url := fmt.Sprintf("%s/api/tenants/%s/sessions/%s/events", base, s.TenantID, s.SessionID)
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		msg, _ := readN(resp.Body, 256)
		return fmt.Errorf("append status=%d body=%s", resp.StatusCode, msg)
	}
	return nil
}

// runFork times `n` sequential forks of one parent session seeded with
// `seed` events, and prints latency percentiles.
func runFork(client *http.Client, base string, seed, n int) {
	parent, err := openSession(client, base, "t-fork", "a-fork", "parent")
	if err != nil {
		log.Fatalf("open parent: %v", err)
	}
	for i := 0; i < seed; i++ {
		if err := appendEvent(client, base, parent, i); err != nil {
			log.Fatalf("seed append %d: %v", i, err)
		}
	}
	url := fmt.Sprintf("%s/api/tenants/%s/sessions/%s/fork", base, parent.TenantID, parent.SessionID)
	lats := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		body, _ := json.Marshal(map[string]any{"alias": fmt.Sprintf("child-%d", i)})
		t0 := time.Now()
		resp, err := client.Post(url, "application/json", bytes.NewReader(body))
		elapsed := time.Since(t0)
		if err != nil {
			log.Fatalf("fork %d: %v", i, err)
		}
		if resp.StatusCode >= 400 {
			msg, _ := readN(resp.Body, 256)
			log.Fatalf("fork %d status=%d body=%s", i, resp.StatusCode, msg)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		lats = append(lats, elapsed)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration { return lats[int(float64(len(lats)-1)*p)] }
	var mean time.Duration
	for _, d := range lats {
		mean += d
	}
	mean /= time.Duration(len(lats))
	out := map[string]any{
		"mode":  "fork",
		"seed":  seed,
		"forks": n,
		"latency_ms": map[string]float64{
			"mean": float64(mean.Microseconds()) / 1000,
			"p50":  float64(pct(0.50).Microseconds()) / 1000,
			"p90":  float64(pct(0.90).Microseconds()) / 1000,
			"p99":  float64(pct(0.99).Microseconds()) / 1000,
			"max":  float64(pct(1.00).Microseconds()) / 1000,
		},
	}
	buf, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(buf))
}

func main() {
	var (
		base        string
		mode        string
		concurrency int
		tenants     int
		durationS   int
		fetchN      int
		payloadB    int
		forkSeed    int
		forkN       int
	)
	flag.StringVar(&base, "base", "http://127.0.0.1:7878", "REST base URL")
	flag.StringVar(&mode, "mode", "append", "benchmark mode: append | fork")
	flag.IntVar(&concurrency, "c", 16, "number of concurrent workers")
	flag.IntVar(&tenants, "tenants", 8, "number of distinct tenants")
	flag.IntVar(&durationS, "duration", 30, "run duration in seconds")
	flag.IntVar(&fetchN, "fetch", 0, "fetch_last_n per request")
	flag.IntVar(&payloadB, "payload", 256, "approximate payload bytes per event")
	flag.IntVar(&forkSeed, "fork-seed", 20, "events seeded on the parent before forking (fork mode)")
	flag.IntVar(&forkN, "fork-n", 100, "number of sequential forks to time (fork mode)")
	flag.Parse()

	payload := make([]byte, payloadB)
	for i := range payload {
		payload[i] = 'x'
	}
	text := string(payload)

	transport := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		MaxConnsPerHost:     concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: transport}

	if mode == "fork" {
		runFork(client, base, forkSeed, forkN)
		return
	}

	// Setup: open one session per worker and capture canonical IDs.
	sessions := make([]session, concurrency)
	for w := 0; w < concurrency; w++ {
		workspace := fmt.Sprintf("t-perf-%d", w%tenants)
		s, err := openSession(client, base, workspace, "a-perf", fmt.Sprintf("s-w%d", w))
		if err != nil {
			log.Fatalf("open session for worker %d: %v", w, err)
		}
		sessions[w] = s
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationS)*time.Second)
	defer cancel()

	var (
		ok         atomic.Uint64
		errs       atomic.Uint64
		sampleOnce sync.Once
		sampleErr  string
		latsMu     sync.Mutex
		lats       = make([]time.Duration, 0, 1<<16)
	)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			s := sessions[worker]
			url := fmt.Sprintf("%s/api/tenants/%s/sessions/%s/append-and-fetch", base, s.TenantID, s.SessionID)
			seq := 0
			for {
				if ctx.Err() != nil {
					return
				}
				body, _ := json.Marshal(map[string]any{
					"agent_id": s.AgentID,
					"kind":     "message",
					"payload":  map[string]any{"text": text, "seq": seq},
					"last":     fetchN,
				})
				t0 := time.Now()
				resp, err := client.Post(url, "application/json", bytes.NewReader(body))
				elapsed := time.Since(t0)
				if err != nil {
					errs.Add(1)
					sampleOnce.Do(func() { sampleErr = fmt.Sprintf("transport: %v", err) })
					continue
				}
				if resp.StatusCode >= 400 {
					errs.Add(1)
					msg, _ := readN(resp.Body, 256)
					sampleOnce.Do(func() {
						sampleErr = fmt.Sprintf("status=%d body=%s", resp.StatusCode, msg)
					})
				} else {
					ok.Add(1)
				}
				// Must drain body before close for keep-alive reuse.
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				latsMu.Lock()
				lats = append(lats, elapsed)
				latsMu.Unlock()
				seq++
			}
		}(w)
	}
	wg.Wait()
	total := time.Since(start).Seconds()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		i := int(float64(len(lats)-1) * p)
		return lats[i]
	}
	var mean time.Duration
	for _, d := range lats {
		mean += d
	}
	if len(lats) > 0 {
		mean /= time.Duration(len(lats))
	}
	successes := ok.Load()

	out := map[string]any{
		"base":         base,
		"concurrency":  concurrency,
		"tenants":      tenants,
		"duration_s":   total,
		"payload_b":    payloadB,
		"fetch_n":      fetchN,
		"ok":           successes,
		"errors":       errs.Load(),
		"rps":          float64(successes) / total,
		"sample_error": sampleErr,
		"latency_ms": map[string]float64{
			"mean": float64(mean.Microseconds()) / 1000,
			"p50":  float64(pct(0.50).Microseconds()) / 1000,
			"p90":  float64(pct(0.90).Microseconds()) / 1000,
			"p99":  float64(pct(0.99).Microseconds()) / 1000,
			"max":  float64(pct(1.00).Microseconds()) / 1000,
		},
	}
	buf, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(buf))
	log.Printf("ok=%d err=%d rps=%.1f p50=%.2fms p99=%.2fms",
		successes, errs.Load(), float64(successes)/total,
		float64(pct(0.50).Microseconds())/1000,
		float64(pct(0.99).Microseconds())/1000)
}
