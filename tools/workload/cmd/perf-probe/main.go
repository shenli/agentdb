// perf-probe fires concurrent append_and_fetch requests at agentdb-server
// and reports throughput + latency percentiles. Intended for manual perf
// runs, not for CI.
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

func main() {
	var (
		base        string
		concurrency int
		tenants     int
		durationS   int
		fetchN      int
		payloadB    int
	)
	flag.StringVar(&base, "base", "http://127.0.0.1:7878", "REST base URL")
	flag.IntVar(&concurrency, "c", 16, "number of concurrent workers")
	flag.IntVar(&tenants, "tenants", 8, "number of distinct tenants")
	flag.IntVar(&durationS, "duration", 30, "run duration in seconds")
	flag.IntVar(&fetchN, "fetch", 0, "fetch_last_n per request")
	flag.IntVar(&payloadB, "payload", 256, "approximate payload bytes per event")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationS)*time.Second)
	defer cancel()

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
			seq := 0
			for {
				if ctx.Err() != nil {
					return
				}
				tenant := fmt.Sprintf("t-perf-%d", worker%tenants)
				session := fmt.Sprintf("s-w%d", worker)
				body, _ := json.Marshal(map[string]any{
					"event": map[string]any{
						"kind": "message",
						"text": text,
						"seq":  seq,
					},
					"fetch_last_n": fetchN,
				})
				url := fmt.Sprintf("%s/api/tenants/%s/sessions/%s/append_and_fetch", base, tenant, session)
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
					body, _ := readN(resp.Body, 256)
					sampleOnce.Do(func() {
						sampleErr = fmt.Sprintf("status=%d body=%s", resp.StatusCode, body)
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
		"base":        base,
		"concurrency": concurrency,
		"tenants":     tenants,
		"duration_s":  total,
		"payload_b":   payloadB,
		"fetch_n":     fetchN,
		"ok":          successes,
		"errors":      errs.Load(),
		"rps":         float64(successes) / total,
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
