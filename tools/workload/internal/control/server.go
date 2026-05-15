package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/shenli/agentdb/tools/workload/internal/client"
)

type Server struct {
	Addr string
	REST *client.REST
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/demo/burst", s.handleBurst)
	mux.HandleFunc("/demo/fork", s.handleFork)
	mux.HandleFunc("/demo/cold", s.handleCold)
	mux.HandleFunc("/demo/hybrid", s.handleHybrid)
	srv := &http.Server{Addr: s.Addr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	log.Printf("control server listening on %s", s.Addr)
	return srv.ListenAndServe()
}

func (s *Server) handleBurst(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	if tenant == "" {
		http.Error(w, "missing ?tenant", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		_ = s.REST.Append(ctx, tenant, "burst-"+tenant, map[string]any{
			"kind": "burst",
			"i":    i,
		}, 0)
	}
	writeOK(w)
}

func (s *Server) handleFork(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	parent := r.URL.Query().Get("session")
	if tenant == "" || parent == "" {
		http.Error(w, "missing ?tenant or ?session", 400)
		return
	}
	child := fmt.Sprintf("%s-fork-%d", parent, time.Now().Unix())
	if err := s.REST.Fork(r.Context(), tenant, parent, child); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeOK(w)
}

func (s *Server) handleCold(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant")
	session := r.URL.Query().Get("session")
	if tenant == "" || session == "" {
		http.Error(w, "missing ?tenant or ?session", 400)
		return
	}
	body, _ := json.Marshal(map[string]string{
		"tenant_id":  tenant,
		"session_id": session,
		"target":     "cold",
	})
	url := fmt.Sprintf("%s/api/admin/tier/force", s.REST.Base)
	req, err := http.NewRequestWithContext(r.Context(), "POST", url, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.REST.HTTP.Do(req)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = resp.Body.Close()
	writeOK(w)
}

func (s *Server) handleHybrid(w http.ResponseWriter, r *http.Request) {
	writeOK(w)
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}
