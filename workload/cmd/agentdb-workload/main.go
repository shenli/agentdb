package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/shenli/agentdb/workload/internal/client"
	"github.com/shenli/agentdb/workload/internal/persona"

	"gopkg.in/yaml.v3"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path to workload config YAML")
	flag.Parse()

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("loaded %d personas", len(cfg.Personas))

	ctx, cancel := context.WithCancel(context.Background())
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigC
		log.Printf("shutting down")
		cancel()
	}()

	rest := &client.REST{Base: cfg.RESTBase, HTTP: &http.Client{Timeout: 5 * time.Second}}
	mongoCli, err := client.NewMongo(ctx, cfg.MongoURI)
	if err != nil {
		log.Printf("mongo connect: %v (continuing without search)", err)
	}

	var wg sync.WaitGroup
	for i := range cfg.Personas {
		p := &cfg.Personas[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			runTenant(ctx, p, rest, mongoCli)
		}()
	}
	wg.Wait()
}

func runTenant(ctx context.Context, p *persona.Persona, rest *client.REST, mcli *client.Mongo) {
	ticker := time.NewTicker(time.Duration(60.0/p.TicketRatePerMin*1000) * time.Millisecond)
	defer ticker.Stop()
	sem := make(chan struct{}, p.ConcurrentSessionsCap)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case sem <- struct{}{}:
			default:
				continue
			}
			go func() {
				defer func() { <-sem }()
				sr := persona.SessionRun{
					SessionID: persona.NewSessionID(p),
					Persona:   p,
					Appender:  rest,
					Searcher:  asSearcher(mcli),
				}
				sr.Run(ctx, rand.New(rand.NewSource(time.Now().UnixNano())))
			}()
		}
	}
}

func asSearcher(m *client.Mongo) persona.Searcher {
	if m == nil {
		return nil
	}
	return m
}

func loadConfig(path string) (*persona.Config, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c persona.Config
	if err := yaml.Unmarshal(buf, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
