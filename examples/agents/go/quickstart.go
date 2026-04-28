// Quickstart agent — three turns of "thought-act-observe" with
// working memory backed by AgentDB.
//
// Each turn does the canonical agent flow:
//
//  1. Pull the recent context (last N events).
//  2. Decide an action based on that context.
//  3. Append the new thought / action / observation so the next
//     turn — or a forked branch — sees it.
//
// Steps 1 + 3 happen in one round trip via Append + FetchLastN.
// That's the AgentDB hot-path primitive.
//
// Run me:
//
//	# In one terminal:
//	cargo run -p agentdb-server -- serve --dev
//
//	# In another:
//	cd examples/agents/go && go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/shenli/agentdb/clients/go/agentdb"
)

const sessionID = "quickstart-agent-go"

type chatPayload struct {
	Role        string `json:"role"`
	Text        string `json:"text,omitempty"`
	Name        string `json:"name,omitempty"`
	Result      *int   `json:"result,omitempty"`
	ContextSize *int   `json:"context_size,omitempty"`
}

func thoughtForTurn(turn, contextSize int) chatPayload {
	switch turn {
	case 0:
		return chatPayload{Role: "user", Text: "What's 2 + 2?"}
	case 1:
		return chatPayload{Role: "assistant", Text: "Looking back, the user asked '2 + 2'. The answer is 4."}
	default:
		four := 4
		return chatPayload{Role: "tool", Name: "calculator", Result: &four, ContextSize: &contextSize}
	}
}

func main() {
	url := envOr("AGENTDB_URL", "http://127.0.0.1:7878")
	user := envOr("AGENTDB_USER", "demo")
	password := envOr("AGENTDB_PASSWORD", "demo-secret")
	ctx := context.Background()

	client := agentdb.New(url, agentdb.WithBasicAuth(user, password))
	if err := client.HealthReady(ctx); err != nil {
		fmt.Fprintf(os.Stderr,
			"AgentDB not ready at %s. Start it with:\n"+
				"    cargo run -p agentdb-server -- serve --dev\n"+
				"err: %v\n", url, err)
		os.Exit(1)
	}

	sess := client.Session(sessionID)
	fmt.Printf("# Session: id=%q\n\n", sess.ID())

	for turn := 0; turn < 3; turn++ {
		ev := thoughtForTurn(turn, 0)
		res, err := sess.Append(ctx, ev, agentdb.AppendOptions{FetchLastN: 5})
		if err != nil {
			fmt.Fprintln(os.Stderr, "append failed:", err)
			os.Exit(1)
		}
		fmt.Printf("--- turn %d (seq=%d) ---\n", turn, res.Seq)
		printRecent(res.Recent)
		fmt.Println()
	}

	// Fork: branch the agent off into an alternate timeline.
	fmt.Println("# Forking into an alternate timeline...")
	branch, err := sess.Fork(ctx, sessionID+"-branch-a")
	if err != nil {
		fmt.Fprintln(os.Stderr, "fork failed:", err)
		os.Exit(1)
	}
	branchEv := chatPayload{Role: "user", Text: "What if it were 3 + 3 instead?"}
	branchRes, err := branch.Append(ctx, branchEv, agentdb.AppendOptions{FetchLastN: 10})
	if err != nil {
		fmt.Fprintln(os.Stderr, "branch append failed:", err)
		os.Exit(1)
	}
	fmt.Printf("--- branch turn 0 (seq=%d) ---\n", branchRes.Seq)
	printRecent(branchRes.Recent)
}

func printRecent(events []agentdb.Event) {
	for _, ev := range events {
		var p chatPayload
		_ = json.Unmarshal(ev.Payload, &p)
		text := p.Text
		if text == "" && p.Result != nil {
			text = fmt.Sprintf("%d", *p.Result)
		}
		if text == "" {
			text = "(no text)"
		}
		fmt.Printf("  [%3d] %9s: %s\n", ev.Seq, p.Role, strings.TrimSpace(text))
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
