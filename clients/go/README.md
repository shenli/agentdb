# agentdb (Go)

Go client for [AgentDB](https://github.com/shenli/agentdb) — agent-native
operational database. Stdlib only; no third-party dependencies.

## Install

```bash
go get github.com/shenli/agentdb/clients/go/agentdb
```

(Module publishing lands once AgentDB cuts a v0.1.0 tag; in the
meantime `go get` from a fork or use a local replace directive.)

## Five-line hello world

Start the server in dev mode:

```bash
cargo run -p agentdb-server -- serve --dev
```

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/shenli/agentdb/clients/go/agentdb"
)

func main() {
    client := agentdb.New("http://127.0.0.1:7878",
        agentdb.WithBasicAuth("alice", "hunter2"))

    sess := client.Session("agent-1")
    res, err := sess.Append(context.Background(),
        map[string]any{"role": "user", "text": "hello"},
        agentdb.AppendOptions{FetchLastN: 5},
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("seq:", res.Seq, "recent:", len(res.Recent))
}
```

Tenant is derived server-side from the Basic-auth user's record —
there is no client-side tenant argument.

## Auth model

| Route type | Auth option |
|---|---|
| Session / subscribe | `WithBasicAuth(user, password)` |
| Channel (broadcast / subscribe) | `WithBearerToken(token)` or `WithTokenProvider(fn)` |

Both can be supplied together — Basic auth covers session routes;
Bearer covers channel routes.

## Fork

```go
parent := client.Session("agent-1")
parent.Append(ctx, map[string]any{"role": "user", "text": "hello"})
child, err := parent.Fork(ctx, "agent-1-branch-a")
if err != nil { /* handle */ }
child.Append(ctx, map[string]any{"role": "tool", "text": "branch off"})
```

The child sees a frozen snapshot of the parent's events at fork
time, MVCC-backed.

## Errors

Every server-surfaced failure is a typed struct satisfying the
`agentdb.Error` interface. Use `errors.As` to extract a
specific kind:

| HTTP | Type            | Typical cause                                 |
|------|-----------------|-----------------------------------------------|
| 400  | `*BadRequest`   | malformed body, tenant resolution failed      |
| 401  | `*Unauthorized` | missing/invalid `Authorization`               |
| 403  | `*Forbidden`    | authed user can't act on this tenant/session  |
| 404  | `*NotFound`     | tenant or session doesn't exist               |
| 409  | `*Conflict`     | resource already exists / state collision     |
| 421  | `*Misdirected`  | multi-node routing drift                      |
| 429  | `*RateLimited`  | quota exhausted; carries `RetryAfterMs`       |
| 5xx  | `*ServerError`  | forwarding failure, fault injection, draining |

```go
import "errors"

if _, err := sess.Append(ctx, payload); err != nil {
    var rl *agentdb.RateLimited
    if errors.As(err, &rl) {
        time.Sleep(time.Duration(rl.RetryAfterMs) * time.Millisecond)
        // retry
    } else {
        return fmt.Errorf("agentdb append: %w", err)
    }
}
```

## Why no `Session.Recent(n)` for pure reads?

`Append + FetchLastN` is the canonical hot path. Every agent
turn writes *something* and wants the recent-N for context;
combining the two saves a round trip.

For genuine read-only queries (dashboards, replays, audit), use
the standard MongoDB Go driver against the wire port:

```go
import "go.mongodb.org/mongo-driver/v2/mongo"
import "go.mongodb.org/mongo-driver/v2/mongo/options"

mc, _ := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:7879"))
events, _ := mc.Database("demo").
    Collection("events").
    Find(ctx, bson.M{"session_id": "agent-1"})
```

## Testing the SDK

The smoke test spawns `agentdb serve --dev` as a subprocess.
Build the server first:

```bash
cargo build -p agentdb-server
go test ./...
```

The test is skipped automatically if the server binary doesn't exist.
