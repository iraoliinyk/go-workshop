# ENV.md — Configuration via `.env` (caarlos0/env + godotenv)

## Context

`ch-2` currently loads config with `kelseyhightower/envconfig` **inside** `consumer.Start`
(`internal/consumer/consumer.go`), into a tag-less `Specification` struct, and calls
`log.Fatal` on error. The server port `:7001` is hardcoded twice in `cmd/server/main.go`,
the `Accept` header is hardcoded, and a `WikiURL` constant is left commented out.

**Why the values come back zero/empty:** `envconfig` (and `caarlos0/env`) read the **OS process
environment** via `os.Getenv` — they do **not** parse `.env` files. Nothing in the code loads
`.env`, and the VSCode debug config has no `envFile`, so `WIKI_*` vars are unset at runtime and
the struct is left at its zero values (`0`, `""`). `envconfig.Process` does not error on missing
vars unless a field is marked `required`, which is why it fails silently.

**Goal:** one source of truth for config, loaded once at startup from `.env` (via `godotenv`)
and parsed with `caarlos0/env`, then **injected** into the HTTP server and the consumer. This
applies SRP (config isolated from business logic), DIP/ISP (consumer receives only what it needs),
and DRY (port/defaults defined once).

---

## Target architecture

```
cmd/server/main.go        -> loads config ONCE, owns lifecycle, wires dependencies
internal/config/config.go -> Config struct + Load() (godotenv + caarlos0/env). NEW package.
internal/consumer/consumer.go -> receives a small consumer.Config value; no env access, no log.Fatal
.env                      -> WIKI_* keys (local dev only; gitignored)
.env.example              -> committed template
```

Dependency direction: `main -> config`, `main -> consumer`. The `consumer` package does **not**
import `config` — it declares its own `consumer.Config` with exactly the fields it needs
(Interface Segregation / Dependency Inversion). `main` maps `config.Config` → `consumer.Config`.

---

## Step 1 — Dependencies (`go.mod`)

From `ch-2/`:

```bash
go get github.com/caarlos0/env/v11
go get github.com/joho/godotenv
go mod tidy   # removes github.com/kelseyhightower/envconfig once it is no longer imported
```

Expected `require` block afterward:

```go
require (
	github.com/caarlos0/env/v11 v11.x.x
	github.com/joho/godotenv v1.x.x
)
```

---

## Step 2 — New file `internal/config/config.go`

A single place that produces a validated `Config` from `.env` + OS environment. OS env vars
always override `.env`. `godotenv.Load()` is guarded so a missing `.env` is not fatal
(defaults + real env still work in prod/CI).

```go
package config

import (
	"log"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config is the single source of truth for runtime configuration.
// Field defaults live here (envDefault) so the app runs with zero setup.
type Config struct {
	Port      int    `env:"WIKI_PORT"       envDefault:"7001"`
	URL       string `env:"WIKI_URL"        envDefault:"https://stream.wikimedia.org/v2/stream/recentchange"`
	UserAgent string `env:"WIKI_USER_AGENT" envDefault:"wiki-stream-consumer/1.0 (https://github.com/you/wiki-stream)"`
	Accept    string `env:"WIKI_ACCEPT"     envDefault:"application/json"`
}

// Load reads .env (if present) into the process environment, then parses env vars
// into Config. A missing .env is not an error — real environment variables and
// defaults take over. Call this exactly once, at startup.
func Load() (Config, error) {
	// Best-effort: .env is a local-dev convenience, not required.
	if err := godotenv.Load(); err != nil {
		log.Printf("config: no .env loaded (%v); using environment and defaults", err)
	}
	return env.ParseAs[Config]()
}
```

Notes:
- Explicit `env:"WIKI_USER_AGENT"` tag avoids the camelCase gotcha (the old code's
  `UserAgent` field mapped to `WIKI_USERAGENT`, not `WIKI_USER_AGENT`).
- `env.ParseAs[Config]()` is the generic form (caarlos0/env v9+). Equivalent:
  `var c Config; err := env.Parse(&c)`.
- To make a field mandatory instead of defaulted, use `env:"WIKI_URL,required"`.

---

## Step 3 — `internal/consumer/consumer.go`

Remove config loading from the consumer. Accept an injected value; return errors (no
`log.Fatal`). Delete the dead commented `WikiURL` constant.

**3a. Remove** the `github.com/kelseyhightower/envconfig` import, the `Specification` struct,
the `envconfig.Process(...)` call, the `log.Fatal`, the debug `fmt.Printf`, and the commented
`// const WikiURL = ...` line.

**3b. Add** a consumer-local config type (only the fields this package needs — ISP):

```go
// Config holds exactly what the consumer needs. main maps config.Config into this,
// so the consumer package stays independent of the global config package.
type Config struct {
	URL       string
	UserAgent string
	Accept    string
}
```

**3c. Change the signature** and use the injected values:

```go
func Start(ctx context.Context, cfg Config, rec Recorder) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
	if err != nil {
		return &apperrors.ConnectionError{Err: fmt.Errorf("build request: %w", err)}
	}

	req.Header.Set("User-Agent", cfg.UserAgent)
	req.Header.Set("Accept", cfg.Accept) // was hardcoded "application/json"

	// ... rest unchanged ...
}
```

Result: the consumer no longer touches the environment, no longer terminates the process,
and is trivially testable by passing a `consumer.Config` literal.

---

## Step 4 — `cmd/server/main.go`

Load config once, own the failure, drive the port from config (single source — DRY), and inject
into the consumer.

**4a.** At the top of `main()`:

```go
cfg, err := config.Load()
if err != nil {
	log.Fatalf("config: %v", err) // main owns process lifecycle; fatal is acceptable here
}
```

Add the import `"ch-2/internal/config"`.

**4b.** Inject into the consumer goroutine:

```go
go func() {
	consumerErr <- consumer.Start(ctx, consumer.Config{
		URL:       cfg.URL,
		UserAgent: cfg.UserAgent,
		Accept:    cfg.Accept,
	}, st)
}()
```

**4c.** Drive the server address from config, computed once and reused (removes the two
hardcoded `:7001` literals at lines 42 and 86):

```go
addr := fmt.Sprintf(":%d", cfg.Port)

server := &http.Server{
	Addr:         addr,
	Handler:      mux,
	ReadTimeout:  5 * time.Second,
	WriteTimeout: 10 * time.Second,
}
// ...
log.Printf("listening on %s", addr)
```

Add `"fmt"` to the imports if not already present.

---

## Step 5 — `.env` and `.env.example`

`.env` (local, gitignored) — keys must match the `env:` tags exactly:

```dotenv
WIKI_PORT=7001
WIKI_URL=https://stream.wikimedia.org/v2/stream/recentchange
WIKI_USER_AGENT=wiki-stream-consumer/1.0 (https://github.com/you/wiki-stream)
WIKI_ACCEPT=application/json
```

Commit a template `.env.example` with the same keys (placeholder/example values) so teammates
know what to set.

Add `.env` to `.gitignore` (do not commit secrets/local overrides):

```gitignore
# local environment
.env
```

---

## Step 6 — VSCode: no launch.json change needed

The `Run Wiki Stream Consumer` config already sets `"cwd": "${workspaceFolder}"` (= `ch-2/`),
and `godotenv.Load()` reads `./.env` relative to cwd — so debugging picks up `.env` automatically.
(If you preferred the no-dependency route, `"envFile": "${workspaceFolder}/.env"` in the config
would work too, but with godotenv it is redundant.)

---

## SOLID / Clean Code / DRY mapping

- **SRP** — config reading moves out of `consumer.Start` into `internal/config`; `main` owns
  wiring and lifecycle. Each unit has one reason to change.
- **DIP** — `consumer` depends on an injected `consumer.Config` value, not on the environment or
  the global config package. Easy to substitute in tests.
- **ISP** — `consumer.Config` exposes only `URL/UserAgent/Accept`; the consumer never sees `Port`.
- **DRY** — port and every default defined once (`envDefault` tags); `addr` computed once and
  reused; removed duplicate `:7001` literals and the dead commented `WikiURL` constant.
- **Clean Code** — no `log.Fatal` buried in a library function; explicit env-var names via tags
  (no camelCase surprises); dead code deleted.

---

## Verification (after implementing)

1. `go mod tidy && go build ./...` — compiles; `envconfig` gone from `go.mod`.
2. Debug: set a breakpoint just after `config.Load()` in `main.go` (or at the top of
   `consumer.Start`), launch via the green ▷ in **Run and Debug** (not Code Runner / not F5),
   and inspect the values — `Port=7001`, `URL`, `UserAgent`, `Accept` are populated (not zero).
3. `curl localhost:7001/stats` returns JSON while running.
4. **Prove config drives the server (DRY):** set `WIKI_PORT=8080` in `.env`, re-run, confirm the
   log says `listening on :8080` and `curl localhost:8080/stats` works.
5. **Prove defaults + guard:** rename `.env` away, run `go run ./cmd/server` — it still starts on
   `:7001` using `envDefault`, logging the "no .env loaded" notice (not a crash).
6. `go test ./...` — consumer tests pass by passing a `consumer.Config{}` literal (no env needed).
