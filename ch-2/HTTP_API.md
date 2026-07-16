# HTTP_API.md — Extracting the HTTP handlers out of `main.go`

## 1. Architectural assessment of `cmd/server/main.go`

`main()` currently carries **eight** responsibilities: context lifecycle, config load,
consumer startup, **route registration + inline handler logic**, server construction, signal
handling, graceful shutdown, and consumer-error handling.

Most of those are legitimate for a `main` — it is the **composition root**, and wiring
dependencies together is exactly its job. The part that does *not* belong there is the **HTTP
transport logic**: the two `HandleFunc` closures contain response behaviour (status, headers,
JSON encoding), and that is a separate concern from process bootstrap.

### Clean Code / SOLID findings

| Principle | Finding | Where |
|---|---|---|
| **SRP** | `main()` both wires the process *and* implements HTTP handlers. Two reasons to change. | `main.go:42-50` |
| **DRY** | `w.Header().Set("Content-Type", "application/json")` + `json.NewEncoder(w).Encode(...)` is duplicated in both handlers. | `main.go:44-45`, `48-49` |
| **DIP** | Handlers close over the concrete `*stats.Stats`. They only need "something that returns a snapshot" — an interface — not the concrete type. | `main.go:47-50` |
| **OCP** | Adding/altering a route means editing `main()`. Routing should be extendable without touching the composition root. | `main.go:42-50` |
| **Error handling** | `json.NewEncoder(w).Encode(...)` return value is ignored in both handlers. | `main.go:45`, `49` |
| **Testability** | Handlers defined as anonymous closures inside `main()` cannot be unit-tested without booting the whole process. | `main.go:43-50` |

**Verdict:** the structure is reasonable for a small program, but the HTTP layer is tangled into
`main`. Extracting it is worthwhile and idiomatic — it fixes SRP, DRY, DIP, OCP and makes the
handlers unit-testable.

**Yes — `GET /status` and `GET /stats` can and should be separated into their own file.**

---

## 2. Recommended design: an `internal/httpapi` package

Create a dedicated **transport layer** package. This is the standard Go layering:
`main` (composition) → `httpapi` (transport) → `stats` (domain).

```
cmd/server/main.go            -> composition root: wires deps, owns lifecycle
internal/httpapi/api.go       -> API struct, router, handlers, writeJSON helper   (NEW)
internal/httpapi/api_test.go  -> handler unit tests via httptest                  (NEW)
internal/stats/…              -> unchanged domain logic
```

**Why a package (`internal/httpapi`) and not just a second file in `package main`?**
- A file in `package main` *can* be tested (via a `package main` test), but the transport layer
  is not reusable and stays coupled to the binary. A separate package gives a clean import
  boundary, real unit tests with `httptest`, and room to grow (middleware, more routes).
- `internal/` keeps it private to this module — no external API surface is exposed.

**Why a struct (`API`) with method handlers, not free functions?**
- Handlers need a dependency (`Snapshotter`). A struct holds it once and every handler method
  reads it — this scales cleanly as more dependencies (logger, clock, more stores) are added,
  without turning each handler into a closure factory. This is the idiomatic "handler container"
  pattern.

**Why the `Snapshotter` interface (DIP)?**
- The API only needs to *read* a stats snapshot. Depending on a one-method interface instead of
  `*stats.Stats` inverts the dependency, documents the exact requirement (ISP), and lets tests
  pass a trivial stub. `*stats.Stats` satisfies it automatically — no change to `stats`.

---

## 3. Step-by-step instructions

### Step 1 — Create `internal/httpapi/api.go`

```go
package httpapi

import (
	statsmodels "ch-2/internal/stats/models"
	"encoding/json"
	"log"
	"net/http"
)

// Snapshotter is the read-only view of stats the HTTP API needs.
// *stats.Stats satisfies it (its Snapshot() returns statsmodels.StatsSnapshot,
// via the type alias in stats.go), so no change to the stats package is required.
type Snapshotter interface {
	Snapshot() statsmodels.StatsSnapshot
}

// API holds the dependencies shared by all HTTP handlers.
type API struct {
	stats Snapshotter
}

// New constructs an API with its dependencies injected.
func New(stats Snapshotter) *API {
	return &API{stats: stats}
}

// Router registers every route and returns the handler to mount on http.Server.
// Adding a route happens here, never in main().
func (a *API) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", a.handleStatus)
	mux.HandleFunc("GET /stats", a.handleStats)
	return mux
}

// handleStatus is a liveness probe — no dependencies.
func (a *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStats returns the current stats snapshot.
func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.stats.Snapshot())
}

// writeJSON centralises the response convention (header + status + body),
// removing the duplication that was in the two inline handlers.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("httpapi: encode response: %v", err) // was silently ignored before
	}
}
```

### Step 2 — Slim down `cmd/server/main.go`

Replace the inline `mux` block (`main.go:42-50`) with two lines that build the router from the
new package:

```go
// was: mux := http.NewServeMux(); mux.HandleFunc("GET /status", ...); mux.HandleFunc("GET /stats", ...)
api := httpapi.New(st)

addr := fmt.Sprintf(":%d", cfg.Port)
server := &http.Server{
	Addr:         addr,
	Handler:      api.Router(),
	ReadTimeout:  5 * time.Second,
	WriteTimeout: 10 * time.Second,
}
```

Then update the imports in `main.go`:
- **Add** `"ch-2/internal/httpapi"`.
- **Remove** `"encoding/json"` and `"net/http"` **only if** they are no longer used elsewhere in
  the file. (`net/http` is still used for `http.Server` and `http.ErrServerClosed`, so it stays;
  `encoding/json` is no longer used once the handlers move out, so it can be removed.)

`main()` keeps its single responsibility — composition and lifecycle — and no longer contains any
HTTP handler logic.

### Step 3 — Verify

```bash
go build ./...
go vet ./...
go test ./...
```

---

## 4. Test coverage (existing behaviour only)

The extraction makes the handlers unit-testable with `net/http/httptest` — no server, no network,
no goroutines. These tests assert **only the behaviour that already exists** (status code,
`Content-Type`, and JSON body). No new endpoints or logic are introduced to inflate coverage.

Create `internal/httpapi/api_test.go`:

```go
package httpapi_test

import (
	"ch-2/internal/httpapi"
	statsmodels "ch-2/internal/stats/models"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// stubStats is a hand-written test double for the Snapshotter interface —
// this is why the DIP interface matters: no real *stats.Stats needed.
type stubStats struct{ snap statsmodels.StatsSnapshot }

func (s stubStats) Snapshot() statsmodels.StatsSnapshot { return s.snap }

func TestStatus_ReturnsOK(t *testing.T) {
	api := httpapi.New(stubStats{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)

	api.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: want application/json, got %q", ct)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf(`body: want status "ok", got %q`, body["status"])
	}
}

func TestStats_ReturnsSnapshot(t *testing.T) {
	want := statsmodels.StatsSnapshot{
		TotalMessages: 7,
		DistinctUsers: 3,
		BotEdits:      2,
		HumanEdits:    5,
		ByServerURL:   map[string]int64{"https://en.wikipedia.org": 4},
	}
	api := httpapi.New(stubStats{snap: want})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)

	api.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: want application/json, got %q", ct)
	}
	var got statsmodels.StatsSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got.TotalMessages != want.TotalMessages ||
		got.DistinctUsers != want.DistinctUsers ||
		got.BotEdits != want.BotEdits ||
		got.HumanEdits != want.HumanEdits ||
		got.ByServerURL["https://en.wikipedia.org"] != 4 {
		t.Errorf("snapshot mismatch: want %+v, got %+v", want, got)
	}
}
```

Optional (stdlib behaviour, not our logic — include only if you want the guarantee documented):
`ServeMux` with a `GET /stats` pattern returns **405 Method Not Allowed** for `POST /stats`
automatically. A one-line test can pin that, but it verifies the standard library, not code we
wrote.

**What is intentionally NOT tested:** graceful shutdown, signal handling, and the consumer
goroutine live in `main()` and are integration concerns; unit tests here cover only the two
handlers' request/response contract. No production code is added merely to raise the number.

---

## 5. Summary of choices

- **New `internal/httpapi` package** — separates transport from composition (SRP), gives a clean,
  private, testable boundary, and is the idiomatic Go layering.
- **`API` struct + method handlers** — holds injected deps once; scales without closure factories.
- **`Snapshotter` interface** — DIP/ISP: the API depends on the one method it uses, not on
  `*stats.Stats`; enables a trivial test stub. `stats` needs no change.
- **`writeJSON` helper** — removes the duplicated header/encode lines (DRY) and stops silently
  discarding the encode error.
- **`main()` reduced to wiring** — `api := httpapi.New(st)` and `Handler: api.Router()`; the HTTP
  handler logic leaves the composition root entirely.

### Optional follow-up (not required for this task)

Extract the server setup + graceful shutdown into a `func run(ctx, cfg) error` and keep `main()`
as `if err := run(...); err != nil { log.Fatal(err) }`. This is the well-known Go
"`main` calls `run`" idiom and makes the lifecycle testable too — but it is a separate refactor
from the handler extraction you asked about.
