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
		log.Printf("httpapi: encode response: %v", err)
	}
}
