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
