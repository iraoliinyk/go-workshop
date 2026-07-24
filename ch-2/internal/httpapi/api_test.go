package httpapi_test

import (
	"ch-2/internal/httpapi"
	statsmodels "ch-2/internal/stats/models"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubStats is a hand-written test double for the Snapshotter interface;
// no real *stats.Stats needed.
type stubStats struct{ snap statsmodels.StatsSnapshot }

func (s stubStats) Snapshot() statsmodels.StatsSnapshot { return s.snap }

func TestStatus_ReturnsOK(t *testing.T) {
	api := httpapi.New(stubStats{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)

	api.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
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

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var got statsmodels.StatsSnapshot
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, want, got)
}
