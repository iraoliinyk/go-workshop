package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"ch-2/internal/httpapi"
	statsmodels "ch-2/internal/stats/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestStatus_ReturnsOK(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := httpapi.New(NewMocksnapshotter(ctrl))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)

	api.Router().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
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

	ctrl := gomock.NewController(t)
	stats := NewMocksnapshotter(ctrl)
	stats.EXPECT().Snapshot().Return(want).Times(1)

	api := httpapi.New(stats)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)

	api.Router().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var got statsmodels.StatsSnapshot
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, want, got)
}
