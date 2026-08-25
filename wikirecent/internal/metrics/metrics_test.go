package metrics_test

import (
	"testing"

	"wikirecent/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestPublishCounters_IncrementTheCountersTheyWereGiven(t *testing.T) {
	counters := metrics.NewEvents(prometheus.NewRegistry())
	obs := metrics.NewPublishCounters(counters.PersistedToRedpanda, counters.Failed)

	obs.Published()

	assert.Equal(t, 1.0, testutil.ToFloat64(counters.PersistedToRedpanda))
	assert.Zero(t, testutil.ToFloat64(counters.Failed), "Published must not touch the failure counter")

	obs.PublishFailed()

	assert.Equal(t, 1.0, testutil.ToFloat64(counters.PersistedToRedpanda), "PublishFailed must not touch the success counter")
	assert.Equal(t, 1.0, testutil.ToFloat64(counters.Failed))
}

func TestBatchCounters_IncrementTheCountersTheyWereGiven(t *testing.T) {
	counters := metrics.NewEvents(prometheus.NewRegistry())
	obs := metrics.NewBatchCounters(counters.ConsumedFromRedpanda, counters.Processed, counters.Failed)

	obs.Consumed(3)
	obs.Processed()
	obs.Failed()

	assert.Equal(t, 3.0, testutil.ToFloat64(counters.ConsumedFromRedpanda), "Consumed adds the batch size, not one")
	assert.Equal(t, 1.0, testutil.ToFloat64(counters.Processed))
	assert.Equal(t, 1.0, testutil.ToFloat64(counters.Failed))
}
