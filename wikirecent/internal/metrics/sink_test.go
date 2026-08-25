package metrics_test

import (
	"context"
	"errors"
	"testing"

	"wikirecent/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func newCountingSink(t *testing.T) (*metrics.CountingSink, *Mocksink, prometheus.Counter) {
	t.Helper()

	next := NewMocksink(gomock.NewController(t))
	counters := metrics.NewEvents(prometheus.NewRegistry())

	return metrics.NewCountingSink(next, counters.ConsumedFromStream), next, counters.ConsumedFromStream
}

func TestCountingSink_CountsEveryPayload(t *testing.T) {
	sink, next, counter := newCountingSink(t)
	payload := []byte(`{"user":"iryna"}`)

	next.EXPECT().Publish(gomock.Any(), payload).Return(nil).Times(3)

	for range 3 {
		require.NoError(t, sink.Publish(context.Background(), payload))
	}

	assert.Equal(t, 3.0, testutil.ToFloat64(counter))
}

func TestCountingSink_CountsEvenWhenTheNextSinkFails(t *testing.T) {
	sink, next, counter := newCountingSink(t)
	wantErr := errors.New("broker refused the record")

	next.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(wantErr).Times(1)

	err := sink.Publish(context.Background(), []byte(`{"user":"iryna"}`))

	require.ErrorIs(t, err, wantErr, "the wrapper must pass the error through unchanged")
	assert.Equal(t, 1.0, testutil.ToFloat64(counter),
		"the counter measures what arrived from the stream, not what succeeded")
}

// gomock rejects a call carrying different bytes or a different context.
func TestCountingSink_PassesThePayloadAndContextThrough(t *testing.T) {
	sink, next, _ := newCountingSink(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	payload := []byte(`{"user":"iryna","bot":false,"server_url":"https://ca.wikipedia.org"}`)

	next.EXPECT().Publish(ctx, payload).Return(nil).Times(1)

	require.NoError(t, sink.Publish(ctx, payload))
}
