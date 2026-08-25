package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Events struct {
	ConsumedFromStream   prometheus.Counter
	PersistedToRedpanda  prometheus.Counter
	ConsumedFromRedpanda prometheus.Counter
	Processed            prometheus.Counter
	Failed               prometheus.Counter
}

const namespace = "wikistream"

func NewEvents(reg prometheus.Registerer) *Events {
	f := promauto.With(reg)
	return &Events{
		ConsumedFromStream: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_consumed_from_stream_total",
			Help:      "Payloads read from the Wikimedia SSE stream",
		}),

		PersistedToRedpanda: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_persisted_to_redpanda_total",
			Help:      "Events persisted to Redpanda",
		}),

		ConsumedFromRedpanda: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_consumed_from_redpanda_total",
			Help:      "Events consumed from Redpanda",
		}),

		Processed: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_processed_total",
			Help:      "Events processed successfully",
		}),

		Failed: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_failed_total",
			Help:      "Events that failed to be processed",
		}),
	}
}

func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

type sink interface {
	Publish(ctx context.Context, payload []byte) error
}

type CountingSink struct {
	next    sink
	counter prometheus.Counter
}

func NewCountingSink(next sink, counter prometheus.Counter) *CountingSink {
	return &CountingSink{next: next, counter: counter}
}

func (s *CountingSink) Publish(ctx context.Context, payload []byte) error {
	s.counter.Inc()
	return s.next.Publish(ctx, payload)
}

type PublishCounters struct {
	ok     prometheus.Counter
	failed prometheus.Counter
}

func NewPublishCounters(ok, failed prometheus.Counter) PublishCounters {
	return PublishCounters{ok: ok, failed: failed}
}

func (pc PublishCounters) Published() { pc.ok.Inc() }

func (pc PublishCounters) PublishFailed() { pc.failed.Inc() }

type BatchCounters struct {
	consumed  prometheus.Counter
	processed prometheus.Counter
	failed    prometheus.Counter
}

func (c BatchCounters) Consumed(n int) { c.consumed.Add(float64(n)) }
func (c BatchCounters) Processed()     { c.processed.Inc() }
func (c BatchCounters) Failed()        { c.failed.Inc() }

func NewBatchCounters(consumed, processed, failed prometheus.Counter) BatchCounters {
	return BatchCounters{consumed: consumed, processed: processed, failed: failed}
}
