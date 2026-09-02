package broker

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"
	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"
	"wikirecent/internal/stats/statsmodels"

	"github.com/twmb/franz-go/pkg/kgo"
)

type SubscriberConfig struct {
	Brokers        []string
	Topic          string
	Group          string
	FetchMinBytes  int32
	FetchMaxWait   time.Duration
	MaxPollRecords int
	// DrainGrace is a fallback timeout so draining doesn't instantly fail and
	// trigger the exact retry loop it was created to prevent
	DrainGrace time.Duration
}

const defaultDrainGrace = 5 * time.Second

type Subscriber struct {
	client RecordPoller
	log    applog.Logger
	stats  Recorder
	dec    Decoder
	writer DeltaWriter
	obs    BatchObserver
	cfg    SubscriberConfig
}

func NewSubscriber(cfg SubscriberConfig, log applog.Logger, stats Recorder,
	dec Decoder, w DeltaWriter, obs BatchObserver) (*Subscriber, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("wiki-consumer"),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.FetchMinBytes(cfg.FetchMinBytes),
		kgo.FetchMaxWait(cfg.FetchMaxWait),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, &apperrors.ConsumeError{Err: err}
	}

	return NewSubscriberWithClient(client, log, stats, dec, w, obs, cfg), nil
}

// NewSubscriberWithClient builds a Subscriber on a poller the caller already has.
// NewSubscriber is the normal path; this is the seam that lets a caller supply its
// own client, which is how the tests reach the poll loop without a live broker.
func NewSubscriberWithClient(client RecordPoller, log applog.Logger, stats Recorder,
	dec Decoder, writer DeltaWriter, obs BatchObserver, cfg SubscriberConfig) *Subscriber {
	if cfg.DrainGrace <= 0 {
		cfg.DrainGrace = defaultDrainGrace
	}
	return &Subscriber{client: client, log: log, stats: stats, dec: dec, writer: writer, obs: obs, cfg: cfg}
}

func (s *Subscriber) Connect(ctx context.Context) error {
	if err := s.client.Ping(ctx); err != nil {
		return &apperrors.ConsumeError{Err: err}
	}
	return nil
}

func (s *Subscriber) Run(ctx context.Context) error {
	for {
		// The one place a shutdown leaves the loop: before a new fetch starts. Records
		// already in hand are drained below instead of dropped.
		if ctxDone(ctx) {
			return nil
		}
		fetches := s.client.PollRecords(ctx, s.cfg.MaxPollRecords)
		if fetches.IsClientClosed() {
			return nil // graceful stop, not a failure
		}
		for _, e := range fetches.Errors() {
			s.log.AppErrorf(e.Err, "fetch error on %s/%d", e.Topic, e.Partition)
		}

		records := fetches.Records()
		if len(records) == 0 {
			// Nothing fetched, so there is nothing to drain. Leaving without
			// AllowRebalance is safe here: Close uses CloseAllowingRebalance.
			if ctxDone(ctx) {
				return nil
			}
			s.client.AllowRebalance()
			continue
		}

		pollCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.DrainGrace)
		if ctxDone(ctx) {
			s.log.Debugf("draining %d records on shutdown", len(records))
		}
		s.handlePoll(pollCtx, records)
		cancel()

		s.client.AllowRebalance()

		// The poll above was the drain. Leaving now is safe: nothing is in hand.
		if ctxDone(ctx) {
			return nil
		}
	}
}

func (s *Subscriber) handlePoll(ctx context.Context, records []*kgo.Record) {
	deltas := s.aggregate(records) // one Delta per partition

	// Durable before acknowledged. A failed write must replay the poll, never commit it.
	if err := s.writer.AddDeltas(ctx, deltas); err != nil {
		s.log.AppErrorf(err, "delta write failed, replaying poll")
		return
	}

	if err := s.client.CommitRecords(ctx, records...); err != nil {
		// Not acknowledged, so not visible. These records will be delivered again, and
		// counting them here as well would put the view ahead of the store until the
		// next restart.
		s.log.AppErrorf(err, "commit failed, not applying the poll")
		return
	}

	// After the commit, so the in-memory view only ever holds committed data.
	for _, d := range deltas {
		s.stats.Apply(d)
	}
}

// aggregate folds a poll into one Delta per partition, and does the per-record
// accounting HandleBatch used to do.
func (s *Subscriber) aggregate(records []*kgo.Record) []statsmodels.Delta {
	s.obs.Consumed(len(records))

	byPartition := make(map[int32]*statsmodels.Delta)

	for _, record := range records {

		agg, ok := byPartition[record.Partition]

		if !ok {
			agg = &statsmodels.Delta{
				Key:         statsmodels.DeltaKey{Partition: record.Partition, StartOffset: math.MaxInt64},
				EndOffset:   -1,
				ByServerURL: make(map[string]int64),
			}
			byPartition[record.Partition] = agg
		}

		if record.Offset < agg.Key.StartOffset {
			agg.Key.StartOffset = record.Offset
			t := record.Timestamp.UTC()
			agg.Key.Day = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		}
		if record.Offset > agg.EndOffset {
			agg.EndOffset = record.Offset
		}

		event, err := s.dec.Decode(record.Value)
		if err != nil {
			// A broken payload never parses, so retrying cannot help. Drop it and keep
			// the poll alive: one bad record must not block a partition.
			s.obs.Failed()
			s.log.AppErrorf(err, "skipping bad record at %s/%d offset %d",
				record.Topic, record.Partition, record.Offset)
			continue
		}

		agg.Messages++
		if event.Bot {
			agg.BotEdits++
		} else {
			agg.HumanEdits++
		}
		// Empty is not a value, it is a missing field. Counting it would add a phantom
		// distinct user.
		if event.User != "" {
			agg.Users = append(agg.Users, event.User)
		}
		if event.ServerURL != "" {
			agg.ByServerURL[event.ServerURL]++
		}
		s.obs.Processed()
	}

	deltas := make([]statsmodels.Delta, 0, len(byPartition))
	for _, agg := range byPartition {
		deltas = append(deltas, *agg)
	}
	// Ranging a map gives a random order. A stable one keeps the batch and the tests
	// predictable.
	slices.SortFunc(deltas, func(a, b statsmodels.Delta) int {
		return cmp.Compare(a.Key.Partition, b.Key.Partition)
	})
	return deltas
}

// ctxDone reports whether ctx is finished, without blocking.
func ctxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// Close ignores ctx because CloseAllowingRebalance takes none: it blocks until the
// group leave and the client shutdown finish.
func (s *Subscriber) Close(_ context.Context) error {
	s.client.CloseAllowingRebalance()
	return nil
}
