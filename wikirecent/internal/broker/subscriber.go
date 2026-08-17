package broker

import (
	"context"
	"encoding/json"
	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"
	"wikirecent/internal/batch"
	"wikirecent/internal/events"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const (
	maxBatchSize   = 100
	maxPollRecords = 1000
)

type SubscriberConfig struct {
	Brokers []string
	Topic   string
	Group   string
}

type Subscriber struct {
	client recordPoller
	log    applog.Logger
	stats  recorder
}

func NewSubscriber(cfg SubscriberConfig, log applog.Logger, stats recorder) (*Subscriber, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("wiki-consumer"),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, &apperrors.ConsumeError{Err: err}
	}

	return &Subscriber{client: client, log: log, stats: stats}, nil
}

// Connect checks that the broker answers.
func (s *Subscriber) Connect(ctx context.Context) error {
	if err := s.client.Ping(ctx); err != nil {
		return &apperrors.ConsumeError{Err: err}
	}
	return nil
}

func (s *Subscriber) Run(ctx context.Context) error {
	for {
		if ctxDone(ctx) {
			return nil
		}
		fetches := s.client.PollRecords(ctx, maxPollRecords)
		// A poll can return records and a cancelled context at the same time.
		if fetches.IsClientClosed() || ctxDone(ctx) {
			return nil // graceful stop, not a failure
		}
		for _, e := range fetches.Errors() {
			s.log.AppErrorf(e.Err, "fetch error on %s/%d", e.Topic, e.Partition)
		}

		records := fetches.Records()
		if len(records) == 0 {
			s.client.AllowRebalance()
			continue
		}

		// Fan out per partition, then in chunks of maxBatchSize within a partition, so
		// the biggest partition does not set the pace of the whole poll.
		g, gctx := errgroup.WithContext(ctx)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return // BatchSlice would hand back one empty batch to run
			}
			for _, sub := range batch.BatchSlice(p.Records, maxBatchSize) {
				g.Go(func() error { return s.handleBatch(gctx, sub) })
			}
		})

		if err := g.Wait(); err != nil {
			s.log.AppErrorf(err, "batch failed, not committing")
			s.client.AllowRebalance()
			continue
		}

		// The single acknowledgement point: one commit per poll, only after every
		// sub-batch succeeded, so any failure replays the whole poll.
		if err := s.client.CommitRecords(ctx, records...); err != nil {
			s.log.AppErrorf(err, "commit failed")
		}
		s.client.AllowRebalance()
	}
}

func (s *Subscriber) handleBatch(ctx context.Context, batch []*kgo.Record) error {
	if ctxDone(ctx) {
		return ctx.Err()
	}

	for _, record := range batch {
		event, perr := decodeRecord(record.Value)
		if perr != nil {
			// A broken payload never parses, so retrying it cannot help. Drop it and
			// keep the batch alive: one bad record must not block a partition.
			// Candidate for DLQ logic in ch-10.
			s.log.AppErrorf(perr, "skipping bad record at %s/%d offset %d",
				record.Topic, record.Partition, record.Offset)
			continue
		}
		s.stats.Record(event)
	}

	// Checked again, so a shutdown in the middle of a batch does not lead to a commit.
	if ctxDone(ctx) {
		return ctx.Err()
	}
	return nil
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

func decodeRecord(value []byte) (events.WikiEvent, *apperrors.ParseError) {
	var event events.WikiEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return events.WikiEvent{}, &apperrors.ParseError{Line: string(value), Err: err}
	}
	return event, nil
}
