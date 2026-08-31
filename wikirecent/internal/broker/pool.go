package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"
)

type PoolConfig struct {
	SubscriberConfig // every worker joins the same group with the same fetch settings
	Workers          int
	MaxPartitions    int32
}

type Pool struct {
	subs []*Subscriber
	log  applog.Logger
}

func NewPool(cfg PoolConfig, log applog.Logger, stats Recorder,
	dec Decoder, w DeltaWriter, obs BatchObserver) (*Pool, error) {

	if cfg.Workers <= 0 {
		return nil, &apperrors.ConfigError{
			Err: fmt.Errorf("broker: workers must be positive, got %d", cfg.Workers),
		}
	}
	if cfg.MaxPartitions <= 0 {
		return nil, &apperrors.ConfigError{
			Err: fmt.Errorf("broker: max partitions must be positive, got %d", cfg.MaxPartitions),
		}
	}

	// Capped, not rejected: fewer workers than partitions is a valid choice, and each
	// one simply takes more than one partition. A member beyond the partition count is
	// the broken case — it never gets an assignment and only slows rebalancing down.
	workers := cfg.Workers
	if partitions := int(cfg.MaxPartitions); workers > partitions {
		log.Warnf("broker: capping workers from %d to the %d partitions of topic %q",
			workers, partitions, cfg.Topic)
		workers = partitions
	}

	p := &Pool{subs: make([]*Subscriber, 0, workers), log: log}

	for i := range workers {
		sub, err := NewSubscriber(cfg.SubscriberConfig, log.With("worker", i), stats, dec, w, obs)
		if err != nil {
			_ = p.Close(context.Background()) // do not leak the ones already built
			return nil, err
		}
		p.subs = append(p.subs, sub)
	}
	return p, nil
}

// Connect fails on the first unreachable worker, because a consumer that cannot
// reach the broker should not start at all. The caller closes the Pool.
func (p *Pool) Connect(ctx context.Context) error {
	for _, sub := range p.subs {
		if err := sub.Connect(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Run starts one supervised goroutine per worker and blocks until every one has
// returned. The caller runs it in its own goroutine and waits on a done channel —
// the same shape cmd/consumer already uses for the flusher.
func (p *Pool) Run(ctx context.Context) error {

	var wg sync.WaitGroup

	for i, sub := range p.subs {
		wg.Add(1)
		go func(id int, s *Subscriber) {
			defer wg.Done()
			p.supervise(ctx, id, s)
		}(i, sub)
	}
	wg.Wait() // the join: "Run returned" now means "no worker is still polling"
	return nil
}

const (
	workerBaseBackoff = time.Second
	workerMaxBackoff  = time.Minute
	// A run this long counts as healthy, so the next failure retries fast instead of
	// inheriting the delay from an outage that is already over.
	healthyRun = 30 * time.Second
)

// supervise restarts a worker that stops with an error. Today Subscriber.Run only
// returns nil, so this is a guard for a future fatal path rather than a live retry.
func (p *Pool) supervise(ctx context.Context, id int, s *Subscriber) {
	backoff := workerBaseBackoff

	for {
		if ctx.Err() != nil {
			return
		}

		started := time.Now()
		err := s.Run(ctx)

		// nil means the loop chose to stop: cancellation, or the client was closed.
		// Restarting then would spin on a dead client.
		if err == nil || ctx.Err() != nil {
			return
		}

		if time.Since(started) > healthyRun {
			backoff = workerBaseBackoff
		}
		p.log.AppErrorf(err, "worker %d stopped, restarting in %s", id, backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = min(backoff*2, workerMaxBackoff)
		}
	}
}

func (p *Pool) Close(ctx context.Context) error {
	var errs []error
	for _, sub := range p.subs {
		if err := sub.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
