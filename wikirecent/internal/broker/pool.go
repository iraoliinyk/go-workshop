package broker

import (
	"context"
	"errors"
	"sync"
	"time"
	"wikirecent/internal/applog"
)

type PoolConfig struct {
	SubscriberConfig // Brokers, Topic, Group — every worker joins the same group
	Workers          int
}

type Pool struct {
	subs []*Subscriber
	log  applog.Logger
}

func NewPool(cfg PoolConfig, log applog.Logger, stats Recorder,
	dec Decoder, w DeltaWriter, obs BatchObserver) (*Pool, error) {
	p := &Pool{subs: make([]*Subscriber, 0, cfg.Workers), log: log}
	for i := range cfg.Workers {
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
