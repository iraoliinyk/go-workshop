package flusher

import (
	"context"
	"fmt"
	"time"

	"wikirecent/internal/applog"
	"wikirecent/internal/stats/statsmodels"
)

// Snapshotter reads the current statistics from memory.
type Snapshotter interface {
	Snapshot() statsmodels.Snapshot
}

// Sink stores snapshots. It has only the write method, because the flusher never
// reads data back and so should not be able to see Series.
type Sink interface {
	SaveSnapshot(ctx context.Context, at time.Time, snap statsmodels.Snapshot) error
}

type Config struct {
	Interval      time.Duration // how often we save a snapshot
	ShutdownGrace time.Duration // the maximum time the last save may take
	Log           applog.Logger // zero value is PROD, which still reports a failed flush
}

type Flusher struct {
	stats Snapshotter
	sink  Sink
	cfg   Config
	// logf takes the error as well as the message, so the apperrors code lands
	// as an attribute instead of being pasted into the text.
	Logf func(err error, format string, args ...any)

	NewTicker func(time.Duration) (<-chan time.Time, func())
	now       func() time.Time
}

func New(s Snapshotter, sink Sink, cfg Config) (*Flusher, error) {
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("flusher: interval must be positive, got %s", cfg.Interval)
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 5 * time.Second
	}
	return &Flusher{
		// logf stays a plain function so a test can swap it. It is AppErrorf, not
		// Debugf, because nobody watches a request while this loop runs, so a
		// dropped snapshot must be visible in PROD too.
		stats: s, sink: sink, cfg: cfg, Logf: cfg.Log.AppErrorf,
		NewTicker: func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		},
		now: func() time.Time { return time.Now().UTC() },
	}, nil
}

// Run saves a snapshot on every tick until ctx is cancelled, so the caller should
// start it in its own goroutine. A failed save is only logged, but the error of the
// last save is returned, because that one closes the series.
// Redpanda Connect candidate for ch-10.
func (f *Flusher) Run(ctx context.Context) error {
	tick, stop := f.NewTicker(f.cfg.Interval)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			// A new context, because the cancelled one would be refused at once and
			// the last snapshot would be lost.
			fctx, cancel := context.WithTimeout(context.Background(), f.cfg.ShutdownGrace)
			defer cancel()
			if err := f.sink.SaveSnapshot(fctx, f.now(), f.stats.Snapshot()); err != nil {
				return fmt.Errorf("flusher: shutdown flush: %w", err)
			}
			return nil
		case t := <-tick:
			if err := f.sink.SaveSnapshot(ctx, t.UTC(), f.stats.Snapshot()); err != nil {
				// SaveSnapshot returns *apperrors.RepositoryError; passing err
				// itself lets logf attach the code. logf is the only sink here,
				// so a test can still silence it.
				f.Logf(err, "stats flush: %v", err)
			}
		}
	}
}
