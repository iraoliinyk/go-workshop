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
	Snapshot(ctx context.Context) (statsmodels.Snapshot, error)
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

	// Logf takes the error as well as the message, so the apperrors code lands
	// as an attribute instead of being pasted into the text.
	Logf      func(err error, format string, args ...any)
	NewTicker func(time.Duration) (<-chan time.Time, func())
	Now       func() time.Time
}

type Flusher struct {
	stats     Snapshotter
	sink      Sink
	cfg       Config
	logf      func(err error, format string, args ...any)
	newTicker func(time.Duration) (<-chan time.Time, func())
	now       func() time.Time
}

func New(s Snapshotter, sink Sink, cfg Config) (*Flusher, error) {
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("flusher: interval must be positive, got %s", cfg.Interval)
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 5 * time.Second
	}
	// The default logger is AppErrorf, not Debugf, because nobody watches a request
	// while this loop runs, so a dropped snapshot must be visible in PROD too.
	if cfg.Logf == nil {
		cfg.Logf = cfg.Log.AppErrorf
	}
	if cfg.NewTicker == nil {
		cfg.NewTicker = func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		}
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}

	return &Flusher{
		stats: s, sink: sink, cfg: cfg,
		logf:      cfg.Logf,
		newTicker: cfg.NewTicker,
		now:       cfg.Now,
	}, nil
}

func (f *Flusher) Run(ctx context.Context) error {
	tick, stop := f.newTicker(f.cfg.Interval)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), f.cfg.ShutdownGrace)
			defer cancel()
			snap, err := f.stats.Snapshot(fctx)
			if err != nil {
				return fmt.Errorf("flusher: shutdown read: %w", err)
			}
			if err := f.sink.SaveSnapshot(fctx, f.now(), snap); err != nil {
				return fmt.Errorf("flusher: shutdown flush: %w", err)
			}
			return nil

		case t := <-tick:
			snap, err := f.stats.Snapshot(ctx)
			if err != nil {
				f.logf(err, "stats read failed: %v", err)
				continue
			}
			if err := f.sink.SaveSnapshot(ctx, t.UTC(), snap); err != nil {
				f.logf(err, "stats flush: %v", err)
			}
		}
	}
}
