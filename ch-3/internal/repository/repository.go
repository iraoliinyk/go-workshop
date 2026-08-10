package repository

import (
	"context"
	"fmt"
	"io"
	"time"

	"ch-3/internal/auth"
	"ch-3/internal/db/cassandra"
	"ch-3/internal/repository/memory"
	"ch-3/internal/stats/statsmodels"
)

type StatsStore interface {
	// SaveSnapshot adds one point to the time series. Called by the flush
	// ticker and once more on shutdown.
	SaveSnapshot(ctx context.Context, at time.Time, snap statsmodels.Snapshot) error
	// Series reads points back for a day and time range. Used by tests;
	// Grafana queries Cassandra directly.
	Series(ctx context.Context, day, from, to time.Time) ([]statsmodels.SnapshotPoint, error)
}

// Stores holds the stores chosen at startup and owns their lifetime.
type Stores struct {
	Stats  StatsStore
	Users  auth.UserStore
	Tokens auth.RevocationStore
	io.Closer
}

// Config chooses which backend New builds.
type Config struct {
	Backend   string
	Cassandra cassandra.Config
}

// nopCloser is for backends with nothing to close, such as memory.
type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func New(ctx context.Context, cfg Config) (*Stores, error) {
	switch cfg.Backend {
	case "", "in-memory":
		return &Stores{
			Stats:  memory.NewStatsStore(),
			Users:  memory.NewUserStore(),
			Tokens: memory.NewRevocationStore(),
			Closer: nopCloser{},
		}, nil
	case "cassandra":
		sess, err := cassandra.Connect(ctx, cfg.Cassandra)
		if err != nil {
			return nil, err
		}
		// All three stores share one session, so accounts, revoked tokens and
		// snapshots all survive a restart.
		return &Stores{
			Stats:  cassandra.NewStatsStore(sess, cfg.Cassandra),
			Users:  cassandra.NewUserStore(sess),
			Tokens: cassandra.NewRevocationStore(sess),
			// Not the bare session: its Close() returns nothing, so it does
			// not satisfy io.Closer on its own.
			Closer: cassandra.Closer(sess),
		}, nil
	default:
		return nil, fmt.Errorf("repository: unknown backend %q", cfg.Backend)
	}
}
