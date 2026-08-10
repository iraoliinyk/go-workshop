package memory

import (
	"context"
	"sync"
	"time"

	"ch-4/internal/stats/statsmodels"
)

// StatsStore keeps the time series in memory by adding snapshots to a slice, and is
// safe for concurrent use. It is the default backend and the one the tests use, so it
// must stay free of any database dependency.
type StatsStore struct {
	mu     sync.RWMutex
	points []statsmodels.SnapshotPoint
}

func NewStatsStore() *StatsStore { return &StatsStore{} }

// SaveSnapshot adds one point. ctx is unused because there is no I/O, but it stays
// to match the interface.
func (s *StatsStore) SaveSnapshot(_ context.Context, at time.Time, snap statsmodels.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.points = append(s.points, statsmodels.SnapshotPoint{
		At:            at.UTC(),
		TotalMessages: snap.TotalMessages,
		BotEdits:      snap.BotEdits,
		HumanEdits:    snap.HumanEdits,
		DistinctUsers: snap.DistinctUsers,
	})
	return nil
}

// Series returns the points between from and to, both included. The day argument
// only matters to Cassandra, so it is ignored here: the range decides the result.
func (s *StatsStore) Series(_ context.Context, _ /* day */ time.Time, from, to time.Time) ([]statsmodels.SnapshotPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []statsmodels.SnapshotPoint
	for _, p := range s.points {
		if !p.At.Before(from) && !p.At.After(to) { // from <= p.At <= to
			out = append(out, p)
		}
	}
	return out, nil
}
