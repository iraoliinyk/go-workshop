package memory

import (
	"context"
	"sync"
	"time"

	"wikirecent/internal/stats/statsmodels"
)

// StatsStore keeps the time series in memory by adding snapshots to a slice, and is
// safe for concurrent use. It is the default backend and the one the tests use, so it
// must stay free of any database dependency.
type StatsStore struct {
	mu     sync.RWMutex
	points []statsmodels.SnapshotPoint
	deltas map[statsmodels.DeltaKey]statsmodels.Delta // mirrors Cassandra primary key
}

func NewStatsStore() *StatsStore {
	return &StatsStore{deltas: make(map[statsmodels.DeltaKey]statsmodels.Delta)}
}

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

// Totals mirrors Cassandra sums one day of deltas.
func (s *StatsStore) Totals(ctx context.Context, day time.Time) (statsmodels.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	d := day.UTC()
	dayKey := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)

	var snap statsmodels.Snapshot
	for key, delta := range s.deltas {
		if !key.Day.Equal(dayKey) {
			continue
		}
		snap.TotalMessages += delta.Messages
		snap.BotEdits += delta.BotEdits
		snap.HumanEdits += delta.HumanEdits
	}
	return snap, nil
}

func (s *StatsStore) AddDeltas(ctx context.Context, deltas []statsmodels.Delta) error {
	if len(deltas) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range deltas {
		s.deltas[d.Key] = d
	}
	return nil
}
