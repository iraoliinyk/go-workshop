package stats

import (
	consumermodels "ch-2/internal/consumer/models"
	statsmodels "ch-2/internal/stats/models"
	"sync"
	"time"
)

type StatsSnapshot = statsmodels.StatsSnapshot

type Stats struct {
	mu            sync.RWMutex
	totalMessages int64
	users         map[string]struct{} // set — tracks distinct usernames
	botEdits      int64
	humanEdits    int64
	byServerURL   map[string]int64
	lastEvent     time.Time
}

// factory
func New() *Stats {
	return &Stats{
		users:       make(map[string]struct{}),
		byServerURL: make(map[string]int64),
	}
}

func (s *Stats) Record(event consumermodels.WikiEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalMessages++

	if event.User != "" {
		s.users[event.User] = struct{}{}
	}

	if event.Bot {
		s.botEdits++
	} else {
		s.humanEdits++
	}

	if event.ServerURL != "" {
		s.byServerURL[event.ServerURL]++
	}

	s.lastEvent = time.Now()
}

func (s *Stats) Snapshot() StatsSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	byServerURL := make(map[string]int64, len(s.byServerURL))
	for k, v := range s.byServerURL {
		byServerURL[k] = v
	}

	return StatsSnapshot{
		TotalMessages: s.totalMessages,
		DistinctUsers: int64(len(s.users)),
		BotEdits:      s.botEdits,
		HumanEdits:    s.humanEdits,
		ByServerURL:   byServerURL,
		LastEvent:     s.lastEvent,
	}
}
