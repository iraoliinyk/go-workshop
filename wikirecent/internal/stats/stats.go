package stats

import (
	"sync"
	"time"
	"wikirecent/internal/stats/statsmodels"
)

type Stats struct {
	mu            sync.RWMutex
	totalMessages int64
	users         map[string]struct{} // used as a set, to count distinct usernames
	botEdits      int64
	humanEdits    int64
	byServerURL   map[string]int64
	lastEvent     time.Time
}

// New returns empty stats with both maps ready to use.
func New() *Stats {
	return &Stats{
		users:       make(map[string]struct{}),
		byServerURL: make(map[string]int64),
	}
}

func (s *Stats) Apply(d statsmodels.Delta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalMessages += d.Messages
	s.botEdits += d.BotEdits
	s.humanEdits += d.HumanEdits
	for _, u := range d.Users {
		s.users[u] = struct{}{}
	}
	for url, hits := range d.ByServerURL {
		s.byServerURL[url] += hits
	}
	s.lastEvent = time.Now()
}

func (s *Stats) Seed(snap statsmodels.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalMessages = snap.TotalMessages
	s.botEdits = snap.BotEdits
	s.humanEdits = snap.HumanEdits
}

func (s *Stats) Snapshot() statsmodels.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	byServerURL := make(map[string]int64, len(s.byServerURL))
	for k, v := range s.byServerURL {
		byServerURL[k] = v
	}

	return statsmodels.Snapshot{
		TotalMessages: s.totalMessages,
		DistinctUsers: int64(len(s.users)),
		BotEdits:      s.botEdits,
		HumanEdits:    s.humanEdits,
		ByServerURL:   byServerURL,
		LastEvent:     s.lastEvent,
	}
}
