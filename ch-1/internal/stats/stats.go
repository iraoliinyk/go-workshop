package stats

import (
	"ch-1/internal/consumer"
	"sync"
	"time"
)

type StatsSnapshot struct {
	TotalMessages int64            `json:"total_messages"`
	DistinctUsers int64            `json:"distinct_users"`
	BotEdits      int64            `json:"bot_edits"`
	HumanEdits    int64            `json:"human_edits"`
	ByServerURL   map[string]int64 `json:"by_server_url"`
	LastEvent     time.Time        `json:"last_event"`
}

type Stats struct {
	mu            sync.RWMutex
	totalMessages int64
	users         map[string]struct{} // set — tracks distinct usernames
	botEdits      int64
	humanEdits    int64
	byServerURL   map[string]int64
	lastEvent     time.Time
}

func New() *Stats {
	return &Stats{
		users:       make(map[string]struct{}),
		byServerURL: make(map[string]int64),
	}
}

func (s *Stats) Record(event consumer.WikiEvent) {
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
