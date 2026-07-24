package stats_test

import (
	consumermodels "ch-2/internal/consumer/models"
	"ch-2/internal/stats"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// helper — builds a WikiEvent with sensible defaults, override via fields
func mockEvent(user string, bot bool, serverURL string) consumermodels.WikiEvent {
	return consumermodels.WikiEvent{
		User:      user,
		Bot:       bot,
		ServerURL: serverURL,
	}
}

func TestNew_InitialisesEmptyStats(t *testing.T) {
	st := stats.New()
	snap := st.Snapshot()

	require.Equal(t, int64(0), snap.TotalMessages)
	require.Equal(t, int64(0), snap.DistinctUsers)
	require.Equal(t, int64(0), snap.BotEdits)
	require.Equal(t, int64(0), snap.BotEdits)
	require.Equal(t, int64(0), snap.HumanEdits)
	require.Equal(t, 0, len(snap.ByServerURL))
}

func TestRecord_CountsTotalMessages(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("john", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("carol", true, "https://en.wikipedia.org"))

	got := st.Snapshot().TotalMessages

	require.Equal(t, int64(3), got)
}

func TestRecord_CountsDistinctUsers(t *testing.T) {
	st := stats.New()

	// iryna appears twice — should only count once
	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("john", false, "https://en.wikipedia.org"))

	if got := st.Snapshot().DistinctUsers; got != 2 {
		t.Errorf("expected 2 distinct users, got %d", got)
	}
}

func TestRecord_IgnoresEmptyUser(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("", false, "https://en.wikipedia.org"))

	if got := st.Snapshot().DistinctUsers; got != 0 {
		t.Errorf("expected 0 distinct users for empty username, got %d", got)
	}
}

func TestRecord_SeparatesBotAndHumanEdits(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("human1", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("human2", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("bot1", true, "https://en.wikipedia.org"))

	snap := st.Snapshot()
	if snap.HumanEdits != 2 {
		t.Errorf("expected 2 human edits, got %d", snap.HumanEdits)
	}
	if snap.BotEdits != 1 {
		t.Errorf("expected 1 bot edit, got %d", snap.BotEdits)
	}
}

func TestRecord_CountsByServerURL(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("john", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("carol", false, "https://commons.wikimedia.org"))

	snap := st.Snapshot()
	if snap.ByServerURL["https://en.wikipedia.org"] != 2 {
		t.Errorf("expected 2 for en.wikipedia.org, got %d", snap.ByServerURL["https://en.wikipedia.org"])
	}
	if snap.ByServerURL["https://commons.wikimedia.org"] != 1 {
		t.Errorf("expected 1 for commons.wikimedia.org, got %d", snap.ByServerURL["https://commons.wikimedia.org"])
	}
}

func TestRecord_IgnoresEmptyServerURL(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("iryna", false, ""))

	if got := len(st.Snapshot().ByServerURL); got != 0 {
		t.Errorf("expected empty ByServerURL map, got %d entries", got)
	}
}

func TestSnapshot_IsDeepCopy(t *testing.T) {
	st := stats.New()
	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))

	snap := st.Snapshot()
	// mutating the snapshot map must not affect internal state
	snap.ByServerURL["https://en.wikipedia.org"] = 999

	fresh := st.Snapshot()
	if fresh.ByServerURL["https://en.wikipedia.org"] != 1 {
		t.Error("Snapshot did not return a deep copy — internal map was mutated")
	}
}

func TestRecord_ConcurrentSafety(t *testing.T) {
	st := stats.New()
	var wg sync.WaitGroup
	const goroutines = 100

	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st.Record(mockEvent("user", i%2 == 0, "https://en.wikipedia.org"))
		}(i)
	}
	wg.Wait()

	if got := st.Snapshot().TotalMessages; got != goroutines {
		t.Errorf("expected %d total messages after concurrent writes, got %d", goroutines, got)
	}
}
