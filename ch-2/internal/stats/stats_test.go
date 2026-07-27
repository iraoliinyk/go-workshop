package stats_test

import (
	"sync"
	"testing"

	consumermodels "ch-2/internal/consumer/models"
	"ch-2/internal/stats"

	"github.com/stretchr/testify/assert"
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

	assert.Equal(t, int64(0), snap.TotalMessages)
	assert.Equal(t, int64(0), snap.DistinctUsers)
	assert.Equal(t, int64(0), snap.BotEdits)
	assert.Equal(t, int64(0), snap.HumanEdits)
	assert.Empty(t, snap.ByServerURL)
}

func TestRecord_CountsTotalMessages(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("john", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("carol", true, "https://en.wikipedia.org"))

	got := st.Snapshot().TotalMessages

	assert.Equal(t, int64(3), got)
}

func TestRecord_CountsDistinctUsers(t *testing.T) {
	st := stats.New()

	// iryna appears twice — should only count once
	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("john", false, "https://en.wikipedia.org"))

	assert.Equal(t, int64(2), st.Snapshot().DistinctUsers)
}

func TestRecord_IgnoresEmptyUser(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("", false, "https://en.wikipedia.org"))

	assert.Equal(t, int64(0), st.Snapshot().DistinctUsers)
}

func TestRecord_SeparatesBotAndHumanEdits(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("human1", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("human2", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("bot1", true, "https://en.wikipedia.org"))

	snap := st.Snapshot()
	assert.Equal(t, int64(2), snap.HumanEdits)
	assert.Equal(t, int64(1), snap.BotEdits)
}

func TestRecord_CountsByServerURL(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("john", false, "https://en.wikipedia.org"))
	st.Record(mockEvent("carol", false, "https://commons.wikimedia.org"))

	snap := st.Snapshot()
	assert.Equal(t, int64(2), snap.ByServerURL["https://en.wikipedia.org"])
	assert.Equal(t, int64(1), snap.ByServerURL["https://commons.wikimedia.org"])
}

func TestRecord_IgnoresEmptyServerURL(t *testing.T) {
	st := stats.New()

	st.Record(mockEvent("iryna", false, ""))

	assert.Empty(t, st.Snapshot().ByServerURL)
}

func TestSnapshot_IsDeepCopy(t *testing.T) {
	st := stats.New()
	st.Record(mockEvent("iryna", false, "https://en.wikipedia.org"))

	snap := st.Snapshot()
	// mutating the snapshot map must not affect internal state
	snap.ByServerURL["https://en.wikipedia.org"] = 999

	fresh := st.Snapshot()
	assert.Equal(t, int64(1), fresh.ByServerURL["https://en.wikipedia.org"],
		"Snapshot did not return a deep copy — internal map was mutated")
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

	assert.Equal(t, int64(goroutines), st.Snapshot().TotalMessages)
}
