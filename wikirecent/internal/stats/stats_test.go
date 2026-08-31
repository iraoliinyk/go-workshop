package stats_test

import (
	"sync"
	"testing"

	"wikirecent/internal/stats"
	"wikirecent/internal/stats/statsmodels"

	"github.com/stretchr/testify/assert"
)

const enWiki = "https://en.wikipedia.org"

func TestNew_InitialisesEmptyStats(t *testing.T) {
	st := stats.New()
	snap := st.Snapshot()

	assert.Equal(t, int64(0), snap.TotalMessages)
	assert.Equal(t, int64(0), snap.DistinctUsers)
	assert.Equal(t, int64(0), snap.BotEdits)
	assert.Equal(t, int64(0), snap.HumanEdits)
	assert.Empty(t, snap.ByServerURL)
}

// Two Deltas, because Apply now takes one poll at a time and the totals have to
// survive across polls.
func TestApply_CountsTotalMessages(t *testing.T) {
	st := stats.New()

	st.Apply(statsmodels.Delta{Messages: 2})
	st.Apply(statsmodels.Delta{Messages: 1})

	assert.Equal(t, int64(3), st.Snapshot().TotalMessages)
}

func TestApply_CountsDistinctUsers(t *testing.T) {
	st := stats.New()

	// iryna appears twice in one Delta and again in the next, but counts once.
	st.Apply(statsmodels.Delta{Messages: 2, Users: []string{"iryna", "iryna"}})
	st.Apply(statsmodels.Delta{Messages: 2, Users: []string{"iryna", "john"}})

	assert.Equal(t, int64(2), st.Snapshot().DistinctUsers)
}

// Apply does no filtering: whatever Subscriber.aggregate put in the Delta is counted.
// That is the contract, not an oversight — a fold carries no per-event context — and
// it is why aggregate is the one that drops an empty username.
func TestApply_DoesNotFilterEmptyUsername(t *testing.T) {
	st := stats.New()

	st.Apply(statsmodels.Delta{Messages: 1, Users: []string{""}})

	assert.Equal(t, int64(1), st.Snapshot().DistinctUsers)
}

func TestApply_SeparatesBotAndHumanEdits(t *testing.T) {
	st := stats.New()

	st.Apply(statsmodels.Delta{Messages: 2, HumanEdits: 2})
	st.Apply(statsmodels.Delta{Messages: 1, BotEdits: 1})

	snap := st.Snapshot()
	assert.Equal(t, int64(2), snap.HumanEdits)
	assert.Equal(t, int64(1), snap.BotEdits)
}

// The en.wikipedia key appears in both Deltas, so this also covers the merge rather
// than a plain overwrite.
func TestApply_SumsByServerURL(t *testing.T) {
	st := stats.New()

	st.Apply(statsmodels.Delta{Messages: 1, ByServerURL: map[string]int64{enWiki: 1}})
	st.Apply(statsmodels.Delta{Messages: 2, ByServerURL: map[string]int64{
		enWiki:                          1,
		"https://commons.wikimedia.org": 1,
	}})

	snap := st.Snapshot()
	assert.Equal(t, int64(2), snap.ByServerURL[enWiki])
	assert.Equal(t, int64(1), snap.ByServerURL["https://commons.wikimedia.org"])
}

// Same contract as the empty username: aggregate filters, Apply sums what it is given.
func TestApply_DoesNotFilterEmptyServerURL(t *testing.T) {
	st := stats.New()

	st.Apply(statsmodels.Delta{Messages: 1, ByServerURL: map[string]int64{"": 1}})

	assert.Equal(t, int64(1), st.Snapshot().ByServerURL[""])
}

func TestSnapshot_IsDeepCopy(t *testing.T) {
	st := stats.New()
	st.Apply(statsmodels.Delta{Messages: 1, ByServerURL: map[string]int64{enWiki: 1}})

	snap := st.Snapshot()
	// changing the returned map must not change the stats themselves
	snap.ByServerURL[enWiki] = 999

	fresh := st.Snapshot()
	assert.Equal(t, int64(1), fresh.ByServerURL[enWiki],
		"Snapshot did not return a deep copy — internal map was mutated")
}

func TestApply_ConcurrentSafety(t *testing.T) {
	st := stats.New()
	var wg sync.WaitGroup
	const goroutines = 100

	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st.Apply(statsmodels.Delta{
				Messages:    1,
				BotEdits:    int64(i % 2),
				HumanEdits:  int64(1 - i%2),
				Users:       []string{"user"},
				ByServerURL: map[string]int64{enWiki: 1},
			})
		}(i)
	}
	wg.Wait()

	snap := st.Snapshot()
	assert.Equal(t, int64(goroutines), snap.TotalMessages)
	assert.Equal(t, int64(goroutines), snap.BotEdits+snap.HumanEdits)
	assert.Equal(t, int64(goroutines), snap.ByServerURL[enWiki])
}
