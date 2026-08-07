//go:build integration

package cassandra_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"ch-4/internal/apperrors"
	"ch-4/internal/db/cassandra"
	"ch-4/internal/stats/statsmodels"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newStatsStore returns a store and the session behind it, which the tests need for
// reading system_schema and for clearing their partitions.
func newStatsStore(t *testing.T) (*cassandra.StatsStore, *gocql.Session) {
	t.Helper()
	sess := connect(t)
	return cassandra.NewStatsStore(sess, itConfig(t)), sess
}

// clearDays empties whole day partitions of stats_snapshot before a test writes to
// them. Runs share one keyspace and nothing truncates it, so a run against broken
// code would otherwise leave rows in a partition a later run asserts is empty.
func clearDays(t *testing.T, sess *gocql.Session, days ...time.Time) {
	t.Helper()
	for _, d := range days {
		// The partition key is the whole of it, so this is a single-partition delete.
		err := sess.Query(`DELETE FROM stats_snapshot WHERE day = ?`, d.UTC()).
			ExecContext(context.Background())
		require.NoErrorf(t, err, "clear day %s", d.Format(time.DateOnly))
	}
}

// midnight builds a UTC day key the way SaveSnapshot does.
func midnight(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestStatsStore_SeriesReturnsSavedSnapshotsNewestFirst(t *testing.T) {
	store, sess := newStatsStore(t)
	ctx := context.Background()

	// A fixed date in the past, not time.Now(). It gives an exact value to compare
	// against, and it keeps this test's partition away from any real data on the
	// node.
	day := midnight(2001, 2, 3)
	base := day.Add(4*time.Hour + 5*time.Minute + 6*time.Second)
	clearDays(t, sess, day, day.AddDate(0, 0, 1)) // the next day is asserted empty below

	// Three points, rising over time, like the real flusher writes.
	want := []statsmodels.Snapshot{
		{TotalMessages: 10, BotEdits: 4, HumanEdits: 6, DistinctUsers: 3,
			ByServerURL: map[string]int64{"ua.wikipedia.org": 7, "en.wikipedia.org": 3}},
		{TotalMessages: 20, BotEdits: 9, HumanEdits: 11, DistinctUsers: 5,
			ByServerURL: map[string]int64{"ua.wikipedia.org": 14, "en.wikipedia.org": 6}},
		{TotalMessages: 30, BotEdits: 12, HumanEdits: 18, DistinctUsers: 8,
			ByServerURL: map[string]int64{"ua.wikipedia.org": 21, "en.wikipedia.org": 9}},
	}
	for i, snap := range want {
		at := base.Add(time.Duration(i) * 10 * time.Second) // the default flush interval
		require.NoErrorf(t, store.SaveSnapshot(ctx, at, snap), "SaveSnapshot #%d", i)
	}

	// Read the whole range back. The table orders rows newest first, so check that
	// order instead of assuming the oldest comes first.
	got, err := store.Series(ctx, day, base.Add(-time.Minute), base.Add(time.Minute))
	require.NoError(t, err, "Series")
	require.Len(t, got, len(want), "one point per SaveSnapshot")

	for i, p := range got {
		w := want[len(want)-1-i] // newest first, so walk want backwards
		assert.Equalf(t, w.TotalMessages, p.TotalMessages, "point %d total_messages", i)
		assert.Equalf(t, w.BotEdits, p.BotEdits, "point %d bot_edits", i)
		assert.Equalf(t, w.HumanEdits, p.HumanEdits, "point %d human_edits", i)
		assert.Equalf(t, w.DistinctUsers, p.DistinctUsers, "point %d distinct_users", i)
	}
	assert.True(t, got[0].At.After(got[len(got)-1].At), "newest row must come first")

	// The timestamp must come back unchanged. This is where a mix-up between UTC and
	// local time would show, and the memory store cannot catch that.
	newest := base.Add(2 * 10 * time.Second)
	assert.WithinDuration(t, newest, got[0].At.UTC(), time.Millisecond, "timestamp round trip")

	// The day comes from at, so asking for a different day must return nothing.
	other, err := store.Series(ctx, day.AddDate(0, 0, 1), base.Add(-time.Minute), base.Add(time.Minute))
	require.NoError(t, err)
	assert.Empty(t, other, "wrong day partition must return nothing")
}

// selectStatsSnapshotColumns reads the shape of the primary key back from the node,
// which is the only way to see what the CREATE TABLE in bootstrap.go really produced.
const selectStatsSnapshotColumns = `SELECT column_name, kind, position, clustering_order
	FROM system_schema.columns WHERE keyspace_name = ? AND table_name = 'stats_snapshot'`

func TestStatsStore_PrimaryKeyPartitionsByDayAndClustersByTime(t *testing.T) {
	store, sess := newStatsStore(t)
	ctx := context.Background()

	t.Run("the schema says so", func(t *testing.T) {
		type column struct {
			kind     string // partition_key | clustering | regular
			position int    // the column's place within its part of the key, -1 for regular
			order    string // asc | desc | none
		}

		cols := map[string]column{}
		iter := sess.Query(selectStatsSnapshotColumns, itConfig(t).Keyspace).IterContext(ctx)
		var name string
		var c column
		for iter.Scan(&name, &c.kind, &c.position, &c.order) {
			cols[name] = c
		}
		require.NoError(t, iter.Close())
		require.NotEmpty(t, cols, "stats_snapshot must exist; Connect creates it")

		assert.Equal(t, "partition_key", cols["day"].kind, "day must be the partition key")

		var partitionCols []string
		for name, c := range cols {
			if c.kind == "partition_key" {
				partitionCols = append(partitionCols, name)
			}
		}
		assert.Equal(t, []string{"day"}, partitionCols, "day must be the whole partition key")

		assert.Equal(t, "clustering", cols["snapshot_ts"].kind, "snapshot_ts must cluster the partition")
		assert.Equal(t, "desc", cols["snapshot_ts"].order, "rows must be stored newest first")

		// Everything else must stay out of the key, or a snapshot with a new counter
		// value would become a new row instead of replacing the old one.
		for _, name := range []string{"total_messages", "bot_edits", "human_edits", "distinct_users"} {
			assert.Equalf(t, "regular", cols[name].kind, "%s must not be part of the key", name)
		}
	})

	t.Run("one second across midnight is two partitions", func(t *testing.T) {
		first, second := midnight(2001, 5, 10), midnight(2001, 5, 11)
		clearDays(t, sess, first, second)

		lastOfFirst := second.Add(-time.Second) // 2001-05-10 23:59:59Z
		firstOfSecond := second                 // 2001-05-11 00:00:00Z
		require.NoError(t, store.SaveSnapshot(ctx, lastOfFirst, statsmodels.Snapshot{TotalMessages: 111}))
		require.NoError(t, store.SaveSnapshot(ctx, firstOfSecond, statsmodels.Snapshot{TotalMessages: 222}))

		// One window, wide enough to hold both timestamps. Only the day differs between
		// the two reads, so anything they return differently is the partition key.
		from, to := lastOfFirst.Add(-time.Hour), firstOfSecond.Add(time.Hour)

		got, err := store.Series(ctx, first, from, to)
		require.NoError(t, err)
		require.Len(t, got, 1, "the window spans both rows, but day %s holds one", first.Format(time.DateOnly))
		assert.Equal(t, int64(111), got[0].TotalMessages)

		got, err = store.Series(ctx, second, from, to)
		require.NoError(t, err)
		require.Len(t, got, 1, "midnight belongs to the new day")
		assert.Equal(t, int64(222), got[0].TotalMessages)
	})
}

func TestStatsStore_FailureIsRepositoryError(t *testing.T) {
	store, sess := newStatsStore(t)
	sess.Close() // Close is idempotent, so the t.Cleanup in connect stays safe.
	ctx := context.Background()

	day := midnight(2001, 11, 30)

	err := store.SaveSnapshot(ctx, day.Add(time.Hour), statsmodels.Snapshot{TotalMessages: 1})
	require.Error(t, err)
	_, ok := errors.AsType[*apperrors.RepositoryError](err)
	assert.True(t, ok, "SaveSnapshot: want *apperrors.RepositoryError, got %T: %v", err, err)

	_, err = store.Series(ctx, day, day, day.Add(24*time.Hour))
	require.Error(t, err)
	_, ok = errors.AsType[*apperrors.RepositoryError](err)
	assert.True(t, ok, "Series: want *apperrors.RepositoryError, got %T: %v", err, err)
}
