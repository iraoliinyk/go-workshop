package cassandra

import (
	"context"
	"errors"
	"time"

	"wikirecent/internal/apperrors"
	"wikirecent/internal/stats/statsmodels"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type StatsStore struct {
	sess *gocql.Session
	cfg  Config
}

func NewStatsStore(sess *gocql.Session, cfg Config) *StatsStore {
	return &StatsStore{sess: sess, cfg: cfg}
}

const insertStatsSnapshot = `INSERT INTO stats_snapshot
	(day, snapshot_ts, total_messages, bot_edits, human_edits, distinct_users)
	VALUES (?, ?, ?, ?, ?, ?)`

const insertServerSnapshot = `INSERT INTO server_snapshot
	(server_url, day, snapshot_ts, hits) VALUES (?, ?, ?, ?)`

const insertDelta = `INSERT INTO stats_delta
	(day, partition, end_offset, total_messages, bot_edits, human_edits)
	VALUES (?, ?, ?, ?, ?, ?)`

func (s *StatsStore) SaveSnapshot(ctx context.Context, at time.Time, snap statsmodels.Snapshot) error {
	at = at.UTC()
	// The partition key is the day at UTC midnight, taken from at.
	day := time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC)

	if err := s.sess.Query(insertStatsSnapshot,
		day, at, snap.TotalMessages, snap.BotEdits, snap.HumanEdits, snap.DistinctUsers,
	).ExecContext(ctx); err != nil {
		return &apperrors.RepositoryError{Err: err}
	}

	// One row per server, written one at a time. Batching them is a later concern.
	for url, hits := range snap.ByServerURL {
		if err := s.sess.Query(insertServerSnapshot, url, day, at, hits).ExecContext(ctx); err != nil {
			return &apperrors.RepositoryError{Err: err}
		}
	}
	return nil
}

func (s *StatsStore) Totals(ctx context.Context, day time.Time) (statsmodels.Snapshot, error) {
	// todo-ch-8: single-partition is not the same as small. SUM reads every row in the
	// day partition, and stats_delta gets one row per poll per Kafka partition, so a busy
	// day is millions of rows in this one read. The fix is a running-total row per day,
	// not a bigger timeout. Failing here only costs a start from zero, so it can wait.
	const q = `SELECT SUM(total_messages), SUM(bot_edits), SUM(human_edits)
		FROM stats_delta WHERE day = ?`

	d := day.UTC()
	dayKey := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)

	var snap statsmodels.Snapshot
	err := s.sess.Query(q, dayKey).
		ScanContext(ctx, &snap.TotalMessages, &snap.BotEdits, &snap.HumanEdits)

	// A day with nothing written yet is not a failure. The first start of the day
	// lands here, and zero is the correct answer.
	if errors.Is(err, gocql.ErrNotFound) {
		return statsmodels.Snapshot{}, nil
	}
	if err != nil {
		return statsmodels.Snapshot{}, &apperrors.RepositoryError{Err: err}
	}
	return snap, nil
}

func (s *StatsStore) AddDeltas(ctx context.Context, deltas []statsmodels.Delta) error {
	if len(deltas) == 0 {
		return nil
	}
	// Unlogged, and every row shares the day partition key, so this is one round trip
	// to one node — the only shape where a Cassandra batch is a saving, not a trap.
	b := s.sess.Batch(gocql.UnloggedBatch)
	for _, d := range deltas {
		b.Query(insertDelta, d.Key.Day, d.Key.Partition, d.Key.EndOffset,
			d.Messages, d.BotEdits, d.HumanEdits)
	}
	if err := b.ExecContext(ctx); err != nil {
		return &apperrors.RepositoryError{Err: err}
	}
	return nil
}
