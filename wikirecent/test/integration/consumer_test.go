//go:build integration

package integration

import (
	"testing"
	"time"

	"wikirecent/internal/db/cassandra"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deployment is one topic, one group and one keyspace.
type deployment struct {
	topic      string
	group      string
	keyspace   string
	partitions int32
}

func newDeployment(t *testing.T, partitions int32) deployment {
	t.Helper()
	return deployment{
		topic:      newTopic(t, partitions),
		group:      uniqueName("group"),
		keyspace:   uniqueName("ks"),
		partitions: partitions,
	}
}

// run starts a consumer for this deployment. Calling it twice is a restart.
func (d deployment) run(t *testing.T, workers int) *consumerRun {
	t.Helper()
	return startConsumer(t, consumerOptions{
		topic: d.topic, group: d.group, keyspace: d.keyspace,
		workers: workers, partitions: d.partitions,
	})
}

func TestColdStart_TheSchemaIsCreatedAndTheFirstRestartHolds(t *testing.T) {
	node := newCassandraNode(t)
	dep := newDeployment(t, 3)
	publish(t, dep.topic, editStream(60, time.Now()))

	opts := consumerOptions{
		topic: dep.topic, group: dep.group, keyspace: dep.keyspace,
		workers: 3, partitions: dep.partitions, cassandraNode: node,
	}

	first := startConsumer(t, opts)
	assert.Zero(t, first.snapshot().TotalMessages, "an empty database restores nothing")

	first.waitForTotal(t, 60)
	waitUntilCaughtUp(t, dep.group, dep.topic)
	first.Stop(t)

	sess := cqlSessionAt(t, node, dep.keyspace)
	assert.ElementsMatch(t,
		[]string{"stats_snapshot", "server_snapshot", "user_accounts", "revoked_tokens", "stats_poll"},
		tableNames(t, sess, dep.keyspace),
		"bootstrap must create every table on a fresh node")

	assert.EqualValues(t, 60, sumMessages(readDeltas(t, sess, time.Now().UTC())))

	second := startConsumer(t, opts)
	assert.EqualValues(t, 60, second.snapshot().TotalMessages,
		"the first restart must serve what the first run stored")
}

func TestRestart_TotalsAreRestoredFromTheDatabase(t *testing.T) {
	dep := newDeployment(t, 3)
	edits := editStream(120, time.Now())
	publish(t, dep.topic, edits)

	first := dep.run(t, 3)
	first.waitForTotal(t, 120)
	first.Stop(t)

	second := dep.run(t, 3)

	restored := second.snapshot()
	assert.EqualValues(t, 120, restored.TotalMessages, "the total must survive the restart")
	assert.Equal(t, botsIn(edits), restored.BotEdits, "bot edits must survive too")
	assert.EqualValues(t, 120-botsIn(edits), restored.HumanEdits)

	require.Never(t, func() bool {
		return second.snapshot().TotalMessages != 120
	}, 3*time.Second, 200*time.Millisecond, "a restart must not count anything twice")

	assert.Zero(t, restored.DistinctUsers, "distinct users are not restored")
}

func TestReplay_TheSameRecordsRewriteTheirOwnRows(t *testing.T) {
	dep := newDeployment(t, 3)
	publish(t, dep.topic, editStream(120, time.Now()))
	sess := cqlSession(t, dep.keyspace)
	today := time.Now().UTC()

	first := dep.run(t, 3)
	first.waitForTotal(t, 120)
	waitUntilCaughtUp(t, dep.group, dep.topic)
	first.Stop(t)

	rowsBefore := readDeltas(t, sess, today)
	require.NotEmpty(t, rowsBefore)

	rewindToStart(t, dep.group, dep.topic)

	second := dep.run(t, 3)
	waitUntilCaughtUp(t, dep.group, dep.topic)
	second.Stop(t)

	rowsAfter := readDeltas(t, sess, today)
	assert.ElementsMatch(t, rowsBefore, rowsAfter,
		"a replayed poll must overwrite its own rows, not add new ones")

	store := cassandra.NewStatsStore(sess, cassandraConfig(dep.keyspace))
	totals, err := store.Totals(t.Context(), today)
	require.NoError(t, err)
	assert.EqualValues(t, 120, totals.TotalMessages, "the stored total must not double")

	third := dep.run(t, 3)
	assert.EqualValues(t, 120, third.snapshot().TotalMessages,
		"a restart must serve the stored total, not the drifted one")
}

func TestWorkers_ThreeMembersShareTheTopic(t *testing.T) {
	dep := newDeployment(t, 3)
	publish(t, dep.topic, editStream(240, time.Now()))

	run := dep.run(t, 3)

	// `rpk group describe`. One client per worker.
	require.Eventuallyf(t, func() bool {
		return groupMembers(t, dep.group) == 3
	}, catchUp, 200*time.Millisecond, "want 3 group members, got %d", groupMembers(t, dep.group))

	run.waitForTotal(t, 240)
	waitUntilCaughtUp(t, dep.group, dep.topic)

	assert.EqualValues(t, 240, run.snapshot().TotalMessages, "every record counted exactly once")
	assert.Len(t, committedOffsets(t, dep.group, dep.topic), 3, "all three partitions were consumed")
}

// If CONSUMER_WORKERS higher than the partition count, and the extra members do not join.
func TestWorkers_MoreWorkersThanPartitionsAreCapped(t *testing.T) {
	dep := newDeployment(t, 3)
	publish(t, dep.topic, editStream(60, time.Now()))

	run := startConsumer(t, consumerOptions{
		topic: dep.topic, group: dep.group, keyspace: dep.keyspace,
		workers: 8, partitions: dep.partitions,
	})

	run.waitForTotal(t, 60)
	assert.Equal(t, 3, groupMembers(t, dep.group), "the pool must cap the workers at the partition count")
}

func TestWrites_OnePollIsOneRowPerPartition(t *testing.T) {
	dep := newDeployment(t, 3)
	publish(t, dep.topic, editStream(300, time.Now()))
	sess := cqlSession(t, dep.keyspace)

	run := dep.run(t, 3)
	run.waitForTotal(t, 300)
	waitUntilCaughtUp(t, dep.group, dep.topic)
	run.Stop(t)

	rows := readDeltas(t, sess, time.Now().UTC())
	require.NotEmpty(t, rows, "300 records must leave at least one row")

	assert.Lessf(t, len(rows), 30, "300 records produced %d rows; a poll should write one row per partition", len(rows))

	var total int64
	for _, r := range rows {
		total += r.Messages
	}
	assert.EqualValues(t, 300, total, "the rows must add up to what was published")
}

func TestDatabaseOutage_NothingIsAcknowledgedAndARestartRecoversEverything(t *testing.T) {
	dep := newDeployment(t, 3)
	publish(t, dep.topic, editStream(90, time.Now()))

	sess := cqlSession(t, dep.keyspace)
	writer := newBrokenWriter(cassandra.NewStatsStore(sess, cassandraConfig(dep.keyspace)))

	outage := startConsumer(t, consumerOptions{
		topic: dep.topic, group: dep.group, keyspace: dep.keyspace,
		workers: 3, partitions: dep.partitions,
		writer: writer,
	})

	// Wait until the consumer has really tried, otherwise the assertions below pass
	// before anything happened.
	require.Eventually(t, func() bool {
		return writer.calls.Load() >= 1
	}, catchUp, 50*time.Millisecond, "the poll loop never reached the writer")

	assert.Empty(t, committedOffsets(t, dep.group, dep.topic),
		"an offset must never be acknowledged while its Delta is not durable")
	assert.Zero(t, outage.snapshot().TotalMessages,
		"the live view must not show a poll that was never written")
	assert.Empty(t, readDeltas(t, sess, time.Now().UTC()), "nothing was written")

	outage.Stop(t)

	recovered := dep.run(t, 3)
	recovered.waitForTotal(t, 90)
	waitUntilCaughtUp(t, dep.group, dep.topic)

	assert.EqualValues(t, 90, recovered.snapshot().TotalMessages,
		"every event must be counted once after the outage, not lost and not doubled")

	var total int64
	for _, r := range readDeltas(t, sess, time.Now().UTC()) {
		total += r.Messages
	}
	assert.EqualValues(t, 90, total, "the stored total must match what was published")
}

func TestBadRecord_ThePartitionKeepsMoving(t *testing.T) {
	// One partition, so the order is the order the records were published in.
	dep := newDeployment(t, 1)
	at := time.Now()

	good := editStream(3, at)
	publish(t, dep.topic, good[:1])
	publishRaw(t, dep.topic, at, []byte{0x08}) // One bad record must not stop the partition behind it.
	publish(t, dep.topic, good[1:])

	run := startConsumer(t, consumerOptions{
		topic: dep.topic, group: dep.group, keyspace: dep.keyspace,
		workers: 1, partitions: 1,
	})
	run.waitForTotal(t, 3)
	waitUntilCaughtUp(t, dep.group, dep.topic)

	assert.EqualValues(t, 3, run.snapshot().TotalMessages, "the three good records are counted")

	consumed := testutil.ToFloat64(run.Events.ConsumedFromRedpanda)
	processed := testutil.ToFloat64(run.Events.Processed)
	failed := testutil.ToFloat64(run.Events.Failed)
	assert.Equal(t, float64(4), consumed, "all four records were fetched")
	assert.Equal(t, float64(1), failed, "the broken one is counted as failed")
	assert.Equal(t, consumed, processed+failed, "consumed must equal processed plus failed")

	assert.Equal(t, endOffsets(t, dep.topic), committedOffsets(t, dep.group, dep.topic),
		"a record that cannot be decoded must still be acknowledged")
}

func TestLateEvents_TheDayComesFromTheRecordNotTheClock(t *testing.T) {
	dep := newDeployment(t, 3)
	yesterday := time.Now().AddDate(0, 0, -1)
	publish(t, dep.topic, editStream(60, yesterday))

	sess := cqlSession(t, dep.keyspace)
	run := dep.run(t, 3)
	run.waitForTotal(t, 60)
	waitUntilCaughtUp(t, dep.group, dep.topic)
	run.Stop(t)

	assert.Empty(t, readDeltas(t, sess, time.Now().UTC()),
		"nothing may land on today: every record carries yesterday's timestamp")

	var total int64
	for _, r := range readDeltas(t, sess, yesterday) {
		total += r.Messages
	}
	assert.EqualValues(t, 60, total, "the rows belong to the day the edits were made")

	restarted := dep.run(t, 3)
	assert.Zero(t, restarted.snapshot().TotalMessages,
		"the startup restore reads today's partition, so late events are not in it")
}
