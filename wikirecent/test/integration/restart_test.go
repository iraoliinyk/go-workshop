//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const backlog = 3000

const pollSize = 25

func runFirstHalf(t *testing.T, dep deployment) []deltaRow {
	t.Helper()
	publish(t, dep.topic, editStream(backlog, time.Now()))

	first := startConsumer(t, consumerOptions{
		topic: dep.topic, group: dep.group, keyspace: dep.keyspace,
		workers: 3, partitions: dep.partitions, maxPollRecords: pollSize,
	})
	require.Eventually(t, func() bool {
		return first.snapshot().TotalMessages > 0
	}, catchUp, 10*time.Millisecond, "the first run never counted anything")
	first.Stop(t)

	require.Less(t, first.snapshot().TotalMessages, int64(backlog),
		"the stop has to land mid-stream for this test to mean anything")

	return readDeltas(t, cqlSession(t, dep.keyspace), time.Now().UTC())
}

func finish(t *testing.T, dep deployment, maxPollRecords int) *consumerRun {
	t.Helper()
	run := startConsumer(t, consumerOptions{
		topic: dep.topic, group: dep.group, keyspace: dep.keyspace,
		workers: 3, partitions: dep.partitions, maxPollRecords: maxPollRecords,
	})
	run.waitForTotal(t, backlog)
	waitUntilCaughtUp(t, dep.group, dep.topic)
	return run
}

func TestRestartMidStream_NothingIsLostAndNothingIsDoubled(t *testing.T) {
	dep := newDeployment(t, 3)
	sess := cqlSession(t, dep.keyspace)

	runFirstHalf(t, dep)
	second := finish(t, dep, pollSize)
	second.Stop(t)

	rows := readDeltas(t, sess, time.Now().UTC())
	assertNoOverlap(t, rows)
	assert.EqualValuesf(t, backlog, sumMessages(rows),
		"the stored total must be exactly what was published.\nrows:\n%s", formatRows(rows))

	restored := dep.run(t, 3)
	assert.EqualValues(t, backlog, restored.snapshot().TotalMessages,
		"a restart must serve exactly what was published")
}

func TestLiveViewAfterARestart_MustMatchTheStoredTotal(t *testing.T) {
	dep := newDeployment(t, 3)
	sess := cqlSession(t, dep.keyspace)

	runFirstHalf(t, dep)
	second := finish(t, dep, pollSize)

	stored := sumMessages(readDeltas(t, sess, time.Now().UTC()))
	assert.EqualValuesf(t, backlog, stored, "the stored total must be exactly what was published")
	assert.EqualValuesf(t, stored, second.snapshot().TotalMessages,
		"the view of the run that finished the backlog must match the stored total of %d", stored)
}

func TestCrashBetweenWriteAndCommit_TheReplayRewritesTheSameRows(t *testing.T) {
	dep := newDeployment(t, 3)
	sess := cqlSession(t, dep.keyspace)

	before := runFirstHalf(t, dep)
	loseTheLastCommit(t, dep.group, dep.topic, pollSize)
	t.Logf("staged a lost commit; %d rows are durable, committed offsets now %v",
		len(before), committedOffsets(t, dep.group, dep.topic))

	restarted := finish(t, dep, pollSize)
	restarted.Stop(t)

	after := readDeltas(t, sess, time.Now().UTC())
	assertNoOverlap(t, after)
	assert.EqualValuesf(t, backlog, sumMessages(after),
		"a replay after a lost commit must not change the stored total.\nbefore:\n%s\nrows that overlap them:\n%s",
		formatRows(before), formatRows(overlapping(after, before)))

	restored := dep.run(t, 3)
	assert.EqualValues(t, backlog, restored.snapshot().TotalMessages,
		"the restored view must be exactly what was published")
}
