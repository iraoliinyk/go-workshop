//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSmoke_TheHarnessItself fails fast and clearly when the containers are up but
// the wiring around them is wrong, so a broken harness does not look like a broken
// consumer in every other test.
func TestSmoke_TheHarnessItself(t *testing.T) {
	ctx := context.Background()

	topic := newTopic(t, 3)
	require.Equal(t, 3, partitionCount(t, topic), "newTopic must create the partitions it was asked for")

	publish(t, topic, editStream(6, time.Now()))
	assert.Equal(t, map[int32]int64{0: 2, 1: 2, 2: 2}, endOffsets(t, topic),
		"publish must spread the records over the partitions, two each")

	sess := cqlSession(t, uniqueName("ks"))
	var rows int
	require.NoError(t, sess.Query(`SELECT COUNT(*) FROM stats_poll`).ScanContext(ctx, &rows),
		"Connect must create the keyspace and stats_poll")
	assert.Zero(t, rows, "a new keyspace starts empty")
}
