//go:build integration

package integration

import (
	"context"
	"testing"

	"wikirecent/internal/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColdStart_BootstrapCreatesEverySchemaTable(t *testing.T) {
	node := newCassandraNode(t)
	keyspace := uniqueName("ks")

	stores, err := repository.New(context.Background(), repository.Config{
		Backend:   "cassandra",
		Cassandra: cassandraConfigAt(node, keyspace),
	})
	require.NoError(t, err)
	defer func() { _ = stores.Close() }()

	sess := cqlSessionAt(t, node, keyspace)
	assert.ElementsMatch(t,
		[]string{"stats_snapshot", "server_snapshot", "user_accounts",
			"revoked_tokens", "stats_poll", "distinct_users"},
		tableNames(t, sess, keyspace),
		"bootstrap must create every table on a fresh node")
}
