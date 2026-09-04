//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"wikirecent/internal/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	rpcnImage       = "docker.redpanda.com/redpandadata/connect:4.45.1"
	rpcnReadyBudget = time.Minute
)

// runRPCN starts rpcn-connect wired to the shared redpanda and cassandra containers
// over sharedNetwork, running the same deploy/rpcn/ingest.yaml as docker-compose.yaml.
func runRPCN(t *testing.T, topic, dlqTopic, keyspace string) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, rpcnImage,
		network.WithNetwork(nil, sharedNetwork),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{
				HostFilePath:      "../../deploy/rpcn/ingest.yaml",
				ContainerFilePath: "/etc/rpcn/ingest.yaml",
				FileMode:          0o644,
			},

			testcontainers.ContainerFile{
				HostFilePath:      "../../proto/wikirecent/v1/wiki_event.proto",
				ContainerFilePath: "/protos/wikirecent/v1/wiki_event.proto",
				FileMode:          0o644,
			},
		),
		testcontainers.WithCmd("run", "/etc/rpcn/ingest.yaml"),
		testcontainers.WithEnv(map[string]string{
			"REDPANDA_BROKERS":   redpandaNetworkAddr,
			"REDPANDA_TOPIC":     topic,
			"REDPANDA_DLQ_TOPIC": dlqTopic,
			"REDPANDA_GROUP":     uniqueName("rpcn_group"),
			"CASSANDRA_HOSTS":    cassandraNetworkAlias,
			"CASSANDRA_KEYSPACE": keyspace,
		}),
		testcontainers.WithExposedPorts("4195/tcp"),

		testcontainers.WithWaitStrategyAndDeadline(rpcnReadyBudget,
			wait.ForHTTP("/ready").WithPort("4195/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err, "start rpcn-connect")
	time.Sleep(2 * time.Second)
}

// consumeOne reads a single record from topic, or fails the test after timeout.
func consumeOne(t *testing.T, topic string, timeout time.Duration) *kgo.Record {
	t.Helper()
	cl := kafkaClient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		var found *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if found == nil {
				found = r
			}
		})
		if found != nil {
			return found
		}
	}
	require.Fail(t, "no record arrived", "topic %q produced nothing within %s", topic, timeout)
	return nil
}

func TestRPCN_IngestsValidRecordsAndRoutesBadOnesToDLQ(t *testing.T) {
	topic := newTopic(t, 3)
	dlqTopic := newTopic(t, 1)
	keyspace := uniqueName("ks")

	stores, err := repository.New(context.Background(), repository.Config{
		Backend:   "cassandra",
		Cassandra: cassandraConfigAt(cassandraHost, keyspace),
	})
	require.NoError(t, err, "bootstrap the keyspace rpcn-connect will write into")
	t.Cleanup(func() { _ = stores.Close() })

	runRPCN(t, topic, dlqTopic, keyspace)

	at := time.Now()
	edits := editStream(8, at)
	publish(t, topic, edits)

	badPayload := []byte("not a valid protobuf message")
	publishRaw(t, topic, at, badPayload)

	wantBotEdits := int64(0)
	wantUsers := map[string]bool{}
	for _, e := range edits {
		if e.bot {
			wantBotEdits++
		}
		wantUsers[e.user] = true
	}

	require.Eventually(t, func() bool {
		s, err := stores.Stats.Totals(context.Background(), at)
		return err == nil && s.TotalMessages == int64(len(edits))
	}, 45*time.Second, 500*time.Millisecond,
		"rpcn-connect must fold every valid record into stats_poll for %s", at)

	snap, err := stores.Stats.Totals(context.Background(), at)
	require.NoError(t, err)
	assert.Equal(t, wantBotEdits, snap.BotEdits, "bot_edits")
	assert.Equal(t, int64(len(edits))-wantBotEdits, snap.HumanEdits, "human_edits")
	assert.Equal(t, int64(len(wantUsers)), snap.DistinctUsers, "distinct_users")

	dlqRecord := consumeOne(t, dlqTopic, 30*time.Second)
	assert.Equal(t, badPayload, dlqRecord.Value,
		"the dlq leg must forward the original bytes untouched, not a reconstruction")
}
