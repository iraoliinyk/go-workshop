//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"wikirecent/internal/codec"
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

func TestRPCN_ACanaryEventInTheBatchDoesNotStallIt(t *testing.T) {
	topic := newTopic(t, 1)
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
	canary := []byte(`{"meta":{"domain":"canary","stream":"mediawiki.recentchange","dt":"2026-01-01T00:00:00Z"}}`)
	canaryWire, err := codec.EncodeFromJSON(canary)
	require.NoError(t, err)

	edits := editStream(4, at)
	wantUsers := map[string]bool{}
	for _, e := range edits {
		wantUsers[e.user] = true
	}

	// One publishRaw call, one ProduceSync: the canary and the good edits' wire bytes go to the
	// broker together, which is what lands them in the same rpcn-connect batch.
	values := [][]byte{canaryWire}
	for _, e := range edits {
		values = append(values, e.wire(t))
	}
	publishRaw(t, topic, at, values...)

	// +1: the canary is not a decode failure — it is valid protobuf, just with every field but
	// meta absent — so it counts toward batch_size()/total_messages like any other record.
	wantMessages := int64(len(edits)) + 1

	require.Eventually(t, func() bool {
		s, err := stores.Stats.Totals(context.Background(), at)
		return err == nil && s.TotalMessages == wantMessages
	}, 45*time.Second, 500*time.Millisecond,
		"a canary event in the batch must not block the real edits behind it from reaching stats_poll")

	snap, err := stores.Stats.Totals(context.Background(), at)
	require.NoError(t, err)
	assert.Equal(t, int64(len(wantUsers)), snap.DistinctUsers,
		"the canary's absent user must not count as a null distinct user")

	// The strongest confirmation the partition is not stuck: a second, later batch also lands.
	moreEdits := editStream(3, at)
	publish(t, topic, moreEdits)

	require.Eventually(t, func() bool {
		s, err := stores.Stats.Totals(context.Background(), at)
		return err == nil && s.TotalMessages == wantMessages+int64(len(moreEdits))
	}, 45*time.Second, 500*time.Millisecond,
		"the partition must keep advancing after the canary's batch, not stall on it")
}
