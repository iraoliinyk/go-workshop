//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"wikirecent/internal/codec"
	"wikirecent/internal/db/cassandra"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const cqlTimeout = 15 * time.Second

var nameCounter atomic.Int64

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, nameCounter.Add(1))
}

func kafkaClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(kafkaBroker)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}

func admin(t *testing.T) *kadm.Client {
	t.Helper()
	require.NotNil(t, sharedAdmin, "TestMain did not build the admin client")
	return sharedAdmin
}

// newTopic creates a topic the way docker-compose.yaml creates the real one.
func newTopic(t *testing.T, partitions int32) string {
	t.Helper()
	name := uniqueName("wiki_it")
	createTime := "CreateTime"

	resp, err := admin(t).CreateTopic(context.Background(), partitions, 1,
		map[string]*string{"message.timestamp.type": &createTime}, name)
	require.NoError(t, err)
	require.NoError(t, resp.Err)
	return name
}

// edit is one wiki event as a user would see it in the stream.
type edit struct {
	user      string
	bot       bool
	serverURL string
	at        time.Time // the record timestamp, and therefore the day of its Delta
}

// wire encodes the edit the way cmd/producer does: Wikimedia JSON in, protobuf out.
func (e edit) wire(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"user":       e.user,
		"bot":        e.bot,
		"server_url": e.serverURL,
	})
	require.NoError(t, err)

	value, err := codec.EncodeFromJSON(raw)
	require.NoError(t, err)
	return value
}

// editStream builds n events that look like the real stream: a few repeated users,
// one bot in every four, two servers.
func editStream(n int, at time.Time) []edit {
	users := []string{"Salvia Brandybuck", "Dandelia Smallburrow", "Ruby Fairbairn", "Gilly Townsend"}
	servers := []string{"https://uk.wikipedia.org", "https://en.wikipedia.org"}

	out := make([]edit, 0, n)
	for i := range n {
		out = append(out, edit{
			user:      users[i%len(users)],
			bot:       i%4 == 0,
			serverURL: servers[i%len(servers)],
			at:        at,
		})
	}
	return out
}

func publish(t *testing.T, topic string, edits []edit) {
	t.Helper()
	values := make([][]byte, 0, len(edits))
	for _, e := range edits {
		values = append(values, e.wire(t))
	}
	// Every edit of one call carries the same timestamp in these tests.
	at := time.Now()
	if len(edits) > 0 {
		at = edits[0].at
	}
	publishRaw(t, topic, at, values...)
}

func publishRaw(t *testing.T, topic string, at time.Time, values ...[]byte) {
	t.Helper()
	cl := kafkaClient(t,
		kgo.DefaultProduceTopic(topic),
		kgo.DefaultProduceTopicAlways(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	partitions := int32(partitionCount(t, topic))
	records := make([]*kgo.Record, 0, len(values))
	for i, v := range values {
		records = append(records, &kgo.Record{
			Value:     v,
			Timestamp: at,
			Partition: int32(i) % partitions,
		})
	}
	require.NoError(t, cl.ProduceSync(context.Background(), records...).FirstErr())
}

func partitionCount(t *testing.T, topic string) int {
	t.Helper()
	topics, err := admin(t).ListTopics(context.Background(), topic)
	require.NoError(t, err)
	require.Contains(t, topics, topic)
	return len(topics[topic].Partitions)
}

func endOffsets(t *testing.T, topic string) map[int32]int64 {
	t.Helper()
	listed, err := admin(t).ListEndOffsets(context.Background(), topic)
	require.NoError(t, err)
	require.NoError(t, listed.Error())

	out := map[int32]int64{}
	listed.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			out[o.Partition] = o.Offset
		}
	})
	return out
}

func newCassandraNode(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	node, err := runCassandra(ctx)
	testcontainers.CleanupContainer(t, node)
	require.NoError(t, err, "start a Cassandra node for this test")

	host, err := node.ConnectionHost(ctx)
	require.NoError(t, err)
	return host
}

func cassandraConfigAt(host, keyspace string) cassandra.Config {
	return cassandra.Config{
		Hosts:             []string{host},
		Keyspace:          keyspace,
		ReplicationFactor: 1,
		Consistency:       gocql.LocalQuorum,
		Timeout:           cqlTimeout,
	}
}

func cqlSession(t *testing.T, keyspace string) *gocql.Session {
	t.Helper()
	return cqlSessionAt(t, cassandraHost, keyspace)
}

func cqlSessionAt(t *testing.T, host, keyspace string) *gocql.Session {
	t.Helper()
	sess, err := cassandra.Connect(context.Background(), cassandraConfigAt(host, keyspace))
	require.NoError(t, err)
	t.Cleanup(sess.Close)
	return sess
}

func tableNames(t *testing.T, sess *gocql.Session, keyspace string) []string {
	t.Helper()
	const q = `SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`

	iter := sess.Query(q, keyspace).IterContext(context.Background())
	var out []string
	var name string
	for iter.Scan(&name) {
		out = append(out, name)
	}
	require.NoError(t, iter.Close())
	return out
}
