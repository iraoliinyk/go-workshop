//go:build integration

package integration

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"wikirecent/internal/applog"
	"wikirecent/internal/broker"
	"wikirecent/internal/codec"
	"wikirecent/internal/db/cassandra"
	"wikirecent/internal/metrics"
	"wikirecent/internal/repository"
	"wikirecent/internal/stats"
	"wikirecent/internal/stats/statsmodels"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// catchUp bounds every "wait until the consumer has counted everything" loop. It
	// is long because a first poll pays for the group to form.
	catchUp = 45 * time.Second

	// stopBudget is cmd/consumer's shutdownTimeout.
	stopBudget = 15 * time.Second

	cqlTimeout = 15 * time.Second
)

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

func botsIn(edits []edit) int64 {
	var n int64
	for _, e := range edits {
		if e.bot {
			n++
		}
	}
	return n
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

func committedOffsets(t *testing.T, group, topic string) map[int32]int64 {
	t.Helper()
	resps, err := admin(t).FetchOffsetsForTopics(context.Background(), group, topic)
	require.NoError(t, err)

	out := map[int32]int64{}
	resps.Each(func(r kadm.OffsetResponse) {
		// Kafka answers -1 for a partition the group never committed. Dropping
		// those is what lets a test say "nothing was acknowledged" with Empty.
		if r.Err == nil && r.Topic == topic && r.At >= 0 {
			out[r.Partition] = r.At
		}
	})
	return out
}

func rewindToStart(t *testing.T, group, topic string) {
	t.Helper()
	adm := admin(t)

	starts, err := adm.ListStartOffsets(context.Background(), topic)
	require.NoError(t, err)
	require.NoError(t, adm.CommitAllOffsets(context.Background(), group, starts.Offsets()))
}

func loseTheLastCommit(t *testing.T, group, topic string, pollRecords int64) {
	t.Helper()
	adm := admin(t)

	var back kadm.Offsets
	for partition, at := range committedOffsets(t, group, topic) {
		back.Add(kadm.Offset{
			Topic:       topic,
			Partition:   partition,
			At:          max(0, at-pollRecords),
			LeaderEpoch: -1,
		})
	}
	require.NotEmpty(t, back, "nothing was committed, so there is no commit to lose")
	require.NoError(t, adm.CommitAllOffsets(context.Background(), group, back))
}

func assertNoOverlap(t *testing.T, rows []deltaRow) {
	t.Helper()
	byPartition := map[int32][]deltaRow{}
	for _, r := range rows {
		byPartition[r.Partition] = append(byPartition[r.Partition], r)
	}

	for partition, partitionRows := range byPartition {
		slices.SortFunc(partitionRows, func(a, b deltaRow) int {
			return cmp.Compare(a.StartOffset, b.StartOffset)
		})
		for i := 1; i < len(partitionRows); i++ {
			prev, this := partitionRows[i-1], partitionRows[i]
			assert.Greaterf(t, this.StartOffset, prev.EndOffset,
				"partition %d: rows %d..%d and %d..%d both claim offset %d\n%s",
				partition, prev.StartOffset, prev.EndOffset, this.StartOffset, this.EndOffset,
				this.StartOffset, formatRows(partitionRows))
		}
	}
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

func waitUntilCaughtUp(t *testing.T, group, topic string) {
	t.Helper()
	want := endOffsets(t, topic)
	require.Eventuallyf(t, func() bool {
		got := committedOffsets(t, group, topic)
		if len(got) != len(want) {
			return false
		}
		for partition, end := range want {
			if got[partition] < end {
				return false
			}
		}
		return true
	}, catchUp, 100*time.Millisecond,
		"the group did not commit up to the end of the topic in %s: want %v, got %v",
		catchUp, want, committedOffsets(t, group, topic))
}

func groupMembers(t *testing.T, group string) int {
	t.Helper()
	groups, err := admin(t).DescribeGroups(context.Background(), group)
	require.NoError(t, err)
	return len(groups[group].Members)
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

func cassandraConfig(keyspace string) cassandra.Config {
	return cassandraConfigAt(cassandraHost, keyspace)
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

func realStore(t *testing.T, keyspace string) *cassandra.StatsStore {
	t.Helper()
	return cassandra.NewStatsStore(cqlSession(t, keyspace), cassandraConfig(keyspace))
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

type deltaRow struct {
	Partition   int32
	StartOffset int64
	EndOffset   int64
	Messages    int64
	BotEdits    int64
	HumanEdits  int64
}

func readDeltas(t *testing.T, sess *gocql.Session, day time.Time) []deltaRow {
	t.Helper()
	const q = `SELECT partition, start_offset, end_offset, total_messages, bot_edits, human_edits
		FROM stats_poll WHERE day = ?`

	iter := sess.Query(q, dayOf(day)).IterContext(context.Background())
	var out []deltaRow
	var r deltaRow
	for iter.Scan(&r.Partition, &r.StartOffset, &r.EndOffset, &r.Messages, &r.BotEdits, &r.HumanEdits) {
		out = append(out, r)
	}
	require.NoError(t, iter.Close())
	return out
}

func dayOf(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

type consumerOptions struct {
	topic          string
	group          string
	keyspace       string
	workers        int
	partitions     int32
	maxPollRecords int
	cassandraNode  string
	writer         broker.DeltaWriter
	parent         context.Context
}

type consumerRun struct {
	Stats  *stats.Stats
	Store  repository.StatsStore
	Events *metrics.Events

	pool    *broker.Pool
	stores  *repository.Stores
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool
}

func startConsumer(t *testing.T, opts consumerOptions) *consumerRun {
	t.Helper()
	parent := opts.parent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)

	node := opts.cassandraNode
	if node == "" {
		node = cassandraHost
	}

	stores, err := repository.New(ctx, repository.Config{
		Backend:   "cassandra",
		Cassandra: cassandraConfigAt(node, opts.keyspace),
	})
	require.NoError(t, err)

	liveStats := stats.New()
	totals, err := stores.Stats.Totals(ctx, time.Now().UTC())
	require.NoError(t, err, "the startup restore must not fail in a test")
	liveStats.Seed(totals)

	writer := broker.DeltaWriter(stores.Stats)
	if opts.writer != nil {
		writer = opts.writer
	}

	events := metrics.NewEvents(metrics.NewRegistry())
	observer := metrics.NewBatchCounters(events.ConsumedFromRedpanda, events.Processed, events.Failed)

	logger, err := applog.New(applog.ModeProd, io.Discard)
	require.NoError(t, err)

	maxPollRecords := opts.maxPollRecords
	if maxPollRecords == 0 {
		maxPollRecords = 1000
	}

	pool, err := broker.NewPool(broker.PoolConfig{
		SubscriberConfig: broker.SubscriberConfig{
			Brokers: []string{kafkaBroker},
			Topic:   opts.topic,
			Group:   opts.group,
			// The production defaults from config.go.
			MaxPollRecords: maxPollRecords,
			FetchMinBytes:  10240,
			FetchMaxWait:   100 * time.Millisecond,
			DrainGrace:     5 * time.Second,
		},
		Workers:       opts.workers,
		MaxPartitions: opts.partitions,
	}, logger, liveStats, codec.EventDecoder{}, writer, observer)
	require.NoError(t, err)
	require.NoError(t, pool.Connect(ctx))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = pool.Run(ctx)
	}()

	run := &consumerRun{
		Stats:  liveStats,
		Store:  stores.Stats,
		Events: events,
		pool:   pool,
		stores: stores,
		cancel: cancel,
		done:   done,
	}
	// A test that stops the run itself is the normal case; this catches the rest,
	// including a test that fails half way.
	t.Cleanup(func() { run.Stop(t) })
	return run
}

func (r *consumerRun) Stop(t *testing.T) {
	t.Helper()
	if r.stopped {
		return
	}
	r.stopped = true

	r.cancel()
	select {
	case <-r.done:
	case <-time.After(stopBudget):
		t.Fatalf("the pool did not stop within %s", stopBudget)
	}
	require.NoError(t, r.pool.Close(context.Background()))
	require.NoError(t, r.stores.Close())
}

func (r *consumerRun) snapshot() statsmodels.Snapshot { return r.Stats.Snapshot() }

func (r *consumerRun) waitForTotal(t *testing.T, want int64) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		return r.snapshot().TotalMessages >= want
	}, catchUp, 50*time.Millisecond,
		"the consumer did not count %d messages in %s; it reached %d",
		want, catchUp, r.snapshot().TotalMessages)
}

type brokenWriter struct {
	inner  broker.DeltaWriter
	broken atomic.Bool
	calls  atomic.Int64
}

func newBrokenWriter(inner broker.DeltaWriter) *brokenWriter {
	w := &brokenWriter{inner: inner}
	w.broken.Store(true)
	return w
}

func (w *brokenWriter) AddDeltas(ctx context.Context, deltas []statsmodels.Delta) error {
	w.calls.Add(1)
	if w.broken.Load() {
		return fmt.Errorf("cassandra is down")
	}
	return w.inner.AddDeltas(ctx, deltas)
}

func (w *brokenWriter) repair() { w.broken.Store(false) }

type crashingWriter struct {
	inner   broker.DeltaWriter
	after   int64  // the write to crash on, counting from 1
	crash   func() // cancels the run's context, so the commit after the write fails
	calls   atomic.Int64
	crashed atomic.Bool
}

func (w *crashingWriter) AddDeltas(ctx context.Context, deltas []statsmodels.Delta) error {
	n := w.calls.Add(1)
	if err := w.inner.AddDeltas(ctx, deltas); err != nil {
		return err
	}
	if n >= w.after && w.crashed.CompareAndSwap(false, true) {
		w.crash()
	}
	return nil
}

func sumMessages(rows []deltaRow) int64 {
	var total int64
	for _, r := range rows {
		total += r.Messages
	}
	return total
}

func formatRows(rows []deltaRow) string {
	if len(rows) == 0 {
		return "  (none)"
	}
	slices.SortFunc(rows, func(a, b deltaRow) int {
		if a.Partition != b.Partition {
			return cmp.Compare(a.Partition, b.Partition)
		}
		return cmp.Compare(a.StartOffset, b.StartOffset)
	})

	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "  partition=%d offsets %d..%-6d messages=%d\n",
			r.Partition, r.StartOffset, r.EndOffset, r.Messages)
	}
	return strings.TrimRight(b.String(), "\n")
}
func overlapping(rows, older []deltaRow) []deltaRow {
	var out []deltaRow
	for _, r := range rows {
		for _, o := range older {
			if r.Partition != o.Partition || r.Key() == o.Key() {
				continue
			}
			if r.StartOffset <= o.EndOffset && o.StartOffset <= r.EndOffset {
				out = append(out, r)
				break
			}
		}
	}
	return out
}

// Key is the row's identity in stats_poll, which is what an overwrite replaces.
func (r deltaRow) Key() [2]int64 { return [2]int64{int64(r.Partition), r.StartOffset} }
