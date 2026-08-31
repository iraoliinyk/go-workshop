package broker_test

import (
	"context"
	"errors"
	"testing"

	"wikirecent/internal/applog"
	"wikirecent/internal/broker"
	"wikirecent/internal/events"
	"wikirecent/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
)

const validEvent = `{"user":"iryna","bot":false,"server_url":"https://ca.wikipedia.org"}`

// testMaxPollRecords is matched exactly in every PollRecords expectation, so a config
// that never reaches the Subscriber shows up here as a failed expectation. franz-go
// reads 0 as "no limit", which is why a dropped config would otherwise be silent.
const testMaxPollRecords = 500

func testConfig() broker.SubscriberConfig {
	return broker.SubscriberConfig{MaxPollRecords: testMaxPollRecords}
}

// scriptPolls returns each fetch once, in order, and cancels the context on every
// poll after that, so Run leaves through its own graceful-stop path instead of
// spinning.
func scriptPolls(poller *MockRecordPoller, cancel context.CancelFunc, fetches ...kgo.Fetches) {
	for _, f := range fetches {
		poller.EXPECT().PollRecords(gomock.Any(), testMaxPollRecords).Return(f)
	}
	poller.EXPECT().PollRecords(gomock.Any(), testMaxPollRecords).
		DoAndReturn(func(context.Context, int) kgo.Fetches {
			cancel()
			return nil
		}).AnyTimes()
}

// testEnv wires a Subscriber onto the generated mocks, with no broker behind it.
// Expectations are declared per test; anything not expected fails the test.
type testEnv struct {
	sub    *broker.Subscriber
	poller *MockRecordPoller
	stats  *MockRecorder
	ctx    context.Context
	cancel context.CancelFunc
	dec    *MockDecoder
	writer *MockDeltaWriter
	obs    *MockBatchObserver
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	ctrl := gomock.NewController(t) // Finish runs via t.Cleanup
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	env := &testEnv{
		poller: NewMockRecordPoller(ctrl),
		stats:  NewMockRecorder(ctrl),
		ctx:    ctx,
		cancel: cancel,
		dec:    NewMockDecoder(ctrl),
		writer: NewMockDeltaWriter(ctrl),
		obs:    NewMockBatchObserver(ctrl),
	}

	env.obs.EXPECT().Consumed(gomock.Any()).AnyTimes()
	env.obs.EXPECT().Processed().AnyTimes()
	env.obs.EXPECT().Failed().AnyTimes()

	env.sub = broker.NewSubscriberWithClient(env.poller, applog.Logger{}, env.stats, env.dec,
		env.writer, env.obs, testConfig())
	return env
}

func (env *testEnv) expectPolls(fetches ...kgo.Fetches) {
	scriptPolls(env.poller, env.cancel, fetches...)
}

// expectWrites lets every delta write succeed. The test about a failed write sets its
// own expectation instead, because an AnyTimes stub declared here would match first.
func (env *testEnv) expectWrites() {
	env.writer.EXPECT().AddDeltas(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
}

func (env *testEnv) expectDecodes(n int) {
	env.dec.EXPECT().Decode(gomock.Any()).
		Return(events.WikiEvent{User: "iryna", ServerURL: "https://ca.wikipedia.org"}, nil).
		Times(n)
}

// The topic has 3 partitions. Fixtures name a partition explicitly, because one Delta
// per partition is one of the two things under test.
const testTopic = "wiki"

// makeRecords builds count records on one partition, starting at firstOffset.
func makeRecords(partition int32, firstOffset int64, count int) []*kgo.Record {
	out := make([]*kgo.Record, 0, count)
	for i := range count {
		out = append(out, &kgo.Record{
			Topic:     testTopic,
			Partition: partition,
			Offset:    firstOffset + int64(i),
			Value:     []byte(validEvent),
		})
	}
	return out
}

func partOf(partition int32, recs []*kgo.Record) kgo.FetchPartition {
	return kgo.FetchPartition{Partition: partition, Records: recs}
}

// fetchOf wraps partitions of one topic the way a single poll returns them.
func fetchOf(parts ...kgo.FetchPartition) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      testTopic,
		Partitions: parts,
	}}}}
}

func TestRun_CommitsOnceWithEveryRecordOfThePoll(t *testing.T) {
	env := newTestEnv(t)
	env.expectPolls(fetchOf(
		partOf(0, makeRecords(0, 400, 3)),
		partOf(1, makeRecords(1, 900, 2)),
	))

	var committed []*kgo.Record
	// Times(1) is the assertion: a second commit for the same poll fails here.
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, rs ...*kgo.Record) error {
			committed = rs
			return nil
		}).Times(1)
	env.poller.EXPECT().AllowRebalance().Times(1)
	// Two partitions in the poll, so two Deltas — not one per record.
	env.stats.EXPECT().Apply(gomock.Any()).Times(2)
	env.expectWrites()
	env.expectDecodes(5)

	require.NoError(t, env.sub.Run(env.ctx))

	assert.Len(t, committed, 5, "every record of the poll, across both partitions")

	// CommitRecords derives the highest offset+1 per partition, so the highest record
	// of EVERY partition has to be in the slice or that partition's offset lags.
	highest := map[int32]int64{}
	for _, r := range committed {
		if r.Offset > highest[r.Partition] {
			highest[r.Partition] = r.Offset
		}
	}
	assert.Equal(t, map[int32]int64{0: 402, 1: 901}, highest)
}

func TestRun_CommitFailureIsLoggedAndTheLoopContinues(t *testing.T) {
	poll := fetchOf(partOf(0, makeRecords(0, 0, 2)))

	env := newTestEnv(t)
	env.expectPolls(poll, poll)

	// Both commits fail. Two AllowRebalance calls prove the loop went round again
	// instead of stopping, and the records stay counted: redelivery is expected.
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).
		Return(errors.New("commit boom")).Times(2)
	env.poller.EXPECT().AllowRebalance().Times(2)
	env.stats.EXPECT().Apply(gomock.Any()).Times(2)
	env.expectWrites()
	env.expectDecodes(4)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_FetchErrorCommitsNothing(t *testing.T) {
	broken := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      testTopic,
		Partitions: []kgo.FetchPartition{{Partition: 0, Err: errors.New("unknown topic")}},
	}}}}

	env := newTestEnv(t)
	env.expectPolls(broken)

	// No CommitRecords, no AddDeltas and no Apply expectation: any of those calls
	// fails the test. An error-only fetch carries no records, so there is nothing to
	// aggregate and nothing to acknowledge. AllowRebalance must still pair with the poll.
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_EmptyPollCommitsNothing(t *testing.T) {
	// FetchMaxWait expiring with no new records is the common case, not an edge one.
	env := newTestEnv(t)
	env.expectPolls(fetchOf(partOf(0, nil)))

	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_EmptyPartitionsWithinAPollAreHarmless(t *testing.T) {
	env := newTestEnv(t)
	env.expectPolls(fetchOf(
		partOf(0, nil),
		partOf(1, makeRecords(1, 0, 2)),
		partOf(2, nil),
	))

	// One Delta, not three: a partition with no records produces none.
	env.stats.EXPECT().Apply(gomock.Any()).Times(1)
	env.expectWrites()
	env.expectDecodes(2)
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_SkipsBadRecordAndStillCommitsThePoll(t *testing.T) {
	bad := []byte(`{not json`)
	batch := makeRecords(0, 0, 3)
	batch[1].Value = bad

	env := newTestEnv(t)
	env.expectPolls(fetchOf(partOf(0, batch)))

	// Exactly 2 of the 3 records reach the Delta counters. The commit still happens:
	// a record that can never be decoded must not hold up its partition.
	env.dec.EXPECT().Decode(bad).
		Return(events.WikiEvent{}, errors.New("bad payload")).Times(1)
	env.dec.EXPECT().Decode(gomock.Not(gomock.Eq(bad))).
		Return(events.WikiEvent{User: "iryna", ServerURL: "https://ca.wikipedia.org"}, nil).Times(2)

	env.stats.EXPECT().Apply(gomock.Any()).Times(1)
	env.expectWrites()
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_ReturnsNilOnShutdown(t *testing.T) {
	env := newTestEnv(t)
	env.cancel()
	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_WriteFailureCommitsNothing(t *testing.T) {
	env := newTestEnv(t)
	env.expectPolls(fetchOf(partOf(0, makeRecords(0, 0, 3))))

	// No CommitRecords and no Apply expectation: either call fails the test. An offset
	// must never be acknowledged while its Delta is not durable, and the in-memory view
	// must not show a poll that was never written.
	env.writer.EXPECT().AddDeltas(gomock.Any(), gomock.Any()).
		Return(errors.New("write boom")).Times(1)
	env.expectDecodes(3)
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestClose_AllowsRebalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	poller := NewMockRecordPoller(ctrl)
	dec := NewMockDecoder(ctrl)
	obs := NewMockBatchObserver(ctrl)
	sub := broker.NewSubscriberWithClient(poller, applog.Logger{}, NewMockRecorder(ctrl), dec,
		NewMockDeltaWriter(ctrl), obs, testConfig())

	// Plain Close would hang after a poll that never allowed a rebalance.
	poller.EXPECT().CloseAllowingRebalance().Times(1)

	require.NoError(t, sub.Close(context.Background()))
}

// countedEnv uses real counters, not MockBatchObserver, so a test asserts the
// numbers rather than a call count.
type countedEnv struct {
	sub      *broker.Subscriber
	poller   *MockRecordPoller
	dec      *MockDecoder
	stats    *MockRecorder
	writer   *MockDeltaWriter
	counters *metrics.Events
	ctx      context.Context
	cancel   context.CancelFunc
}

func newCountedEnv(t *testing.T) *countedEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	counters := metrics.NewEvents(prometheus.NewRegistry())
	env := &countedEnv{
		poller:   NewMockRecordPoller(ctrl),
		dec:      NewMockDecoder(ctrl),
		stats:    NewMockRecorder(ctrl),
		writer:   NewMockDeltaWriter(ctrl),
		counters: counters,
		ctx:      ctx,
		cancel:   cancel,
	}

	// These tests assert the Prometheus counters. Everything the poll loop does around
	// them is allowed but not counted, so the assertions stay about the numbers.
	env.stats.EXPECT().Apply(gomock.Any()).AnyTimes()
	env.writer.EXPECT().AddDeltas(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	env.poller.EXPECT().AllowRebalance().AnyTimes()

	env.sub = broker.NewSubscriberWithClient(env.poller, applog.Logger{}, env.stats, env.dec,
		env.writer,
		metrics.NewBatchCounters(counters.ConsumedFromRedpanda, counters.Processed, counters.Failed),
		testConfig())
	return env
}

func (env *countedEnv) expectPolls(fetches ...kgo.Fetches) {
	scriptPolls(env.poller, env.cancel, fetches...)
}

func (env *countedEnv) expectDecodeMix(bad, good int) []byte {
	badPayload := []byte(`{not proto`)
	if bad > 0 {
		env.dec.EXPECT().Decode(badPayload).
			Return(events.WikiEvent{}, errors.New("bad payload")).Times(bad)
	}
	if good > 0 {
		env.dec.EXPECT().Decode(gomock.Not(gomock.Eq(badPayload))).
			Return(events.WikiEvent{User: "iryna", ServerURL: "https://ca.wikipedia.org"}, nil).Times(good)
	}
	return badPayload
}

func TestRun_CountsConsumedProcessedAndFailed(t *testing.T) {
	env := newCountedEnv(t)
	batch := makeRecords(0, 0, 3)
	batch[1].Value = env.expectDecodeMix(1, 2)
	env.expectPolls(fetchOf(partOf(0, batch)))

	require.NoError(t, env.sub.Run(env.ctx))

	assert.Equal(t, 3.0, testutil.ToFloat64(env.counters.ConsumedFromRedpanda))
	assert.Equal(t, 2.0, testutil.ToFloat64(env.counters.Processed))
	assert.Equal(t, 1.0, testutil.ToFloat64(env.counters.Failed))
}

// The dashboard draws consumed and processed on one chart and reads the distance
// between them as the failure rate. A lost record makes that chart lie silently.
func TestRun_KeepsConsumedEqualToProcessedPlusFailed(t *testing.T) {
	tests := []struct {
		name string
		size int
		bad  []int
	}{
		{name: "all good", size: 4},
		{name: "all bad", size: 4, bad: []int{0, 1, 2, 3}},
		{name: "first bad", size: 4, bad: []int{0}},
		{name: "last bad", size: 4, bad: []int{3}},
		{name: "alternating", size: 5, bad: []int{1, 3}},
		{name: "single good record", size: 1},
		{name: "single bad record", size: 1, bad: []int{0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newCountedEnv(t)
			good := tt.size - len(tt.bad)
			badPayload := env.expectDecodeMix(len(tt.bad), good)

			batch := makeRecords(0, 0, tt.size)
			for _, i := range tt.bad {
				batch[i].Value = badPayload
			}
			env.expectPolls(fetchOf(partOf(0, batch)))

			require.NoError(t, env.sub.Run(env.ctx))

			consumed := testutil.ToFloat64(env.counters.ConsumedFromRedpanda)
			processed := testutil.ToFloat64(env.counters.Processed)
			failed := testutil.ToFloat64(env.counters.Failed)

			assert.Equal(t, float64(tt.size), consumed)
			assert.Equal(t, float64(good), processed)
			assert.Equal(t, float64(len(tt.bad)), failed)
			assert.Equal(t, consumed, processed+failed, "consumed must equal processed + failed")
		})
	}
}

// One poll spanning every partition of the topic. Consumed is counted once for the
// whole poll, while Processed is counted per record, so the two only agree if the
// per-partition fold visits every record exactly once.
func TestRun_CountsEveryPartitionOfOnePoll(t *testing.T) {
	env := newCountedEnv(t)

	const perPartition = 150
	total := 3 * perPartition

	env.expectPolls(fetchOf(
		partOf(0, makeRecords(0, 0, perPartition)),
		partOf(1, makeRecords(1, 0, perPartition)),
		partOf(2, makeRecords(2, 0, perPartition)),
	))
	env.expectDecodeMix(0, total)

	require.NoError(t, env.sub.Run(env.ctx))

	assert.Equal(t, float64(total), testutil.ToFloat64(env.counters.ConsumedFromRedpanda))
	assert.Equal(t, float64(total), testutil.ToFloat64(env.counters.Processed))
	assert.Zero(t, testutil.ToFloat64(env.counters.Failed))
}
