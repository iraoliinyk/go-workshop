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

// testEnv wires a Subscriber onto the generated mocks, with no broker behind it.
// Expectations are declared per test; anything not expected fails the test.
type testEnv struct {
	sub    *broker.Subscriber
	poller *MockRecordPoller
	stats  *MockRecorder
	ctx    context.Context
	cancel context.CancelFunc
	dec    *MockDecoder
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
		obs:    NewMockBatchObserver(ctrl),
	}

	env.obs.EXPECT().Consumed(gomock.Any()).AnyTimes()
	env.obs.EXPECT().Processed().AnyTimes()
	env.obs.EXPECT().Failed().AnyTimes()

	env.sub = broker.NewSubscriberWithClient(env.poller, applog.Logger{}, env.stats, env.dec, env.obs)
	return env
}

// expectPolls scripts the poll loop: each fetch is returned once, in order, and
// every poll after that cancels the context so Run leaves through its own
// graceful-stop path instead of spinning.
func (env *testEnv) expectPolls(fetches ...kgo.Fetches) {
	for _, f := range fetches {
		env.poller.EXPECT().PollRecords(gomock.Any(), gomock.Any()).Return(f)
	}
	env.poller.EXPECT().PollRecords(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, int) kgo.Fetches {
			env.cancel()
			return nil
		}).AnyTimes()
}

func (env *testEnv) expectDecodes(n int) {
	env.dec.EXPECT().Decode(gomock.Any()).
		Return(events.WikiEvent{User: "iryna", ServerURL: "https://ca.wikipedia.org"}, nil).
		Times(n)
}

// The topic has 3 partitions. Fixtures name a partition explicitly, because the
// per-partition fan-out is one of the two things under test.
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
	env.stats.EXPECT().Record(gomock.Any()).Times(5)
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
	env.stats.EXPECT().Record(gomock.Any()).Times(4)
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

	// No CommitRecords and no Record expectation: either call fails the test. An
	// error-only fetch carries no records, so there is nothing to run and nothing to
	// acknowledge. AllowRebalance must still pair with the poll.
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

	env.stats.EXPECT().Record(gomock.Any()).Times(2)
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

	// Exactly 2 of the 3 records may reach the recorder. The commit still happens:
	// a record that can never be decoded must not hold up its partition.
	env.dec.EXPECT().Decode(bad).
		Return(events.WikiEvent{}, errors.New("bad payload")).Times(1)
	env.dec.EXPECT().Decode(gomock.Not(gomock.Eq(bad))).
		Return(events.WikiEvent{User: "iryna", ServerURL: "https://ca.wikipedia.org"}, nil).Times(2)

	env.stats.EXPECT().Record(gomock.Any()).Times(2)
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_ReturnsNilOnShutdown(t *testing.T) {
	env := newTestEnv(t)
	env.cancel()
	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_ShutdownMidBatchCommitsNothing(t *testing.T) {
	env := newTestEnv(t)
	env.expectPolls(fetchOf(partOf(0, makeRecords(0, 0, 3))))

	// Cancelling from inside Record is what puts the shutdown in the middle of the
	// batch. No CommitRecords expectation: a commit here would acknowledge records
	// whose batch never finished.
	env.expectDecodes(3)
	env.stats.EXPECT().Record(gomock.Any()).Times(3).
		Do(func(events.WikiEvent) { env.cancel() })
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestClose_AllowsRebalance(t *testing.T) {
	ctrl := gomock.NewController(t)
	poller := NewMockRecordPoller(ctrl)
	dec := NewMockDecoder(ctrl)
	obs := NewMockBatchObserver(ctrl)
	sub := broker.NewSubscriberWithClient(poller, applog.Logger{}, NewMockRecorder(ctrl), dec, obs)

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
	counters *metrics.Events
}

func newCountedEnv(t *testing.T) *countedEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	counters := metrics.NewEvents(prometheus.NewRegistry())
	env := &countedEnv{
		poller:   NewMockRecordPoller(ctrl),
		dec:      NewMockDecoder(ctrl),
		stats:    NewMockRecorder(ctrl),
		counters: counters,
	}
	env.sub = broker.NewSubscriberWithClient(env.poller, applog.Logger{}, env.stats, env.dec,
		metrics.NewBatchCounters(counters.ConsumedFromRedpanda, counters.Processed, counters.Failed))
	return env
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
		env.stats.EXPECT().Record(gomock.Any()).Times(good)
	}
	return badPayload
}

func TestHandleBatch_CountsConsumedProcessedAndFailed(t *testing.T) {
	env := newCountedEnv(t)
	batch := makeRecords(0, 0, 3)
	batch[1].Value = env.expectDecodeMix(1, 2)

	require.NoError(t, env.sub.HandleBatch(context.Background(), batch))

	assert.Equal(t, 3.0, testutil.ToFloat64(env.counters.ConsumedFromRedpanda))
	assert.Equal(t, 2.0, testutil.ToFloat64(env.counters.Processed))
	assert.Equal(t, 1.0, testutil.ToFloat64(env.counters.Failed))
}

// The dashboard draws consumed and processed on one chart and reads the distance
// between them as the failure rate. A lost record makes that chart lie silently.
func TestHandleBatch_KeepsConsumedEqualToProcessedPlusFailed(t *testing.T) {
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

			require.NoError(t, env.sub.HandleBatch(context.Background(), batch))

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

// A batch abandoned at shutdown was never consumed. Counting it would leave a
// permanent gap between consumed and processed+failed after every restart.
func TestHandleBatch_CountsNothingWhenTheContextIsAlreadyDone(t *testing.T) {
	env := newCountedEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// No Decode or Record expectation: HandleBatch must return before either.
	require.Error(t, env.sub.HandleBatch(ctx, makeRecords(0, 0, 3)))

	assert.Zero(t, testutil.ToFloat64(env.counters.ConsumedFromRedpanda))
	assert.Zero(t, testutil.ToFloat64(env.counters.Processed))
	assert.Zero(t, testutil.ToFloat64(env.counters.Failed))
}

// Only meaningful under -race: it is what proves the counters are safe for the
// concurrent HandleBatch goroutines Run fans out.
func TestRun_CountsAcrossConcurrentSubBatches(t *testing.T) {
	env := newCountedEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Over maxBatchSize (100), so each partition splits in two: six sub-batches.
	const perPartition = 150
	total := 3 * perPartition

	env.poller.EXPECT().PollRecords(gomock.Any(), gomock.Any()).Return(fetchOf(
		partOf(0, makeRecords(0, 0, perPartition)),
		partOf(1, makeRecords(1, 0, perPartition)),
		partOf(2, makeRecords(2, 0, perPartition)),
	))
	env.poller.EXPECT().PollRecords(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, int) kgo.Fetches {
			cancel()
			return nil
		}).AnyTimes()

	env.expectDecodeMix(0, total)
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(ctx))

	assert.Equal(t, float64(total), testutil.ToFloat64(env.counters.ConsumedFromRedpanda))
	assert.Equal(t, float64(total), testutil.ToFloat64(env.counters.Processed))
	assert.Zero(t, testutil.ToFloat64(env.counters.Failed))
}
