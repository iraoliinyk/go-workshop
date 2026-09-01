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

const testMaxPollRecords = 500

func testConfig() broker.SubscriberConfig {
	return broker.SubscriberConfig{MaxPollRecords: testMaxPollRecords}
}

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

func (env *testEnv) expectWrites() {
	env.writer.EXPECT().AddDeltas(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
}

func (env *testEnv) expectDecodes(n int) {
	env.dec.EXPECT().Decode(gomock.Any()).
		Return(events.WikiEvent{User: "iryna", ServerURL: "https://ca.wikipedia.org"}, nil).
		Times(n)
}

const testTopic = "wiki"

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
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).
		Return(errors.New("commit crashes")).Times(2)
	env.poller.EXPECT().AllowRebalance().Times(2)
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

	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_EmptyPollCommitsNothing(t *testing.T) {
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
	env.stats.EXPECT().Apply(gomock.Any()).Times(1)
	env.expectWrites()
	env.expectDecodes(2)
	env.poller.EXPECT().CommitRecords(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	env.poller.EXPECT().AllowRebalance().Times(1)

	require.NoError(t, env.sub.Run(env.ctx))
}

func TestRun_SkipsBadRecordAndStillCommitsThePoll(t *testing.T) {
	corruptedPayload := []byte(`{not json`)
	batch := makeRecords(0, 0, 3)
	batch[1].Value = corruptedPayload

	env := newTestEnv(t)
	env.expectPolls(fetchOf(partOf(0, batch)))

	env.dec.EXPECT().Decode(corruptedPayload).
		Return(events.WikiEvent{}, errors.New("bad payload")).Times(1)
	env.dec.EXPECT().Decode(gomock.Not(gomock.Eq(corruptedPayload))).
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
	env.writer.EXPECT().AddDeltas(gomock.Any(), gomock.Any()).
		Return(errors.New("write error")).Times(1)
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
	corruptedPayload := []byte(`{not proto`)
	if bad > 0 {
		env.dec.EXPECT().Decode(corruptedPayload).
			Return(events.WikiEvent{}, errors.New("bad payload")).Times(bad)
	}
	if good > 0 {
		env.dec.EXPECT().Decode(gomock.Not(gomock.Eq(corruptedPayload))).
			Return(events.WikiEvent{User: "iryna", ServerURL: "https://ca.wikipedia.org"}, nil).Times(good)
	}
	return corruptedPayload
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
