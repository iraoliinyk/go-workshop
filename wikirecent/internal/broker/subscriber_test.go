package broker_test

import (
	"context"
	"errors"
	"testing"

	"wikirecent/internal/applog"
	"wikirecent/internal/broker"
	"wikirecent/internal/events"

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
	}
	env.sub = broker.NewSubscriberWithClient(env.poller, applog.Logger{}, env.stats, env.dec)
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
	sub := broker.NewSubscriberWithClient(poller, applog.Logger{}, NewMockRecorder(ctrl), dec)

	// Plain Close would hang after a poll that never allowed a rebalance.
	poller.EXPECT().CloseAllowingRebalance().Times(1)

	require.NoError(t, sub.Close(context.Background()))
}
