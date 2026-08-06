package flusher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"ch-3/internal/stats/statsmodels"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// call records one SaveSnapshot call: when it happened, whether the context
// was still alive at that moment, and what data was saved.
type call struct {
	at     time.Time
	ctxErr error
	snap   statsmodels.Snapshot
}

type fakeSink struct {
	mu sync.Mutex
	// one entry per call: the time, the state of the context, and the snapshot
	calls []call
	// errors to return, one per call. The list becomes empty and then we
	// return nil, so a test can make only the first save fail.
	errs []error
}

func (s *fakeSink) SaveSnapshot(ctx context.Context, at time.Time, snap statsmodels.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call{at: at, ctxErr: ctx.Err(), snap: snap})
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return err
	}
	return nil
}

// newTestFlusher builds a Flusher whose clock the test owns: sending a value into the
// returned tick channel makes it save once. The stopped channel is closed when the
// flusher stops its ticker.
func newTestFlusher(t *testing.T, s Snapshotter, sink Sink) (*Flusher, chan time.Time, chan struct{}) {
	t.Helper()

	// require, not assert: if New fails there is no Flusher to work with, and
	// the test would panic on a nil pointer instead of showing this error.
	f, err := New(s, sink, Config{Interval: time.Hour})
	require.NoError(t, err, "New must accept a valid test config")

	tick := make(chan time.Time)
	stopped := make(chan struct{})
	f.newTicker = func(time.Duration) (<-chan time.Time, func()) {
		return tick, func() { close(stopped) }
	}
	f.logf = func(error, string, ...any) {} // keep the test output clean

	return f, tick, stopped
}

// requireStopped fails the test if the flusher did not stop its ticker.
func requireStopped(t *testing.T, stopped chan struct{}) {
	t.Helper()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		require.Fail(t, "flusher did not stop its ticker")
	}
}

// snapshotCalls returns a copy of everything the sink recorded, safe to read while
// the flusher goroutine is still running.
func (s *fakeSink) snapshotCalls() []call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]call(nil), s.calls...)
}

// fakeStats is a Snapshotter that always returns the same data.
type fakeStats struct{ snap statsmodels.Snapshot }

func (f fakeStats) Snapshot() statsmodels.Snapshot { return f.snap }

// runFlusher starts f.Run in its own goroutine. It returns a function that
// cancels the context and waits for Run to finish, so every test can stop the
// flusher in one line and read the error it returned.
func runFlusher(t *testing.T, f *Flusher) (stop func() error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()

	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(time.Second):
			require.Fail(t, "Run did not return after the context was cancelled")
			return nil
		}
	}
}

func TestRun_FlushesOnEveryTick(t *testing.T) {
	sink := &fakeSink{}
	f, tick, _ := newTestFlusher(t, fakeStats{}, sink)
	stop := runFlusher(t, f)

	// The channel has no buffer, so the second send can only finish after Run
	// has handled the first one. This makes the test exact without any sleep.
	tick <- time.Now()
	tick <- time.Now()
	tick <- time.Now()

	require.NoError(t, stop())
	// 3 ticks + 1 save on shutdown
	assert.Len(t, sink.snapshotCalls(), 4)
}

// TestRun_FinalFlushOnCancel checks that cancelling the context produces one last save.
func TestRun_FinalFlushOnCancel(t *testing.T) {
	sink := &fakeSink{}
	f, tick, _ := newTestFlusher(t, fakeStats{}, sink)
	stop := runFlusher(t, f)

	tick <- time.Now()

	require.NoError(t, stop())
	assert.Len(t, sink.snapshotCalls(), 2, "one tick and one save on shutdown")
}

func TestRun_FinalFlushUsesLiveContext(t *testing.T) {
	sink := &fakeSink{}
	f, _, _ := newTestFlusher(t, fakeStats{}, sink)
	stop := runFlusher(t, f)

	require.NoError(t, stop()) // no ticks: the only save is the one on shutdown

	calls := sink.snapshotCalls()
	require.Len(t, calls, 1)
	assert.NoError(t, calls[0].ctxErr, "the save on shutdown needs a context that is not cancelled")
}

func TestRun_ShutdownFlushErrorIsReturned(t *testing.T) {
	errBoom := errors.New("database is down")
	sink := &fakeSink{errs: []error{errBoom}} // no ticks, so this hits the last save
	f, _, _ := newTestFlusher(t, fakeStats{}, sink)
	stop := runFlusher(t, f)

	err := stop()
	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom, "the original error must stay reachable")
	assert.Contains(t, err.Error(), "shutdown flush")
}

func TestRun_TickerIsStopped(t *testing.T) {
	sink := &fakeSink{}
	f, _, stopped := newTestFlusher(t, fakeStats{}, sink)
	stop := runFlusher(t, f)

	require.NoError(t, stop())
	requireStopped(t, stopped)
}
