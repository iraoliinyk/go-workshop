package broker_test

import (
	"context"
	"errors"
	"testing"

	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"
	"wikirecent/internal/broker"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
)

// validEvent is opaque payload bytes for Publish, which never decodes what it
// forwards — no real protobuf-encoded event is needed here.
const validEvent = "wiki-event-payload"

func TestPublisher_ConnectPingsTheBroker(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockRecordProducer(ctrl)
	observer := NewMockPublishObserver(ctrl)
	p := broker.NewPublisherWithClient(client, applog.Logger{}, observer)

	ctx := context.Background()
	// The exact ctx, not gomock.Any(): a lost context would make Connect hang
	// past the caller's deadline.
	client.EXPECT().Ping(ctx).Return(nil)

	require.NoError(t, p.Connect(ctx))
}

func TestPublisher_ConnectWrapsAPingFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockRecordProducer(ctrl)
	observer := NewMockPublishObserver(ctrl)
	p := broker.NewPublisherWithClient(client, applog.Logger{}, observer)

	dialErr := errors.New("dial tcp 127.0.0.1:9092: connect: connection refused")
	client.EXPECT().Ping(gomock.Any()).Return(dialErr)

	err := p.Connect(context.Background())

	require.Error(t, err, "a dead broker must stop startup, not be swallowed")
	var publishErr *apperrors.PublishError
	require.ErrorAs(t, err, &publishErr)
	require.ErrorIs(t, err, dialErr)
}

// A key would hash every record to one partition. Publish leaves it unset on
// purpose, so the broker spreads the load round-robin across all three.
func TestPublisher_PublishSendsNoKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockRecordProducer(ctrl)
	observer := NewMockPublishObserver(ctrl)
	p := broker.NewPublisherWithClient(client, applog.Logger{}, observer)

	var got *kgo.Record
	client.EXPECT().
		Produce(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, r *kgo.Record, _ func(*kgo.Record, error)) {
			got = r
		})

	payload := []byte(validEvent)
	require.NoError(t, p.Publish(context.Background(), payload))

	require.NotNil(t, got, "Publish must hand one record to the client")
	require.Nil(t, got.Key, "want no key, got %q: a key pins every record to one partition", got.Key)
	require.Equal(t, payload, got.Value)
	// The topic is set once by DefaultProduceTopic in NewPublisher, not per record.
	require.Empty(t, got.Topic)
}

func capturePromise(t *testing.T) (p *broker.Publisher, observer *MockPublishObserver, runPromise func(error)) {
	t.Helper()
	ctrl := gomock.NewController(t)
	client := NewMockRecordProducer(ctrl)
	observer = NewMockPublishObserver(ctrl)
	p = broker.NewPublisherWithClient(client, applog.Logger{}, observer)

	var promise func(*kgo.Record, error)
	client.EXPECT().
		Produce(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *kgo.Record, fn func(*kgo.Record, error)) {
			promise = fn
		})

	return p, observer, func(err error) {
		require.NotNil(t, promise, "Produce was never called, so there is no promise to run")
		promise(&kgo.Record{}, err)
	}
}

// No expectation on the observer: gomock fails on any call, and that is the
// assertion. Produce is async, so counting here would count buffered records.
func TestPublish_DoesNotCountBeforeThePromiseRuns(t *testing.T) {
	p, _, _ := capturePromise(t)

	require.NoError(t, p.Publish(context.Background(), []byte(validEvent)))
}

func TestPublish_CountsPersistedWhenThePromiseSucceeds(t *testing.T) {
	p, observer, runPromise := capturePromise(t)
	// No PublishFailed expectation: a successful delivery must not touch it.
	observer.EXPECT().Published().Times(1)

	require.NoError(t, p.Publish(context.Background(), []byte(validEvent)))
	runPromise(nil)
}

func TestPublish_CountsFailedWhenThePromiseErrors(t *testing.T) {
	p, observer, runPromise := capturePromise(t)
	observer.EXPECT().PublishFailed().Times(1)

	require.NoError(t, p.Publish(context.Background(), []byte(validEvent)))
	runPromise(errors.New("MESSAGE_TOO_LARGE"))
}

// Shutdown suppresses the log line but never the counter: otherwise
// persisted + failed stops matching what we tried to produce.
func TestPublish_CountsFailedOnACancelledContext(t *testing.T) {
	p, observer, runPromise := capturePromise(t)
	observer.EXPECT().PublishFailed().Times(1)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, p.Publish(ctx, []byte(validEvent)))
	cancel()

	runPromise(errors.New("client closed"))
}

func TestPublish_WithNilObserverDoesNotPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockRecordProducer(ctrl)
	p := broker.NewPublisherWithClient(client, applog.Logger{}, nil)

	var promise func(*kgo.Record, error)
	client.EXPECT().
		Produce(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *kgo.Record, fn func(*kgo.Record, error)) {
			promise = fn
		}).Times(2)

	require.NoError(t, p.Publish(context.Background(), []byte(validEvent)))
	require.NotPanics(t, func() { promise(&kgo.Record{}, nil) })

	require.NoError(t, p.Publish(context.Background(), []byte(validEvent)))
	require.NotPanics(t, func() { promise(&kgo.Record{}, errors.New("boom")) })
}
