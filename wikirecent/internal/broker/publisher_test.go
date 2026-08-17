package broker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
)

func TestNewPublisher_RejectsAnEmptyTopicAtStartup(t *testing.T) {
	_, err := NewPublisher(PublisherConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "",
	}, applog.Logger{})

	require.Error(t, err, "an empty topic must fail here, not once per record")
	// The message names the option, which is the proof that it is what rejected it.
	require.Contains(t, err.Error(), "DefaultProduceTopicAlways")
	require.True(t, strings.HasPrefix(err.Error(), "PublishError:"), "want the app error type, got %v", err)
}

func TestPublisher_ConnectPingsTheBroker(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockrecordProducer(ctrl)
	p := &Publisher{client: client, log: applog.Logger{}}

	ctx := context.Background()
	// The exact ctx, not gomock.Any(): a lost context would make Connect hang
	// past the caller's deadline.
	client.EXPECT().Ping(ctx).Return(nil)

	require.NoError(t, p.Connect(ctx))
}

func TestPublisher_ConnectWrapsAPingFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockrecordProducer(ctrl)
	p := &Publisher{client: client, log: applog.Logger{}}

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
	client := NewMockrecordProducer(ctrl)
	p := &Publisher{client: client, log: applog.Logger{}}
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
