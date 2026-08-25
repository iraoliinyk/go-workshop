package broker

import (
	"context"
	"wikirecent/internal/events"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The interfaces below are exported so the mocks can live in package broker_test and
// every test stays black-box. NewPublisherWithClient and NewSubscriberWithClient are
// the seams that take them.
//go:generate go tool mockgen -source=broker.go -destination=mock_broker_test.go -package=broker_test -typed

type Adapter interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

var _ Adapter = (*Publisher)(nil)
var _ Adapter = (*Subscriber)(nil)

type Recorder interface {
	Record(event events.WikiEvent)
}

type Decoder interface {
	Decode(value []byte) (events.WikiEvent, error)
}

type RecordProducer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Ping(ctx context.Context) error
	Flush(ctx context.Context) error
	Close()
}

type RecordPoller interface {
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	Ping(ctx context.Context) error
	AllowRebalance()
	CloseAllowingRebalance()
}

type PublishObserver interface {
	Published()
	PublishFailed()
}

type BatchObserver interface {
	Consumed(n int)
	Processed()
	Failed()
}
