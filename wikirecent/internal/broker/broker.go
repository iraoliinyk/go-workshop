package broker

import (
	"wikirecent/internal/events"
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// -package=broker, not broker_test: recorder, recordProducer and recordPoller are
// unexported, so the mocks must live in the same package to implement them.
//go:generate go tool mockgen -source=broker.go -destination=mock_broker_test.go -package=broker -typed

type Adapter interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

var _ Adapter = (*Publisher)(nil)
var _ Adapter = (*Subscriber)(nil)

type recorder interface {
	Record(event events.WikiEvent)
}

type recordProducer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Ping(ctx context.Context) error
	Flush(ctx context.Context) error
	Close()
}

type recordPoller interface {
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	Ping(ctx context.Context) error
	AllowRebalance()
	CloseAllowingRebalance()
}
