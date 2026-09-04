package broker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

//go:generate go tool mockgen -source=broker.go -destination=mock_broker_test.go -package=broker_test -typed

type Adapter interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

var _ Adapter = (*Publisher)(nil)

type RecordProducer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Ping(ctx context.Context) error
	Flush(ctx context.Context) error
	Close()
}
type PublishObserver interface {
	Published()
	PublishFailed()
}
