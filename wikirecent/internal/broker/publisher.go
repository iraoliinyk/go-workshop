package broker

import (
	"context"
	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config is the publisher's half of the broker settings.
type PublisherConfig struct {
	Brokers     []string
	Topic       string
	ContentType string
	ProtoType   string
}

type Publisher struct {
	client RecordProducer
	log    applog.Logger
}

func NewPublisher(cfg PublisherConfig, log applog.Logger) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("wiki-producer"),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.DefaultProduceTopicAlways(),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, &apperrors.PublishError{Err: err}
	}

	return NewPublisherWithClient(client, log), nil
}

// NewPublisherWithClient builds a Publisher on a producer the caller already has.
// NewPublisher is the normal path; this is the seam that lets a caller supply its
// own client, which is how the tests reach the type without a live broker.
func NewPublisherWithClient(client RecordProducer, log applog.Logger) *Publisher {
	return &Publisher{client: client, log: log}
}

// Connect checks that at least one seed broker answers, so a wrong address fails
// at startup instead of silently filling the produce buffer.
func (p *Publisher) Connect(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return &apperrors.PublishError{Err: err}
	}
	return nil
}

func (p *Publisher) Publish(ctx context.Context, payload []byte) error {
	// No key, on purpose: let round-robin do its job.
	// The promise, not the return value, is where a delivery failure shows up:
	// Produce is async, so Publish returns before the record is accepted.
	p.client.Produce(ctx, &kgo.Record{Value: payload}, func(_ *kgo.Record, err error) {
		if err != nil && ctx.Err() == nil {
			p.log.AppErrorf(err, "publish failed: %v", err)
		}
	})
	return nil
}

func (p *Publisher) Close(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return &apperrors.PublishError{Err: err}
	}
	p.client.Close()
	return nil
}
