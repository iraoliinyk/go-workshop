package broker

import (
	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config is the publisher's half of the broker settings.
type PublisherConfig struct {
	Brokers []string
	Topic   string
}

type Publisher struct {
	client recordProducer
	log    applog.Logger
}

func NewPublisher(cfg PublisherConfig, log applog.Logger) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("wiki-producer"),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.DefaultProduceTopicAlways(),
	)
	if err != nil {
		return nil, &apperrors.PublishError{Err: err}
	}

	return &Publisher{client: client, log: log}, nil
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
			p.log.AppErrorf(err, "publish failed")
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
