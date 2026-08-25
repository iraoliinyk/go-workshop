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
	obs    PublishObserver
}

func NewPublisher(cfg PublisherConfig, log applog.Logger, obs PublishObserver) (*Publisher, error) {
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

	return NewPublisherWithClient(client, log, obs), nil
}

// NewPublisherWithClient builds a Publisher on a producer the caller already has.
// NewPublisher is the normal path; this is the seam that lets a caller supply its
// own client, which is how the tests reach the type without a live broker.
func NewPublisherWithClient(client RecordProducer, log applog.Logger, obs PublishObserver) *Publisher {
	return &Publisher{client: client, log: log, obs: orNoopPublishObserver(obs)}
}

// orNoopPublishObserver keeps Publish free of nil checks, the same way
// lifecycle.orNoop does for its hooks.
func orNoopPublishObserver(obs PublishObserver) PublishObserver {
	if obs != nil {
		return obs
	}
	return noopPublishObserver{}
}

type noopPublishObserver struct{}

func (noopPublishObserver) Published()     {}
func (noopPublishObserver) PublishFailed() {}

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
	// Produce is async, so Publish returns before the record is accepted. Both
	// counters belong in here for that reason: counting on the way out would count
	// records that are only buffered.
	p.client.Produce(ctx, &kgo.Record{Value: payload}, func(_ *kgo.Record, err error) {
		if err != nil {
			p.obs.PublishFailed()
			// The log is suppressed during shutdown, the counter is not: otherwise
			// persisted + failed stops matching what we tried to produce.
			if ctx.Err() == nil {
				p.log.AppErrorf(err, "publish failed: %v", err)
			}
			return
		}
		p.obs.Published()
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
