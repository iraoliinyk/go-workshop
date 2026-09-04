package codec

import (
	"context"
	"wikirecent/internal/applog"
)

//go:generate go tool mockgen -source=sink.go -destination=mock_sink_test.go -package=codec_test -typed

type Sink interface {
	Publish(ctx context.Context, payload []byte) error
}

type ProtoSink struct {
	next Sink
	log  applog.Logger
}

func NewProtoSink(next Sink, log applog.Logger) *ProtoSink {
	return &ProtoSink{next: next, log: log}
}

func (s *ProtoSink) Publish(ctx context.Context, payload []byte) error {
	out, err := EncodeFromJSON(payload)
	if err != nil {
		s.log.AppErrorf(err, "skipping unparsable event: %v", err)
		return nil
	}
	return s.next.Publish(ctx, out)
}
