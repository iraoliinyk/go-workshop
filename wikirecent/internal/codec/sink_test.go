package codec_test

import (
	"context"
	"errors"
	"testing"

	"wikirecent/internal/applog"
	"wikirecent/internal/codec"
	wikiv1 "wikirecent/internal/genproto/wikirecent/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func TestProtoSink_PublishesProtobufNotJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	next := NewMockSink(ctrl)

	var forwarded []byte
	next.EXPECT().Publish(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, payload []byte) error {
			forwarded = payload
			return nil
		}).Times(1)

	sink := codec.NewProtoSink(next, applog.Logger{})
	require.NoError(t, sink.Publish(context.Background(), []byte(editEvent)))

	require.NotEmpty(t, forwarded)
	assert.NotEqual(t, editEvent, string(forwarded), "the JSON must not be forwarded as-is")

	var event wikiv1.WikiEvent
	require.NoError(t, proto.Unmarshal(forwarded, &event))
	assert.Equal(t, "iryna", event.GetUser())
}

func TestProtoSink_SkipsUnparsablePayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	next := NewMockSink(ctrl)
	sink := codec.NewProtoSink(next, applog.Logger{})
	assert.NoError(t, sink.Publish(context.Background(), []byte(`{not json`)))
}

func TestProtoSink_PassesBrokerFailureBack(t *testing.T) {
	ctrl := gomock.NewController(t)
	next := NewMockSink(ctrl)

	brokerDown := errors.New("broker down")
	next.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(brokerDown).Times(1)

	sink := codec.NewProtoSink(next, applog.Logger{})
	err := sink.Publish(context.Background(), []byte(editEvent))

	require.ErrorIs(t, err, brokerDown, "a broker failure is not a bad record")
}
