package wikistream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"wikirecent/internal/applog"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// A real Wikimedia event id: a JSON array of Kafka offsets. It carries brackets,
// quotes and colons, so it also shows the header value is passed through unchanged.
const (
	firstEventID  = `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":5647231}]`
	secondEventID = `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":5647232}]`
)

func testConfig() Config {
	return Config{
		URL:       "http://stream.test/v2/stream/recentchange",
		UserAgent: "wikirecent-test",
		Accept:    "text/event-stream",
	}
}

// discardLogger keeps the retry lines Start writes out of the test output.
func discardLogger(t *testing.T) applog.Logger {
	t.Helper()
	log, err := applog.New(applog.ModeProd, io.Discard)
	require.NoError(t, err)
	return log
}

// fastBackoff shrinks the retry delay for the reconnect tests, so they cost
// milliseconds instead of a second per retry.
func fastBackoff(t *testing.T) {
	t.Helper()
	old := baseBackoff
	baseBackoff = time.Millisecond
	t.Cleanup(func() { baseBackoff = old })
}

func okResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestOpenStream_SendsNoLastEventIDOnTheFirstConnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockdoer(ctrl)

	var got http.Header
	client.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		got = req.Header.Clone()
		return okResponse(""), nil
	})

	stream, err := openStream(context.Background(), testConfig(), client, "")
	require.NoError(t, err)
	defer stream.Close()

	// Absent, not empty: "Last-Event-ID: " would ask the server to resume from
	// position "", which is not the same as "start wherever you like".
	require.Empty(t, got.Values("Last-Event-ID"), "the first connect has nothing to resume from")
	require.Equal(t, "wikirecent-test", got.Get("User-Agent"))
	require.Equal(t, "text/event-stream", got.Get("Accept"))
}

func TestOpenStream_SendsTheLastEventIDOnAResume(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockdoer(ctrl)

	var got string
	client.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		got = req.Header.Get("Last-Event-ID")
		return okResponse(""), nil
	})

	stream, err := openStream(context.Background(), testConfig(), client, firstEventID)
	require.NoError(t, err)
	defer stream.Close()

	require.Equal(t, firstEventID, got)
}

func TestReadStream_ReturnsTheIDOfTheLastEventItRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)

	body := "event: message\n" +
		"id: " + firstEventID + "\n" +
		`data: {"user":"Iryna"}` + "\n" +
		"\n" +
		"event: message\n" +
		"id: " + secondEventID + "\n" +
		`data: {"user":"Bot"}` + "\n" +
		"\n"

	gomock.InOrder(
		sink.EXPECT().Publish(gomock.Any(), []byte(`{"user":"Iryna"}`)).Return(nil),
		sink.EXPECT().Publish(gomock.Any(), []byte(`{"user":"Bot"}`)).Return(nil),
	)

	got, err := readStream(context.Background(), strings.NewReader(body), sink, "")

	require.ErrorIs(t, err, io.EOF, "the stream ended, so the read must report it")
	require.Equal(t, secondEventID, got, "want the newest id, not the first one seen")
}

func TestReadStream_ReturnsTheIDItSawWhenPublishFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)

	brokerErr := errors.New("broker refused the record")
	sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(brokerErr)

	body := "id: " + firstEventID + "\n" + `data: {"user":"Iryna"}` + "\n"

	got, err := readStream(context.Background(), strings.NewReader(body), sink, "")

	require.ErrorIs(t, err, brokerErr)
	require.Equal(t, firstEventID, got)
}

func TestStart_ResumesFromTheLastEventIDAfterAReconnect(t *testing.T) {
	fastBackoff(t)
	ctrl := gomock.NewController(t)
	client := NewMockdoer(ctrl)
	sink := NewMockSink(ctrl)
	ctx, cancel := context.WithCancel(context.Background())

	sink.EXPECT().Publish(gomock.Any(), []byte(`{"user":"Iryna"}`)).Return(nil)

	var sent []string // the Last-Event-ID header of each request, in order
	record := func(req *http.Request) { sent = append(sent, req.Header.Get("Last-Event-ID")) }

	gomock.InOrder(
		client.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			record(req)
			return okResponse("id: " + firstEventID + "\n" + `data: {"user":"Iryna"}` + "\n"), nil
		}),
		client.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			record(req)
			cancel()
			return nil, errors.New("connection refused")
		}),
	)

	err := Start(ctx, testConfig(), client, sink, discardLogger(t))

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"", firstEventID}, sent,
		"the second connect must ask the server to continue after the last event")
}

func TestStart_KeepsTheLastEventIDWhenAReconnectIsRejected(t *testing.T) {
	fastBackoff(t)
	ctrl := gomock.NewController(t)
	client := NewMockdoer(ctrl)
	sink := NewMockSink(ctrl)
	ctx, cancel := context.WithCancel(context.Background())

	sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)

	var sent []string
	record := func(req *http.Request) { sent = append(sent, req.Header.Get("Last-Event-ID")) }

	gomock.InOrder(
		client.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			record(req)
			return okResponse("id: " + firstEventID + "\n" + `data: {"user":"Iryna"}` + "\n"), nil
		}),
		// A rejected connect never reaches readStream, so it must not touch the id.
		client.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			record(req)
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Body:       io.NopCloser(strings.NewReader("rate limited")),
			}, nil
		}),
		client.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
			record(req)
			cancel()
			return nil, errors.New("connection refused")
		}),
	)

	err := Start(ctx, testConfig(), client, sink, discardLogger(t))

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"", firstEventID, firstEventID}, sent,
		"a 429 must not send us back to the head of the stream")
}
