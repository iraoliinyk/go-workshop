package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"ch-4/internal/apperrors"
	"ch-4/internal/consumer/consumermodels"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestParseEvent_ValidDataLine(t *testing.T) {
	event := consumermodels.WikiEvent{
		User:      "iryna",
		Bot:       false,
		ServerURL: "https://en.wikipedia.org",
	}
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	// parseEvent returns *apperrors.ParseError (concrete type), so use require.Nil,
	// NOT require.NoError — a typed nil pointer boxed into error is a non-nil interface.
	got, parseErr := parseEvent("data: " + string(payload))
	require.Nil(t, parseErr)

	assert.Equal(t, event.User, got.User)
	assert.Equal(t, event.Bot, got.Bot)
	assert.Equal(t, event.ServerURL, got.ServerURL)
}

func TestParseEvent_InvalidJSON(t *testing.T) {
	_, err := parseEvent("data: {not valid json}")

	var parseErr *apperrors.ParseError
	require.ErrorAs(t, err, &parseErr)
}

func TestParseEvent_StripsDataPrefix(t *testing.T) {
	event := consumermodels.WikiEvent{User: "john"}
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	got, parseErr := parseEvent("data: " + string(payload))
	require.Nil(t, parseErr)
	assert.Equal(t, "john", got.User)
}

func TestParseEvent_ErrorCode(t *testing.T) {
	_, err := parseEvent("data: {not valid json}")

	assert.Equal(t, "PARSE_ERROR", err.Code())
	require.Error(t, err)
}

// makeResp builds a *http.Response with a string body.
func makeResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
}

// errBody yields an error mid-stream so scanner.Err() fires (drives StreamError).
type errBody struct{}

func (errBody) Read(p []byte) (int, error) { return 0, fmt.Errorf("connection reset") }
func (errBody) Close() error               { return nil }

func TestStart_RecordsValidEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	body := `data: {"user":"iryna","bot":false,"server_url":"https://en.wikipedia.org"}` + "\n" +
		"\n" + // blank separator — must be skipped
		`data: {"user":"oli"}` + "\n"

	d := NewMockdoer(ctrl)
	d.EXPECT().Do(gomock.Any()).Return(makeResp(http.StatusOK, body), nil)

	// InOrder encodes the sequence the stub could only check after the fact —
	// a wrong order or an extra Record now fails inside the mock, not in an assert.
	rec := NewMockrecorder(ctrl)
	gomock.InOrder(
		rec.EXPECT().Record(consumermodels.WikiEvent{
			User:      "iryna",
			Bot:       false,
			ServerURL: "https://en.wikipedia.org",
		}),
		rec.EXPECT().Record(consumermodels.WikiEvent{User: "oli"}),
	)

	err := Start(context.Background(), Config{URL: "http://x"}, d, rec)

	require.NoError(t, err)
}

func TestStart_SkipsUnparseableLines(t *testing.T) {
	ctrl := gomock.NewController(t)
	body := `data: {not valid json}` + "\n" +
		`data: {"user":"iryna"}` + "\n"

	d := NewMockdoer(ctrl)
	d.EXPECT().Do(gomock.Any()).Return(makeResp(http.StatusOK, body), nil)

	// Exactly one Record, for the valid line only: the bad line being skipped is
	// asserted by the absence of a second expectation, which ctrl enforces.
	rec := NewMockrecorder(ctrl)
	rec.EXPECT().Record(consumermodels.WikiEvent{User: "iryna"}).Times(1)

	err := Start(context.Background(), Config{URL: "http://x"}, d, rec)

	// parse errors are logged and skipped — stream is not broken.
	require.NoError(t, err)
}

func TestStart_SetsRequiredHeaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfg := Config{
		URL:       "http://x",
		UserAgent: "wiki-stream-consumer/1.0",
		Accept:    "application/json",
	}

	// DoAndReturn replaces the stub's gotReq field: inspect the argument at call
	// time instead of storing it for a later assert.
	d := NewMockdoer(ctrl)
	d.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, cfg.URL, req.URL.String())
		assert.Equal(t, cfg.UserAgent, req.Header.Get("User-Agent"))
		assert.Equal(t, cfg.Accept, req.Header.Get("Accept"))
		return makeResp(http.StatusOK, ""), nil
	})

	// Empty body: no events, so Record must never be called.
	err := Start(context.Background(), cfg, d, NewMockrecorder(ctrl))

	require.NoError(t, err)
}

func TestStart_ClientError(t *testing.T) {
	ctrl := gomock.NewController(t)
	d := NewMockdoer(ctrl)
	d.EXPECT().Do(gomock.Any()).Return(nil, fmt.Errorf("dial tcp: refused"))

	err := Start(context.Background(), Config{URL: "http://x"}, d, NewMockrecorder(ctrl))

	var connErr *apperrors.ConnectionError
	require.ErrorAs(t, err, &connErr)
}

func TestStart_NonOKStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	d := NewMockdoer(ctrl)
	d.EXPECT().Do(gomock.Any()).Return(makeResp(http.StatusInternalServerError, "boom"), nil)

	err := Start(context.Background(), Config{URL: "http://x"}, d, NewMockrecorder(ctrl))

	var connErr *apperrors.ConnectionError
	require.ErrorAs(t, err, &connErr)
	assert.Equal(t, http.StatusInternalServerError, connErr.StatusCode)
}

func TestStart_StreamError(t *testing.T) {
	ctrl := gomock.NewController(t)
	d := NewMockdoer(ctrl)
	d.EXPECT().Do(gomock.Any()).
		Return(&http.Response{StatusCode: http.StatusOK, Body: errBody{}}, nil)

	err := Start(context.Background(), Config{URL: "http://x"}, d, NewMockrecorder(ctrl))

	var streamErr *apperrors.StreamError
	require.ErrorAs(t, err, &streamErr)
}
