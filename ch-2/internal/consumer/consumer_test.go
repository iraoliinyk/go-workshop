package consumer

import (
	"ch-2/internal/apperrors"
	"ch-2/internal/consumer/models"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEvent_ValidDataLine(t *testing.T) {
	event := models.WikiEvent{
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
	event := models.WikiEvent{User: "john"}
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

// ---- Start ------------------------------------------------------------

// stubDoer returns a canned response/error and captures the request it received
// (so the header test can assert on it).
type stubDoer struct {
	gotReq *http.Request
	resp   *http.Response
	err    error
}

func (s *stubDoer) Do(req *http.Request) (*http.Response, error) {
	s.gotReq = req
	return s.resp, s.err
}

// stubRecorder captures every event Start records.
type stubRecorder struct{ events []models.WikiEvent }

func (r *stubRecorder) Record(e models.WikiEvent) { r.events = append(r.events, e) }

// makeResp builds a *http.Response with a string body.
func makeResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
}

// errBody yields an error mid-stream so scanner.Err() fires (drives StreamError).
type errBody struct{}

func (errBody) Read(p []byte) (int, error) { return 0, fmt.Errorf("connection reset") }
func (errBody) Close() error               { return nil }

func TestStart_RecordsValidEvents(t *testing.T) {
	body := `data: {"user":"iryna","bot":false,"server_url":"https://en.wikipedia.org"}` + "\n" +
		"\n" + // blank separator — must be skipped
		`data: {"user":"oli"}` + "\n"
	d := &stubDoer{resp: makeResp(http.StatusOK, body)}
	rec := &stubRecorder{}

	err := Start(context.Background(), Config{URL: "http://x"}, d, rec)

	require.NoError(t, err)
	require.Len(t, rec.events, 2)
	assert.Equal(t, "iryna", rec.events[0].User)
	assert.Equal(t, "oli", rec.events[1].User)
}

func TestStart_SkipsUnparseableLines(t *testing.T) {
	body := `data: {not valid json}` + "\n" +
		`data: {"user":"iryna"}` + "\n"
	d := &stubDoer{resp: makeResp(http.StatusOK, body)}
	rec := &stubRecorder{}

	err := Start(context.Background(), Config{URL: "http://x"}, d, rec)

	// parse errors are logged and skipped — stream is not broken.
	require.NoError(t, err)
	require.Len(t, rec.events, 1)
	assert.Equal(t, "iryna", rec.events[0].User)
}

func TestStart_SetsRequiredHeaders(t *testing.T) {
	d := &stubDoer{resp: makeResp(http.StatusOK, "")}
	cfg := Config{
		URL:       "http://x",
		UserAgent: "wiki-stream-consumer/1.0",
		Accept:    "application/json",
	}

	err := Start(context.Background(), cfg, d, &stubRecorder{})

	require.NoError(t, err)
	require.NotNil(t, d.gotReq)
	assert.Equal(t, cfg.UserAgent, d.gotReq.Header.Get("User-Agent"))
	assert.Equal(t, cfg.Accept, d.gotReq.Header.Get("Accept"))
}

func TestStart_ClientError(t *testing.T) {
	d := &stubDoer{err: fmt.Errorf("dial tcp: refused")}

	err := Start(context.Background(), Config{URL: "http://x"}, d, &stubRecorder{})

	var connErr *apperrors.ConnectionError
	require.ErrorAs(t, err, &connErr)
}

func TestStart_NonOKStatus(t *testing.T) {
	d := &stubDoer{resp: makeResp(http.StatusInternalServerError, "boom")}

	err := Start(context.Background(), Config{URL: "http://x"}, d, &stubRecorder{})

	var connErr *apperrors.ConnectionError
	require.ErrorAs(t, err, &connErr)
	assert.Equal(t, http.StatusInternalServerError, connErr.StatusCode)
}

func TestStart_StreamError(t *testing.T) {
	d := &stubDoer{resp: &http.Response{StatusCode: http.StatusOK, Body: errBody{}}}

	err := Start(context.Background(), Config{URL: "http://x"}, d, &stubRecorder{})

	var streamErr *apperrors.StreamError
	require.ErrorAs(t, err, &streamErr)
}
