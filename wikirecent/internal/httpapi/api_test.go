package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"wikirecent/internal/applog"
	"wikirecent/internal/auth"
	"wikirecent/internal/httpapi"
	"wikirecent/internal/metrics"
	"wikirecent/internal/repository/memory"
	"wikirecent/internal/stats/statsmodels"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// testBcryptCost is the lowest cost auth.New accepts. bcrypt.MinCost (4) would be
// faster, but the 10..31 check rejects it on purpose, and lowering that limit just
// to speed up tests would weaken the real check.
const testBcryptCost = 10

// newTestAPI returns an API backed by memory stores, the *auth.Service behind it so
// a test can create tokens with tokenFor, and the stats mock so a test can set its
// own expectation. No default expectation is set: a test that does not name /stats
// must not call Snapshot, and ctrl enforces that.
func newTestAPI(t *testing.T) (*httpapi.API, *auth.Service, *Mocksnapshotter) {
	t.Helper()
	return newTestAPILogged(t, quietLogger(t))
}

// newTestAPILogged is newTestAPI with the logger chosen by the caller, for a test
// that has to read what was written.
func newTestAPILogged(t *testing.T, logger applog.Logger) (*httpapi.API, *auth.Service, *Mocksnapshotter) {
	t.Helper()
	reg := metrics.NewRegistry()
	users, tokens, metricsHandler := memory.NewUserStore(), memory.NewRevocationStore(), metrics.Handler(reg)
	svc, err := auth.New(users, tokens, auth.Config{
		Secret: strings.Repeat("x", 32), // exactly the 32-byte minimum
		Issuer: "wiki-stream-go",
		TTL:    time.Hour,
		Cost:   testBcryptCost,
		Log:    logger,
	})
	require.NoError(t, err)

	stats := NewMocksnapshotter(gomock.NewController(t))
	return httpapi.New(stats, svc, logger, metricsHandler), svc, stats
}

// quietLogger is PROD pointed at io.Discard, not the zero Logger, which would print
// real error lines and stack traces over a passing run. Swap in os.Stderr or
// applog.ModeDebug when a failing test is worth watching.
func quietLogger(t *testing.T) applog.Logger {
	t.Helper()
	logger, err := applog.New(applog.ModeProd, io.Discard)
	require.NoError(t, err)
	return logger
}

// bufferLogger is PROD writing to a buffer, so a test can assert on the lines. PROD,
// not DEBUG, because the point is what survives in production, where LogRequests is
// switched off and only Errorf and AppErrorf still write.
//
// The buffer is guarded: the handler writes to it on the server goroutine while the
// test reads it, and -race would otherwise flag that.
func bufferLogger(t *testing.T) (applog.Logger, *syncBuffer) {
	t.Helper()
	buf := &syncBuffer{}
	logger, err := applog.New(applog.ModeProd, buf)
	require.NoError(t, err)
	return logger, buf
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// tokenFor registers a user and returns a valid bearer token for them.
func tokenFor(t *testing.T, svc *auth.Service, email, pw string) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, svc.Register(ctx, auth.RegisterRequest{Email: email, Password: pw}))
	raw, _, err := svc.Login(ctx, email, pw)
	require.NoError(t, err)
	return raw
}

func TestStatus_ReturnsOK(t *testing.T) {
	api, _, _ := newTestAPI(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)

	api.Router().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestStats_ReturnsSnapshot(t *testing.T) {
	want := statsmodels.Snapshot{
		TotalMessages: 7,
		DistinctUsers: 3,
		BotEdits:      2,
		HumanEdits:    5,
		ByServerURL:   map[string]int64{"https://en.wikipedia.org": 4},
	}

	api, svc, stats := newTestAPI(t)
	stats.EXPECT().Snapshot().Return(want).Times(1)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	// /stats is protected now, so the request needs a bearer token.
	req.Header.Set("Authorization", "Bearer "+tokenFor(t, svc, "a@b.co", "correcthorse"))

	api.Router().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var got statsmodels.Snapshot
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, want, got)
}

// TestStats_WithoutTokenIs401 protects the default-deny setup. If someone removes
// Guard from Router, this is the test that fails.
func TestStats_WithoutTokenIs401(t *testing.T) {
	api, _, _ := newTestAPI(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)

	api.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.True(t, strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer "),
		"want a Bearer challenge, got %q", rec.Header().Get("WWW-Authenticate"))
}

// TestLogout_ThenSameTokenIs401 is the only test that shows IsRevoked runs on every
// request. The token is still correctly signed and has not expired, so the 401 can
// only come from the revocation store.
func TestLogout_ThenSameTokenIs401(t *testing.T) {
	api, svc, _ := newTestAPI(t)
	token := tokenFor(t, svc, "a@b.co", "correcthorse")
	router := api.Router()

	logout := httptest.NewRecorder()
	logoutReq := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logoutReq.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(logout, logoutReq)
	require.Equal(t, http.StatusNoContent, logout.Code)

	reuse := httptest.NewRecorder()
	reuseReq := httptest.NewRequest(http.MethodGet, "/stats", nil)
	reuseReq.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(reuse, reuseReq)

	// The mock has no Snapshot expectation, so reaching the handler would also fail.
	assert.Equal(t, http.StatusUnauthorized, reuse.Code, "reused revoked token")
}

// TestLogin_UnknownUserAndWrongPasswordGiveSameResponse checks that a client cannot
// tell the two failures apart, so login cannot be used to find valid emails.
func TestLogin_UnknownUserAndWrongPasswordGiveSameResponse(t *testing.T) {
	api, svc, _ := newTestAPI(t)
	require.NoError(t, svc.Register(context.Background(),
		auth.RegisterRequest{Email: "known@b.co", Password: "correcthorse"}))
	router := api.Router()

	post := func(body string) (int, string) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(body))
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}

	unknownCode, unknownBody := post(`{"email":"nobody@b.co","password":"correcthorse"}`)
	wrongCode, wrongBody := post(`{"email":"known@b.co","password":"wrongpassword"}`)

	require.Equal(t, http.StatusUnauthorized, unknownCode)
	require.Equal(t, http.StatusUnauthorized, wrongCode)
	assert.Equal(t, unknownBody, wrongBody, "responses differ, leaking whether the account exists")
}

func TestRegister_StatusMapping(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"valid", `{"email":"new@b.co","password":"correcthorse"}`, http.StatusCreated},
		{"bad email", `{"email":"not-an-email","password":"correcthorse"}`, http.StatusBadRequest},
		{"short password", `{"email":"x@b.co","password":"short"}`, http.StatusBadRequest},
		{"unknown field", `{"email":"x@b.co","password":"correcthorse","role":"admin"}`, http.StatusBadRequest},
		{"malformed json", `{`, http.StatusBadRequest},
	}
	// One API for all subtests. Each case uses a different email, so they do not
	// affect each other, and the bcrypt dummy hash in auth.New is built once
	// instead of five times.
	api, _, _ := newTestAPI(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/auth/register", strings.NewReader(c.body))
			api.Router().ServeHTTP(rec, req)
			assert.Equal(t, c.want, rec.Code, "body %s", rec.Body.String())
		})
	}
}

func TestRegister_DuplicateIs409(t *testing.T) {
	api, _, _ := newTestAPI(t)
	router := api.Router()
	body := `{"email":"dup@b.co","password":"correcthorse"}`

	first := httptest.NewRecorder()
	router.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/auth/register", strings.NewReader(body)))
	require.Equal(t, http.StatusCreated, first.Code, "first register")

	second := httptest.NewRecorder()
	router.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/auth/register", strings.NewReader(body)))
	assert.Equal(t, http.StatusConflict, second.Code, "duplicate register")
}

// TestRequestID_ReturnsGeneratedUUID checks that a request with no X-Request-Id gets
// one. Nothing else asserts the header, so without this test removing RequestID from
// Router would break incident debugging and leave every test green.
func TestRequestID_ReturnsGeneratedUUID(t *testing.T) {
	api, _, _ := newTestAPI(t)
	rec := httptest.NewRecorder()

	api.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	got := rec.Header().Get("X-Request-Id")
	require.NotEmpty(t, got, "every response must carry a request id")

	id, err := uuid.Parse(got)
	require.NoErrorf(t, err, "X-Request-Id %q must be a UUID", got)
	assert.Equal(t, uuid.Version(4), id.Version(), "must be a random UUID, not time-based")
}

// TestRequestID_ReusesSafeInboundHeader covers the reuse path: behind a proxy that
// already assigned an id, this service must keep it so the two logs line up.
func TestRequestID_ReusesSafeInboundHeader(t *testing.T) {
	api, _, _ := newTestAPI(t)
	rec := httptest.NewRecorder()

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("X-Request-Id", "gateway-trace-0001")
	api.Router().ServeHTTP(rec, req)

	assert.Equal(t, "gateway-trace-0001", rec.Header().Get("X-Request-Id"))
}

// TestRequestID_ReplacesUnsafeInboundHeader is why the reuse above is bounded. The id
// is echoed in a header and written to the logs, so a caller must not be able to put
// a newline or an unbounded string into either.
func TestRequestID_ReplacesUnsafeInboundHeader(t *testing.T) {
	cases := map[string]string{
		"newline":      "abc\ndef",
		"tab":          "abc\tdef",
		"space":        "abc def",
		"too long":     strings.Repeat("x", 129),
		"non-ascii":    "trace-é",
		"control byte": "abc\x00def",
	}
	// One API for all subtests: each builds a bcrypt dummy hash, which is the slowest
	// part of newTestAPI, and none of these cases touches state.
	api, _, _ := newTestAPI(t)
	for name, inbound := range cases {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()

			req := httptest.NewRequest(http.MethodGet, "/status", nil)
			req.Header.Set("X-Request-Id", inbound)
			api.Router().ServeHTTP(rec, req)

			got := rec.Header().Get("X-Request-Id")
			assert.NotEqual(t, inbound, got, "the unsafe value must not be echoed")
			_, err := uuid.Parse(got)
			assert.NoErrorf(t, err, "want a generated UUID, got %q", got)
		})
	}
}

// TestRequestID_AppearsInProdErrorLine is the one that shows the id is worth having.
// A panic answers 500 and Recoverer logs it at error level, which is the only level
// PROD keeps, so the line must carry the same id the caller was given. That pairing is
// the whole point: a report of "I got a 500, here is my X-Request-Id" has to be
// enough to find the line.
func TestRequestID_AppearsInProdErrorLine(t *testing.T) {
	logger, logs := bufferLogger(t)
	api, svc, stats := newTestAPILogged(t, logger)

	// A panic inside the handler, so Recoverer runs. /stats is protected, so the
	// request needs a token first.
	stats.EXPECT().Snapshot().DoAndReturn(func() statsmodels.Snapshot {
		panic("boom from the stats mock")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Authorization", "Bearer "+tokenFor(t, svc, "a@b.co", "correcthorse"))
	api.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusInternalServerError, rec.Code, "a panic must become a 500")

	id := rec.Header().Get("X-Request-Id")
	require.NotEmpty(t, id)

	line := logs.String()
	require.NotEmpty(t, line, "PROD must still write the panic at error level")
	assert.Containsf(t, line, id, "the log line must carry the id the caller got (%s)", id)
	assert.Contains(t, line, "panicked", "the line must say what happened")
	assert.Contains(t, line, "request_id=", "the id must be an attribute, not pasted into the text")
}

func newAPIWithMetricsHandler(t *testing.T, metricsHandler http.Handler) *httpapi.API {
	t.Helper()
	logger := quietLogger(t)
	svc, err := auth.New(memory.NewUserStore(), memory.NewRevocationStore(), auth.Config{
		Secret: strings.Repeat("x", 32),
		Issuer: "wiki-stream-go",
		TTL:    time.Hour,
		Cost:   testBcryptCost,
		Log:    logger,
	})
	require.NoError(t, err)

	stats := NewMocksnapshotter(gomock.NewController(t))
	return httpapi.New(stats, svc, logger, metricsHandler)
}

func getMetrics(t *testing.T, api *httpapi.API) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	// No Authorization header, on purpose: Prometheus sends none.
	api.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec
}

// Missing from the public map, Guard answers 401 and Prometheus reads the target
// as DOWN with the dashboard silently empty and no error anywhere.
func TestMetrics_IsPublic(t *testing.T) {
	api := newAPIWithMetricsHandler(t, metrics.Handler(metrics.NewRegistry()))

	rec := getMetrics(t, api)

	require.Equal(t, http.StatusOK, rec.Code, "the scrape must not need a token")
}

func TestMetrics_ExposesTheEventCounters(t *testing.T) {
	reg := metrics.NewRegistry()
	metrics.NewEvents(reg)
	api := newAPIWithMetricsHandler(t, metrics.Handler(reg))

	rec := getMetrics(t, api)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "wikistream_events_consumed_from_redpanda_total")
}

// 401 and not 404: mux.Handler returns an empty pattern for an unmatched path,
// and "" is not in the public map, so Guard answers before net/http can.
func TestRouter_WithoutMetricsHandler_DoesNotServeMetrics(t *testing.T) {
	api := newAPIWithMetricsHandler(t, nil)

	rec := getMetrics(t, api)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
