package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"
	"wikirecent/internal/auth"
	"wikirecent/internal/stats/statsmodels"
)

// Mocks live in mock_api_test.go. Generated into package httpapi_test (not httpapi),
// so the api_test.go can reach them.
//go:generate go tool mockgen -source=api.go -destination=mock_api_test.go -package=httpapi_test -typed

type snapshotter interface {
	Snapshot(ctx context.Context) (statsmodels.Snapshot, error)
}

// API holds the dependencies shared by all HTTP handlers.
type API struct {
	stats   snapshotter
	auth    *auth.Service
	log     applog.Logger
	metrics http.Handler
}

func New(stats snapshotter, authSvc *auth.Service, logger applog.Logger, metricsHandler http.Handler) *API {
	return &API{
		stats:   stats,
		auth:    authSvc,
		log:     logger,
		metrics: metricsHandler,
	}
}

func (a *API) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", a.handleStatus)           // public
	mux.HandleFunc("POST /auth/register", a.handleRegister) // public
	mux.HandleFunc("POST /auth/login", a.handleLogin)       // public
	mux.HandleFunc("POST /auth/logout", a.handleLogout)     // protected
	mux.HandleFunc("GET /stats", a.handleStats)             // protected

	if a.metrics != nil {
		mux.Handle("GET /metrics", a.metrics)
	}

	// The rest are protected by default.
	public := map[string]struct{}{
		"GET /status":         {},
		"POST /auth/register": {},
		"POST /auth/login":    {},
		"GET /metrics":        {}, // Prometheus sends no bearer token
	}

	// Order matters. RequestID is outermost, so every line below can carry the id.
	// LogRequests goes outside Guard, so the access log also sees the 401s and 503s
	// Guard answers before any handler runs. Recoverer goes inside LogRequests so the
	// access log still records the 500 it writes, and outside Guard so a panic in the
	// auth path is caught too.
	return RequestID(a.LogRequests(a.Recoverer(a.auth.Guard(mux, public))))
}

// handleStatus answers the health check.
func (a *API) handleStatus(w http.ResponseWriter, r *http.Request) {
	a.writeJSONLog(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	snap, err := a.stats.Snapshot(r.Context())
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	a.writeJSONLog(w, r, http.StatusOK, snap)
}

func writeJSON(w http.ResponseWriter, status int, payload any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(payload)
}

// writeJSONLog writes payload and reports a failed encode at error level, in both
// modes. The status line is already on the wire by then, so nothing downstream can
// turn it into a 500 and this log line is the only trace it leaves.
func (a *API) writeJSONLog(w http.ResponseWriter, r *http.Request, status int, payload any) {
	if err := writeJSON(w, status, payload); err != nil {
		a.log.Ctx(r.Context()).Errorf("httpapi: %s %s -> %d: encode response: %v",
			r.Method, r.URL.Path, status, err)
	}
}

// maxAuthBody caps the request body for the auth endpoints. Credentials are
// small, so 4 KiB is plenty and keeps a huge body from wasting memory.
const maxAuthBody = 4_096

func (a *API) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req auth.RegisterRequest
	if !a.decodeJSON(w, r, &req) {
		return
	}
	if err := a.auth.Register(r.Context(), req); err != nil {
		a.writeError(w, r, err)
		return
	}
	a.writeJSONLog(w, r, http.StatusCreated, map[string]string{"status": "registered"})
}

func (a *API) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req auth.LoginRequest
	if !a.decodeJSON(w, r, &req) {
		return
	}
	token, expiresAt, err := a.auth.Login(r.Context(), req.Email, req.Password)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	a.writeJSONLog(w, r, http.StatusOK, auth.TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		// expires_in is the number of seconds left, not a timestamp.
		ExpiresIn: int64(time.Until(expiresAt).Seconds()),
	})
}

func (a *API) handleLogout(w http.ResponseWriter, r *http.Request) {
	// Guard has already checked the token, so ok is true. The check is here to
	// show that link and to fail safely if this route ever becomes public.
	claims, ok := auth.ClaimsFrom(r.Context())
	if !ok {
		a.writeError(w, r, &apperrors.UnauthorizedError{Err: errors.New("no claims in context")})
		return
	}
	if err := a.auth.Logout(r.Context(), claims); err != nil {
		a.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxAuthBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		a.writeError(w, r, &apperrors.ValidationError{Err: err})
		return false
	}
	return true
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeError is the only place a domain error becomes an HTTP status, and adding a new
// error type without adding a branch here gives a 500.
func (a *API) writeError(w http.ResponseWriter, r *http.Request, err error) {
	if e, ok := errors.AsType[*apperrors.ValidationError](err); ok {
		a.logError(r, e, http.StatusBadRequest)
		a.writeJSONLog(w, r, http.StatusBadRequest, errorBody{e.Code(), e.Err.Error()})
		return
	}
	if e, ok := errors.AsType[*apperrors.UnauthorizedError](err); ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="wikirecent"`)
		a.logError(r, e, http.StatusUnauthorized)
		// One general message on purpose: never say which check failed.
		a.writeJSONLog(w, r, http.StatusUnauthorized, errorBody{e.Code(), "invalid credentials"})
		return
	}
	if e, ok := errors.AsType[*apperrors.ConflictError](err); ok {
		a.logError(r, e, http.StatusConflict)
		a.writeJSONLog(w, r, http.StatusConflict, errorBody{e.Code(), "email already registered"})
		return
	}
	if e, ok := errors.AsType[*apperrors.NotFoundError](err); ok {
		a.logError(r, e, http.StatusNotFound)
		a.writeJSONLog(w, r, http.StatusNotFound, errorBody{e.Code(), "not found"})
		return
	}
	if e, ok := errors.AsType[*apperrors.RepositoryError](err); ok {
		// The store is down, so the answer is "unavailable", not "ok".
		a.logError(r, e, http.StatusServiceUnavailable)
		a.writeJSONLog(w, r, http.StatusServiceUnavailable, errorBody{e.Code(), "store unavailable"})
		return
	}

	// Unmapped: keep the code when the error carries one, so a missing branch is
	// still identifiable in the log rather than an anonymous 500.
	a.log.Ctx(r.Context()).AppErrorf(err, "httpapi: %s %s -> 500 unmapped error: %v",
		r.Method, r.URL.Path, err)
	a.writeJSONLog(w, r, http.StatusInternalServerError, errorBody{"INTERNAL_ERROR", "internal error"})
}

func (a *API) logError(r *http.Request, err apperrors.Error, status int) {
	log := a.log.Ctx(r.Context())
	if status >= http.StatusInternalServerError {
		log.AppErrorf(err, "httpapi: %s %s -> %d: %v", r.Method, r.URL.Path, status, err)
		return
	}
	log.Warnf("[%s] httpapi: %s %s -> %d: %v", err.Code(), r.Method, r.URL.Path, status, err)
}
