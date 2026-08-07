package httpapi

import (
	"net/http"
	"runtime/debug"
	"time"

	"ch-4/internal/applog"

	"github.com/felixge/httpsnoop"
	"github.com/google/uuid"
)

const requestIDHeader = "X-Request-Id"

// maxRequestIDLen bounds an inbound id, which is caller input.
const maxRequestIDLen = 128

// RequestID reuses the caller's X-Request-Id, or generates one when there is none,
// and puts it in the response header and the request context so every log line of
// this request can carry it.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := inboundRequestID(r)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(applog.WithRequestID(r.Context(), id)))
	})
}

// inboundRequestID returns the caller's X-Request-Id when it is safe to reuse, so a
// trace started by a proxy or gateway continues through this service. It returns ""
// for anything too long or outside printable ASCII, because the value is echoed in a
// response header and written to the logs.
func inboundRequestID(r *http.Request) string {
	id := r.Header.Get(requestIDHeader)
	if id == "" || len(id) > maxRequestIDLen {
		return ""
	}
	for i := 0; i < len(id); i++ {
		if id[i] <= ' ' || id[i] > '~' {
			return ""
		}
	}
	return id
}

// LogRequests logs one line per request, once the response is written.
func (a *API) LogRequests(next http.Handler) http.Handler {
	if !a.log.Enabled() {
		return next // PROD: no wrapper at all
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := httpsnoop.CaptureMetrics(next, w, r)
		// Microseconds, not milliseconds
		a.logAtStatus(r, m.Code, "%s %s -> %d (%dB) in %s",
			r.Method, r.URL.Path, m.Code, m.Written, m.Duration.Round(time.Microsecond))
	})
}

// logAtStatus writes one line at the level the status deserves: a 4xx is the caller's
// fault and warns, a 5xx is ours and errors, everything else is a diagnostic. This is
// also what gives a DEBUG stream its tint colours: yellow for warnings, red for errors.
func (a *API) logAtStatus(r *http.Request, status int, format string, args ...any) {
	log := a.log.Ctx(r.Context())
	switch {
	case status >= http.StatusInternalServerError:
		log.Errorf(format, args...)
	case status >= http.StatusBadRequest:
		log.Warnf(format, args...)
	default:
		log.Debugf(format, args...)
	}
}

// Recoverer turns a panic in a handler into a 500 and a logged stack trace. Without
// it net/http catches the panic itself, kills the connection, and leaves nothing to
// see in PROD.
func (a *API) Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var wrote bool
		snoop := httpsnoop.Wrap(w, httpsnoop.Hooks{
			WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
				return func(code int) { wrote = true; next(code) }
			},
			Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return func(b []byte) (int, error) { wrote = true; return next(b) }
			},
		})

		defer func() {
			// recover returns any, not error: the value is whatever was passed to
			// panic, so it is often a plain string.
			panicked := recover()
			if panicked == nil {
				return
			}
			// ErrAbortHandler is net/http's "drop this connection quietly" signal
			if panicked == http.ErrAbortHandler {
				panic(panicked)
			}
			a.log.Ctx(r.Context()).Errorf("httpapi: %s %s panicked: %v\n%s",
				r.Method, r.URL.Path, panicked, debug.Stack())
			if wrote {
				return // a partial response is out; there is no status left to set
			}
			a.writeJSONLog(w, r, http.StatusInternalServerError, errorBody{"INTERNAL_ERROR", "internal error"})
		}()

		next.ServeHTTP(snoop, r)
	})
}
