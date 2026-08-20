package auth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Guard wraps mux so every route needs a token unless its registered pattern is
// listed in public. Spring Security has .anyExchange().authenticated() for this; Go's
// standard library has nothing similar, so it is written by hand.
func (s *Service) Guard(mux *http.ServeMux, public map[string]struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// mux.Handler gives the pattern the route was registered under.
		_, pattern := mux.Handler(r)
		if _, isPublic := public[pattern]; isPublic {
			mux.ServeHTTP(w, r)
			return
		}
		s.authenticate(mux, w, r)
	})
}

// authenticate checks the bearer token, checks whether it was revoked, and puts the
// claims into the request context before calling next.
func (s *Service) authenticate(next http.Handler, w http.ResponseWriter, r *http.Request) {
	raw, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		s.unauthorized(w, r, "missing or malformed Authorization header")
		return
	}

	claims, err := s.Verify(raw)
	if err != nil {
		s.unauthorized(w, r, fmt.Sprintf("token rejected: %v", err))
		return
	}

	revoked, err := s.tokens.IsRevoked(r.Context(), claims.ID)
	if err != nil {
		// Error level, not debug: this answers 503, and a revocation store that is
		// down must not be visible only to whoever is running in DEBUG.
		s.log.Ctx(r.Context()).AppErrorf(err, "auth: %s %s: revocation store unavailable: %v",
			r.Method, r.URL.Path, err)
		writeAuthError(w, http.StatusServiceUnavailable, "REPOSITORY_ERROR", "revocation store unavailable")
		return
	}
	if revoked {
		s.unauthorized(w, r, "token revoked (jti "+claims.ID+")")
		return
	}

	next.ServeHTTP(w, r.WithContext(WithClaims(r.Context(), claims)))
}

// bearerToken splits an Authorization header value. RFC 7235 makes the scheme name
// case-insensitive, so "bearer x" is valid, but the token itself is not.
func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}

// unauthorized answers the one generic 401 and, in DEBUG only, records why. A
// rejected token is the caller's problem.
func (s *Service) unauthorized(w http.ResponseWriter, r *http.Request, reason string) {
	s.log.Ctx(r.Context()).Warnf("[UNAUTHORIZED_ERROR] auth: %s %s rejected: %s",
		r.Method, r.URL.Path, reason)
	w.Header().Set("WWW-Authenticate", `Bearer realm="wiki-stream-go"`)
	writeAuthError(w, http.StatusUnauthorized, "UNAUTHORIZED_ERROR", "unauthorized")
}

func writeAuthError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": msg})
}
