// Shared server lifecycle with startup and shutdown steps
package lifecycle

import (
	"context"
	"errors"
	"net/http"
	"time"
	"wikirecent/internal/applog"
)

const (
	defaultShutdownTimeout = 5 * time.Second
	readTimeout            = 5 * time.Second
	readHeaderTimeout      = 2 * time.Second
	writeTimeout           = 10 * time.Second
)

type Hook func(ctx context.Context) error

type Config struct {
	Addr            string
	Handler         http.Handler
	Startup         Hook // optional; runs once, after the listener goroutine starts
	Shutdown        Hook // optional; adapters flush and close here
	ShutdownTimeout time.Duration
	Log             applog.Logger // zero value is PROD, which still reports an error
}

type Runner struct {
	server          *http.Server
	log             applog.Logger
	startup         Hook
	shutdownHook    Hook
	shutdownTimeout time.Duration
}

// New rejects a half-configured server and fills in the rest.
func New(cfg Config) (*Runner, error) {
	switch {
	case cfg.Addr != "" && cfg.Handler == nil:
		return nil, errors.New("lifecycle: Addr is set but Handler is nil")
	case cfg.Addr == "" && cfg.Handler != nil:
		return nil, errors.New("lifecycle: Handler is set but Addr is empty")
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}

	r := &Runner{
		log:             cfg.Log,
		startup:         orNoop(cfg.Startup),
		shutdownHook:    orNoop(cfg.Shutdown),
		shutdownTimeout: cfg.ShutdownTimeout,
	}
	if cfg.Handler != nil {
		r.server = &http.Server{
			Addr:              cfg.Addr,
			Handler:           cfg.Handler,
			ReadTimeout:       readTimeout,
			ReadHeaderTimeout: readHeaderTimeout,
			WriteTimeout:      writeTimeout,
		}
	}
	return r, nil
}

// orNoop keeps Run free of nil checks.
func orNoop(h Hook) Hook {
	if h != nil {
		return h
	}
	return func(context.Context) error { return nil }
}

func (r *Runner) Run(ctx context.Context) {
	if r.server != nil {
		go func() {
			if err := r.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				r.log.AppErrorf(err, "server error")
			}
		}()
	}

	if err := r.startup(ctx); err != nil {
		r.log.AppErrorf(err, "startup hook failed")
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancel()
	if r.server != nil {
		_ = r.server.Shutdown(shutdownCtx)
	}
	_ = r.shutdownHook(shutdownCtx) // adapters flush and close here
}
