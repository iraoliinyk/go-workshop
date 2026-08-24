package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"wikirecent/internal/applog"
	"wikirecent/internal/broker"
	"wikirecent/internal/codec"
	"wikirecent/internal/config"
	"wikirecent/internal/lifecycle"
	"wikirecent/internal/wikistream"
)

func main() {
	cfg, err := config.LoadProducer()
	if err != nil {
		log.Fatalf("config: %v", err) // main owns the process, so it may exit
	}

	logger, err := applog.New(cfg.Logger, os.Stderr)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	pub, err := broker.NewPublisher(broker.PublisherConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
	}, logger)
	if err != nil {
		log.Fatalf("broker: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runner, err := lifecycle.New(lifecycle.Config{
		Addr:     ":" + strconv.Itoa(cfg.Port),
		Handler:  livenessRouter(),
		Startup:  startup(logger, cfg, pub),
		Shutdown: pub.Close,
		Log:      logger,
		// ShutdownTimeout omitted: 5s is plenty for one Flush.
	})
	if err != nil {
		log.Fatalf("lifecycle: %v", err)
	}

	logger.Debugf("producer starting on :%d, topic %q", cfg.Port, cfg.Topic)
	runner.Run(ctx)
}

// livenessRouter serves the one route the container healthcheck needs. It is not
// httpapi.Router: that needs an *auth.Service and a stats recorder, neither of
// which the producer has.
func livenessRouter() http.Handler {
	mux := http.NewServeMux()
	// Plain GET: busybox `wget --spider`
	mux.HandleFunc("GET /liveness", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// startup pings the broker, then drives the stream from its own goroutine.
func startup(logger applog.Logger, cfg config.Producer, pub *broker.Publisher) lifecycle.Hook {
	return func(ctx context.Context) error {
		if err := pub.Connect(ctx); err != nil {
			return err
		}

		// Its own goroutine, because Start only returns when ctx is cancelled: it
		// reconnects on failure rather than giving the process back.
		go func() {
			sink := codec.NewProtoSink(pub, logger)
			err := wikistream.Start(ctx, wikistream.Config{
				URL:       cfg.URL,
				UserAgent: cfg.UserAgent,
				Accept:    cfg.Accept,
			}, http.DefaultClient, sink, logger)
			// A cancelled context is the shutdown signal, not a problem to report.
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.AppErrorf(err, "wiki stream ended")
			}
		}()
		return nil
	}
}
