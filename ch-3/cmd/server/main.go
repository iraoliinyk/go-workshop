package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"ch-3/internal/apperrors"
	"ch-3/internal/applog"
	"ch-3/internal/auth"
	"ch-3/internal/config"
	"ch-3/internal/consumer"
	"ch-3/internal/db/cassandra"
	"ch-3/internal/flusher"
	"ch-3/internal/httpapi"
	"ch-3/internal/repository"
	"ch-3/internal/stats"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	liveStats := stats.New()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err) // main owns the process, so it may exit
	}
	if cfg.StatsFlushInterval <= 0 {
		log.Fatalf("config: STATS_FLUSH_INTERVAL must be positive, got %s", cfg.StatsFlushInterval)
	}

	// Built before anything that logs. An unrecognised LOGGER is fatal rather
	// than a silent fallback to PROD, which would look like "logging is broken".
	logger, err := applog.New(cfg.Logger, os.Stderr)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("log mode: %s", strings.ToUpper(cfg.Logger))

	stores, err := repository.New(ctx, repository.Config{
		Backend: cfg.DBBackend,
		Cassandra: cassandra.Config{
			Hosts:       cfg.CassandraHosts,
			Keyspace:    cfg.CassandraKeyspace,
			Consistency: gocql.ParseConsistency(cfg.CassandraConsistency),
			Timeout:     cfg.CassandraTimeout,
		},
	})
	if err != nil {
		log.Fatalf("repository: %v", err)
	}
	defer stores.Close()

	snapshotFlusher, err := flusher.New(liveStats, stores.Stats, flusher.Config{
		Interval:      cfg.StatsFlushInterval,
		ShutdownGrace: 5 * time.Second,
		Log:           logger,
	})
	if err != nil {
		// flusher.New checks the interval, so main no longer needs its own check.
		log.Fatalf("flusher: %v", err)
	}

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		if err := snapshotFlusher.Run(ctx); err != nil {
			log.Printf("%v", err)
		}
	}()

	// consumerErr carries the final error from the consumer goroutine, so main
	// decides when to stop the process instead of consumer.Start.
	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Start(ctx, consumer.Config{
			URL:       cfg.URL,
			UserAgent: cfg.UserAgent,
			Accept:    cfg.Accept,
		}, http.DefaultClient, liveStats)
	}()

	// Built before ListenAndServe, so a bad secret or cost stops the process now
	// instead of failing every request once traffic arrives.
	authSvc, err := auth.New(stores.Users, stores.Tokens, auth.Config{
		Secret: cfg.JWTSecret,
		Issuer: cfg.JWTIssuer,
		TTL:    cfg.AccessTokenTTL,
		Cost:   cfg.BcryptCost,
		Log:    logger,
	})
	if err != nil {
		log.Fatalf("auth: %v", err) // main owns the process, so it may exit
	}

	api := httpapi.New(liveStats, authSvc, logger)

	// No retry with backoff for now: Compose restarts the container instead.
	addr := fmt.Sprintf(":%d", cfg.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      api.Router(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// shutdown stops everything in order. Both the signal handler and the
	// consumer-error path call it.
	shutdown := func(reason string) {
		log.Printf("shutting down: %s", reason)
		cancel() // stop the consumer goroutine

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		// Stop accepting new requests and let the current ones finish.
		if err := server.Shutdown(shutdownCtx); err != nil {
			shutdownError := &apperrors.ShutdownError{Err: err}
			log.Printf("[%s] %v", shutdownError.Code(), shutdownError)
		}

		// Wait for the flusher's last save. cancel() above asked Run to make it.
		// Without this line main can return while SaveSnapshot is still running,
		// which loses the final snapshot and lets stores.Close() close the session
		// under it. ShutdownGrace limits the save, so this wait cannot hang.
		<-flushDone
	}

	done := make(chan struct{}) // used only as a signal, so it carries no value
	go func() {
		defer close(done) // closing tells every reader that shutdown has finished
		select {
		case sig := <-quit:
			shutdown(sig.String())
		case err := <-consumerErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				if appErr, ok := errors.AsType[apperrors.Error](err); ok {
					log.Printf("[%s] consumer failed: %v", appErr.Code(), appErr)
				} else {
					log.Printf("consumer failed: %v", err)
				}
			}
			shutdown("consumer exited")
		}
	}()

	log.Println("listening on " + addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-done // wait for shutdown to finish
}
