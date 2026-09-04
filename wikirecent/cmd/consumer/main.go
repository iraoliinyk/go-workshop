package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"wikirecent/internal/applog"
	"wikirecent/internal/auth"
	"wikirecent/internal/config"
	"wikirecent/internal/db/cassandra"
	"wikirecent/internal/flusher"
	"wikirecent/internal/httpapi"
	"wikirecent/internal/lifecycle"
	"wikirecent/internal/repository"
	"wikirecent/internal/stats/statsmodels"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const (
	flushGrace      = 5 * time.Second
	shutdownTimeout = 15 * time.Second
)

func main() {
	cfg, err := config.LoadConsumer()
	if err != nil {
		log.Fatalf("config: %v", err) // main owns the process, so it may exit
	}

	logger, err := applog.New(cfg.Logger, os.Stderr)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	stores, err := repository.New(ctx, repository.Config{
		Backend: cfg.DBBackend,
		Cassandra: cassandra.Config{
			Hosts:                cfg.CassandraHosts,
			Keyspace:             cfg.CassandraKeyspace,
			DC:                   cfg.CassandraLocalDC,
			ReplicationFactor:    cfg.CassandraReplicationFactor,
			DisablePeerDiscovery: cfg.CassandraDisablePeerDiscovery,
			Consistency:          gocql.ParseConsistency(cfg.CassandraConsistency),
			Timeout:              cfg.CassandraTimeout,
		},
	})
	if err != nil {
		log.Fatalf("repository: %v", err)
	}
	defer func() { _ = stores.Close() }()

	snapshotFlusher, err := flusher.New(stores.Stats, stores.Stats, flusher.Config{
		Interval:      cfg.StatsFlushInterval,
		ShutdownGrace: flushGrace,
		Log:           logger,
	})
	if err != nil {
		log.Fatalf("flusher: %v", err)
	}

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		if err := snapshotFlusher.Run(ctx); err != nil {
			logger.AppErrorf(err, "flusher stopped")
		}
	}()

	authSvc, err := auth.New(stores.Users, stores.Tokens, auth.Config{
		Secret: cfg.JWTSecret,
		Issuer: cfg.JWTIssuer,
		TTL:    cfg.AccessTokenTTL,
		Cost:   cfg.BcryptCost,
		Log:    logger,
	})
	if err != nil {
		log.Fatalf("auth: %v", err)
	}

	api := httpapi.New(statsReader{stores.Stats}, authSvc, logger, nil)

	runner, err := lifecycle.New(lifecycle.Config{
		Addr:            ":" + strconv.Itoa(cfg.Port),
		Handler:         api.Router(),
		Shutdown:        shutdown(logger, flushDone),
		ShutdownTimeout: shutdownTimeout,
		Log:             logger,
	})
	if err != nil {
		log.Fatalf("lifecycle: %v", err)
	}

	logger.Debugf("consumer listening on :%d, topic %q, group %q", cfg.Port, cfg.Topic, cfg.Group)
	runner.Run(ctx)
}

type statsReader struct{ store repository.StatsStore }

func (r statsReader) Snapshot(ctx context.Context) (statsmodels.Snapshot, error) {
	return r.store.LatestSnapshot(ctx, time.Now().UTC())
}

// waitFor blocks until done is closed, bounded by the runner's shutdown budget so one
// stuck goroutine cannot hold the process open.
func waitFor(ctx context.Context, logger applog.Logger, done <-chan struct{}, what string) {
	select {
	case <-done:
	case <-ctx.Done():
		logger.AppErrorf(ctx.Err(), "gave up waiting for %s", what)
	}
}

// shutdown unwinds what the runner does not own.
func shutdown(logger applog.Logger, flushDone <-chan struct{}) lifecycle.Hook {
	return func(ctx context.Context) error {
		waitFor(ctx, logger, flushDone, "the last snapshot flush")
		return nil
	}
}
