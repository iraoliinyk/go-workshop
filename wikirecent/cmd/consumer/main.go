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
	"wikirecent/internal/broker"
	"wikirecent/internal/codec"
	"wikirecent/internal/config"
	"wikirecent/internal/db/cassandra"
	"wikirecent/internal/flusher"
	"wikirecent/internal/httpapi"
	"wikirecent/internal/lifecycle"
	"wikirecent/internal/metrics"
	"wikirecent/internal/repository"
	"wikirecent/internal/stats"

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

	liveStats := stats.New()

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

	snapshotFlusher, err := flusher.New(liveStats, stores.Stats, flusher.Config{
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

	reg := metrics.NewRegistry()
	eventMetrics := metrics.NewEvents(reg)

	batchObserver := metrics.NewBatchCounters(
		eventMetrics.ConsumedFromRedpanda,
		eventMetrics.Processed,
		eventMetrics.Failed,
	)

	subscriberConfig := broker.SubscriberConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		Group:          cfg.Group,
		MaxPollRecords: cfg.MaxPollRecords,
		FetchMinBytes:  cfg.FetchMinBytes,
		FetchMaxWait:   cfg.FetchMaxWait,
		DrainGrace:     cfg.ConsumerDrainGrace,
	}

	totals, err := stores.Stats.Totals(ctx, time.Now().UTC())
	if err != nil {
		logger.AppErrorf(err, "could not restore totals, starting from zero")
	} else {
		liveStats.Seed(totals)
	}

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

	api := httpapi.New(liveStats, authSvc, logger, metrics.Handler(reg))

	pool, err := broker.NewPool(broker.PoolConfig{
		SubscriberConfig: subscriberConfig,
		Workers:          cfg.ConsumerWorkers,
		MaxPartitions:    cfg.MaxPartitions,
	}, logger, liveStats, codec.EventDecoder{}, stores.Stats, batchObserver)
	if err != nil {
		log.Fatalf("broker: %v", err)
	}
	poolDone := make(chan struct{})

	runner, err := lifecycle.New(lifecycle.Config{
		Addr:            ":" + strconv.Itoa(cfg.Port),
		Handler:         api.Router(),
		Startup:         startup(logger, pool, poolDone),
		Shutdown:        shutdown(logger, pool, poolDone, flushDone),
		ShutdownTimeout: shutdownTimeout,
		Log:             logger,
	})
	if err != nil {
		log.Fatalf("lifecycle: %v", err)
	}

	logger.Debugf("consumer listening on :%d, topic %q, group %q", cfg.Port, cfg.Topic, cfg.Group)
	runner.Run(ctx)
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

// startup pings the broker, then runs the poll loop in its own goroutine.
func startup(logger applog.Logger, pool *broker.Pool, poolDone chan struct{}) lifecycle.Hook {
	return func(ctx context.Context) error {
		if err := pool.Connect(ctx); err != nil {
			return err
		}
		go func() {
			defer close(poolDone)
			if err := pool.Run(ctx); err != nil {
				logger.AppErrorf(err, "consumer pool stopped")
			}
		}()
		return nil
	}
}

// shutdown unwinds what the runner does not own.
func shutdown(logger applog.Logger, pool *broker.Pool,
	poolDone, flushDone <-chan struct{}) lifecycle.Hook {
	return func(ctx context.Context) error {
		// Wait for the workers to leave their poll loops BEFORE taking their clients away.
		waitFor(ctx, logger, poolDone, "the consumer pool")
		if err := pool.Close(ctx); err != nil {
			logger.AppErrorf(err, "pool close failed")
		}

		// Last, because the flusher's final save runs on a fresh context, and main's
		// deferred stores.Close() would close the session under it.
		waitFor(ctx, logger, flushDone, "the last snapshot flush")
		return nil
	}
}
