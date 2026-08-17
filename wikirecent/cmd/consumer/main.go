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
	"wikirecent/internal/config"
	"wikirecent/internal/db/cassandra"
	"wikirecent/internal/flusher"
	"wikirecent/internal/httpapi"
	"wikirecent/internal/lifecycle"
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
			Hosts:       cfg.CassandraHosts,
			Keyspace:    cfg.CassandraKeyspace,
			Consistency: gocql.ParseConsistency(cfg.CassandraConsistency),
			Timeout:     cfg.CassandraTimeout,
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

	sub, err := broker.NewSubscriber(broker.SubscriberConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		Group:   cfg.Group,
	}, logger, liveStats)
	if err != nil {
		log.Fatalf("broker: %v", err)
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

	api := httpapi.New(liveStats, authSvc, logger)

	runner, err := lifecycle.New(lifecycle.Config{
		Addr:            ":" + strconv.Itoa(cfg.Port),
		Handler:         api.Router(),
		Startup:         startup(logger, sub),
		Shutdown:        shutdown(logger, sub, flushDone),
		ShutdownTimeout: shutdownTimeout,
		Log:             logger,
	})
	if err != nil {
		log.Fatalf("lifecycle: %v", err)
	}

	logger.Debugf("consumer listening on :%d, topic %q, group %q", cfg.Port, cfg.Topic, cfg.Group)
	runner.Run(ctx)
}

// startup pings the broker, then runs the poll loop in its own goroutine.
func startup(logger applog.Logger, sub *broker.Subscriber) lifecycle.Hook {
	return func(ctx context.Context) error {
		if err := sub.Connect(ctx); err != nil {
			return err
		}
		go func() {
			if err := sub.Run(ctx); err != nil {
				logger.AppErrorf(err, "subscription ended")
			}
		}()
		return nil
	}
}

// shutdown unwinds what the runner does not own.
func shutdown(logger applog.Logger, sub *broker.Subscriber, flushDone <-chan struct{}) lifecycle.Hook {
	return func(ctx context.Context) error {
		if err := sub.Close(ctx); err != nil {
			logger.AppErrorf(err, "subscriber close failed")
		}

		// The signal already cancelled the flusher's context, so its final save is
		// running now. Wait for it, but inside the runner's budget: a hung
		// SaveSnapshot must not hold the process open past ShutdownTimeout.
		select {
		case <-flushDone:
		case <-ctx.Done():
			logger.AppErrorf(ctx.Err(), "gave up waiting for the final snapshot")
		}
		return nil
	}
}
