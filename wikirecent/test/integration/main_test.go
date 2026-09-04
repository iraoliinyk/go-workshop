//go:build integration

// Package integration runs consumer against real infrastructure: a Redpanda
// broker and a Cassandra node, both started by testcontainers.
//
//	go test -tags=integration ./test/integration/...
//
// Docker is the only requirement.
package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tccassandra "github.com/testcontainers/testcontainers-go/modules/cassandra"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const (
	redpandaImage         = "redpandadata/redpanda:v25.1.7"
	cassandraImage        = "cassandra:5.0.8"
	startupBudget         = 5 * time.Minute
	brokerReadyBudget     = time.Minute
	redpandaNetworkAlias  = "redpanda"
	redpandaNetworkAddr   = redpandaNetworkAlias + ":29092"
	cassandraNetworkAlias = "cassandra"
)

var (
	kafkaBroker   string // host:port of the Redpanda Kafka API
	cassandraHost string // host:port of the CQL port

	sharedAdmin   *kadm.Client
	sharedNetwork *testcontainers.DockerNetwork // lets sidecar containers, e.g. rpcn-connect, reach the two above
)

func runRedpanda(ctx context.Context) (*tcredpanda.Container, error) {
	return tcredpanda.Run(ctx, redpandaImage,
		network.WithNetwork([]string{redpandaNetworkAlias}, sharedNetwork),
		// Only the compose file's own address is registered by default, so a
		// sidecar container on sharedNetwork needs its own advertised listener.
		tcredpanda.WithListener(redpandaNetworkAddr),
	)
}

func waitForBroker(ctx context.Context, client *kgo.Client) error {
	deadline := time.Now().Add(brokerReadyBudget)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := client.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("broker at %s did not answer within %s: %w",
				kafkaBroker, brokerReadyBudget, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// runCassandra starts a Cassandra node. newCassandraNode calls this directly for a
// throwaway per-test node; run below adds sharedNetwork so sidecar containers, e.g.
// rpcn-connect, can also reach the one shared node it starts.
func runCassandra(ctx context.Context, extra ...testcontainers.ContainerCustomizer) (*tccassandra.CassandraContainer, error) {
	opts := append([]testcontainers.ContainerCustomizer{
		testcontainers.WithEnv(map[string]string{
			"CASSANDRA_CLUSTER_NAME": "wikistream_it",
			"MAX_HEAP_SIZE":          "512M",
			"HEAP_NEWSIZE":           "100M",
		}),
		testcontainers.WithAdditionalWaitStrategyAndDeadline(startupBudget,
			wait.ForListeningPort("9042/tcp")),
	}, extra...)
	return tccassandra.Run(ctx, cassandraImage, opts...)
}

func TestMain(m *testing.M) {
	// A separate function, because os.Exit does not run deferred calls and the
	// containers have to be removed even when a test panics.
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	started := time.Now()

	var (
		redpandaNode  *tcredpanda.Container
		cassandraNode *tccassandra.CassandraContainer
	)

	defer func() {
		if err := testcontainers.TerminateContainer(redpandaNode); err != nil {
			log.Printf("integration: failed to terminate redpanda: %s", err)
		}
		if err := testcontainers.TerminateContainer(cassandraNode); err != nil {
			log.Printf("integration: failed to terminate cassandra: %s", err)
		}
		// After both containers, since a network with any container still attached
		// to it cannot be removed.
		if sharedNetwork != nil {
			if err := sharedNetwork.Remove(ctx); err != nil {
				log.Printf("integration: failed to remove shared network: %s", err)
			}
		}
	}()

	var err error
	if sharedNetwork, err = network.New(ctx); err != nil {
		log.Printf("integration: could not create the shared network: %s", err)
		return 1
	}

	startup, startupCtx := errgroup.WithContext(ctx)
	startup.Go(func() error {
		var err error
		if redpandaNode, err = runRedpanda(startupCtx); err != nil {
			return err
		}
		kafkaBroker, err = redpandaNode.KafkaSeedBroker(startupCtx)
		return err
	})
	startup.Go(func() error {
		var err error
		if cassandraNode, err = runCassandra(startupCtx, network.WithNetwork([]string{cassandraNetworkAlias}, sharedNetwork)); err != nil {
			return err
		}
		cassandraHost, err = cassandraNode.ConnectionHost(startupCtx)
		return err
	})
	if err := startup.Wait(); err != nil {
		log.Printf("integration: could not start the containers: %s", err)
		log.Printf("integration: these tests start everything themselves; Docker has to be running")
		return 1
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaBroker))
	if err != nil {
		log.Printf("integration: could not build a client for %s: %s", kafkaBroker, err)
		return 1
	}
	defer client.Close()
	sharedAdmin = kadm.NewClient(client)

	if err := waitForBroker(ctx, client); err != nil {
		log.Printf("integration: %s", err)
		return 1
	}

	log.Printf("integration: redpanda at %s, cassandra at %s, ready in %s",
		kafkaBroker, cassandraHost, time.Since(started).Round(time.Second))

	return m.Run()
}
