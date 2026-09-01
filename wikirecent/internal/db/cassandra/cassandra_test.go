//go:build integration

//	 Run with:
//		docker compose up -d cassandra1
//		go test -tags=integration ./internal/db/cassandra/...

package cassandra_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"wikirecent/internal/db/cassandra"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultHost is where docker-compose.yaml publishes cassandra1, the only node it
// publishes.
const defaultHost = "127.0.0.1:9042"

// testHosts is where every test connects. TestMain sets it once, before any test
// runs, and nothing writes to it afterwards.
var testHosts []string

func TestMain(m *testing.M) {
	testHosts = strings.Split(defaultHost, ",")
	if h := os.Getenv("CASSANDRA_HOSTS"); h != "" {
		testHosts = strings.Split(h, ",")
	}

	// Fail here rather than let every test time out one by one against a node that is
	// not there. A closed port usually means compose was not started, so say so.
	if err := waitForCQL(testHosts[0], 30*time.Second); err != nil {
		log.Printf("integration: no Cassandra at %s: %v", testHosts[0], err)
		log.Printf("integration: start one with `docker compose up -d cassandra1`, or set CASSANDRA_HOSTS")
		os.Exit(1)
	}
	log.Printf("integration: using cassandra at %s", strings.Join(testHosts, ","))

	os.Exit(m.Run())
}

// waitForCQL dials the CQL port until it answers, because compose up -d returns long
// before Cassandra binds 9042. A plain TCP dial is enough: the first Connect does the
// real handshake and reports any failure properly.
func waitForCQL(host string, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	var lastErr error
	for {
		conn, err := net.DialTimeout("tcp", host, 2*time.Second)
		if err == nil {
			return conn.Close()
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("not reachable within %s: %w", budget, lastErr)
		}
		time.Sleep(time.Second)
	}
}

// itConfig points at testHosts and uses its own keyspace, so a run against a
// shared node cannot damage the dev data in "wikistream".
func itConfig(t *testing.T) cassandra.Config {
	t.Helper()
	require.NotEmpty(t, testHosts, "TestMain did not set testHosts")

	keyspace := "wikistream_it"
	if k := os.Getenv("CASSANDRA_IT_KEYSPACE"); k != "" {
		keyspace = k
	}

	return cassandra.Config{
		Hosts:    testHosts,
		Keyspace: keyspace,
		// 1, so these tests pass against a single node and do not need the rest of the
		// cluster up. No DC either: Connect reads the name from the node, which keeps
		// this working against both a bare node (datacenter1) and compose (dc1).
		ReplicationFactor: 1,
		// testHosts is reachable, the compose peers are not: they announce container
		// IPs. Following them costs 48s of connect timeouts per session.
		DisablePeerDiscovery: true,
		// The level the app runs at, for reads and writes both. Every test depends on
		// it: a revoked token has to be visible to the very next read.
		Consistency: gocql.LocalQuorum,
		// Long, because the first CREATE TABLE on a new node is far slower than
		// a normal query.
		Timeout: 15 * time.Second,
	}
}

// connect opens a session and closes it when the test ends. Connect also creates
// the keyspace and the four tables, so calling it here tests bootstrap.go too.
func connect(t *testing.T) *gocql.Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sess, err := cassandra.Connect(ctx, itConfig(t))
	require.NoError(t, err, "Connect")
	t.Cleanup(func() { sess.Close() })
	return sess
}

// unique stops repeated runs from clashing. Nothing deletes rows between runs, so
// any test that expects a first write to succeed needs a key no run has used yet.
func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestConnect_RejectsBadKeyspace(t *testing.T) {
	cfg := itConfig(t)
	cfg.Keyspace = "bad; DROP KEYSPACE wikistream"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := cassandra.Connect(ctx, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid keyspace name")
}
