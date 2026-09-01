package cassandra

import (
	"context"
	"fmt"
	"io"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type Config struct {
	Hosts    []string
	Keyspace string
	// DC pins one datacenter of several. Empty takes the one the contact node
	// reports, which is what a single datacenter wants.
	DC                string
	ReplicationFactor int
	// DisablePeerDiscovery keeps the driver on Hosts and stops it from following
	// gossip to the rest of the cluster. Needed when the other nodes are announced
	// on addresses this process cannot route to, which is the case from the host
	// against docker-compose: only cassandra1 is published, and the peers advertise
	// container IPs. Without it every session spends ConnectTimeout on each
	// unreachable peer before it gives up.
	DisablePeerDiscovery bool
	Consistency          gocql.Consistency
	Timeout              time.Duration
}

// 1, because that is the only factor a single node can serve at QUORUM.
const defaultReplicationFactor = 1

func Connect(ctx context.Context, cfg Config) (*gocql.Session, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("cassandra: no hosts configured")
	}
	if cfg.Keyspace == "" {
		return nil, fmt.Errorf("cassandra: no keyspace configured")
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = defaultReplicationFactor
	}

	// Two steps, because a pooled session cannot switch keyspaces with USE.
	// First create the keyspace from a session that has none.
	dc, err := bootstrapKeyspace(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// Carry the resolved name forward, so the session routes to the datacenter the
	// replicas actually went into.
	cfg.DC = dc

	// Then open the session the app will use and create the tables in it.
	sess, err := cfg.cluster(cfg.Keyspace).CreateSession()
	if err != nil {
		return nil, fmt.Errorf("cassandra: open session: %w", err)
	}
	if err := bootstrapTables(ctx, sess); err != nil {
		sess.Close()
		return nil, err
	}
	return sess, nil
}

// cluster builds a ClusterConfig for the given keyspace. Pass "" to get a config
// with no keyspace, which bootstrap needs to create the keyspace itself.
func (c Config) cluster(keyspace string) *gocql.ClusterConfig {
	cl := gocql.NewCluster(c.Hosts...)
	cl.Keyspace = keyspace
	cl.Consistency = c.Consistency
	cl.Timeout = c.Timeout
	cl.ConnectTimeout = c.Timeout
	// Token-aware, so a query goes to a node that holds the row instead of paying an
	// extra hop through a coordinator that has to forward it.
	fallback := gocql.RoundRobinHostPolicy()
	if c.DC != "" {
		// Only once the datacenter is known: with an empty name every host counts as
		// remote, and the preference would mean nothing.
		fallback = gocql.DCAwareRoundRobinPolicy(c.DC)
	}
	cl.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	if c.DisablePeerDiscovery {
		// Both are needed: the lookup flag skips reading system.peers at startup, the
		// filter drops any peer that a later gossip event still announces.
		cl.DisableInitialHostLookup = true
		cl.HostFilter = gocql.WhiteListHostFilter(c.Hosts...)
	}
	// Retries are safe because each snapshot writes complete bigint values rather
	// than counters, so writing the same row twice gives the same result.
	cl.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	return cl
}

// Closer wraps a session as an io.Closer, which the bare session is not: its own
// Close returns nothing.
func Closer(sess *gocql.Session) io.Closer { return sessionCloser{sess} }

type sessionCloser struct{ *gocql.Session }

func (c sessionCloser) Close() error { c.Session.Close(); return nil }
