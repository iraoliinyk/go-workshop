package cassandra

import (
	"context"
	"fmt"
	"io"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type Config struct {
	Hosts       []string
	Keyspace    string
	Consistency gocql.Consistency
	Timeout     time.Duration
}

func Connect(ctx context.Context, cfg Config) (*gocql.Session, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("cassandra: no hosts configured")
	}
	if cfg.Keyspace == "" {
		return nil, fmt.Errorf("cassandra: no keyspace configured")
	}

	// Two steps, because a pooled session cannot switch keyspaces with USE.
	// First create the keyspace from a session that has none.
	if err := bootstrapKeyspace(ctx, cfg); err != nil {
		return nil, err
	}

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
