package cassandra

import (
	"context"
	"fmt"
	"regexp"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// The datacenter is named rather than using the 'replication_factor' shorthand:
// the shorthand only expands over the datacenters that exist at creation time, so
// a datacenter added later would get no replicas and nothing would say so.
const createKeyspace = `CREATE KEYSPACE IF NOT EXISTS %s
	WITH replication = {'class': 'NetworkTopologyStrategy', '%s': %d}`

// keyspaceRE is Cassandra's rule for a keyspace name: letters, digits and
// underscores, up to 48 characters.
var keyspaceRE = regexp.MustCompile(`^[a-zA-Z0-9_]{1,48}$`)

// Same reason as keyspaceRE: the name is formatted into the CQL string, not bound
// as a parameter. Hyphens pass because cassandra-rackdc.properties allows them.
var dcRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,48}$`)

// The name comes from the node's snitch, so the client cannot derive it:
// SimpleSnitch answers "datacenter1", GossipingPropertyFileSnitch answers whatever
// cassandra-rackdc.properties says.
const selectLocalDC = `SELECT data_center FROM system.local`

// bootstrapKeyspace returns the datacenter it replicated into, which the caller
// needs to route later queries to the same place.
func bootstrapKeyspace(ctx context.Context, cfg Config) (string, error) {
	if !keyspaceRE.MatchString(cfg.Keyspace) {
		return "", fmt.Errorf("cassandra: invalid keyspace name %q (want %s)", cfg.Keyspace, keyspaceRE)
	}
	if cfg.ReplicationFactor < 1 {
		return "", fmt.Errorf("cassandra: replication factor %d must be at least 1", cfg.ReplicationFactor)
	}
	// A session with no keyspace, because the keyspace does not exist yet.
	sess, err := cfg.cluster("").CreateSession()
	if err != nil {
		return "", fmt.Errorf("cassandra: create session: %w", err)
	}
	defer sess.Close()

	dc := cfg.DC
	if dc == "" {
		if err := sess.Query(selectLocalDC).Consistency(gocql.One).ScanContext(ctx, &dc); err != nil {
			return "", fmt.Errorf("cassandra: read local datacenter: %w", err)
		}
	}
	if !dcRE.MatchString(dc) {
		return "", fmt.Errorf("cassandra: invalid datacenter name %q (want %s)", dc, dcRE)
	}

	stmt := fmt.Sprintf(createKeyspace, cfg.Keyspace, dc, cfg.ReplicationFactor)
	if err := sess.Query(stmt).ExecContext(ctx); err != nil {
		return "", fmt.Errorf("cassandra: create keyspace %q: %w", cfg.Keyspace, err)
	}
	return dc, nil
}

const createStatsSnapshot = `CREATE TABLE IF NOT EXISTS stats_snapshot (
	day date, snapshot_ts timestamp,
	total_messages bigint, bot_edits bigint, human_edits bigint, distinct_users bigint,
	PRIMARY KEY ((day), snapshot_ts)
) WITH CLUSTERING ORDER BY (snapshot_ts DESC)`

const createServerSnapshot = `CREATE TABLE IF NOT EXISTS server_snapshot (
	server_url text, day date, snapshot_ts timestamp, hits bigint,
	PRIMARY KEY ((server_url, day), snapshot_ts)
) WITH CLUSTERING ORDER BY (snapshot_ts DESC)`

const createUserAccounts = `CREATE TABLE IF NOT EXISTS user_accounts (
	email text PRIMARY KEY, password_hash text, created_at timestamp, active boolean)`

const createRevokedTokens = `CREATE TABLE IF NOT EXISTS revoked_tokens (
	jti text PRIMARY KEY, email text, revoked_at timestamp)`

const createStatsPoll = `CREATE TABLE IF NOT EXISTS stats_poll (
	day date, partition int, start_offset bigint, end_offset bigint,
	total_messages bigint, bot_edits bigint, human_edits bigint,
	PRIMARY KEY ((day), partition, start_offset)
) WITH CLUSTERING ORDER BY (partition ASC, start_offset DESC)`

func bootstrapTables(ctx context.Context, sess *gocql.Session) error {
	for _, stmt := range []string{
		createStatsSnapshot, createServerSnapshot, createUserAccounts, createRevokedTokens, createStatsPoll,
	} {
		if err := sess.Query(stmt).ExecContext(ctx); err != nil {
			return fmt.Errorf("cassandra: create table: %w", err)
		}
	}
	return nil
}
