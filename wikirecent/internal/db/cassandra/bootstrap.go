package cassandra

import (
	"context"
	"fmt"
	"regexp"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const createKeyspace = `CREATE KEYSPACE IF NOT EXISTS %s
	WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`

// keyspaceRE is Cassandra's rule for a keyspace name: letters, digits and
// underscores, up to 48 characters.
var keyspaceRE = regexp.MustCompile(`^[a-zA-Z0-9_]{1,48}$`)

func bootstrapKeyspace(ctx context.Context, cfg Config) error {
	if !keyspaceRE.MatchString(cfg.Keyspace) {
		return fmt.Errorf("cassandra: invalid keyspace name %q (want %s)", cfg.Keyspace, keyspaceRE)
	}
	// A session with no keyspace, because the keyspace does not exist yet.
	sess, err := cfg.cluster("").CreateSession()
	if err != nil {
		return fmt.Errorf("cassandra: create session: %w", err)
	}
	defer sess.Close()
	if err := sess.Query(fmt.Sprintf(createKeyspace, cfg.Keyspace)).ExecContext(ctx); err != nil {
		return fmt.Errorf("cassandra: create keyspace %q: %w", cfg.Keyspace, err)
	}
	return nil
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

const createStatsDelta = `CREATE TABLE IF NOT EXISTS stats_delta (
	day date, partition int, end_offset bigint,
	total_messages bigint, bot_edits bigint, human_edits bigint,
	PRIMARY KEY ((day), partition, end_offset)
) WITH CLUSTERING ORDER BY (partition ASC, end_offset DESC)`

func bootstrapTables(ctx context.Context, sess *gocql.Session) error {
	for _, stmt := range []string{
		createStatsSnapshot, createServerSnapshot, createUserAccounts, createRevokedTokens, createStatsDelta,
	} {
		if err := sess.Query(stmt).ExecContext(ctx); err != nil {
			return fmt.Errorf("cassandra: create table: %w", err)
		}
	}
	return nil
}
