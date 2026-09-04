-- Reference only. Nothing loads this file: internal/db/cassandra/bootstrap.go
-- creates the keyspace and the tables. Change that file first, then mirror it here.
--
-- dc1 is what docker-compose.yaml names its nodes. A node with no
-- cassandra-rackdc.properties calls itself datacenter1 instead.
CREATE KEYSPACE IF NOT EXISTS wikistream
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'dc1': 3
    };

-- Overall stats series: distinct users + bots vs non-bots over time.
CREATE TABLE IF NOT EXISTS stats_snapshot (
    day            date,        -- partition key (UTC midnight)
    snapshot_ts    timestamp,   -- clustering: flush time (UTC)
    total_messages bigint,
    bot_edits      bigint,
    human_edits    bigint,
    distinct_users bigint,
    PRIMARY KEY ((day), snapshot_ts)
) WITH CLUSTERING ORDER BY (snapshot_ts DESC);

CREATE TABLE IF NOT EXISTS server_snapshot (
    server_url  text,
    day         date,
    snapshot_ts timestamp,
    hits        bigint,
    PRIMARY KEY ((server_url, day), snapshot_ts)
) WITH CLUSTERING ORDER BY (snapshot_ts DESC);

-- Auth: accounts + revoked tokens.
CREATE TABLE IF NOT EXISTS user_accounts (
    email         text PRIMARY KEY,   -- normalized lower/trim
    password_hash text,
    created_at    timestamp,
    active        boolean);

CREATE TABLE IF NOT EXISTS revoked_tokens (
    jti        text PRIMARY KEY,
    email      text,
    revoked_at timestamp);

CREATE TABLE IF NOT EXISTS stats_poll (
    day            date,
    partition      int,
    start_offset   bigint,
    end_offset     bigint,
    total_messages bigint,
    bot_edits      bigint,
    human_edits    bigint,
    PRIMARY KEY ((day), partition, start_offset)
) WITH CLUSTERING ORDER BY (partition ASC, start_offset DESC);

CREATE TABLE IF NOT EXISTS distinct_users (day  date, user text, PRIMARY KEY ((day), user));
