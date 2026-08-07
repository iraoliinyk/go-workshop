# CH-3 — Wikipedia Stream Consumer

A Go service that consumes the Wikimedia event stream, aggregates statistics, and
serves them over HTTP. It keeps its data in memory or persists a time series to
Cassandra, and `/stats` is protected by JWT auth.

Server on **`:7001`**:

| Method | Path             | Access    | Description                      |
| ------ | ---------------- | --------- | -------------------------------- |
| `GET`  | `/status`        | public    | Health check                     |
| `POST` | `/auth/register` | public    | Create an account                |
| `POST` | `/auth/login`    | public    | Exchange credentials for a token |
| `POST` | `/auth/logout`   | protected | Revoke the presented token       |
| `GET`  | `/stats`         | protected | Current aggregated statistics    |

All commands assume you are in `ch-4/`. You need a `.env` with `JWT_SECRET`
(≥ 32 bytes) — the app refuses to start without it. Copy `.env.example` to start.

---

## Everything in Docker

```bash
docker compose up -d --build
docker compose ps          # both services; cassandra should be (healthy)
docker compose logs -f wiki
```

`wiki` waits for Cassandra's healthcheck, so the first start takes about a minute
while the node boots.

Read the persisted series back:

```bash
docker exec cassandra cqlsh -e \
  "SELECT snapshot_ts, total_messages, bot_edits, human_edits, distinct_users
     FROM wikistream.stats_snapshot WHERE day = toDate(now()) LIMIT 5;"
```

The `WHERE day = toDate(now())` matters. `day` is the partition key, so without it
the query scans every partition and returns rows in token order — which is usually
an old day, not the run you just started. With it you get today's partition, newest
first.

### Tear down

```bash
docker compose down       # keeps the cassandra-data volume: accounts and the
                          # time series survive the restart
docker compose down -v    # also wipes the volume
```

---

## Cassandra in Docker, app on the host

```bash
docker compose up -d cassandra
go run ./cmd/server
```

Uses `.env`, which the host process does read (the container does not — Compose
injects those variables through `env_file` instead).

---

## In-memory, no database

Either as a container:

```bash
docker compose run -d --rm --no-deps --service-ports \
  -e DB_BACKEND=in-memory -e STATS_FLUSH_INTERVAL=10s wiki
```

`--no-deps` skips Cassandra, `--service-ports` publishes `7001`. **Stop the main
service first** (`docker compose stop wiki`) or the port is already taken:

```text
Bind for 0.0.0.0:7001 failed: port is already allocated
```

Compose leaves the failed container behind — `docker rm -f <name>` to clear it.

Or on the host:

```bash
DB_BACKEND=in-memory STATS_FLUSH_INTERVAL=10s go run ./cmd/server
```

---

## Try the API — Postman

`postman_collection.json` (Collection v2.1) walks the whole auth flow against a
running server. Import it in Postman: **Import → File → `postman_collection.json`**.

Ten requests, in order — run them top to bottom, or use **Run collection**:

| # | Request                          | Expect |
| - | -------------------------------- | ------ |
| 1 | Health Check                     | 200    |
| 2 | Register User                    | 201    |
| 3 | Register Duplicate               | 409    |
| 4 | Register Weak Password           | 400    |
| 5 | Login                            | 200    |
| 6 | Login Wrong Password             | 401    |
| 7 | Fetch Stats (with token)         | 200    |
| 8 | Fetch Stats without token        | 401    |
| 9 | Logout                           | 204    |
| 10| Verify Revoked Token             | 401    |

Every request carries test scripts, so failures show up in Postman's **Test
Results** tab rather than needing to be eyeballed. Requests 8 and 10 are the ones
that give the run its meaning: `/stats` is refused *before* a token exists and again
*after* logout.

You do not need to copy the token by hand — **Login** stores it in the collection
variable `ACCESS_TOKEN`, and the protected requests read it from there. **Register
User** also regenerates `TEST_EMAIL` with a timestamp on each run, so repeating the
collection does not fail on an address that already exists.

Collection variables, editable under the collection's **Variables** tab:

| Variable        | Default                    |
| --------------- | -------------------------- |
| `BASE_URL`      | `http://localhost:7001`    |
| `TEST_EMAIL`    | `go-ch4@example.test`      |
| `TEST_PASSWORD` | `correct-horse-battery`    |

Point `BASE_URL` elsewhere to test another instance. The routes are **not**
versioned — there is no `/v1` prefix.

To run it from the command line instead, install
[newman](https://github.com/postmanlabs/newman) (not currently installed here):

```bash
npx newman run postman_collection.json
```

---

## Tests

Regular tests, no database:

```bash
go test -race ./...
```

Integration tests against a real node:

```bash
docker compose up -d cassandra
go test -tags=integration -race -count=1 ./internal/db/cassandra/...
```

They are behind `//go:build integration`, so without the tag that package reports
`no test files` — expected. `-count=1` defeats the test cache, which would
otherwise hide the node's current state. The tag only *adds* files, so one command
runs both suites:

```bash
go test -tags=integration -race ./...
```

A single test (the tag is still required, or `-run` matches nothing):

```bash
go test -tags=integration -race -v -run TestStatsStore_PrimaryKey ./internal/db/cassandra/
```

`TestMain` waits up to 30s for port 9042 and then fails with a hint, so a run
started immediately after `compose up -d` on a cold node may need a retry.

Point the tests elsewhere with `CASSANDRA_HOSTS` (default `127.0.0.1:9042`) or
`CASSANDRA_IT_KEYSPACE` (default `wikistream_it`, kept separate from your dev data
in `wikistream`). Bootstrap uses `CREATE TABLE IF NOT EXISTS` and never migrates an
existing keyspace, so a changed `PRIMARY KEY` in `bootstrap.go` is only visible to
the schema assertions in a **fresh** keyspace.

---

## Configuration

Read from the environment; `.env` in the working directory is loaded first, and
real environment variables win over it.

| Env var                 | Default          | Purpose                             |
| ----------------------- | ---------------- | ----------------------------------- |
| `CONSUMER_PORT`         | `7001`           | HTTP listen port                    |
| `DB_BACKEND`            | `in-memory`      | `in-memory` \| `cassandra`          |
| `LOGGER`                | `DEBUG`          | `DEBUG` \| `PROD`                   |
| `STATS_FLUSH_INTERVAL`  | `10s`            | How often a snapshot is persisted   |
| `JWT_SECRET`            | **required**     | HS256 key, **≥ 32 bytes**           |
| `JWT_ISSUER`            | `wiki-stream-go` | `iss` claim                         |
| `ACCESS_TOKEN_TTL`      | `1h`             | Token lifetime                      |
| `BCRYPT_COST`           | `12`             | Password hashing cost (10..31)      |
| `CASSANDRA_HOSTS`       | `127.0.0.1`      | Comma-separated contact points      |
| `CASSANDRA_KEYSPACE`    | `wikistream`     | Created on startup if missing       |
| `CASSANDRA_CONSISTENCY` | `QUORUM`         | Read + write consistency            |
| `CASSANDRA_TIMEOUT`     | `5s`             | Per-query timeout                   |

`DEBUG` logs every request; `PROD` logs only errors.

---

## Troubleshooting

**`required environment variable "JWT_SECRET" is not set`** — no `.env` in the
working directory, or it does not define `JWT_SECRET`.

**`missing unit in duration "10"`** — durations need a unit. Use
`STATS_FLUSH_INTERVAL=10s`, not `=10`. Same for `ACCESS_TOKEN_TTL` and
`CASSANDRA_TIMEOUT`.

**`bind: address already in use`** — something already holds `7001`. Usually the
compose `wiki` service; `docker compose stop wiki`, or set `CONSUMER_PORT`.

**`gocql: unable to connect to initial hosts`** — Cassandra is not up yet. Check
`docker compose ps` for `(healthy)`; a cold start takes about a minute.

**Account gone after a restart** — you were on `DB_BACKEND=in-memory`, or you ran
`docker compose down -v` and wiped the volume.
