# Go Workshop

This repository contains the code for a Go backend engineering workshop.

## Structure

`wikirecent/` holds the active project. It is one Go module with two entry
points, `cmd/producer` and `cmd/consumer`. All new work happens here.

`ch-1`, `ch-2`, and `ch-3` are frozen snapshots of the earlier chapters. Each is
its own Go module and still runs on its own, but they are kept for reference
only. Chapter `ch-4` was renamed to `wikirecent` — copying the whole tree into a
new `ch-5` directory would have duplicated too much code.

## Environment variables

The `.env` file is not committed to version control, but `.env.example` is. To
run the project locally, go into `wikirecent/` and copy the example file first:

```bash
cp .env.example .env
```

## How to run wikirecent

There are two ways to run it:

1. **Everything in Docker** — one command, closest to production.
2. **Infrastructure in Docker, the two Go apps from the command line** — for development, so you can
   rebuild an app in one second without rebuilding an image.

Every command below runs from the module directory, not from the repository root:

```bash
cd wikirecent
```

### Everything in Docker

```bash
docker compose up -d --build
docker compose ps # check that everything is up
docker compose logs -f producer consumer # watch the logs
docker compose exec redpanda rpk topic describe wiki.recentchange -p # prove that data is flowing
```

You can verify the app flow by running the Postman collection. It is in `wikirecent/postman_collection.json`.

To stop the stack:

```bash
docker compose stop            # stop the containers, keep the data
docker compose down            # remove the containers, keep the volumes
docker compose down -v         # remove the volumes too: deletes Cassandra and Redpanda data
```

### Infrastructure in Docker, the apps from the command line

```bash
docker compose up -d --wait cassandra redpanda console # db first
docker compose up -d redpanda-init # create the topic
docker compose ps -a # check the infrastructure
```

Run producer in its own terminal

```bash
REDPANDA_BROKERS=localhost:19092 \
go run ./cmd/producer
```

```text
DBG producer starting on :7002, topic "wiki.recentchange"
```

Run consumer in its own terminal. `JWT_SECRET` is required and has no default, so
`.env` must exist in `wikirecent/` or the consumer exits at startup.

```bash
REDPANDA_BROKERS=localhost:19092 \
DB_BACKEND=cassandra \
go run ./cmd/consumer
```

```text
DBG consumer listening on :7001, topic "wiki.recentchange", group "wiki-stats"
```

Stop either one with `Ctrl+C`. Then stop the infrastructure:

```bash
docker compose down
```

## Lint

Check the whole project

```bash
golangci-lint run ./...
```

Extras:

```bash
golangci-lint run --fix ./...   # apply auto-fixes where a linter supports them
golangci-lint fmt ./...         # format only (gofmt/goimports)
```

## Unit tests

```bash
go test -race ./...
```

```bash
go test -race ./internal/broker/...        # one package
go test -race -run TestPublisher ./...     # one test by name
go test -race -count=1 ./...               # ignore cached results
go test -race -v ./internal/flusher/...    # show each test name
```

## Integration tests

The Cassandra integration tests need a live Cassandra node.
Run first:

```bash
docker compose up -d cassandra
CASSANDRA_HOSTS=127.0.0.1:9042 go test -tags=integration ./...
```

## One line before pushing

```bash
golangci-lint run ./... && go test -race ./...
```
