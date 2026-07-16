# CH-2 — Wikipedia Stream Consumer

A Go service that consumes the Wikimedia event stream, aggregates statistics, and
exposes them over HTTP. The server listens on **`:7000`** and provides two endpoints:

| Method | Path      | Description                     |
| ------ | --------- | ------------------------------- |
| `GET`  | `/status` | Health check (`{"status":"ok"}`) |
| `GET`  | `/stats`  | Current aggregated statistics    |

> All commands assume you are in the `ch-2/` directory:
>
> ```bash
> cd ch-2
> ```

---

## Run the app

```bash
go run ./cmd/server
```

The server starts on `:7001`. Stop it with `Ctrl+C` — it shuts down gracefully
(handles `SIGINT`/`SIGTERM`).

---

## Run all tests

```bash
go test ./...
```

With the race detector and verbose output (matches the VS Code test config):

```bash
go test -race -v ./...
```

---

## Run a specific test

Run a single package:

```bash
go test -race -v ./internal/consumer
```

Run a single test by name with `-run` (accepts a regex):

```bash
go test -race -v -run TestParseEvent_ValidDataLine ./internal/consumer
```

Run all tests matching a prefix across packages:

```bash
go test -race -v -run TestParseEvent ./...
```

Available tests:

- `internal/consumer` — `TestParseEvent_ValidDataLine`, `TestParseEvent_InvalidJSON`,
  `TestParseEvent_StripsDataPrefix`, `TestParseEvent_ErrorCode`
- `internal/stats` — `TestNew_InitialisesEmptyStats`, `TestRecord_CountsTotalMessages`,
  `TestRecord_CountsDistinctUsers`, `TestRecord_IgnoresEmptyUser`,
  `TestRecord_SeparatesBotAndHumanEdits`, `TestRecord_CountsByServerURL`,
  `TestRecord_IgnoresEmptyServerURL`, `TestSnapshot_IsDeepCopy`,
  `TestRecord_ConcurrentSafety`

---

## Call the HTTP endpoints

With the app running (`go run ./cmd/server`), from another terminal:

**Status:**

```bash
curl -s http://localhost:7001/status | jq
```

**Stats:**

```bash
curl -s http://localhost:7001/stats | jq
```

> `jq` is optional — drop `| jq` if you don't have it installed, or use
> `python3 -m json.tool` to pretty-print instead:
>
> ```bash
> curl -s http://localhost:7001/stats | python3 -m json.tool
> ```

---

## Troubleshooting

### `bind: address already in use`

```text
listen tcp :7001: bind: address already in use
```

Port `7001` is already taken by another process (often a previous run of this
app that didn't exit cleanly). Find out which process is using it (macOS/Linux):

```bash
lsof -i :7001
```

Show only the PID of the listening process:

```bash
lsof -ti :7002
```

Free the port by killing that process, then start the app again:

```bash
kill -9 $(lsof -ti :7001)
```

Build and run Docker:

```bash
docker build --tag wiki-recent-go .
```

```bash
docker run --rm --name wiki-recent -d -p 7001:7001 wiki-recent-go
```

```bash
docker compose -f docker-compose.yaml up --build
```

---

## Dockerfile: Alpine variant (for testing)

The default `Dockerfile` uses `FROM scratch` for the smallest possible image
(~9.5 MB). `scratch` has **no shell, no `wget`, and no CA certs**, which makes
debugging and healthchecks harder. For testing — when you want to `docker exec`
into the container or let the Compose `wget` healthcheck pass — swap the runtime
stage for `alpine:3.20` (~18.7 MB):

```dockerfile
# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.26.1 AS build
WORKDIR /ch-2
# Copy manifests first so `go mod download` is cached until deps change
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/wiki ./cmd/server

# ---- runtime stage (Alpine — shell + wget for testing/debugging) ----
FROM alpine:3.20
# Alpine ships no cert bundle by default; the consumer calls
# https://stream.wikimedia.org, so CA certs are required for TLS.
RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=build /out/wiki /wiki
EXPOSE 7001
ENTRYPOINT ["/wiki"]
```

Build, run, and shell in to verify:

```bash
docker build --tag wiki-recent-go:alpine .
docker run --rm --name wiki-recent -d -p 7001:7001 wiki-recent-go:alpine
docker exec -it wiki-recent sh          # possible with Alpine, not with scratch
curl -s http://localhost:7001/stats | jq
```
