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
