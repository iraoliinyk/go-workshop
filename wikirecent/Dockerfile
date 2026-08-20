# syntax=docker/dockerfile:1

# One image holds both binaries, and compose picks one with `command:`
ARG GO_IMAGE="1.26.1-alpine"

# ---- Step 1: Build ----
FROM --platform=$BUILDPLATFORM golang:${GO_IMAGE} AS builder

# BuildKit sets these from the --platform being built, for example linux/arm64.
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

# .dockerignore keeps .env and the local build artifacts out of the context.
COPY . .

# CGO_ENABLED=0 makes both binaries static, which is what lets them run on scratch.
# Build the package, not ./cmd/consumer/main.go, which would compile that one file only.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags="-s -w" -o /wiki-consumer ./cmd/consumer && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags="-s -w" -o /wiki-producer ./cmd/producer

# ---- Step 2: Runtime ----
# scratch holds only the CA bundle, busybox and the two binaries.
FROM scratch

WORKDIR /app

# scratch ships no certs, and the producer needs TLS for https://stream.wikimedia.org.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# A static busybox, added only so the compose healthchecks can run wget.
COPY --from=busybox:1.37-uclibc /bin/busybox /bin/busybox

COPY --from=builder /wiki-consumer ./wiki-consumer
COPY --from=builder /wiki-producer ./wiki-producer

# CMD, not ENTRYPOINT: compose replaces a CMD, but would append to an ENTRYPOINT.
CMD ["/app/wiki-consumer"]
