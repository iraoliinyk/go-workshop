# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.26.1 AS build

WORKDIR /ch-2

# Copy manifests first so `go mod download` is cached until deps change
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# RUN CGO_ENABLED=0 GOOS=linux go build -o /wiki-recent-go ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/wiki ./cmd/server

# busybox (not scratch) so the image ships wget for the container HEALTHCHECK.
# The Go binary is static (CGO_ENABLED=0), so it runs on busybox unchanged.
FROM busybox:1.37
WORKDIR /
# CA certs are required: the consumer calls https://stream.wikimedia.org and
# the busybox base has no cert bundle, so TLS verification would fail without this.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/wiki /wiki
EXPOSE 7001
ENTRYPOINT ["/wiki"]
