# syntax=docker/dockerfile:1

# ---- build stage ----
# Pinned to BUILDPLATFORM — the architecture of the machine running the build —
# so this stage is never emulated. Go cross-compiles on its own (see the build
# command below), which is why a multi-arch build here needs no QEMU.
FROM --platform=$BUILDPLATFORM golang:1.26.1 AS build

# Supplied by BuildKit from the --platform being built, e.g. linux/arm64.
ARG TARGETOS
ARG TARGETARCH

WORKDIR /ch-4

# Copy manifests first so `go mod download` is cached until deps change
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 builds a static binary, so it can run on scratch — and it is also
# what keeps cross-compilation free: no target C toolchain is needed.
# GOOS/GOARCH come from the target platform, so an amd64 runner emits an arm64
# binary natively instead of running the compiler under emulation.
# -s -w removes debug symbols, which the final image does not need.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags="-s -w" -o /out/wiki ./cmd/server

# scratch: the image contains only the CA bundle, the server, and busybox.
FROM scratch
WORKDIR /
# CA certs are required: the consumer calls https://stream.wikimedia.org and
# scratch has no cert bundle, so TLS verification would fail without this.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# A static busybox, added only so the compose healthcheck can run wget.
COPY --from=busybox:1.37-uclibc /bin/busybox /bin/busybox
COPY --from=build /out/wiki /wiki
EXPOSE 7001
ENTRYPOINT ["/wiki"]
