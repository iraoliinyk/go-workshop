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

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/wiki /wiki
EXPOSE 7001
USER nonroot:nonroot
ENTRYPOINT ["/wiki"]
