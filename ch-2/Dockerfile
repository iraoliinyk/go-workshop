# syntax=docker/dockerfile:1

FROM golang:1.26.1

WORKDIR /ch-2

EXPOSE 7001

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /wiki-recent-go ./cmd/server

CMD ["/wiki-recent-go"]
