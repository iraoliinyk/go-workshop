# syntax=docker/dockerfile:1

FROM golang:1.26.1

WORKDIR /ch-2

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /wiki-recent-go ./cmd/server

EXPOSE 7001

CMD ["/wiki-recent-go"]
