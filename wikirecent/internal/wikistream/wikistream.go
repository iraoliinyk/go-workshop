// Wikipedia SSE client
package wikistream

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"wikirecent/internal/apperrors"
	"wikirecent/internal/applog"
)

// Doer is the part of *http.Client this package needs, so a caller can inject its own
// instead of http.DefaultClient. It is exported because Start and OpenStream take it.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

//go:generate go tool mockgen -source=wikistream.go -destination=mock_wikistream_test.go -package=wikistream_test -typed

// Sink receives one raw SSE payload. It may fail, unlike the old recorder, because
// a broker can refuse a write in ways an in-memory counter never could.
type Sink interface {
	Publish(ctx context.Context, payload []byte) error
}

// Config holds exactly what the consumer needs. main maps config.Config into this,
// so the consumer package stays independent of the global config package.
type Config struct {
	URL       string
	UserAgent string
	Accept    string
	// BaseBackoff is the first retry delay; it doubles up to one minute and resets
	// after a good connect. Zero means defaultBaseBackoff. It lives here, and not in
	// a package-level var, so one caller's retry policy cannot change another's.
	BaseBackoff time.Duration
}

// Wikimedia's recentchange event sample:
// event: message
// id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":5647231}]
// data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{...},"user":"Iryna","bot":false,"server_url":"https://en.wikipedia.org",...}
var (
	idTag   = []byte("id:")
	dataTag = []byte("data:")
)

const defaultBaseBackoff = time.Second

// Start reads the Wikimedia stream until ctx is cancelled, publishing every payload
// to sink. It reconnects on failure instead of returning, so a dropped upstream
// connection costs a retry rather than the process.
func Start(ctx context.Context, cfg Config, client Doer, sink Sink, log applog.Logger) error {
	// Resolved once: every later reset must use the same value.
	base := cfg.BaseBackoff
	if base <= 0 {
		base = defaultBaseBackoff
	}

	backoff := base
	var lastEventID = ""

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		stream, err := OpenStream(ctx, cfg, client, lastEventID) // sets Last-Event-ID when we have one
		if err == nil {
			backoff = base // a good connect earns a fresh budget
			lastEventID, err = ReadStream(ctx, stream, sink, lastEventID)
			_ = stream.Close() // close error is not actionable: the read already failed
		}

		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return ctx.Err()
		}
		log.AppErrorf(err, "stream connection lost, retrying in %s", backoff)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff = min(backoff*2, time.Minute)
		}
	}
}

func OpenStream(ctx context.Context, cfg Config, client Doer, lastEventID string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
	if err != nil {
		return nil, &apperrors.ConnectionError{Err: fmt.Errorf("build request: %w", err)}
	}

	// Wikimedia's robot policy requires this: it blocks Go's default user agent.
	req.Header.Set("User-Agent", cfg.UserAgent)
	req.Header.Set("Accept", cfg.Accept)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	// The caller closes the body on success. OpenStream owns it only on its failure
	// paths, so do not add a defer here: it would close the body being returned.
	resp, err := client.Do(req)
	if err != nil {
		return nil, &apperrors.ConnectionError{Err: fmt.Errorf("do request: %w", err)}
	}
	// Check the status before reading anything from the stream.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // read the server's reason first,
		_ = resp.Body.Close()            // then close: only this path owns the body
		return nil, &apperrors.ConnectionError{
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("%s", body),
		}
	}
	return resp.Body, nil
}

// ReadStream consumes one connection until it fails, publishing each payload. It
// returns the most recent event id so the caller can resume from it, on the error
// path as well as the clean one.
//
// It is exported as the pair of OpenStream: one opens a connection, one drains it.
// Both take only exported types, so a caller can drive either half on its own.
func ReadStream(ctx context.Context, stream io.Reader, sink Sink, lastEventID string) (string, error) {
	const readBufferSize = 64 << 10 //64 KiB
	reader := bufio.NewReaderSize(stream, readBufferSize)
	for {
		if ctx.Err() != nil {
			return lastEventID, ctx.Err()
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return lastEventID, err
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue // blank lines only separate events
		}
		// "id:" lines carry the resume token; "data:" lines carry the payload.
		if after, ok := bytes.CutPrefix(line, idTag); ok {
			// Wikimedia puts a JSON array of Kafka offsets here.
			// It will be captured and sent back on reconnect as the Last-Event-ID request header.
			lastEventID = string(bytes.TrimSpace(after))
			continue
		}
		if after, ok := bytes.CutPrefix(line, dataTag); ok {
			if err := sink.Publish(ctx, bytes.TrimSpace(after)); err != nil {
				return lastEventID, err
			}
		}
	}

}
