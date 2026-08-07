package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"ch-4/internal/apperrors"
	"ch-4/internal/consumer/consumermodels"
)

// Mocks for the collaborator interfaces below live in mock_consumer_test.go
// (same package, test-only, so they never reach the production binary).
//go:generate go tool mockgen -source=consumer.go -destination=mock_consumer_test.go -package=consumer -typed

// recorder is what *stats.Stats already provides. Depending on this small interface
// instead of the stats package keeps the two from importing each other.
type recorder interface {
	Record(event consumermodels.WikiEvent)
}

// doer is the part of *http.Client this package needs, so a test can inject its own
// instead of http.DefaultClient.
type doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Config holds exactly what the consumer needs. main maps config.Config into this,
// so the consumer package stays independent of the global config package.
type Config struct {
	URL       string
	UserAgent string
	Accept    string
}

// Start connects to the Wikimedia stream and records events until ctx is cancelled.
// It returns a ConnectionError or a StreamError when it cannot continue, so the
// caller decides whether to stop the process.
func Start(ctx context.Context, cfg Config, client doer, rec recorder) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
	if err != nil {
		return &apperrors.ConnectionError{Err: fmt.Errorf("build request: %w", err)}
	}

	// Wikimedia's robot policy requires this: it blocks Go's default user agent.
	req.Header.Set("User-Agent", cfg.UserAgent)
	req.Header.Set("Accept", cfg.Accept)

	resp, err := client.Do(req)
	if err != nil {
		return &apperrors.ConnectionError{Err: fmt.Errorf("do request: %w", err)}
	}
	defer resp.Body.Close()

	// Check the status before reading anything from the stream.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &apperrors.ConnectionError{
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("%s", body),
		}
	}

	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue // blank lines only separate events
		}
		event, err := parseEvent(line)
		if err != nil {
			// One bad line does not break the stream, so log it and keep going.
			log.Printf("[%s] %v %s", err.Code(), err, line)
			continue
		}
		rec.Record(event)
	}

	// nil when the stream ended cleanly, otherwise the real error.
	if err := scanner.Err(); err != nil {
		return &apperrors.StreamError{Err: err}
	}
	return nil
}

// parseEvent turns one raw data line into a WikiEvent. The caller must pass only
// lines that start with "data:".
func parseEvent(line string) (consumermodels.WikiEvent, *apperrors.ParseError) {
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))

	var event consumermodels.WikiEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return consumermodels.WikiEvent{}, &apperrors.ParseError{Line: line, Err: err}
	}
	return event, nil
}
