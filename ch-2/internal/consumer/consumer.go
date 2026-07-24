package consumer

import (
	"bufio"
	"ch-2/internal/apperrors"
	"ch-2/internal/consumer/models"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

// Recorder is satisfied by *stats.Stats — keeps consumer free of a direct import cycle.
type Recorder interface {
	Record(event models.WikiEvent)
}

// Inject Doer instead of http.DefaultClient
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Config holds exactly what the consumer needs. main maps config.Config into this,
// so the consumer package stays independent of the global config package.
type Config struct {
	URL       string
	UserAgent string
	Accept    string
}

// Start connects to the Wikimedia SSE stream and records events until ctx is cancelled.
// Returns a ConnectionError or StreamError on fatal failure so the caller
// can handle process termination centrally instead of calling log.Fatal here.
func Start(ctx context.Context, cfg Config, client Doer, rec Recorder) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
	if err != nil {
		return &apperrors.ConnectionError{Err: fmt.Errorf("build request: %w", err)}
	}

	// Required by Wikimedia robot policy — generic Go UA is blocked
	req.Header.Set("User-Agent", cfg.UserAgent)
	req.Header.Set("Accept", cfg.Accept)

	resp, err := client.Do(req)
	if err != nil {
		return &apperrors.ConnectionError{Err: fmt.Errorf("do request: %w", err)}
	}
	defer resp.Body.Close()

	// Guard: check status before reading stream
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
			continue // skip blank separator lines
		}
		event, err := parseEvent(line)
		if err != nil {
			// ParseError: log and continue — stream is not broken
			log.Printf("[%s] %v %s", err.Code(), err, line)
			continue
		}
		rec.Record(event)
	}

	// returns nil on clean EOF, or the real error
	if err := scanner.Err(); err != nil {
		return &apperrors.StreamError{Err: err}
	}
	return nil
}

// parseEvent parses a raw SSE data line into a WikiEvent.
// The caller is responsible for passing only lines that start with "data:".
// Returns *apperrors.ParseError if the JSON payload cannot be decoded.
func parseEvent(line string) (models.WikiEvent, *apperrors.ParseError) {
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))

	var event models.WikiEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return models.WikiEvent{}, &apperrors.ParseError{Line: line, Err: err}
	}
	return event, nil
}
