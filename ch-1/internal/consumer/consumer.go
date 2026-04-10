package consumer

import (
	"bufio"
	"ch-1/internal/apperrors"
	"ch-1/internal/consumer/models"
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

const WikiURL = "https://stream.wikimedia.org/v2/stream/recentchange"

// Start connects to the Wikimedia SSE stream and records events until ctx is cancelled.
// Returns a ConnectionError or StreamError on fatal failure so the caller
// can handle process termination centrally instead of calling log.Fatal here.
func Start(ctx context.Context, rec Recorder) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, WikiURL, nil)
	if err != nil {
		return &apperrors.ConnectionError{Err: fmt.Errorf("build request: %w", err)}
	}

	// Required by Wikimedia robot policy — generic Go UA is blocked
	req.Header.Set("User-Agent", "wiki-stream-consumer/1.0 (https://github.com/you/wiki-stream)")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
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

		// Only process data lines — skip event:, id:, comments (:ok) and blank lines.
		// Other SSE line types are valid protocol lines, not JSON payloads.
		if !strings.HasPrefix(line, "data:") {
			continue
		}

		event, err := parseEvent(line)
		if err != nil {
			// ParseError: log and continue — stream is not broken
			log.Printf("[%s] %v", err.Code(), err)
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
