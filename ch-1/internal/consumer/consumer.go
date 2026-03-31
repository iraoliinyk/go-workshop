package consumer

import (
	"bufio"
	"ch-1/internal/consumer/models"
	"context"
	"encoding/json"
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

func Start(ctx context.Context, rec Recorder) {
	// 1. Build the request with a context so it can be cancelled
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		WikiURL,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Required by Wikimedia robot policy — generic Go UA is blocked
	req.Header.Set("User-Agent", "wiki-stream-consumer/1.0 (https://github.com/you/wiki-stream)")
	req.Header.Set("Accept", "application/json")

	// 3. Execute the request — the body stays open indefinitely
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	// Guard: check status before reading stream
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	// 4. Wrap the body in a Scanner to read one line at a time
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := scanner.Text() // one raw line, newline stripped

		// 5. Skip blank lines (SSE event separators)
		if line == "" {
			continue
		}

		// 6. Strip the "data: " prefix to get raw JSON
		payload := strings.TrimPrefix(line, "data:")
		payload = strings.TrimSpace(payload)

		// 7. Decode JSON into the struct
		var event models.WikiEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			log.Printf("failed to parse event: %v", err)
			continue
		}

		rec.Record(event)
	}

	// Scanner.Err() returns nil on clean EOF, or the real error
	if err := scanner.Err(); err != nil {
		log.Printf("stream error: %v", err)
	}
}
