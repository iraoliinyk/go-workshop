package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

type WikiEventMeta struct {
	URI       string `json:"uri"`
	RequestID string `json:"request_id"`
	ID        string `json:"id"`
	Domain    string `json:"domain"`
	Stream    string `json:"stream"`
	DT        string `json:"dt"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type WikiEvent struct {
	Schema        string        `json:"$schema"`
	Meta          WikiEventMeta `json:"meta"`
	ID            int64         `json:"id"`
	Type          string        `json:"type"`
	Namespace     int           `json:"namespace"`
	Title         string        `json:"title"`
	TitleURL      string        `json:"title_url"`
	Comment       string        `json:"comment"`
	Timestamp     int64         `json:"timestamp"`
	User          string        `json:"user"`
	Bot           bool          `json:"bot"`
	NotifyURL     string        `json:"notify_url"`
	ServerURL     string        `json:"server_url"`
	ServerName    string        `json:"server_name"`
	ServerScript  string        `json:"server_script_path"`
	Wiki          string        `json:"wiki"`
	ParsedComment string        `json:"parsedcomment"`
}

const WikiURL = "https://stream.wikimedia.org/v2/stream/recentchange"

func Start(ctx context.Context) {
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
		var event WikiEvent
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			log.Printf("failed to parse event: %v", err)
			continue
		}

		log.Printf("id=%d type=%s user=%s bot=%v title=%q wiki=%s server=%s", event.ID, event.Type, event.User, event.Bot, event.Title, event.Wiki, event.ServerURL)
	}

	// Scanner.Err() returns nil on clean EOF, or the real error
	if err := scanner.Err(); err != nil {
		log.Printf("stream error: %v", err)
	}
}
