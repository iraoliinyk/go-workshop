package consumer

import (
"ch-1/internal/consumer/models"
"encoding/json"
"testing"
)

func TestParseEvent_ValidDataLine(t *testing.T) {
	event := models.WikiEvent{
		ID:        42,
		Type:      "edit",
		User:      "iryna",
		Bot:       false,
		ServerURL: "https://en.wikipedia.org",
		Wiki:      "enwiki",
	}
	payload, _ := json.Marshal(event)

	got, ok := parseEvent("data: " + string(payload))

	if !ok {
		t.Fatal("expected ok=true for a valid data line")
	}
	if got.User != event.User {
		t.Errorf("expected user %q, got %q", event.User, got.User)
	}
	if got.Bot != event.Bot {
		t.Errorf("expected bot=%v, got %v", event.Bot, got.Bot)
	}
	if got.ServerURL != event.ServerURL {
		t.Errorf("expected serverURL %q, got %q", event.ServerURL, got.ServerURL)
	}
}

func TestParseEvent_BlankLine(t *testing.T) {
	_, ok := parseEvent("")
	if ok {
		t.Error("expected ok=false for a blank line")
	}
}

func TestParseEvent_InvalidJSON(t *testing.T) {
	_, ok := parseEvent("data: {not valid json}")
	if ok {
		t.Error("expected ok=false for invalid JSON")
	}
}

func TestParseEvent_StripsDataPrefix(t *testing.T) {
	event := models.WikiEvent{User: "john"}
	payload, _ := json.Marshal(event)

	got, ok := parseEvent("data: " + string(payload))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.User != "john" {
		t.Errorf("expected user john, got %q", got.User)
	}
}

func TestParseEvent_LineWithoutDataPrefix(t *testing.T) {
	// SSE lines like "event: change" or "id: 123" should fail to unmarshal
	_, ok := parseEvent("event: change")
	if ok {
		t.Error("expected ok=false for non-data SSE line")
	}
}
