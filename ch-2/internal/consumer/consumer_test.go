package consumer

import (
	"ch-1/internal/apperrors"
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

	got, err := parseEvent("data: " + string(payload))

	if err != nil {
		t.Fatalf("expected no error for a valid data line, got %v", err)
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

func TestParseEvent_InvalidJSON(t *testing.T) {
	_, err := parseEvent("data: {not valid json}")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
	var parseErr *apperrors.ParseError
	if !isParseError(err, &parseErr) {
		t.Errorf("expected *apperrors.ParseError, got %T", err)
	}
}

func TestParseEvent_StripsDataPrefix(t *testing.T) {
	event := models.WikiEvent{User: "john"}
	payload, _ := json.Marshal(event)

	got, err := parseEvent("data: " + string(payload))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got.User != "john" {
		t.Errorf("expected user john, got %q", got.User)
	}
}

func TestParseEvent_ErrorCode(t *testing.T) {
	_, err := parseEvent("data: {not valid json}")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Code() != "PARSE_ERROR" {
		t.Errorf("expected code PARSE_ERROR, got %q", err.Code())
	}
}

// isParseError is a helper that mirrors errors.As for the concrete pointer type.
func isParseError(err *apperrors.ParseError, target **apperrors.ParseError) bool {
	if err == nil {
		return false
	}
	*target = err
	return true
}
