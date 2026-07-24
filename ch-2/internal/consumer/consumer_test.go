package consumer

import (
	"ch-2/internal/apperrors"
	"ch-2/internal/consumer/models"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEvent_ValidDataLine(t *testing.T) {
	event := models.WikiEvent{
		User:      "iryna",
		Bot:       false,
		ServerURL: "https://en.wikipedia.org",
	}
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	// parseEvent returns *apperrors.ParseError (concrete type), so use require.Nil,
	// NOT require.NoError — a typed nil pointer boxed into error is a non-nil interface.
	got, parseErr := parseEvent("data: " + string(payload))
	require.Nil(t, parseErr)

	assert.Equal(t, event.User, got.User)
	assert.Equal(t, event.Bot, got.Bot)
	assert.Equal(t, event.ServerURL, got.ServerURL)
}

func TestParseEvent_InvalidJSON(t *testing.T) {
	_, err := parseEvent("data: {not valid json}")

	var parseErr *apperrors.ParseError
	require.ErrorAs(t, err, &parseErr)
}

func TestParseEvent_StripsDataPrefix(t *testing.T) {
	event := models.WikiEvent{User: "john"}
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	got, parseErr := parseEvent("data: " + string(payload))
	require.Nil(t, parseErr)
	assert.Equal(t, "john", got.User)
}

func TestParseEvent_ErrorCode(t *testing.T) {
	_, err := parseEvent("data: {not valid json}")

	assert.Equal(t, "PARSE_ERROR", err.Code())
	require.Error(t, err)
}
