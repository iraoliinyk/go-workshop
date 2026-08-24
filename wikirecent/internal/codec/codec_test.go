package codec_test

import (
	"errors"
	"strings"
	"testing"

	"wikirecent/internal/apperrors"
	"wikirecent/internal/codec"
	"wikirecent/internal/events"
	wikiv1 "wikirecent/internal/genproto/wikirecent/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	editEvent = `{"$schema":"/mediawiki/recentchange/1.0.0",` +
		`"meta":{"uri":"https://en.wikipedia.org/wiki/Foo","request_id":"d1f0","id":"9c1b-uuid",` +
		`"dt":"2026-08-24T10:00:00Z","domain":"en.wikipedia.org","stream":"mediawiki.recentchange",` +
		`"topic":"eqiad.mediawiki.recentchange","partition":2,"offset":5647231},` +
		`"id":2145678901,"type":"edit","namespace":0,"title":"Foo",` +
		`"title_url":"https://en.wikipedia.org/wiki/Foo","comment":"fix typo",` +
		`"timestamp":1787565600,"user":"iryna","bot":false,"minor":true,"patrolled":true,` +
		`"length":{"old":1200,"new":1215},"revision":{"old":111,"new":112},` +
		`"server_url":"https://en.wikipedia.org","server_name":"en.wikipedia.org",` +
		`"server_script_path":"/w","wiki":"enwiki","parsedcomment":"fix typo",` +
		`"notify_url":"https://en.wikipedia.org/w/index.php?diff=112"}`

	logEvent = `{"meta":{"domain":"commons.wikimedia.org","dt":"2026-08-24T10:00:02Z"},` +
		`"id":null,"type":"log","log_id":77,"log_type":"upload","log_action":"upload",` +
		`"log_action_comment":"uploaded a new file",` +
		`"log_params":{"img_sha1":"abc","img_timestamp":123},` +
		`"title":"File:Baz.jpg","timestamp":1787565602,"user":"Bot9","bot":true,` +
		`"server_url":"https://commons.wikimedia.org"}`
)

// encode runs the producer half and hands back the decoded message, which is
// what a consumer would see.
func encode(t *testing.T, raw string) *wikiv1.WikiEvent {
	t.Helper()

	out, err := codec.EncodeFromJSON([]byte(raw))
	require.NoError(t, err)
	require.NotEmpty(t, out)

	event, err := codec.Decode(out)
	require.NoError(t, err)
	return event
}

func TestEncodeFromJSON_MapsEveryField(t *testing.T) {
	event := encode(t, editEvent)

	assert.Equal(t, "iryna", event.GetUser())
	assert.False(t, event.GetBot())
	assert.Equal(t, "https://en.wikipedia.org", event.GetServerUrl())
	assert.Equal(t, "edit", event.GetType())
	assert.Equal(t, "Foo", event.GetTitle())
	assert.Equal(t, int32(0), event.GetNamespace())
	assert.True(t, event.GetMinor())
	assert.True(t, event.GetPatrolled())
	assert.Equal(t, int64(2145678901), event.GetId())
	assert.Equal(t, "enwiki", event.GetWiki())
	assert.Equal(t, "fix typo", event.GetComment())
	assert.Equal(t, "https://en.wikipedia.org/wiki/Foo", event.GetTitleUrl())
	assert.Equal(t, "fix typo", event.GetParsedcomment())
	assert.Equal(t, "en.wikipedia.org", event.GetServerName())
	assert.Equal(t, "/w", event.GetServerScriptPath())
	assert.Equal(t, "https://en.wikipedia.org/w/index.php?diff=112", event.GetNotifyUrl())
	assert.Equal(t, "/mediawiki/recentchange/1.0.0", event.GetSchema())

	assert.Equal(t, int64(1200), event.GetLength().GetOldBytes())
	assert.Equal(t, int64(1215), event.GetLength().GetNewBytes())
	assert.Equal(t, int64(111), event.GetRevision().GetOldId())
	assert.Equal(t, int64(112), event.GetRevision().GetNewId())
}

func TestEncodeFromJSON_MapsMeta(t *testing.T) {
	meta := encode(t, editEvent).GetMeta()

	assert.Equal(t, "https://en.wikipedia.org/wiki/Foo", meta.GetUri())
	assert.Equal(t, "d1f0", meta.GetRequestId())
	assert.Equal(t, "9c1b-uuid", meta.GetId())
	assert.Equal(t, "en.wikipedia.org", meta.GetDomain())
	assert.Equal(t, "mediawiki.recentchange", meta.GetStream())
	assert.Equal(t, "eqiad.mediawiki.recentchange", meta.GetTopic())
	assert.Equal(t, int32(2), meta.GetPartition())
	assert.Equal(t, int64(5647231), meta.GetOffset())
}

func TestEncodeFromJSON_KeepsEventWithUnreadableDate(t *testing.T) {
	event := encode(t, `{"user":"iryna","meta":{"dt":"yesterday"},"timestamp":0}`)

	assert.Nil(t, event.GetMeta().GetDt(), "an unreadable date is dropped")
	assert.Nil(t, event.GetTimestamp(), "no timestamp means no value, not the epoch")
	assert.Equal(t, "iryna", event.GetUser(), "the rest of the event survives")
}
func TestEncodeFromJSON_IgnoresFieldsWeDoNotMap(t *testing.T) {
	event := encode(t, `{"user":"iryna","dropped_field":{"nested":[1,2,3]}}`)

	assert.Equal(t, "iryna", event.GetUser())
}

func TestEncodeFromJSON_IsSmallerThanTheJSON(t *testing.T) {
	out, err := codec.EncodeFromJSON([]byte(editEvent))
	require.NoError(t, err)

	assert.Less(t, len(out), len(editEvent))
}

func TestEncodeFromJSON_RejectsBrokenJSON(t *testing.T) {
	_, err := codec.EncodeFromJSON([]byte(`{"user":`))

	require.Error(t, err)
	perr, ok := errors.AsType[*apperrors.ParseError](err)
	require.True(t, ok, "callers switch on this type to decide to skip the record")
	assert.Equal(t, `{"user":`, perr.Line)
}

func TestEncodeFromJSON_ShortensTheReportedLine(t *testing.T) {
	long := `{"user":"` + strings.Repeat("x", 2000)

	_, err := codec.EncodeFromJSON([]byte(long))

	perr, ok := errors.AsType[*apperrors.ParseError](err)
	require.True(t, ok)
	assert.Less(t, len(perr.Line), 300, "one bad event must not fill the log")
}

func TestDecode_RejectsGarbage(t *testing.T) {
	_, err := codec.Decode([]byte{0xff, 0xff, 0xff, 0xff})

	require.Error(t, err)
	perr, ok := errors.AsType[*apperrors.ParseError](err)
	require.True(t, ok)
	assert.Contains(t, perr.Line, "bytes, prefix", "binary must not be dumped into the log")
}

func TestToDomain_KeepsOnlyWhatStatsReads(t *testing.T) {
	got := codec.ToDomain(encode(t, editEvent))

	assert.Equal(t, events.WikiEvent{
		User:      "iryna",
		Bot:       false,
		ServerURL: "https://en.wikipedia.org",
	}, got)
}

func TestToDomain_HandlesNilMessage(t *testing.T) {
	assert.Equal(t, events.WikiEvent{}, codec.ToDomain(nil))
}

func TestEventDecoder_DecodesRecordValue(t *testing.T) {
	value, err := codec.EncodeFromJSON([]byte(logEvent))
	require.NoError(t, err)

	got, err := codec.EventDecoder{}.Decode(value)

	require.NoError(t, err)
	assert.Equal(t, events.WikiEvent{
		User:      "Bot9",
		Bot:       true,
		ServerURL: "https://commons.wikimedia.org",
	}, got)
}

func TestEventDecoder_ReportsBrokenRecord(t *testing.T) {
	got, err := codec.EventDecoder{}.Decode([]byte{0xff, 0xff})

	require.Error(t, err)
	assert.Equal(t, events.WikiEvent{}, got, "a failed decode returns no half-filled event")
}
