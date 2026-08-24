package codec

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"wikirecent/internal/apperrors"
	"wikirecent/internal/events"
	wikiv1 "wikirecent/internal/genproto/wikirecent/v1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func EncodeFromJSON(raw []byte) ([]byte, error) {
	var w wireEvent
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, &apperrors.ParseError{Line: clip(string(raw)), Err: err}
	}

	out, err := proto.Marshal(toProto(&w))
	if err != nil {
		return nil, &apperrors.ParseError{Line: clip(string(raw)), Err: err}
	}
	return out, nil
}

func Decode(value []byte) (*wikiv1.WikiEvent, error) {
	var event wikiv1.WikiEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		line := fmt.Sprintf("%d bytes, prefix %x", len(value), value[:min(16, len(value))])
		return nil, &apperrors.ParseError{Line: line, Err: err}
	}
	return &event, nil
}

func ToDomain(e *wikiv1.WikiEvent) events.WikiEvent {
	return events.WikiEvent{
		User:      e.GetUser(),
		Bot:       e.GetBot(),
		ServerURL: e.GetServerUrl(),
	}
}

type EventDecoder struct{}

func NewEventDecoder() EventDecoder { return EventDecoder{} }

func (EventDecoder) Decode(value []byte) (events.WikiEvent, error) {
	event, err := Decode(value)
	if err != nil {
		return events.WikiEvent{}, err
	}
	return ToDomain(event), nil
}

func toProto(w *wireEvent) *wikiv1.WikiEvent {
	return &wikiv1.WikiEvent{
		User:             w.User,
		Bot:              w.Bot,
		ServerUrl:        w.ServerURL,
		Type:             w.Type,
		Title:            w.Title,
		Timestamp:        unixTimestamp(w.Timestamp),
		Namespace:        w.Namespace,
		Minor:            w.Minor,
		Patrolled:        w.Patrolled,
		Id:               w.ID,
		Wiki:             w.Wiki,
		Comment:          w.Comment,
		Meta:             metaToProto(&w.Meta),
		Length:           pageLength(w.Length),
		Revision:         revisionIds(w.Revision),
		TitleUrl:         w.TitleURL,
		Parsedcomment:    w.Parsedcomment,
		ServerName:       w.ServerName,
		ServerScriptPath: w.ServerScriptPath,
		NotifyUrl:        w.NotifyURL,
		Schema:           w.Schema,
		Log:              logToProto(w),
	}
}

func metaToProto(m *wireMeta) *wikiv1.Meta {
	return &wikiv1.Meta{
		Uri:       m.URI,
		RequestId: m.RequestID,
		Id:        m.ID,
		Dt:        rfc3339Timestamp(m.Dt),
		Domain:    m.Domain,
		Stream:    m.Stream,
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
	}
}

func pageLength(v *wireOldNew) *wikiv1.PageLength {
	if v == nil {
		return nil
	}
	return &wikiv1.PageLength{OldBytes: v.Old, NewBytes: v.New}
}

func revisionIds(v *wireOldNew) *wikiv1.RevisionIds {
	if v == nil {
		return nil
	}
	return &wikiv1.RevisionIds{OldId: v.Old, NewId: v.New}
}

func logToProto(w *wireEvent) *wikiv1.Log {
	if w.LogID == nil && w.LogType == "" && w.LogAction == "" {
		return nil
	}
	return &wikiv1.Log{
		Id:            w.LogID,
		Type:          w.LogType,
		Action:        w.LogAction,
		ActionComment: w.LogActionComment,
		ParamsJson:    string(w.LogParams),
	}
}

func unixTimestamp(sec int64) *timestamppb.Timestamp {
	if sec == 0 {
		return nil
	}
	return timestamppb.New(time.Unix(sec, 0).UTC())
}

func rfc3339Timestamp(s string) *timestamppb.Timestamp {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return timestamppb.New(t.UTC())
}

func clip(s string) string {
	const maxLine = 256
	if len(s) <= maxLine {
		return s
	}
	return strings.ToValidUTF8(s[:maxLine], "") + "…"
}
