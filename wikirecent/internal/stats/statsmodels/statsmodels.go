package statsmodels

import "time"

type Snapshot struct {
	TotalMessages int64            `json:"total_messages"`
	DistinctUsers int64            `json:"distinct_users"`
	BotEdits      int64            `json:"bot_edits"`
	HumanEdits    int64            `json:"human_edits"`
	ByServerURL   map[string]int64 `json:"by_server_url"`
	LastEvent     time.Time        `json:"last_event"`
}

type SnapshotPoint struct {
	At            time.Time
	TotalMessages int64
	BotEdits      int64
	HumanEdits    int64
	DistinctUsers int64
}

// DeltaKey is built from the records themselves, so the same records always give the
// same key. Writing it twice gives the same result as writing it once: the replay
// overwrites its own row instead of adding a second one.
type DeltaKey struct {
	Day       time.Time
	Partition int32
	EndOffset int64
}

type Delta struct {
	Key         DeltaKey
	Messages    int64
	BotEdits    int64
	HumanEdits  int64
	Users       []string // feeds the in-memory set; not summable in the database
	ByServerURL map[string]int64
}
