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
