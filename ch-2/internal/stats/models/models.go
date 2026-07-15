package models

import "time"

type StatsSnapshot struct {
	TotalMessages int64            `json:"total_messages"`
	DistinctUsers int64            `json:"distinct_users"`
	BotEdits      int64            `json:"bot_edits"`
	HumanEdits    int64            `json:"human_edits"`
	ByServerURL   map[string]int64 `json:"by_server_url"`
	LastEvent     time.Time        `json:"last_event"`
}
