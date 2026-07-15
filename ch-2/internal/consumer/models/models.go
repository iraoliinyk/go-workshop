package models

type WikiEventMeta struct {
	URI       string `json:"uri"`
	RequestID string `json:"request_id"`
	ID        string `json:"id"`
}

type WikiEvent struct {
	Schema    string        `json:"$schema"`
	Meta      WikiEventMeta `json:"meta"`
	ID        int64         `json:"id"`
	Type      string        `json:"type"`
	Namespace int           `json:"namespace"`
	Title     string        `json:"title"`
	Timestamp int64         `json:"timestamp"`
	User      string        `json:"user"`
	Bot       bool          `json:"bot"`
	ServerURL string        `json:"server_url"`
}
