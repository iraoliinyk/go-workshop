package models

type WikiEventMeta struct {
	URI string `json:"uri"`
	ID  string `json:"id"`
}

type WikiEvent struct {
	Meta      WikiEventMeta `json:"meta"`
	ID        int64         `json:"id"`
	Type      string        `json:"type"`
	Title     string        `json:"title"`
	Timestamp int64         `json:"timestamp"`
	User      string        `json:"user"`
	Bot       bool          `json:"bot"`
	ServerURL string        `json:"server_url"`
}
