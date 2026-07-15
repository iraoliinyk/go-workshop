package models

type WikiEventMeta struct {
	URI       string `json:"uri"`
	RequestID string `json:"request_id"`
	ID        string `json:"id"`
	Domain    string `json:"domain"`
	Stream    string `json:"stream"`
	DT        string `json:"dt"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type WikiEvent struct {
	Schema        string        `json:"$schema"`
	Meta          WikiEventMeta `json:"meta"`
	ID            int64         `json:"id"`
	Type          string        `json:"type"`
	Namespace     int           `json:"namespace"`
	Title         string        `json:"title"`
	TitleURL      string        `json:"title_url"`
	Comment       string        `json:"comment"`
	Timestamp     int64         `json:"timestamp"`
	User          string        `json:"user"`
	Bot           bool          `json:"bot"`
	NotifyURL     string        `json:"notify_url"`
	ServerURL     string        `json:"server_url"`
	ServerName    string        `json:"server_name"`
	ServerScript  string        `json:"server_script_path"`
	Wiki          string        `json:"wiki"`
	ParsedComment string        `json:"parsedcomment"`
}
