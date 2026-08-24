package codec

import "encoding/json"

type wireEvent struct {
	Schema           string          `json:"$schema"`
	Meta             wireMeta        `json:"meta"`
	ID               *int64          `json:"id"`
	Type             string          `json:"type"`
	Namespace        int32           `json:"namespace"`
	Title            string          `json:"title"`
	TitleURL         string          `json:"title_url"`
	Comment          string          `json:"comment"`
	Parsedcomment    string          `json:"parsedcomment"`
	Timestamp        int64           `json:"timestamp"`
	User             string          `json:"user"`
	Bot              bool            `json:"bot"`
	NotifyURL        string          `json:"notify_url"`
	Minor            bool            `json:"minor"`
	Patrolled        *bool           `json:"patrolled"`
	Length           *wireOldNew     `json:"length"`
	Revision         *wireOldNew     `json:"revision"`
	ServerURL        string          `json:"server_url"`
	ServerName       string          `json:"server_name"`
	ServerScriptPath string          `json:"server_script_path"`
	Wiki             string          `json:"wiki"`
	LogID            *int64          `json:"log_id"`
	LogType          string          `json:"log_type"`
	LogAction        string          `json:"log_action"`
	LogActionComment string          `json:"log_action_comment"`
	LogParams        json.RawMessage `json:"log_params"`
}

type wireMeta struct {
	URI       string `json:"uri"`
	RequestID string `json:"request_id"`
	ID        string `json:"id"`
	Dt        string `json:"dt"`
	Domain    string `json:"domain"`
	Stream    string `json:"stream"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type wireOldNew struct {
	Old *int64 `json:"old"`
	New *int64 `json:"new"`
}
