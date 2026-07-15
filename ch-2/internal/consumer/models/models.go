package models

// WikiEvent holds only the fields the stats logic consumes. The Wikimedia
// stream sends many more (meta, id, type, title, timestamp, …); unmarshalling
// simply ignores any JSON key without a matching field, so unused fields are
// omitted here rather than carried as dead weight.
type WikiEvent struct {
	User      string `json:"user"`
	Bot       bool   `json:"bot"`
	ServerURL string `json:"server_url"`
}
