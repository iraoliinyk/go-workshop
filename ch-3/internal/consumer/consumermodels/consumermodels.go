package consumermodels

// WikiEvent holds only the fields the stats logic uses. Decoding ignores any JSON key
// with no matching field, so the many other fields the stream sends are left out.
type WikiEvent struct {
	User      string `json:"user"`
	Bot       bool   `json:"bot"`
	ServerURL string `json:"server_url"`
}
