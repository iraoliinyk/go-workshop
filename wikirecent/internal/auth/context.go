package auth

import "context"

// ctxKey is an unexported empty struct type, so no other package can create a value
// of it. That is what keeps the key unique: a plain string key could be overwritten
// by any package that happened to choose the same text.
type ctxKey struct{}

var claimsKey ctxKey

// WithClaims returns a copy of ctx that carries the verified claims. Only
// authenticate calls it, so nothing else can add claims.
func WithClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, claimsKey, c)
}

// ClaimsFrom returns the verified claims for the current request. ok is false on a
// public route, where no token was read.
func ClaimsFrom(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsKey).(*Claims)
	return c, ok
}
