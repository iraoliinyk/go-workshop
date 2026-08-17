package auth

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type User struct {
	Email        string
	PasswordHash string
	Active       bool
	CreatedAt    time.Time
}

type Claims struct {
	Email string `json:"email,omitempty"`
	// RegisteredClaims holds the standard JWT fields, named to match the
	//  Spring Boot version:
	//   Issuer    -> iss, who signed the token (JWT_ISSUER)
	//   Subject   -> sub, the user, e.g. iryna@email.com
	//   IssuedAt  -> iat, when it was signed
	//   ExpiresAt -> exp, iat plus the TTL
	//   ID        -> jti, a random UUID used to revoke this one token
	jwt.RegisteredClaims
}

type RegisterRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"` // "Bearer"
	ExpiresIn   int64  `json:"expires_in"` // seconds, not a timestamp
}
