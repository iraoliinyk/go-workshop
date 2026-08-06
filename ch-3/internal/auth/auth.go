package auth

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"ch-3/internal/apperrors"
	"ch-3/internal/applog"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

var errInvalidToken = errors.New("invalid token")

var errInvalidCredentials = errors.New("invalid credentials")

type UserStore interface {
	CreateUser(ctx context.Context, u User) error                // ConflictError if the email is taken
	UserByEmail(ctx context.Context, email string) (User, error) // NotFoundError if there is no such user
}

type RevocationStore interface {
	Revoke(ctx context.Context, jti, email string, exp time.Time) error
	IsRevoked(ctx context.Context, jti string) (bool, error)
}

type Service struct {
	users  UserStore
	tokens RevocationStore
	secret []byte
	parser *jwt.Parser
	issuer string
	ttl    time.Duration
	cost   int
	log    applog.Logger
	// dummyHash is compared when the email is unknown, so a login for a user who
	// does not exist takes the same time as one for a real user.
	dummyHash []byte
}

type Config struct {
	Secret string
	Issuer string
	TTL    time.Duration
	Cost   int
	Log    applog.Logger // zero value is PROD: rejected requests are silent, 5xx is not
}

func New(users UserStore, tokens RevocationStore, config Config) (*Service, error) {
	if len(config.Secret) < 32 {
		return nil, fmt.Errorf("auth: secret must be at least 32 bytes, got %d", len(config.Secret))
	}
	if config.Cost < 10 || config.Cost > 31 {
		return nil, fmt.Errorf("auth: bcrypt cost must be in 10..31, got %d", config.Cost)
	}
	if config.TTL <= 0 {
		return nil, fmt.Errorf("auth: token TTL must be positive, got %s", config.TTL)
	}
	if config.Issuer == "" {
		return nil, errors.New("auth: issuer must not be empty")
	}

	dummyHash, err := bcrypt.GenerateFromPassword([]byte("wiki-stream-go dummy password"), config.Cost)
	if err != nil {
		return nil, fmt.Errorf("auth: build dummy hash: %w", err)
	}

	return &Service{
		users:  users,
		tokens: tokens,
		secret: []byte(config.Secret),
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{"HS256"}),
			jwt.WithIssuer(config.Issuer),
			jwt.WithExpirationRequired(),
		),
		issuer:    config.Issuer,
		ttl:       config.TTL,
		cost:      config.Cost,
		log:       config.Log,
		dummyHash: dummyHash,
	}, nil
}

// issue signs a new access token and returns it with its expiry time. Login is
// the only caller, and this is the only place that creates tokens.
func (s *Service) issue(email string) (string, time.Time, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(s.ttl)

	claims := Claims{
		Email: email,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   email,
			ID:        uuid.NewString(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("auth: sign token: %w", err)
	}
	return signed, expiresAt, nil
}

// Verify parses the token, checks the signature and the standard claims, and returns
// the payload. It does not check revocation: that needs a context and fails with 503
// instead of 401, so authenticate in middleware.go does it.
func (s *Service) Verify(raw string) (*Claims, error) {
	var claims Claims

	tok, err := s.parser.ParseWithClaims(raw, &claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return s.secret, nil
	})
	if err != nil || !tok.Valid {
		return nil, &apperrors.UnauthorizedError{Err: errInvalidToken}
	}
	if claims.ID == "" { // no jti, so the token could never be revoked
		return nil, &apperrors.UnauthorizedError{Err: errInvalidToken}
	}
	return &claims, nil
}

func (s *Service) Login(ctx context.Context, email, pw string) (string, time.Time, error) {
	normalized, err := normalizeEmail(email) // lower case, trimmed
	if err != nil {
		// A 401, not the 400 that Register returns: login has only two answers.
		// Skipping the dummy compare here is safe, because the client can check
		// the email format itself. It cannot check whether an account exists.
		return "", time.Time{}, &apperrors.UnauthorizedError{Err: errInvalidCredentials}
	}

	u, err := s.users.UserByEmail(ctx, normalized)
	if err != nil {
		// The user does not exist, but we still compare a fake hash. Without
		// this the answer would come back faster, and an attacker could measure
		// the time and learn which emails are registered.
		_ = bcrypt.CompareHashAndPassword(s.dummyHash, []byte(pw))
		return "", time.Time{}, &apperrors.UnauthorizedError{Err: errInvalidCredentials}
	}
	// Always compare the password, even for an inactive user, for the same
	// timing reason.
	pwErr := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(pw))
	if !u.Active || pwErr != nil {
		// One error for all three cases, so the client cannot tell which part
		// was wrong.
		return "", time.Time{}, &apperrors.UnauthorizedError{Err: errInvalidCredentials}
	}
	return s.issue(u.Email)
}

var emailRE = regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)

const (
	maxEmailLen      = 254
	minPasswordBytes = 8
	maxPasswordBytes = 72
)

func normalizeEmail(rawEmail string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(rawEmail))
	switch {
	case normalized == "":
		return "", &apperrors.ValidationError{Err: errors.New("email must not be blank")}
	case len(normalized) > maxEmailLen:
		return "", &apperrors.ValidationError{Err: fmt.Errorf("email must be at most %d bytes", maxEmailLen)}
	case !emailRE.MatchString(normalized):
		return "", &apperrors.ValidationError{Err: errors.New("email format is invalid")}
	}
	return normalized, nil
}

func (s *Service) Register(ctx context.Context, req RegisterRequest) error {
	email, err := normalizeEmail(req.Email)
	if err != nil {
		return err // ValidationError -> 400
	}
	if err := validatePassword(req.Password); err != nil {
		return err // ValidationError -> 400
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.cost)
	if err != nil {
		return fmt.Errorf("auth: hash password: %w", err)
	}

	// CreateUser returns a ConflictError when the email is taken, which becomes a 409.
	return s.users.CreateUser(ctx, User{
		Email:        email,
		PasswordHash: string(hash),
		Active:       true,
		CreatedAt:    time.Now().UTC(),
	})
}

func validatePassword(pw string) error {
	if n := len(pw); n < minPasswordBytes || n > maxPasswordBytes {
		return &apperrors.ValidationError{
			Err: fmt.Errorf("password must be %d..%d bytes, got %d", minPasswordBytes, maxPasswordBytes, n),
		}
	}
	return nil
}

func (s *Service) Logout(ctx context.Context, c *Claims) error {
	if c == nil || c.ID == "" {
		return &apperrors.UnauthorizedError{Err: errInvalidToken}
	}
	if c.ExpiresAt == nil {
		return &apperrors.UnauthorizedError{Err: errInvalidToken}
	}

	if err := s.tokens.Revoke(ctx, c.ID, c.Email, c.ExpiresAt.Time); err != nil {
		return err // RepositoryError becomes a 503, so a token is never trusted by mistake
	}
	return nil
}
