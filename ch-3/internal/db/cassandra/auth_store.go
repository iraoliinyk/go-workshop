package cassandra

import (
	"ch-3/internal/apperrors"
	"ch-3/internal/auth"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type UserStore struct{ sess *gocql.Session }

func NewUserStore(sess *gocql.Session) *UserStore { return &UserStore{sess: sess} }

const insertUserIfNotExists = `INSERT INTO user_accounts
	(email, password_hash, created_at, active) VALUES (?, ?, ?, ?) IF NOT EXISTS`

func (s *UserStore) CreateUser(ctx context.Context, u auth.User) error {
	prev := make(map[string]any)
	applied, err := s.sess.Query(insertUserIfNotExists, u.Email, u.PasswordHash, u.CreatedAt, u.Active).
		SerialConsistency(gocql.Serial).
		MapScanCASContext(ctx, prev)
	if err != nil {
		return &apperrors.RepositoryError{Err: err}
	}
	if !applied {
		return &apperrors.ConflictError{Err: fmt.Errorf("email %q already registered", u.Email)}
	}
	return nil
}

const selectUserByEmail = `SELECT email, password_hash, created_at, active
	FROM user_accounts WHERE email = ?`

func (s *UserStore) UserByEmail(ctx context.Context, email string) (auth.User, error) {
	var u auth.User
	err := s.sess.Query(selectUserByEmail, email).
		ScanContext(ctx, &u.Email, &u.PasswordHash, &u.CreatedAt, &u.Active)
	switch {
	case errors.Is(err, gocql.ErrNotFound):
		// Not a RepositoryError: a missing user is a normal outcome that Login
		// turns into a 401, not a storage failure that would become a 503.
		return auth.User{}, &apperrors.NotFoundError{Err: fmt.Errorf("no user %q", email)}
	case err != nil:
		return auth.User{}, &apperrors.RepositoryError{Err: err}
	}
	return u, nil
}

const insertRevoked = `INSERT INTO revoked_tokens (jti, email, revoked_at)
	VALUES (?, ?, ?) USING TTL ?`

const selectRevoked = `SELECT jti FROM revoked_tokens WHERE jti = ?`

type RevocationStore struct{ sess *gocql.Session }

func NewRevocationStore(sess *gocql.Session) *RevocationStore {
	return &RevocationStore{sess: sess}
}

func (s *RevocationStore) Revoke(ctx context.Context, jti, email string, exp time.Time) error {
	// Whole seconds, rounded up, so the row never dies before the token does.
	ttl := int(math.Ceil(time.Until(exp).Seconds()))
	if ttl <= 0 {
		// The token has already expired, so the parser rejects it and no row is
		// needed. Cassandra also refuses a TTL of zero or less.
		return nil
	}
	if err := s.sess.Query(insertRevoked, jti, email, time.Now().UTC(), ttl).ExecContext(ctx); err != nil {
		return &apperrors.RepositoryError{Err: err}
	}
	return nil
}

func (s *RevocationStore) IsRevoked(ctx context.Context, jti string) (bool, error) {
	var found string
	err := s.sess.Query(selectRevoked, jti).ScanContext(ctx, &found)
	switch {
	case errors.Is(err, gocql.ErrNotFound):
		return false, nil
	case err != nil:
		// Return the error so authenticate can answer 503. Returning (false, nil)
		// here would let every revoked token work again whenever Cassandra is
		// unreachable.
		return false, &apperrors.RepositoryError{Err: err}
	}
	return true, nil
}
