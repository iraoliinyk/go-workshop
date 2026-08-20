package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"wikirecent/internal/apperrors"
	"wikirecent/internal/auth"
)

// UserStore is the memory version of the user_accounts table. The map key is the
// already normalized email: normalizing is the auth.Service's job, so this store
// never lowercases or trims anything itself.
type UserStore struct {
	mu    sync.RWMutex
	users map[string]auth.User
}

func NewUserStore() *UserStore {
	return &UserStore{users: make(map[string]auth.User)}
}

// CreateUser does what Cassandra's INSERT ... IF NOT EXISTS does. The lock makes
// the check and the write one step, so two registrations for the same email at
// the same time cannot both succeed.
func (s *UserStore) CreateUser(_ context.Context, u auth.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[u.Email]; exists {
		return &apperrors.ConflictError{Err: fmt.Errorf("email %q already registered", u.Email)}
	}
	s.users[u.Email] = u
	return nil
}

// UserByEmail returns NotFoundError, not RepositoryError, for a missing user.
// An unknown email is a normal outcome that Login turns into a 401, not a
// storage failure that would become a 503.
func (s *UserStore) UserByEmail(_ context.Context, email string) (auth.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[email]
	if !ok {
		return auth.User{}, &apperrors.NotFoundError{Err: fmt.Errorf("no user %q", email)}
	}
	return u, nil
}

// RevocationStore maps a token id (jti) to the time that token expires. Cassandra
// uses a row TTL for this; here the expiry is checked when reading instead.
type RevocationStore struct {
	mu      sync.Mutex // Mutex, not RWMutex: IsRevoked deletes stale entries
	revoked map[string]time.Time
}

func NewRevocationStore() *RevocationStore {
	return &RevocationStore{revoked: make(map[string]time.Time)}
}

// Revoke ignores email: it is only kept for auditing and nothing reads it back.
// Writing the same jti twice does nothing, which makes Logout safe to repeat.
func (s *RevocationStore) Revoke(_ context.Context, jti, _ string, exp time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revoked[jti] = exp
	return nil
}

func (s *RevocationStore) IsRevoked(_ context.Context, jti string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.revoked[jti]
	if !ok {
		return false, nil
	}
	if time.Now().UTC().After(exp) {
		// The token has expired, so the parser rejects it anyway. Deleting the
		// entry here is what Cassandra's row TTL does automatically.
		delete(s.revoked, jti)
		return false, nil
	}
	return true, nil
}
