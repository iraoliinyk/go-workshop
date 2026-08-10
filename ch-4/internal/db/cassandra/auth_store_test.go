//go:build integration

package cassandra_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"ch-4/internal/apperrors"
	"ch-4/internal/auth"
	"ch-4/internal/db/cassandra"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserStore_CreatedUserKeepsAllFields(t *testing.T) {
	sess := connect(t)
	store := cassandra.NewUserStore(sess)
	ctx := context.Background()

	want := auth.User{
		Email:        unique("allfields") + "@example.test",
		PasswordHash: "$2a$10$notarealhashbutlongenoughtolooklikeone",
		Active:       true,
		CreatedAt:    time.Date(2020, 6, 1, 12, 0, 0, 0, time.UTC),
	}
	require.NoError(t, store.CreateUser(ctx, want))

	got, err := store.UserByEmail(ctx, want.Email)
	require.NoError(t, err)
	assert.Equal(t, want.Email, got.Email)
	assert.Equal(t, want.PasswordHash, got.PasswordHash)
	assert.Equal(t, want.Active, got.Active)
	assert.WithinDuration(t, want.CreatedAt, got.CreatedAt.UTC(), time.Millisecond)
}

func TestUserStore_DuplicateIsConflict(t *testing.T) {
	sess := connect(t)
	store := cassandra.NewUserStore(sess)
	ctx := context.Background()

	email := unique("dup") + "@example.test"
	first := auth.User{Email: email, PasswordHash: "first-hash", Active: true, CreatedAt: time.Now().UTC()}
	require.NoError(t, store.CreateUser(ctx, first), "first insert must apply")

	second := auth.User{Email: email, PasswordHash: "second-hash", Active: false, CreatedAt: time.Now().UTC()}
	err := store.CreateUser(ctx, second)
	require.Error(t, err, "duplicate email must not apply")

	conflict, ok := errors.AsType[*apperrors.ConflictError](err)
	require.True(t, ok, "want *apperrors.ConflictError (HTTP 409), got %T: %v", err, err)
	assert.Contains(t, conflict.Error(), email)

	// The second write must not have changed the row.
	got, err := store.UserByEmail(ctx, email)
	require.NoError(t, err)
	assert.Equal(t, "first-hash", got.PasswordHash, "the first write must win")
	assert.True(t, got.Active)
}

func TestUserStore_UnknownEmailIsNotFound(t *testing.T) {
	sess := connect(t)
	store := cassandra.NewUserStore(sess)

	_, err := store.UserByEmail(context.Background(), unique("nobody")+"@example.test")
	require.Error(t, err)

	_, ok := errors.AsType[*apperrors.NotFoundError](err)
	require.True(t, ok, "want *apperrors.NotFoundError, got %T: %v", err, err)

	_, isRepo := errors.AsType[*apperrors.RepositoryError](err)
	assert.False(t, isRepo, "a miss must not look like a store failure")
}

func TestRevocationStore_RevokeThenIsRevoked(t *testing.T) {
	sess := connect(t)
	store := cassandra.NewRevocationStore(sess)
	ctx := context.Background()

	jti := unique("jti")
	revoked, err := store.IsRevoked(ctx, jti)
	require.NoError(t, err)
	require.False(t, revoked, "unknown jti must not be revoked")

	// One hour ahead, like a real access token.
	require.NoError(t, store.Revoke(ctx, jti, "someone@example.test", time.Now().UTC().Add(time.Hour)))

	revoked, err = store.IsRevoked(ctx, jti)
	require.NoError(t, err)
	assert.True(t, revoked, "the next read must see the revocation")

	// Logout must be safe to repeat: a second POST must not fail.
	require.NoError(t, store.Revoke(ctx, jti, "someone@example.test", time.Now().UTC().Add(time.Hour)))
	revoked, err = store.IsRevoked(ctx, jti)
	require.NoError(t, err)
	assert.True(t, revoked)
}

func TestRevocationStore_AlreadyExpiredWritesNothing(t *testing.T) {
	sess := connect(t)
	store := cassandra.NewRevocationStore(sess)
	ctx := context.Background()

	jti := unique("expired")
	require.NoError(t, store.Revoke(ctx, jti, "someone@example.test", time.Now().UTC().Add(-time.Minute)),
		"revoking an expired token is a no-op, not an error")

	revoked, err := store.IsRevoked(ctx, jti)
	require.NoError(t, err)
	assert.False(t, revoked, "no row should have been written")
}
