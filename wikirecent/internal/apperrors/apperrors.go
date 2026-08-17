package apperrors

import "fmt"

// Error is the interface every error in this project satisfies, so one place in
// the HTTP layer can turn any of them into a status code.
type Error interface {
	error
	// Code returns a short machine-readable identifier (e.g. "PARSE_ERROR").
	Code() string
}

// ---- ParseError --------------------------------------------------------

// ParseError means one stream line could not be decoded into a WikiEvent.
// The caller should log it and continue, because the stream still works.
type ParseError struct {
	Line string // the raw SSE line that failed
	Err  error  // underlying decode error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error on line %q: %v", e.Line, e.Err)
}

func (e *ParseError) Code() string { return "PARSE_ERROR" }

// Unwrap lets errors.Is and errors.As reach the error inside.
func (e *ParseError) Unwrap() error { return e.Err }

// ---- ConnectionError ---------------------------------------------------

// ConnectionError means the first HTTP request to the stream failed: a bad status, a
// network problem, or a request that could not be built. The caller should cancel
// the context and shut down.
type ConnectionError struct {
	StatusCode int // 0 if the error is not HTTP-level
	Err        error
}

func (e *ConnectionError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("connection error: unexpected status %d: %v", e.StatusCode, e.Err)
	}
	return fmt.Sprintf("connection error: %v", e.Err)
}

func (e *ConnectionError) Code() string { return "CONNECTION_ERROR" }

func (e *ConnectionError) Unwrap() error { return e.Err }

// ---- ShutdownError -----------------------------------------------------

// ShutdownError means the HTTP server did not stop cleanly, usually because it ran
// out of time. Log the reason so operators know the shutdown was not clean.
type ShutdownError struct {
	Err error
}

func (e *ShutdownError) Error() string {
	return fmt.Sprintf("shutdown did not complete cleanly: %v", e.Err)
}

func (e *ShutdownError) Code() string { return "SHUTDOWN_ERROR" }

func (e *ShutdownError) Unwrap() error { return e.Err }

// ---- RepositoryError -----------------------------------------------------

// RepositoryError means a storage problem: a failed query, a lost connection, or a
// timeout. The HTTP layer answers 503, so the app never pretends the request
// succeeded.
type RepositoryError struct {
	Err error
}

func (e *RepositoryError) Error() string {
	return fmt.Sprintf("infrastructure/storage failure: %v", e.Err)
}

func (e *RepositoryError) Code() string { return "REPOSITORY_ERROR" }

func (e *RepositoryError) Unwrap() error { return e.Err }

// ---- ValidationError -----------------------------------------------------

// ValidationError means the client sent bad input to an auth endpoint. It becomes a
// 400, and its message is the only one shown to the caller, so keep it useful.
type ValidationError struct {
	Err error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("Auth input error: %v", e.Err)
}

func (e *ValidationError) Code() string { return "VALIDATION_ERROR" }

func (e *ValidationError) Unwrap() error { return e.Err }

// ---- UnauthorizedError ---------------------------------------------------

// UnauthorizedError becomes a 401 for both failed logins and rejected tokens. The
// HTTP layer always sends the same message, so nobody can learn which check failed.
type UnauthorizedError struct {
	Err error
}

func (e *UnauthorizedError) Error() string {
	return fmt.Sprintf("UnauthorizedError: %v", e.Err)
}

func (e *UnauthorizedError) Code() string { return "UNAUTHORIZED_ERROR" }

func (e *UnauthorizedError) Unwrap() error { return e.Err }

// ---- ConflictError -------------------------------------------------------

// ConflictError becomes a 409: the email is already registered.
type ConflictError struct {
	Err error
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("ConflictError: %v", e.Err)
}

func (e *ConflictError) Code() string { return "CONFLICT_ERROR" }

func (e *ConflictError) Unwrap() error { return e.Err }

// ---- NotFoundError -------------------------------------------------------

// NotFoundError means there is no user with this email. Login turns it into the same
// 401 as a wrong password, so it never reaches the client as a 404.
type NotFoundError struct {
	Err error
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("NotFoundError: %v", e.Err)
}

func (e *NotFoundError) Code() string { return "NOT_FOUND_ERROR" }

func (e *NotFoundError) Unwrap() error { return e.Err }

type PublishError struct {
	Err error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("PublishError: %v", e.Err)
}

func (e *PublishError) Code() string { return "PUBLISH_ERROR" }

func (e *PublishError) Unwrap() error { return e.Err }

type ConsumeError struct {
	Err error
}

func (e *ConsumeError) Error() string {
	return fmt.Sprintf("ConsumeError: %v", e.Err)
}

func (e *ConsumeError) Code() string { return "CONSUME_ERROR" }

func (e *ConsumeError) Unwrap() error { return e.Err }
