package apperrors

import "fmt"

// Generic AppError, is the common interface every project error satisfies.
type AppError interface {
	error
	// Code returns a short machine-readable identifier (e.g. "PARSE_ERROR").
	Code() string
}

// ---- ParseError --------------------------------------------------------

// ParseError is returned when an SSE line cannot be decoded into a WikiEvent.
// Behaviour: caller should log and continue — the stream is not broken.
type ParseError struct {
	Line string // the raw SSE line that failed
	Err  error  // underlying decode error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error on line %q: %v", e.Line, e.Err)
}

func (e *ParseError) Code() string { return "PARSE_ERROR" }

// Unwrap lets errors.Is / errors.As reach the underlying error.
func (e *ParseError) Unwrap() error { return e.Err }

// ---- StreamError -------------------------------------------------------

// StreamError is returned when the SSE scanner itself reports an error
// (e.g. connection reset mid-stream).
// Behaviour: caller should surface this upward — the consumer cannot continue.
type StreamError struct {
	Err error
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("stream error: %v", e.Err)
}

func (e *StreamError) Code() string { return "STREAM_ERROR" }

func (e *StreamError) Unwrap() error { return e.Err }

// ---- ConnectionError ---------------------------------------------------

// ConnectionError is returned when the initial HTTP request to the SSE
// endpoint fails (bad status, network error, request build failure).
// Behaviour: caller should cancel the context and initiate graceful shutdown.
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

// ShutdownError is returned when the HTTP server's graceful shutdown does
// not complete cleanly (timeout exceeded or other failure).
// Behaviour: log the reason so operators know the shutdown was not clean.
type ShutdownError struct {
	Err error
}

func (e *ShutdownError) Error() string {
	return fmt.Sprintf("shutdown did not complete cleanly: %v", e.Err)
}

func (e *ShutdownError) Code() string { return "SHUTDOWN_ERROR" }

func (e *ShutdownError) Unwrap() error { return e.Err }
