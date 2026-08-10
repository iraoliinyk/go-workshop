package applog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"ch-4/internal/apperrors"

	"github.com/lmittmann/tint"
)

const (
	ModeDebug = "DEBUG"
	ModeProd  = "PROD"
)

type Logger struct {
	debug bool
	log   *slog.Logger
}

var prodLogger = sync.OnceValue(func() *slog.Logger { return slog.New(prodHandler(os.Stderr)) })

func prodHandler(w io.Writer) slog.Handler {
	return tint.NewTextHandler(w, &tint.Options{
		Level:      slog.LevelError,
		TimeFormat: time.RFC3339,
		NoColor:    !isTerminal(w),
	})
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false // io.Discard, a bytes.Buffer, a MultiWriter: never a terminal
	}
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func New(mode string, w io.Writer) (Logger, error) {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case ModeDebug:
		// Colours, level and timestamp; keeps attributes readable in a terminal.
		// TimeOnly because the date is noise in a dev log.
		return Logger{debug: true, log: slog.New(tint.NewTextHandler(w, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.TimeOnly,
			NoColor:    !isTerminal(w),
		}))}, nil
	case ModeProd, "":
		return Logger{log: slog.New(prodHandler(w))}, nil
	default:
		return Logger{}, fmt.Errorf("applog: unknown LOGGER %q (want %s or %s)", mode, ModeDebug, ModeProd)
	}
}

func (l Logger) slogger() *slog.Logger {
	if l.log == nil {
		return prodLogger()
	}
	return l.log
}

func (l Logger) Enabled() bool { return l.debug }

// Debugf writes one diagnostic line in DEBUG mode and nothing in PROD.
func (l Logger) Debugf(format string, args ...any) {
	if !l.debug {
		return // return before Sprintf, so PROD does not pay to format a dropped line
	}
	l.slogger().Debug(fmt.Sprintf(format, args...))
}

// Warnf writes one line for a failure the caller caused, anything that answers 4xx.
func (l Logger) Warnf(format string, args ...any) {
	if !l.slogger().Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	l.slogger().Warn(fmt.Sprintf(format, args...))
}

// Errorf writes one error line in both modes. Use it for failures this process
// owns: a panic, a 500, a response that could not be encoded.
func (l Logger) Errorf(format string, args ...any) {
	l.slogger().Error(fmt.Sprintf(format, args...))
}

// AppErrorf writes one error line in both modes.
func (l Logger) AppErrorf(err error, format string, args ...any) {
	log := l.slogger()
	if appErr, ok := errors.AsType[apperrors.Error](err); ok {
		log = log.With(slog.String("code", appErr.Code()))
	}
	log.Error(fmt.Sprintf(format, args...))
}

func (l Logger) With(args ...any) Logger {
	return Logger{debug: l.debug, log: l.slogger().With(args...)}
}

// Ctx returns a Logger tagged with the request ID carried by ctx, so the lines of
// concurrent requests can be told apart.
func (l Logger) Ctx(ctx context.Context) Logger {
	id := RequestIDFrom(ctx)
	if id == "" {
		return l
	}
	return l.With(slog.String("request_id", id))
}

// requestIDKey is unexported, so no other package's context value can collide with it.
type requestIDKey struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
