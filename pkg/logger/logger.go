package logger

import (
	"log"
	"os"
)

// Logger defines a structured logging interface that can be implemented
// by standard log, zap, slog, or other loggers.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
	Debug(msg string, keysAndValues ...any)
	Fatal(msg string, keysAndValues ...any)
}

// New returns the default SimpleLogger using the standard library log.
func New() Logger {
	return &SimpleLogger{
		writer: log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile),
	}
}
