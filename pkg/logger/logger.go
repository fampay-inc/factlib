package logger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Logger is a simple wrapper around zerolog
type Logger struct {
	zl zerolog.Logger
}

// Config defines the logger configuration
type Config struct {
	Level      string
	WithCaller bool
	Output     io.Writer
}

// New creates a new logger with the provided configuration
func New(cfg Config) *Logger {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	// Set global level
	zerolog.SetGlobalLevel(level)
	zerolog.TimeFieldFormat = time.RFC3339Nano

	// Configure output writer
	writer := cfg.Output
	if writer == nil {
		writer = os.Stdout
	}

	// Use logfmt format (key=value)
	consoleWriter := NewLogfmtWriter(writer)

	// Create logger
	loggerContext := zerolog.New(consoleWriter).With().Timestamp()

	// Add caller if requested
	if cfg.WithCaller {
		loggerContext = loggerContext.Caller()
	}

	// Build the logger
	logger := loggerContext.Logger()

	return &Logger{
		zl: logger,
	}
}

// With returns a new logger with the provided key/value pairs
func (l *Logger) With(key string, value interface{}) *Logger {
	return &Logger{zl: l.zl.With().Interface(key, value).Logger()}
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, keyVals ...interface{}) {
	event := l.zl.Debug()
	addFields(event, keyVals...)
	event.Msg(msg)
}

// Info logs an info message
func (l *Logger) Info(msg string, keyVals ...interface{}) {
	event := l.zl.Info()
	addFields(event, keyVals...)
	event.Msg(msg)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, keyVals ...interface{}) {
	event := l.zl.Warn()
	addFields(event, keyVals...)
	event.Msg(msg)
}

// Error logs an error message
func (l *Logger) Error(msg string, err error, keyVals ...interface{}) {
	event := l.zl.Error()
	if err != nil {
		event = event.Err(err)
	}
	addFields(event, keyVals...)
	event.Msg(msg)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string, err error, keyVals ...interface{}) {
	event := l.zl.Fatal()
	if err != nil {
		event = event.Err(err)
	}
	addFields(event, keyVals...)
	event.Msg(msg)
}

// addFields adds key value pairs to the event
func addFields(event *zerolog.Event, keyVals ...interface{}) {
	if len(keyVals)%2 != 0 {
		event.Interface("UNPAIRED_KEY_VALUE", keyVals)
		return
	}

	for i := 0; i < len(keyVals); i += 2 {
		key, ok := keyVals[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyVals[i])
		}
		event.Interface(key, keyVals[i+1])
	}
}

const (
	// Color codes for terminal output
	Black   = 30
	Red     = 31
	Green   = 32
	Yellow  = 33
	Blue    = 34
	Magenta = 35
	Cyan    = 36
	White   = 37
)

func colorize(key, val string, code int) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m=%s", code, key, val)
}

func extractFilename(filePath string) string {
	parts := strings.Split(filePath, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return filePath
}

func NewLogfmtWriter(writer io.Writer) zerolog.ConsoleWriter {
	return zerolog.ConsoleWriter{
		Out: writer,
		FormatTimestamp: func(i interface{}) string {
			t, _ := time.Parse(time.RFC3339Nano, i.(string))
			return colorize("time", t.Format("2006-01-02T15:04:05.999"), Blue)
		},
		FormatLevel: func(i interface{}) string {
			return colorize("level", i.(string), Blue)
		},
		FormatCaller: func(i interface{}) string {
			return ""
			// return colorize("filename", extractFilename(i.(string)), Cyan)
		},
		FormatMessage: func(i interface{}) string {
			if i == nil || i == "" {
				return ""
			}
			return colorize("msg", i.(string), Green)
		},
		FormatFieldName: func(i interface{}) string {
			return colorize(i.(string), "", Cyan)
		},
		FormatFieldValue: func(i interface{}) string {
			switch v := i.(type) {
			case string:
				return v
			case nil:
				return "null"
			default:
				return fmt.Sprintf("%v", v)
			}
		},
	}
}
