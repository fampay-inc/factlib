package logger

import (
	"fmt"
	"io"
	"os"
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
	JSONOutput bool
	WithCaller bool
}

// New creates a new logger with the provided configuration
func New(cfg Config) *Logger {
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(level)
	zerolog.TimeFieldFormat = time.RFC3339Nano

	var output io.Writer = os.Stdout
	if !cfg.JSONOutput {
		output = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}
	}

	var zl zerolog.Logger
	if cfg.WithCaller {
		zl = zerolog.New(output).With().Timestamp().Caller().Logger()
	} else {
		zl = zerolog.New(output).With().Timestamp().Logger()
	}

	return &Logger{zl: zl}
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
