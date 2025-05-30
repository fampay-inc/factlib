package utils

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ctxKey struct{}

// StandardLogger enforces specific log message formats.
type StandardLogger struct {
	*zap.Logger
}

func (l *StandardLogger) Infof(format string, args ...any) {
	l.Info(fmt.Sprintf(format, args...))
}

func (l *StandardLogger) Debugf(format string, args ...any) {
	l.Debug(fmt.Sprintf(format, args...))
}

func (l *StandardLogger) Errorf(format string, args ...any) {
	l.Error(fmt.Sprintf(format, args...))
}

func (l *StandardLogger) Warnf(format string, args ...any) {
	l.Warn(fmt.Sprintf(format, args...))
}

func (l *StandardLogger) Fatalf(format string, args ...any) {
	l.Fatal(fmt.Sprintf(format, args...))
}

func (l *StandardLogger) Panicf(format string, args ...any) {
	l.Panic(fmt.Sprintf(format, args...))
}

func (l *StandardLogger) Desugar() *zap.Logger {
	return l.Logger
}

func (l *StandardLogger) Debug(msg string, keysAndValues ...any) {
	fields := convertToZapFields(keysAndValues)
	l.Logger.Debug(msg, fields...)
}

func (l *StandardLogger) Info(msg string, keysAndValues ...any) {
	fields := convertToZapFields(keysAndValues)
	l.Logger.Info(msg, fields...)
}

func (l *StandardLogger) Warn(msg string, keysAndValues ...any) {
	fields := convertToZapFields(keysAndValues)
	l.Logger.Warn(msg, fields...)
}

func (l *StandardLogger) Error(msg string, keysAndValues ...any) {
	fields := convertToZapFields(keysAndValues)
	l.Logger.Error(msg, fields...)
}

func (l *StandardLogger) Fatal(msg string, keysAndValues ...any) {
	fields := convertToZapFields(keysAndValues)
	l.Logger.Fatal(msg, fields...)
}

// IntegerLevelEncoder returns custom encoder for level field.
func IntegerLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendInt8((int8(l) + 3) * 10)
}

var AppLogger *StandardLogger = NewLogger()

// NewLogger creates a new application logger.
func NewLogger() *StandardLogger {
	var cfg zap.Config
	outputLevel := zap.InfoLevel
	levelEnv := os.Getenv("LOG_LEVEL")
	if levelEnv != "" {
		levelFromEnv, err := zapcore.ParseLevel(levelEnv)
		if err != nil {
			log.Println(fmt.Errorf("invalid level, defaulting to INFO: %w", err))
		} else {
			outputLevel = levelFromEnv
		}
	}

	var DgnEnv = os.Getenv("DGN")
	if DgnEnv != "local" {
		cfg = zap.NewProductionConfig()
		cfg.OutputPaths = []string{"stdout"}
		cfg.ErrorOutputPaths = []string{"stdout"}
		cfg.InitialFields = map[string]any{"name": "fam.service." + GetConfig().Name, "src_service": GetConfig().SrcSvc}
		cfg.EncoderConfig.EncodeLevel = IntegerLevelEncoder
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		cfg.EncoderConfig.TimeKey = "time"
		cfg.Level = zap.NewAtomicLevelAt(outputLevel)
	} else {
		cfg = zap.NewDevelopmentConfig()
	}

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	return &StandardLogger{Logger: logger}
}

// GetAppLogger returns the global application logger.
func GetAppLogger() *StandardLogger {
	return AppLogger
}

// GetChildLogger creates a child logger with contextual fields.
func GetChildLogger(parent *StandardLogger, childContext map[string]string) *StandardLogger {
	zapFields := make([]zap.Field, 0, len(childContext))
	for k, v := range childContext {
		zapFields = append(zapFields, zap.String(k, v))
	}
	return &StandardLogger{Logger: parent.With(zapFields...)}
}

// GetLogger returns the logger from context or the default app logger.
func GetLogger(ctx context.Context) *StandardLogger {
	if l, ok := ctx.Value(ctxKey{}).(*StandardLogger); ok {
		return l
	} else if l := AppLogger; l != nil {
		return l
	}
	return &StandardLogger{Logger: zap.NewNop()}
}

// LoggerWithCtx returns a new context with the given logger attached.
func LoggerWithCtx(ctx context.Context, l *StandardLogger) context.Context {
	if lp, ok := ctx.Value(ctxKey{}).(*StandardLogger); ok {
		if lp == l {
			return ctx
		}
	}
	return context.WithValue(ctx, ctxKey{}, l)
}

func convertToZapFields(keysAndValues []any) []zap.Field {
	var fields []zap.Field
	length := len(keysAndValues)
	for i := 0; i < length; i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keysAndValues[i])
		}
		var val any
		if i+1 < length {
			val = keysAndValues[i+1]
		} else {
			val = "<missing>"
		}
		fields = append(fields, zap.Any(key, val))
	}
	return fields
}
