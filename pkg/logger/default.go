package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
)

type SimpleLogger struct {
	writer *log.Logger
}

func (l *SimpleLogger) Info(msg string, keysAndValues ...any) {
	l.output("[INFO] ", msg, keysAndValues...)
}

func (l *SimpleLogger) Warn(msg string, keysAndValues ...any) {
	l.output("[WARN] ", msg, keysAndValues...)
}

func (l *SimpleLogger) Error(msg string, keysAndValues ...any) {
	l.output("[ERROR] ", msg, keysAndValues...)
}

func (l *SimpleLogger) Debug(msg string, keysAndValues ...any) {
	l.output("[DEBUG] ", msg, keysAndValues...)
}

func (l *SimpleLogger) Fatal(msg string, keysAndValues ...any) {
	l.output("[FATAL] ", msg, keysAndValues...)
	os.Exit(1)
}

func (l *SimpleLogger) output(prefix string, msg string, keysAndValues ...any) {
	var b strings.Builder
	b.WriteString(msg)

	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			b.WriteString(fmt.Sprintf(" %v=%v", keysAndValues[i], keysAndValues[i+1]))
		} else {
			b.WriteString(fmt.Sprintf(" %v=<missing>", keysAndValues[i]))
		}
	}

	l.writer.SetPrefix(prefix)
	l.writer.Output(3, b.String())
}
