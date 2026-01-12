package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// Logger wraps stdlib log.Logger with convenience methods
type Logger struct {
	*log.Logger
	debug bool
}

// New creates a new logger with timestamp formatting (matching sdhm style)
func New(development bool) (*Logger, error) {
	l := log.New(os.Stdout, "", log.LstdFlags)
	return &Logger{Logger: l, debug: development}, nil
}

// Helper methods for structured logging with level prefixes

func (l *Logger) Info(msg string, keysAndValues ...any) {
	l.Printf("INFO: %s%s", msg, formatFields(keysAndValues...))
}

func (l *Logger) Error(msg string, keysAndValues ...any) {
	l.Printf("ERROR: %s%s", msg, formatFields(keysAndValues...))
}

func (l *Logger) Warn(msg string, keysAndValues ...any) {
	l.Printf("WARN: %s%s", msg, formatFields(keysAndValues...))
}

func (l *Logger) Debug(msg string, keysAndValues ...any) {
	if l.debug {
		l.Printf("DEBUG: %s%s", msg, formatFields(keysAndValues...))
	}
}

// Sync is a no-op for stdlib logger (for compatibility with zap)
func (l *Logger) Sync() error {
	return nil
}

// formatFields formats key-value pairs for logging
func formatFields(keysAndValues ...any) string {
	if len(keysAndValues) == 0 {
		return ""
	}

	var result strings.Builder
	result.WriteString(" [")
	for i := 0; i < len(keysAndValues); i += 2 {
		if i > 0 {
			result.WriteString(", ")
		}
		if i+1 < len(keysAndValues) {
			result.WriteString(formatField(keysAndValues[i], keysAndValues[i+1]))
		}
	}
	result.WriteString("]")
	return result.String()
}

func formatField(key, value any) string {
	return fmt.Sprintf("%v=%v", key, value)
}
