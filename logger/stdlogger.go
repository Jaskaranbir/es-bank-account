package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
)

var logLevelMap = map[string]int{
	"trace": 0,
	"debug": 1,
	"info":  2,
	"warn":  3,
	"error": 4,
}

// StdLogger is Logger backed by "log" package from std-lib.
// Supported log-levels are:
// - trace
// - debug
// - info
// - warn
// - error
// A "prefix" can be specified to help identify logs from specific module.
// Use #NewStdLogger to create new instance.
type StdLogger struct {
	prefix   string
	logLevel int
}

// NewStdLogger creates new instance of StdLogger.
// Logging-level can be configured using `LOG_LEVEL`
// env-var. Default level is `info`.
// Logging-level for an individual prefix can be
// specified by setting env-var `<PREFIX>_LOG_LEVEL`.
func NewStdLogger(prefix string) *StdLogger {
	logLevelStr := os.Getenv(strings.ToUpper(prefix) + "_LOG_LEVEL")
	if logLevelStr == "" {
		logLevelStr = os.Getenv("LOG_LEVEL")
	}
	logLevel, ok := logLevelMap[strings.ToLower(logLevelStr)]
	if !ok {
		logLevel = logLevelMap["info"]
	}

	return &StdLogger{
		prefix:   prefix,
		logLevel: logLevel,
	}
}

func (l *StdLogger) log(level string, s string, v ...interface{}) {
	if logLevelMap[level] < l.logLevel {
		return
	}

	log.Printf(
		"[%s]: [%s]: %s",
		strings.ToUpper(level),
		l.prefix,
		fmt.Sprintf(s, v...),
	)
}

// Trace logs trace-level logs.
func (l *StdLogger) Trace(s string) {
	l.log("trace", s)
}

// Tracef logs trace-level logs after formatting according to a format specifier.
func (l *StdLogger) Tracef(s string, v ...interface{}) {
	l.log("trace", s, v...)
}

// Debug logs debug-level logs.
func (l *StdLogger) Debug(s string) {
	l.log("debug", s)
}

// Debugf logs debug-level logs after formatting according to a format specifier.
func (l *StdLogger) Debugf(s string, v ...interface{}) {
	l.log("debug", s, v...)
}

// Info logs info-level logs.
func (l *StdLogger) Info(s string) {
	l.log("info", s)
}

// Infof logs info-level logs after formatting according to a format specifier.
func (l *StdLogger) Infof(s string, v ...interface{}) {
	l.log("info", s, v...)
}

// Warn logs warn-level logs.
func (l *StdLogger) Warn(s string) {
	l.log("warn", s)
}

// Warnf logs warn-level logs after formatting according to a format specifier.
func (l *StdLogger) Warnf(s string, v ...interface{}) {
	l.log("warn", s, v...)
}

// Error logs error-level logs.
func (l *StdLogger) Error(s string) {
	l.log("error", s)
}

// Errorf logs error-level logs after formatting according to a format specifier.
func (l *StdLogger) Errorf(s string, v ...interface{}) {
	l.log("error", s, v...)
}
