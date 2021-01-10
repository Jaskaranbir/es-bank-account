package logger

// Logger provides interface for logging.
type Logger interface {
	Trace(s string)
	Tracef(s string, v ...interface{})

	Debug(s string)
	Debugf(s string, v ...interface{})

	Info(s string)
	Infof(s string, v ...interface{})

	Warn(s string)
	Warnf(s string, v ...interface{})

	Error(s string)
	Errorf(s string, v ...interface{})
}
