package dynamodbcopy

// Logger defines the logging interface used by the command
type Logger interface {
	Printf(format string, msg ...interface{})
}

type debugLogger struct {
	Logger
	debug bool
}

// NewDebugLogger creates a wrapper around the argument logger to only log when debug flag is true
func NewDebugLogger(logger Logger, debug bool) Logger {
	return debugLogger{
		Logger: logger,
		debug:  debug,
	}
}

// Printf wrapper around the receiver logger. Provides formatted printing according to a format specifier
func (l debugLogger) Printf(format string, msg ...interface{}) {
	if !l.debug {
		return
	}

	l.Logger.Printf(format, msg...)
}
