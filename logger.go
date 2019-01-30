package dynamodbcopy

type Logger interface {
	Printf(format string, msg ...interface{})
}

type debugLogger struct {
	Logger
	debug bool
}

func NewDebugLogger(logger Logger, debug bool) Logger {
	return debugLogger{
		Logger: logger,
		debug:  debug,
	}
}

func (l debugLogger) Printf(format string, msg ...interface{}) {
	if !l.debug {
		return
	}

	l.Logger.Printf(format, msg...)
}
