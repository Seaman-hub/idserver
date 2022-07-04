// Package pools provides general purpose pool implementations.
package pools

import (
	"log"
)

// Logger wraps the Log method for logging.
type Logger interface {
	// Log logs the given arguments.
	Log(v ...interface{})
}

// StdLogger implements the Logger interface.
type StdLogger struct{}

// NewStdLogger creates a new instance of StdLogger.
func NewStdLogger() *StdLogger {
	return &StdLogger{}
}

// Log delegates the logging to the std logger.
func (l *StdLogger) Log(v ...interface{}) {
	log.Println(v...)
}
