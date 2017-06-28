package log

import (
	"github.com/ventu-io/slf"
)

var log slf.Logger = &slf.Noop{}

func SetLog(l slf.Logger) {
	log = l
}

func Log(level slf.Level, msg string) slf.Tracer {
	return log.Log(level, msg)
}

// Debug logs the string with the corresponding level.
func Debug(msg string) slf.Tracer {
	return log.Debug(msg)
}

// Debugf formats and logs the string with the corresponding level.
func Debugf(msg string, args ...interface{}) slf.Tracer {
	return log.Debugf(msg, args...)
}

// Info logs the string with the corresponding level.
func Info(msg string) slf.Tracer {
	return log.Info(msg)
}

// Infof formats and logs the string with the corresponding level.
func Infof(msg string, args ...interface{}) slf.Tracer {
	return log.Infof(msg, args...)
}

// Warn logs the string with the corresponding level.
func Warn(msg string) slf.Tracer {
	return log.Warn(msg)
}

// Warnf formats and logs the string with the corresponding level.
func Warnf(msg string, args ...interface{}) slf.Tracer {
	return log.Warnf(msg, args...)
}

// Error logs the string with the corresponding level.
func Error(msg string) slf.Tracer {
	return log.Error(msg)
}

// Errorf formats and logs the string with the corresponding level.
func Errorf(msg string, args ...interface{}) slf.Tracer {
	return log.Errorf(msg, args...)
}

// Panic logs the string with the corresponding level and panics.
func Panic(msg string) {
	log.Panic(msg)
}

// Panicf formats and logs the string with the corresponding level and panics.
func Panicf(msg string, args ...interface{}) {
	log.Panicf(msg, args...)
}

// Fatal logs the string with the corresponding level and then calls os.Exit(1).
func Fatal(msg string) {
	log.Fatal(msg)
}

// Fatalf formats and logs the string with the corresponding level and then calls os.Exit(1).
func Fatalf(msg string, args ...interface{}) {
	log.Fatalf(msg, args...)
}

func WithContext(context string) slf.StructuredLogger {
	return slf.WithContext(context)
}
