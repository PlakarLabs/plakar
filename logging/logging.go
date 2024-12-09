package logging

import (
	"io"
	"strings"
	"sync"

	"github.com/charmbracelet/log"
)

type Logger struct {
	enableInfo        bool
	enableTracing     bool
	mutraceSubsystems sync.Mutex
	traceSubsystems   map[string]bool
	stdoutLogger      *log.Logger
	stderrLogger      *log.Logger
	infoLogger        *log.Logger
	warnLogger        *log.Logger
	debugLogger       *log.Logger
	traceLogger       *log.Logger
}

func NewLogger(stdout io.Writer, stderr io.Writer) *Logger {
	return &Logger{
		enableInfo:      false,
		enableTracing:   false,
		stdoutLogger:    log.NewWithOptions(stdout, log.Options{}),
		stderrLogger:    log.NewWithOptions(stderr, log.Options{}),
		infoLogger:      log.NewWithOptions(stdout, log.Options{Prefix: "info"}),
		warnLogger:      log.NewWithOptions(stderr, log.Options{Prefix: "warn"}),
		debugLogger:     log.NewWithOptions(stdout, log.Options{Prefix: "debug"}),
		traceLogger:     log.NewWithOptions(stdout, log.Options{Prefix: "trace"}),
		traceSubsystems: make(map[string]bool),
	}
}

func (l *Logger) Printf(format string, args ...interface{}) {
	l.infoLogger.Printf(format, args...)
}

func (l *Logger) Stdout(format string, args ...interface{}) {
	l.stdoutLogger.Printf(format, args...)
}

func (l *Logger) Stderr(format string, args ...interface{}) {
	l.stderrLogger.Printf(format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.enableInfo {
		l.infoLogger.Printf(format, args...)
	}
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.warnLogger.Printf(format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.stderrLogger.Printf(format, args...)
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.debugLogger.Printf(format, args...)
}

func (l *Logger) Trace(subsystem string, format string, args ...interface{}) {
	if l.enableTracing {
		l.mutraceSubsystems.Lock()
		_, exists := l.traceSubsystems[subsystem]
		if !exists {
			_, exists = l.traceSubsystems["all"]
		}
		l.mutraceSubsystems.Unlock()
		if exists {
			l.traceLogger.Printf(subsystem+": "+format, args...)
		}
	}
}

func (l *Logger) EnableInfo() {
	l.enableInfo = true
}

func (l *Logger) EnableTrace(traces string) {
	l.enableTracing = true
	l.traceSubsystems = make(map[string]bool)
	for _, subsystem := range strings.Split(traces, ",") {
		l.traceSubsystems[subsystem] = true
	}
}
