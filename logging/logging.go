package logging

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/charmbracelet/log"
)

type Logger struct {
	stdoutLogger *log.Logger
	infoLogger   *log.Logger
	warnLogger   *log.Logger
	stderrLogger *log.Logger
	debugLogger  *log.Logger
	traceLogger  *log.Logger

	enableInfo    bool
	enableTracing bool

	muTraceSubsystems sync.Mutex
	traceSubsystems   map[string]bool
}

func NewLogger(stdout io.Writer, stderr io.Writer) *Logger {
	return &Logger{
		stdoutLogger: log.NewWithOptions(stdout, log.Options{}),
		infoLogger: log.NewWithOptions(stdout, log.Options{
			Prefix: "info",
		}),
		warnLogger: log.NewWithOptions(stderr, log.Options{
			Prefix: "warn",
		}),
		stderrLogger: log.NewWithOptions(stderr, log.Options{}),

		debugLogger: log.NewWithOptions(stdout, log.Options{
			Prefix: "debug",
		}),
		traceLogger: log.NewWithOptions(stdout, log.Options{
			Prefix: "trace",
		}),
		traceSubsystems: make(map[string]bool),
	}
}

func (l *Logger) Printf(format string, args ...interface{}) {
	l.stdoutLogger.Print(fmt.Sprintf(format, args...))
	// infoChannel <- fmt.Sprintf(format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.enableInfo {
		l.infoLogger.Print(fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.warnLogger.Print(fmt.Sprintf(format, args...))
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.stderrLogger.Print(fmt.Sprintf(format, args...))
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.debugLogger.Print(fmt.Sprintf(format, args...))
}

func (l *Logger) Trace(subsystem string, format string, args ...interface{}) {
	if l.enableTracing {
		l.muTraceSubsystems.Lock()
		_, exists := l.traceSubsystems[subsystem]
		if !exists {
			_, exists = l.traceSubsystems["all"]
		}
		l.muTraceSubsystems.Unlock()
		if exists {
			l.traceLogger.Print(fmt.Sprintf(subsystem+": "+format, args...))
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
