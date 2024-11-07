package logger

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/charmbracelet/log"
)

var stdoutChannel chan string
var stderrChannel chan string
var debugChannel chan string
var traceChannel chan string
var profileChannel chan string

var enableInfo = false
var enableTracing = false
var enableProfiling = false

var mutraceSubsystems sync.Mutex
var traceSubsystems map[string]bool

var stdoutLogger *log.Logger
var stderrLogger *log.Logger
var debugLogger *log.Logger
var traceLogger *log.Logger
var profileLogger *log.Logger

func init() {
	stdoutLogger = log.New(os.Stdout)
	stderrLogger = log.NewWithOptions(os.Stdout, log.Options{
		Prefix: "warn",
	})
	debugLogger = log.NewWithOptions(os.Stdout, log.Options{
		Prefix: "debug",
	})
	traceLogger = log.NewWithOptions(os.Stdout, log.Options{
		Prefix: "trace",
	})
	profileLogger = log.NewWithOptions(os.Stdout, log.Options{
		Prefix: "profile",
	})
}

func Printf(format string, args ...interface{}) {
	stdoutChannel <- fmt.Sprintf(format, args...)
}

func Info(format string, args ...interface{}) {
	if enableInfo {
		stdoutChannel <- fmt.Sprintf(format, args...)
	}
}

func Warn(format string, args ...interface{}) {
	stderrChannel <- fmt.Sprintf(format, args...)
}

func Error(format string, args ...interface{}) {
	stderrChannel <- fmt.Sprintf(format, args...)
}

func Debug(format string, args ...interface{}) {
	debugChannel <- fmt.Sprintf(format, args...)
}

func Trace(subsystem string, format string, args ...interface{}) {
	if enableTracing {
		mutraceSubsystems.Lock()
		_, exists := traceSubsystems[subsystem]
		if !exists {
			_, exists = traceSubsystems["all"]
		}
		mutraceSubsystems.Unlock()
		if exists {
			traceChannel <- fmt.Sprintf(subsystem+": "+format, args...)
		}
	}
}

func Profile(format string, args ...interface{}) {
	if enableProfiling {
		profileChannel <- fmt.Sprintf(format, args...)
	}
}

func EnableInfo() {
	enableInfo = true
}
func EnableTrace(traces string) {
	enableTracing = true
	traceSubsystems = make(map[string]bool)
	for _, subsystem := range strings.Split(traces, ",") {
		traceSubsystems[subsystem] = true
	}
}

func EnableProfiling() {
	enableProfiling = true
}

func Start() func() {
	stdoutChannel = make(chan string)
	stderrChannel = make(chan string)
	debugChannel = make(chan string)
	traceChannel = make(chan string)
	profileChannel = make(chan string)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for msg := range stdoutChannel {
			stdoutLogger.Print(msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for msg := range stderrChannel {
			stderrLogger.Print(msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for msg := range debugChannel {
			debugLogger.Print(msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for msg := range traceChannel {
			traceLogger.Print(msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for msg := range profileChannel {
			profileLogger.Print(msg)
		}
		wg.Done()
	}()

	return func() {
		close(stdoutChannel)
		close(stderrChannel)
		close(debugChannel)
		close(traceChannel)
		close(profileChannel)
		wg.Wait()
	}
}
