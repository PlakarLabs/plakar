package logger

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

var stdoutChannel chan string
var stderrChannel chan string
var debugChannel chan string
var traceChannel chan string

var enableInfo = false
var enableTracing = false
var enableProfiling = false

var mutraceSubsystems sync.Mutex
var traceSubsystems map[string]bool

func Printf(format string, args ...interface{}) {
	stdoutChannel <- fmt.Sprintf(format, args...)
}

func Info(format string, args ...interface{}) {
	if enableInfo {
		stdoutChannel <- fmt.Sprintf(format, args...)
	}
}

func Warn(format string, args ...interface{}) {
	stderrChannel <- fmt.Sprintf("[warning] "+format, args...)
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
			traceChannel <- fmt.Sprintf("[trace]: "+subsystem+": "+format, args...)
		}
	}
}

func Profile(format string, args ...interface{}) {
	if enableProfiling {
		traceChannel <- fmt.Sprintf("[profiling]: "+format, args...)
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for msg := range stdoutChannel {
			fmt.Println(msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for msg := range stderrChannel {
			fmt.Fprintln(os.Stderr, msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for msg := range debugChannel {
			fmt.Println(msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for msg := range traceChannel {
			fmt.Fprintln(os.Stderr, msg)
		}
		wg.Done()
	}()

	return func() {
		close(stdoutChannel)
		close(stderrChannel)
		close(debugChannel)
		close(traceChannel)
		wg.Wait()
	}
}
