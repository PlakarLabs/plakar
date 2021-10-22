package logger

import (
	"fmt"
	"os"
	"sync"
)

var stdoutChannel chan string
var stderrChannel chan string
var debugChannel chan string
var traceChannel chan string

var enableTrace = false

func Info(format string, args ...interface{}) {
	stdoutChannel <- fmt.Sprintf(format, args...)
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

func Trace(format string, args ...interface{}) {
	if enableTrace {
		traceChannel <- fmt.Sprintf(format, args...)
	}
}

func EnableTrace() {
	enableTrace = true
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
			fmt.Println(msg)
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
