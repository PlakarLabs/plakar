package logger

import (
	"fmt"
	"os"
	"sync"
)

var stdoutChannel chan string
var stderrChannel chan string
var verboseChannel chan string
var traceChannel chan string

func Stdout(msg ...string) {
	stdoutChannel <- fmt.Sprint(msg)
}

func Stderr(msg ...string) {
	stderrChannel <- fmt.Sprint(msg)
}

func Verbose(msg ...string) {
	verboseChannel <- fmt.Sprint(msg)
}

func Trace(msg ...string) {
	traceChannel <- fmt.Sprint(msg)
}

func Start() func() {
	stdoutChannel = make(chan string)
	stderrChannel = make(chan string)
	verboseChannel = make(chan string)
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
		for msg := range verboseChannel {
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
		close(verboseChannel)
		close(traceChannel)
		wg.Wait()
	}
}
