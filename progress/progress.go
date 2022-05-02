package progress

import (
	"fmt"

	"github.com/k0kubun/go-ansi"
	"github.com/schollz/progressbar/v3"
)

func NewProgress(name string, description string) chan int64 {
	inputChannel := make(chan int64)
	bar := progressbar.NewOptions(-1,
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(80),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]%s[reset] %s", name, description)),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	go func() {
		for step := range inputChannel {
			bar.Add64(step)
		}
		bar.Finish()
		bar.Close()
		fmt.Println()
	}()
	return inputChannel
}

func NewProgressCount(name string, description string, total int64) chan int64 {
	inputChannel := make(chan int64)
	bar := progressbar.NewOptions(int(total),
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(80),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]%s[reset] %s", name, description)),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	go func() {
		for step := range inputChannel {
			bar.Add64(step)
		}
		bar.Finish()
		bar.Close()
		fmt.Println()
	}()
	return inputChannel
}

func NewProgressBytes(name string, description string, total int64) chan int64 {
	inputChannel := make(chan int64)
	bar := progressbar.NewOptions(int(total),
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(80),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan]%s[reset] %s", name, description)),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	go func() {
		for step := range inputChannel {
			bar.Add64(step)
		}
		bar.Finish()
		bar.Close()
		fmt.Println()
	}()
	return inputChannel
}
