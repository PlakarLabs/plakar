package verify

import (
	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/events"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/charmbracelet/lipgloss"
)

var (
	checkMark = lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00")).SetString("✓")
	crossMark = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")).SetString("✘")
)

func eventsProcessorStdio(ctx *context.Context, quiet bool) chan struct{} {
	done := make(chan struct{})
	go func() {
		for event := range ctx.Events().Listen() {
			switch event := event.(type) {
			case events.DirectoryMissing:
				logger.Warn("%x: %s %s: missing directory", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.FileMissing:
				logger.Warn("%x: %s %s: missing file", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.ObjectMissing:
				logger.Warn("%x: %s %x: missing object", event.SnapshotID[:4], crossMark, event.Checksum)
			case events.ChunkMissing:
				logger.Warn("%x: %s %x: missing chunk", event.SnapshotID[:4], crossMark, event.Checksum)

			case events.DirectoryCorrupted:
				logger.Warn("%x: %s %s: corrupted directory", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.FileCorrupted:
				logger.Warn("%x: %s %s: corrupted file", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.ObjectCorrupted:
				logger.Warn("%x: %s %x: corrupted object", event.SnapshotID[:4], crossMark, event.Checksum)
			case events.ChunkCorrupted:
				logger.Warn("%x: %s %x: corrupted chunk", event.SnapshotID[:4], crossMark, event.Checksum)

			case events.DirectoryOK:
				if !quiet {
					logger.Info("%x: %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			case events.FileOK:
				if !quiet {
					logger.Info("%x: %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			default:
			}
		}
		done <- struct{}{}
	}()
	return done
}
