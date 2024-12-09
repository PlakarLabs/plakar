package check

import (
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logging"
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
				logging.Warn("%x: %s %s: missing directory", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.FileMissing:
				logging.Warn("%x: %s %s: missing file", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.ObjectMissing:
				logging.Warn("%x: %s %x: missing object", event.SnapshotID[:4], crossMark, event.Checksum)
			case events.ChunkMissing:
				logging.Warn("%x: %s %x: missing chunk", event.SnapshotID[:4], crossMark, event.Checksum)

			case events.DirectoryCorrupted:
				logging.Warn("%x: %s %s: corrupted directory", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.FileCorrupted:
				logging.Warn("%x: %s %s: corrupted file", event.SnapshotID[:4], crossMark, event.Pathname)
			case events.ObjectCorrupted:
				logging.Warn("%x: %s %x: corrupted object", event.SnapshotID[:4], crossMark, event.Checksum)
			case events.ChunkCorrupted:
				logging.Warn("%x: %s %x: corrupted chunk", event.SnapshotID[:4], crossMark, event.Checksum)

			case events.DirectoryOK:
				if !quiet {
					logging.Info("%x: %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			case events.FileOK:
				if !quiet {
					logging.Info("%x: %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			default:
			}
		}
		done <- struct{}{}
	}()
	return done
}
