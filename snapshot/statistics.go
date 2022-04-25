package snapshot

import (
	"time"
)

type Statistics struct {
	Duration    time.Duration
	Chunks      uint64
	Objects     uint64
	Files       uint64
	Directories uint64
	NonRegular  uint64
	Pathnames   uint64

	Kind      map[string]uint64
	Type      map[string]uint64
	Extension map[string]uint64

	PercentKind      map[string]float64
	PercentType      map[string]float64
	PercentExtension map[string]float64
}

func NewStatistics() *Statistics {
	return &Statistics{
		Chunks:      0,
		Objects:     0,
		Files:       0,
		Directories: 0,

		Kind:      make(map[string]uint64),
		Type:      make(map[string]uint64),
		Extension: make(map[string]uint64),

		PercentKind:      make(map[string]float64),
		PercentType:      make(map[string]float64),
		PercentExtension: make(map[string]float64),
	}
}
