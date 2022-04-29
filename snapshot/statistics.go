package snapshot

type Statistics struct {
	Kind      map[string]uint64
	Type      map[string]uint64
	Extension map[string]uint64

	PercentKind      map[string]float64
	PercentType      map[string]float64
	PercentExtension map[string]float64
}

func NewStatistics() *Statistics {
	return &Statistics{
		Kind:      make(map[string]uint64),
		Type:      make(map[string]uint64),
		Extension: make(map[string]uint64),

		PercentKind:      make(map[string]float64),
		PercentType:      make(map[string]float64),
		PercentExtension: make(map[string]float64),
	}
}
