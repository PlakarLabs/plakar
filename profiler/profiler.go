package profiler

import (
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/logger"
)

type profiler struct {
	muProfiler sync.Mutex

	events            map[string]bool
	eventDurations    map[string]time.Duration
	eventDurationsMin map[string]time.Duration
	eventDurationsMax map[string]time.Duration

	eventCounts map[string]uint64
}

var profilerSingleton *profiler

func init() {
	profilerSingleton = &profiler{
		events:            make(map[string]bool),
		eventDurations:    make(map[string]time.Duration),
		eventDurationsMin: make(map[string]time.Duration),
		eventDurationsMax: make(map[string]time.Duration),
		eventCounts:       make(map[string]uint64),
	}
}

func RecordEvent(event string, duration time.Duration) {
	profilerSingleton.muProfiler.Lock()
	defer profilerSingleton.muProfiler.Unlock()

	if _, exists := profilerSingleton.events[event]; !exists {
		profilerSingleton.events[event] = true
		profilerSingleton.eventDurations[event] = 0
		profilerSingleton.eventDurationsMin[event] = duration
		profilerSingleton.eventDurationsMax[event] = duration
		profilerSingleton.eventCounts[event] = 0
	}

	profilerSingleton.eventDurations[event] += duration
	if duration < profilerSingleton.eventDurationsMin[event] {
		profilerSingleton.eventDurationsMin[event] = duration
	}
	if duration > profilerSingleton.eventDurationsMax[event] {
		profilerSingleton.eventDurationsMax[event] = duration
	}
	profilerSingleton.eventCounts[event] += 1
}

func Display() {
	profilerSingleton.muProfiler.Lock()
	defer profilerSingleton.muProfiler.Unlock()

	for event := range profilerSingleton.events {
		count := profilerSingleton.eventCounts[event]
		duration := profilerSingleton.eventDurations[event]
		durationMin := profilerSingleton.eventDurationsMin[event]
		durationMax := profilerSingleton.eventDurationsMax[event]
		durationAvg := time.Duration(uint64(profilerSingleton.eventDurations[event]) / profilerSingleton.eventCounts[event])
		logger.Profile("%s: calls=%d, min=%s, avg=%s, max=%s, total=%s", event, count, durationMin, durationAvg, durationMax, duration)
	}
}
