package profiler

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

// Helper function to capture stderr output
func captureStdout(t *testing.T, f func()) string {
	// Backup original os.Stderr
	origStdout := os.Stdout

	// Create a pipe to capture stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}

	// Replace os.Stderr with the write end of the pipe
	os.Stdout = w

	// Ensure that os.Stderr is restored after the test
	defer func() {
		os.Stdout = origStdout
	}()

	// Buffer to store the captured output
	var buf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)

	// Start a goroutine to read from the pipe
	go func() {
		defer wg.Done()
		_, _ = io.Copy(&buf, r)
	}()

	// Execute the function that generates stderr output
	f()

	// Close the write end of the pipe to signal EOF
	w.Close()

	// Wait for the goroutine to finish reading
	wg.Wait()

	return buf.String()
}

// Helper function to reset the profiler singleton
func resetProfiler() {
	profilerSingleton.muProfiler.Lock()
	defer profilerSingleton.muProfiler.Unlock()

	profilerSingleton.events = make(map[string]bool)
	profilerSingleton.eventDurations = make(map[string]time.Duration)
	profilerSingleton.eventDurationsMin = make(map[string]time.Duration)
	profilerSingleton.eventDurationsMax = make(map[string]time.Duration)
	profilerSingleton.eventCounts = make(map[string]uint64)
}

// TestRecordEvent tests the RecordEvent function
func TestRecordEvent(t *testing.T) {
	resetProfiler()

	// Define test cases
	testCases := []struct {
		event    string
		duration time.Duration
	}{
		{"event1", 100 * time.Millisecond},
		{"event1", 200 * time.Millisecond},
		{"event2", 150 * time.Millisecond},
		{"event1", 50 * time.Millisecond},
		{"event2", 300 * time.Millisecond},
	}

	// Record events
	for _, tc := range testCases {
		RecordEvent(tc.event, tc.duration)
	}

	// Acquire lock to safely access profiler data
	profilerSingleton.muProfiler.Lock()
	defer profilerSingleton.muProfiler.Unlock()

	// Validate event1
	if count, ok := profilerSingleton.eventCounts["event1"]; !ok || count != 3 {
		t.Errorf("Expected event1 count to be 3, got %d", count)
	}

	totalDuration1 := profilerSingleton.eventDurations["event1"]
	if totalDuration1 != 350*time.Millisecond {
		t.Errorf("Expected event1 total duration to be 350ms, got %v", totalDuration1)
	}

	minDuration1 := profilerSingleton.eventDurationsMin["event1"]
	if minDuration1 != 50*time.Millisecond {
		t.Errorf("Expected event1 min duration to be 50ms, got %v", minDuration1)
	}

	maxDuration1 := profilerSingleton.eventDurationsMax["event1"]
	if maxDuration1 != 200*time.Millisecond {
		t.Errorf("Expected event1 max duration to be 200ms, got %v", maxDuration1)
	}

	// Validate event2
	if count, ok := profilerSingleton.eventCounts["event2"]; !ok || count != 2 {
		t.Errorf("Expected event2 count to be 2, got %d", count)
	}

	totalDuration2 := profilerSingleton.eventDurations["event2"]
	if totalDuration2 != 450*time.Millisecond {
		t.Errorf("Expected event2 total duration to be 450ms, got %v", totalDuration2)
	}

	minDuration2 := profilerSingleton.eventDurationsMin["event2"]
	if minDuration2 != 150*time.Millisecond {
		t.Errorf("Expected event2 min duration to be 150ms, got %v", minDuration2)
	}

	maxDuration2 := profilerSingleton.eventDurationsMax["event2"]
	if maxDuration2 != 300*time.Millisecond {
		t.Errorf("Expected event2 max duration to be 300ms, got %v", maxDuration2)
	}
}
