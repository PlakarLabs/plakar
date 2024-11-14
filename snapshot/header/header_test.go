package header

import (
	"reflect"
	"testing"
	"time"
)

func TestSortHeaders(t *testing.T) {
	// Define test data
	headers := []Header{
		{CreationTime: time.Now(), Hostname: "server1", FilesCount: 100},
		{CreationTime: time.Now().Add(-24 * time.Hour), Hostname: "server2", FilesCount: 200},
		{CreationTime: time.Now().Add(-48 * time.Hour), Hostname: "server1", FilesCount: 150},
		{CreationTime: time.Now(), Hostname: "server3", FilesCount: 100},
		{CreationTime: time.Now().Add(-24 * time.Hour), Hostname: "server3", FilesCount: 50},
		{CreationTime: time.Now().Add(-72 * time.Hour), Hostname: "server1", FilesCount: 300},
	}

	// Define expected output for sort by CreationTime, Hostname, and FilesCount
	expected1 := []Header{
		headers[5], // CreationTime oldest, Hostname server1, FilesCount 300
		headers[2], // CreationTime second oldest, Hostname server1, FilesCount 150
		headers[1], // CreationTime third oldest, Hostname server2, FilesCount 200
		headers[4], // CreationTime third oldest, Hostname server3, FilesCount 50
		headers[0], // CreationTime most recent, Hostname server1, FilesCount 100
		headers[3], // CreationTime most recent, Hostname server3, FilesCount 100
	}

	// Test case 1: Sort by CreationTime, Hostname, FilesCount
	SortHeaders(headers, []string{"CreationTime", "Hostname", "FilesCount"})
	if !reflect.DeepEqual(headers, expected1) {
		t.Errorf("Test case 1 failed. Expected %v, got %v", expected1, headers)
	}

	// Reset data and define expected output for sort by FilesCount, Hostname, CreationTime
	headers = []Header{
		{CreationTime: time.Now(), Hostname: "server1", FilesCount: 100},
		{CreationTime: time.Now().Add(-24 * time.Hour), Hostname: "server2", FilesCount: 200},
		{CreationTime: time.Now().Add(-48 * time.Hour), Hostname: "server1", FilesCount: 150},
		{CreationTime: time.Now(), Hostname: "server3", FilesCount: 100},
		{CreationTime: time.Now().Add(-24 * time.Hour), Hostname: "server3", FilesCount: 50},
		{CreationTime: time.Now().Add(-72 * time.Hour), Hostname: "server1", FilesCount: 300},
	}

	expected2 := []Header{
		headers[4], // FilesCount lowest, Hostname server3, CreationTime third oldest
		headers[0], // FilesCount 100, Hostname server1, most recent
		headers[3], // FilesCount 100, Hostname server3, most recent
		headers[2], // FilesCount 150, Hostname server1, CreationTime second oldest
		headers[1], // FilesCount 200, Hostname server2, CreationTime third oldest
		headers[5], // FilesCount highest, Hostname server1, CreationTime oldest
	}

	// Test case 2: Sort by FilesCount, Hostname, CreationTime
	SortHeaders(headers, []string{"FilesCount", "Hostname", "CreationTime"})
	if !reflect.DeepEqual(headers, expected2) {
		t.Errorf("Test case 2 failed. Expected %v, got %v", expected2, headers)
	}
}
