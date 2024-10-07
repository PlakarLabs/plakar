package locking

import (
	"strings"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	hostname := "localhost"
	username := "testuser"
	machineID := "machine123"
	processID := 4567
	exclusive := true

	lock := New(hostname, username, machineID, processID, exclusive)

	if lock.Hostname != hostname {
		t.Errorf("Expected Hostname %s, got %s", hostname, lock.Hostname)
	}
	if lock.Username != username {
		t.Errorf("Expected Username %s, got %s", username, lock.Username)
	}
	if lock.MachineID != machineID {
		t.Errorf("Expected MachineID %s, got %s", machineID, lock.MachineID)
	}
	if lock.ProcessID != processID {
		t.Errorf("Expected ProcessID %d, got %d", processID, lock.ProcessID)
	}
	if lock.Exclusive != exclusive {
		t.Errorf("Expected Exclusive %v, got %v", exclusive, lock.Exclusive)
	}

	// Check if Timestamp is recent (within the last second)
	if time.Since(lock.Timestamp) > time.Second {
		t.Errorf("Expected Timestamp to be recent, got %v", lock.Timestamp)
	}
}

func TestSerializeAndDeserialize(t *testing.T) {
	hostname := "localhost"
	username := "testuser"
	machineID := "machine123"
	processID := 4567
	exclusive := true

	originalLock := New(hostname, username, machineID, processID, exclusive)

	// Serialize the lock
	serialized, err := originalLock.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize the lock
	deserializedLock, err := NewFromBytes(serialized)
	if err != nil {
		t.Fatalf("NewFromBytes failed: %v", err)
	}

	// Compare original and deserialized locks
	if deserializedLock.Hostname != originalLock.Hostname {
		t.Errorf("Hostname mismatch: expected %s, got %s", originalLock.Hostname, deserializedLock.Hostname)
	}
	if deserializedLock.Username != originalLock.Username {
		t.Errorf("Username mismatch: expected %s, got %s", originalLock.Username, deserializedLock.Username)
	}
	if deserializedLock.MachineID != originalLock.MachineID {
		t.Errorf("MachineID mismatch: expected %s, got %s", originalLock.MachineID, deserializedLock.MachineID)
	}
	if deserializedLock.ProcessID != originalLock.ProcessID {
		t.Errorf("ProcessID mismatch: expected %d, got %d", originalLock.ProcessID, deserializedLock.ProcessID)
	}
	if deserializedLock.Exclusive != originalLock.Exclusive {
		t.Errorf("Exclusive mismatch: expected %v, got %v", originalLock.Exclusive, deserializedLock.Exclusive)
	}

	// Timestamp should be the same
	if !deserializedLock.Timestamp.Equal(originalLock.Timestamp) {
		t.Errorf("Timestamp mismatch: expected %v, got %v", originalLock.Timestamp, deserializedLock.Timestamp)
	}
}

func TestNewFromBytesError(t *testing.T) {
	// Create invalid serialized data
	invalidData := []byte{0x00, 0x01, 0x02}

	// Attempt to deserialize
	_, err := NewFromBytes(invalidData)
	if err == nil {
		t.Fatalf("Expected error when deserializing invalid data, got nil")
	}

	// Optionally, check the error message
	if !strings.Contains(err.Error(), "msgpack") {
		t.Errorf("Expected msgpack error, got %v", err)
	}
}

func TestExpired(t *testing.T) {
	hostname := "localhost"
	username := "testuser"
	machineID := "machine123"
	processID := 4567
	exclusive := true

	lock := New(hostname, username, machineID, processID, exclusive)

	// Test with a TTL that has not expired
	ttl := 1 * time.Hour
	if lock.Expired(ttl) {
		t.Errorf("Lock should not be expired with TTL %v", ttl)
	}

	// Test with a TTL that has expired by manipulating the Timestamp
	lock.Timestamp = time.Now().Add(-2 * time.Hour)
	if !lock.Expired(ttl) {
		t.Errorf("Lock should be expired with TTL %v", ttl)
	}
}
