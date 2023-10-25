package locking

import (
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"
)

type Lock struct {
	Timestamp time.Time
	Hostname  string
	Username  string
	MachineID string
	ProcessID int
	Exclusive bool
}

func New(hostname string, username string, machineID string, processID int, exclusive bool) *Lock {
	return &Lock{
		Timestamp: time.Now(),
		Hostname:  hostname,
		Username:  username,
		MachineID: machineID,
		ProcessID: processID,
		Exclusive: exclusive,
	}
}

func NewFromBytes(serialized []byte) (*Lock, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.locking.NewFromBytes", time.Since(t0))
		logger.Trace("storage.locking", "NewFromBytes(...): %s", time.Since(t0))
	}()

	var lock Lock
	if err := msgpack.Unmarshal(serialized, &lock); err != nil {
		return nil, err
	}
	return &lock, nil
}

func (lock *Lock) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.locking.Serialize", time.Since(t0))
		logger.Trace("storage.locking", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(lock)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (lock *Lock) Expired(ttl time.Duration) bool {
	return time.Since(lock.Timestamp) > ttl
}
