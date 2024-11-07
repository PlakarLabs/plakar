package events

import (
	"time"
)

type Event interface {
	Timestamp() time.Time
}

/**/
type SnapshotCheckStart struct {
	ts time.Time
}

func SnapshotCheckStartEvent() Event {
	return SnapshotCheckStart{ts: time.Now()}
}
func (e SnapshotCheckStart) Timestamp() time.Time {
	return e.ts
}

/**/
type SnapshotCheckError struct {
	ts      time.Time
	message string
}

func SnapshotCheckErrorEvent(message string) Event {
	return SnapshotCheckError{ts: time.Now(), message: message}
}
func (e SnapshotCheckError) Timestamp() time.Time {
	return e.ts
}
func (e SnapshotCheckError) Message() string {
	return e.message
}

/**/
type SnapshotCheckPathname struct {
	ts time.Time
}

func SnapshotCheckPathnameEvent() Event {
	return SnapshotCheckPathname{ts: time.Now()}
}
func (e SnapshotCheckPathname) Timestamp() time.Time {
	return e.ts
}

/**/
type SnapshotCheckDirectory struct {
	ts time.Time
}

func SnapshotCheckDirectoryEvent() Event {
	return SnapshotCheckDirectory{ts: time.Now()}
}
func (e SnapshotCheckDirectory) Timestamp() time.Time {
	return e.ts
}

/**/
type SnapshotCheckFile struct {
	ts time.Time
}

func SnapshotCheckFileEvent() Event {
	return SnapshotCheckFile{ts: time.Now()}
}
func (e SnapshotCheckFile) Timestamp() time.Time {
	return e.ts
}

/**/
type SnapshotCheckDone struct {
	ts time.Time
}

func SnapshotCheckDoneEvent() Event {
	return SnapshotCheckDone{ts: time.Now()}
}
func (e SnapshotCheckDone) Timestamp() time.Time {
	return e.ts
}
