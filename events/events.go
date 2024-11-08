package events

import (
	"time"
)

type Event interface {
	Timestamp() time.Time
}

/**/
type Start struct {
	ts time.Time
}

func StartEvent() Start {
	return Start{ts: time.Now()}
}
func (e Start) Timestamp() time.Time {
	return e.ts
}

/**/
type Done struct {
	ts time.Time
}

func DoneEvent() Done {
	return Done{ts: time.Now()}
}
func (e Done) Timestamp() time.Time {
	return e.ts
}

/**/
type Warning struct {
	ts      time.Time
	message string
}

func WarningEvent(message string) Warning {
	return Warning{ts: time.Now(), message: message}
}
func (e Warning) Timestamp() time.Time {
	return e.ts
}
func (e Warning) Message() string {
	return e.message
}

/**/
type Error struct {
	ts      time.Time
	message string
}

func ErrorEvent(message string) Error {
	return Error{ts: time.Now(), message: message}
}
func (e Error) Timestamp() time.Time {
	return e.ts
}
func (e Error) Message() string {
	return e.message
}

/**/
type Path struct {
	ts       time.Time
	Pathname string
}

func PathEvent(pathname string) Path {
	return Path{ts: time.Now(), Pathname: pathname}
}
func (e Path) Timestamp() time.Time {
	return e.ts
}

/**/
type Directory struct {
	ts       time.Time
	Pathname string
}

func DirectoryEvent(pathname string) Directory {
	return Directory{ts: time.Now(), Pathname: pathname}
}
func (e Directory) Timestamp() time.Time {
	return e.ts
}

/**/
type File struct {
	ts       time.Time
	Pathname string
}

func FileEvent(pathname string) File {
	return File{ts: time.Now(), Pathname: pathname}
}
func (e File) Timestamp() time.Time {
	return e.ts
}

/**/
type Object struct {
	ts       time.Time
	Checksum [32]byte
}

func ObjectEvent(checksum [32]byte) Object {
	return Object{ts: time.Now(), Checksum: checksum}
}
func (e Object) Timestamp() time.Time {
	return e.ts
}

/**/
type Chunk struct {
	ts       time.Time
	Checksum [32]byte
}

func ChunkEvent(checksum [32]byte) Chunk {
	return Chunk{ts: time.Now(), Checksum: checksum}
}
func (e Chunk) Timestamp() time.Time {
	return e.ts
}

/**/
type DirectoryOK struct {
	ts       time.Time
	Pathname string
}

func DirectoryOKEvent(pathname string) DirectoryOK {
	return DirectoryOK{ts: time.Now(), Pathname: pathname}
}
func (e DirectoryOK) Timestamp() time.Time {
	return e.ts
}

/**/
type DirectoryMissing struct {
	ts       time.Time
	Pathname string
}

func DirectoryMissingEvent(pathname string) DirectoryMissing {
	return DirectoryMissing{ts: time.Now(), Pathname: pathname}
}
func (e DirectoryMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type DirectoryCorrupted struct {
	ts       time.Time
	Pathname string
}

func DirectoryCorruptedEvent(pathname string) DirectoryCorrupted {
	return DirectoryCorrupted{ts: time.Now(), Pathname: pathname}
}
func (e DirectoryCorrupted) Timestamp() time.Time {
	return e.ts
}

/**/
type FileOK struct {
	ts       time.Time
	Pathname string
}

func FileOKEvent(pathname string) FileOK {
	return FileOK{ts: time.Now(), Pathname: pathname}
}
func (e FileOK) Timestamp() time.Time {
	return e.ts
}

/**/
type FileMissing struct {
	ts       time.Time
	Pathname string
}

func FileMissingdEvent(pathname string) FileMissing {
	return FileMissing{ts: time.Now(), Pathname: pathname}
}
func (e FileMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type FileCorrupted struct {
	ts       time.Time
	Pathname string
}

func FileCorruptedEvent(pathname string) FileCorrupted {
	return FileCorrupted{ts: time.Now(), Pathname: pathname}
}
func (e FileCorrupted) Timestamp() time.Time {
	return e.ts
}

/**/
type ObjectOK struct {
	ts       time.Time
	Checksum [32]byte
}

func ObjectOKEvent(checksum [32]byte) ObjectOK {
	return ObjectOK{ts: time.Now(), Checksum: checksum}
}
func (e ObjectOK) Timestamp() time.Time {
	return e.ts
}

/**/
type ObjectMissing struct {
	ts       time.Time
	Checksum [32]byte
}

func ObjectMissingEvent(checksum [32]byte) ObjectMissing {
	return ObjectMissing{ts: time.Now(), Checksum: checksum}
}
func (e ObjectMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type ObjectCorrupted struct {
	ts       time.Time
	Checksum [32]byte
}

func ObjectCorruptedEvent(checksum [32]byte) ObjectCorrupted {
	return ObjectCorrupted{ts: time.Now(), Checksum: checksum}
}
func (e ObjectCorrupted) Timestamp() time.Time {
	return e.ts
}

/**/
type ChunkOK struct {
	ts       time.Time
	Checksum [32]byte
}

func ChunkOKEvent(checksum [32]byte) ObjectOK {
	return ObjectOK{ts: time.Now(), Checksum: checksum}
}
func (e ChunkOK) Timestamp() time.Time {
	return e.ts
}

/**/
type ChunkMissing struct {
	ts       time.Time
	Checksum [32]byte
}

func ChunkMissingEvent(checksum [32]byte) ChunkMissing {
	return ChunkMissing{ts: time.Now(), Checksum: checksum}
}
func (e ChunkMissing) Timestamp() time.Time {
	return e.ts
}

/**/
type ChunkCorrupted struct {
	ts       time.Time
	Checksum [32]byte
}

func ChunkCorruptedEvent(checksum [32]byte) ChunkCorrupted {
	return ChunkCorrupted{ts: time.Now(), Checksum: checksum}
}
func (e ChunkCorrupted) Timestamp() time.Time {
	return e.ts
}
