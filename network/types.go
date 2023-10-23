package network

import (
	"encoding/gob"

	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

type Request struct {
	Uuid    uuid.UUID
	Type    string
	Payload interface{}
}

type ReqCreate struct {
	Repository       string
	RepositoryConfig storage.RepositoryConfig
}

type ResCreate struct {
	Err error
}

type ReqOpen struct {
	Repository string
}

type ResOpen struct {
	RepositoryConfig *storage.RepositoryConfig
	Err              error
}

type ReqClose struct {
	Uuid string
}

type ResClose struct {
	Err error
}

type ReqCommit struct {
	IndexID uuid.UUID
	Data    []byte
}

type ResCommit struct {
	Err error
}

// snapshots
type ReqGetSnapshots struct {
}

type ResGetSnapshots struct {
	Snapshots []uuid.UUID
	Err       error
}

type ReqPutSnapshot struct {
	IndexID uuid.UUID
	Data    []byte
}

type ResPutSnapshot struct {
	Err error
}

type ReqGetSnapshot struct {
	IndexID uuid.UUID
}

type ResGetSnapshot struct {
	Data []byte
	Err  error
}

type ReqDeleteSnapshot struct {
	IndexID uuid.UUID
}

type ResDeleteSnapshot struct {
	Err error
}

// snapshots
type ReqGetLocks struct {
}

type ResGetLocks struct {
	Locks []uuid.UUID
	Err   error
}

type ReqPutLock struct {
	IndexID uuid.UUID
	Data    []byte
}

type ResPutLock struct {
	Err error
}

type ReqGetLock struct {
	IndexID uuid.UUID
}

type ResGetLock struct {
	Data []byte
	Err  error
}

type ReqDeleteLock struct {
	IndexID uuid.UUID
}

type ResDeleteLock struct {
	Err error
}

// blobs
type ReqGetBlobs struct {
}

type ResGetBlobs struct {
	Checksums [][32]byte
	Err       error
}

type ReqPutBlob struct {
	Checksum [32]byte
	Data     []byte
}

type ResPutBlob struct {
	Err error
}

type ReqGetBlob struct {
	Checksum [32]byte
}

type ResGetBlob struct {
	Data []byte
	Err  error
}

type ReqDeleteBlob struct {
	Checksum [32]byte
	Data     []byte
}
type ResDeleteBlob struct {
	Err error
}

// indexes
type ReqGetIndexes struct {
}

type ResGetIndexes struct {
	Checksums [][32]byte
	Err       error
}

type ReqPutIndex struct {
	Checksum [32]byte
	Data     []byte
}

type ResPutIndex struct {
	Err error
}

type ReqGetIndex struct {
	Checksum [32]byte
}

type ResGetIndex struct {
	Data []byte
	Err  error
}

type ReqDeleteIndex struct {
	Checksum [32]byte
}
type ResDeleteIndex struct {
	Err error
}

// packfiles
type ReqGetPackfiles struct {
}

type ResGetPackfiles struct {
	Checksums [][32]byte
	Err       error
}

type ReqPutPackfile struct {
	Checksum [32]byte
	Data     []byte
}

type ResPutPackfile struct {
	Err error
}

type ReqGetPackfile struct {
	Checksum [32]byte
}

type ResGetPackfile struct {
	Data []byte
	Err  error
}

type ReqDeletePackfile struct {
	Checksum [32]byte
}
type ResDeletePackfile struct {
	Err error
}

func ProtocolRegister() {
	gob.Register(Request{})

	gob.Register(ReqCreate{})
	gob.Register(ResCreate{})

	gob.Register(ReqOpen{})
	gob.Register(ResOpen{})

	gob.Register(ReqCommit{})
	gob.Register(ResCommit{})

	gob.Register(ReqClose{})
	gob.Register(ResClose{})

	// snapshots
	gob.Register(ReqGetSnapshots{})
	gob.Register(ResGetSnapshots{})

	gob.Register(ReqPutSnapshot{})
	gob.Register(ResPutSnapshot{})

	gob.Register(ReqGetSnapshot{})
	gob.Register(ResGetSnapshot{})

	gob.Register(ReqDeleteSnapshot{})
	gob.Register(ResDeleteSnapshot{})

	// blobs
	gob.Register(ReqGetBlobs{})
	gob.Register(ResGetBlobs{})

	gob.Register(ReqPutBlob{})
	gob.Register(ResPutBlob{})

	gob.Register(ReqGetBlob{})
	gob.Register(ResGetBlob{})

	gob.Register(ReqDeleteBlob{})
	gob.Register(ResDeleteBlob{})

	// indexes
	gob.Register(ReqGetIndexes{})
	gob.Register(ResGetIndexes{})

	gob.Register(ReqPutIndex{})
	gob.Register(ResPutIndex{})

	gob.Register(ReqGetIndex{})
	gob.Register(ResGetIndex{})

	gob.Register(ReqDeleteIndex{})
	gob.Register(ResDeleteIndex{})

	// packfiles
	gob.Register(ReqGetPackfiles{})
	gob.Register(ResGetPackfiles{})

	gob.Register(ReqPutPackfile{})
	gob.Register(ResPutPackfile{})

	gob.Register(ReqGetPackfile{})
	gob.Register(ResGetPackfile{})

	gob.Register(ReqDeletePackfile{})
	gob.Register(ResDeletePackfile{})

}
