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

type ReqStorePutSnapshot struct {
	IndexID uuid.UUID
	Data    []byte
}

type ResStorePutSnapshot struct {
	Err error
}

type ReqStorePutBlob struct {
	Checksum [32]byte
	Data     []byte
}

type ResStorePutBlob struct {
	Err error
}

type ReqGetBlobs struct {
}

type ResGetBlobs struct {
	Chunks [][32]byte
	Err    error
}

type ReqGetChunks struct {
}

type ResGetChunks struct {
	Chunks [][32]byte
	Err    error
}

type ReqGetObjects struct {
}

type ResGetObjects struct {
	Objects [][32]byte
	Err     error
}

type ReqGetSnapshots struct {
}

type ResGetSnapshots struct {
	Snapshots []uuid.UUID
	Err       error
}

type ReqGetSnapshot struct {
	Uuid uuid.UUID
}

type ResGetSnapshot struct {
	Data []byte
	Err  error
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

type ReqGetChunk struct {
	Checksum [32]byte
}

type ResGetChunk struct {
	Data []byte
	Err  error
}

type ReqCheckObject struct {
	Checksum [32]byte
}

type ResCheckObject struct {
	Exists bool
	Err    error
}

type ReqCheckChunk struct {
	Checksum [32]byte
}

type ResCheckChunk struct {
	Exists bool
	Err    error
}

type ReqDeleteSnapshot struct {
	Uuid uuid.UUID
}

type ResDeleteSnapshot struct {
	Err error
}

type ReqClose struct {
	Uuid string
}

type ResClose struct {
	Err error
}

type ReqTransaction struct {
	Uuid uuid.UUID
}

type ResTransaction struct {
	Uuid uuid.UUID
	Err  error
}

type ReqPutChunk struct {
	Transaction uuid.UUID
	Checksum    [32]byte
	Data        []byte
}

type ResPutChunk struct {
	NBytes int
	Err    error
}

type ReqDeleteChunk struct {
	Checksum [32]byte
	Data     []byte
}
type ResDeleteChunk struct {
	Err error
}

type ReqPutObject struct {
	Transaction uuid.UUID
	Checksum    [32]byte
	Data        []byte
}

type ResPutObject struct {
	Err error
}

type ReqDeleteObject struct {
	Checksum [32]byte
	Data     []byte
}
type ResDeleteObject struct {
	Err error
}

/*
type ReqPutMetadata struct {
	Transaction uuid.UUID
	Data        []byte
}

type ResPutMetadata struct {
	Err error
}
*/

type ReqCommit struct {
	Transaction uuid.UUID
	Data        []byte
}

type ResCommit struct {
	Err error
}

func ProtocolRegister() {
	gob.Register(Request{})

	gob.Register(ReqCreate{})
	gob.Register(ResCreate{})

	gob.Register(ReqOpen{})
	gob.Register(ResOpen{})

	gob.Register(ReqGetSnapshots{})
	gob.Register(ResGetSnapshots{})

	gob.Register(ReqStorePutSnapshot{})
	gob.Register(ResStorePutSnapshot{})

	gob.Register(ReqStorePutBlob{})
	gob.Register(ResStorePutBlob{})

	gob.Register(ReqGetChunks{})
	gob.Register(ResGetChunks{})

	gob.Register(ReqGetObjects{})
	gob.Register(ResGetObjects{})

	gob.Register(ReqGetSnapshot{})
	gob.Register(ResGetSnapshot{})

	gob.Register(ReqGetBlobs{})
	gob.Register(ResGetBlobs{})

	gob.Register(ReqGetBlob{})
	gob.Register(ResGetBlob{})

	gob.Register(ReqDeleteBlob{})
	gob.Register(ResDeleteBlob{})

	gob.Register(ReqGetChunk{})
	gob.Register(ResGetChunk{})

	gob.Register(ReqCheckObject{})
	gob.Register(ResCheckObject{})

	gob.Register(ReqCheckChunk{})
	gob.Register(ResCheckChunk{})

	gob.Register(ReqDeleteSnapshot{})
	gob.Register(ResDeleteSnapshot{})

	gob.Register(ReqClose{})
	gob.Register(ResClose{})

	gob.Register(ReqTransaction{})
	gob.Register(ResTransaction{})

	gob.Register(ReqPutChunk{})
	gob.Register(ResPutChunk{})

	gob.Register(ReqDeleteChunk{})
	gob.Register(ResDeleteChunk{})

	gob.Register(ReqPutObject{})
	gob.Register(ResPutObject{})

	gob.Register(ReqCommit{})
	gob.Register(ResCommit{})
}
