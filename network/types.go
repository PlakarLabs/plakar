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

type ReqStorePutMetadata struct {
	IndexID uuid.UUID
	Data    []byte
}

type ResStorePutMetadata struct {
	Err error
}

type ReqStorePutBlob struct {
	Checksum [32]byte
	Data     []byte
}

type ResStorePutBlob struct {
	Err error
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

type ReqGetIndexes struct {
}

type ResGetIndexes struct {
	Indexes []uuid.UUID
	Err     error
}

type ReqGetMetadata struct {
	Uuid uuid.UUID
}

type ResGetMetadata struct {
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

type ReqGetObject struct {
	Checksum [32]byte
}

type ResGetObject struct {
	Data []byte
	Err  error
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

type ReqPurge struct {
	Uuid uuid.UUID
}

type ResPurge struct {
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

type ReqPutObject struct {
	Transaction uuid.UUID
	Checksum    [32]byte
}

type ResPutObject struct {
	Err error
}

type ReqPutMetadata struct {
	Transaction uuid.UUID
	Data        []byte
}

type ResPutMetadata struct {
	Err error
}

type ReqCommit struct {
	Transaction uuid.UUID
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

	gob.Register(ReqGetIndexes{})
	gob.Register(ResGetIndexes{})

	gob.Register(ReqStorePutMetadata{})
	gob.Register(ResStorePutMetadata{})

	gob.Register(ReqStorePutBlob{})
	gob.Register(ResStorePutBlob{})

	gob.Register(ReqGetChunks{})
	gob.Register(ResGetChunks{})

	gob.Register(ReqGetObjects{})
	gob.Register(ResGetObjects{})

	gob.Register(ReqGetMetadata{})
	gob.Register(ResGetMetadata{})

	gob.Register(ReqGetBlob{})
	gob.Register(ResGetBlob{})

	gob.Register(ReqGetObject{})
	gob.Register(ResGetObject{})

	gob.Register(ReqGetChunk{})
	gob.Register(ResGetChunk{})

	gob.Register(ReqCheckObject{})
	gob.Register(ResCheckObject{})

	gob.Register(ReqCheckChunk{})
	gob.Register(ResCheckChunk{})

	gob.Register(ReqPurge{})
	gob.Register(ResPurge{})

	gob.Register(ReqClose{})
	gob.Register(ResClose{})

	gob.Register(ReqTransaction{})
	gob.Register(ResTransaction{})

	gob.Register(ReqPutChunk{})
	gob.Register(ResPutChunk{})

	gob.Register(ReqPutObject{})
	gob.Register(ResPutObject{})

	gob.Register(ReqPutMetadata{})
	gob.Register(ResPutMetadata{})

	gob.Register(ReqCommit{})
	gob.Register(ResCommit{})
}
