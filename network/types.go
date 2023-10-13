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
	NBytes int
	Err    error
}

type ReqStorePutIndex struct {
	IndexID uuid.UUID
	Data    []byte
}

type ResStorePutIndex struct {
	NBytes int
	Err    error
}

type ReqStorePutFilesystem struct {
	IndexID uuid.UUID
	Data    []byte
}

type ResStorePutFilesystem struct {
	NBytes int
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

type ReqGetIndex struct {
	Uuid uuid.UUID
}

type ResGetIndex struct {
	Data []byte
	Err  error
}

type ReqGetFilesystem struct {
	Uuid uuid.UUID
}

type ResGetFilesystem struct {
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

type ReqReferenceChunks struct {
	Transaction uuid.UUID
	Keys        [][32]byte
}

type ResReferenceChunks struct {
	Exists []bool
	Err    error
}

type ReqReferenceObjects struct {
	Transaction uuid.UUID
	Keys        [][32]byte
}

type ResReferenceObjects struct {
	Exists []bool
	Err    error
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
	Data        []byte
}

type ResPutObject struct {
	NBytes int
	Err    error
}

type ReqPutMetadata struct {
	Transaction uuid.UUID
	Data        []byte
}

type ResPutMetadata struct {
	NBytes int
	Err    error
}

type ReqPutIndex struct {
	Transaction uuid.UUID
	Data        []byte
}

type ResPutIndex struct {
	NBytes int
	Err    error
}

type ReqPutFilesystem struct {
	Transaction uuid.UUID
	Data        []byte
}

type ResPutFilesystem struct {
	NBytes int
	Err    error
}

type ReqCommit struct {
	Transaction uuid.UUID
}

type ResCommit struct {
	Err error
}

type ReqGetChunkRefCount struct {
	Checksum [32]byte
}

type ResGetChunkRefCount struct {
	RefCount uint64
	Err      error
}

type ReqGetObjectRefCount struct {
	Checksum [32]byte
}

type ResGetObjectRefCount struct {
	RefCount uint64
	Err      error
}

type ReqGetObjectSize struct {
	Checksum [32]byte
}

type ResGetObjectSize struct {
	Size uint64
	Err  error
}

type ReqGetChunkSize struct {
	Checksum [32]byte
}

type ResGetChunkSize struct {
	Size uint64
	Err  error
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

	gob.Register(ReqStorePutIndex{})
	gob.Register(ResStorePutIndex{})

	gob.Register(ReqStorePutFilesystem{})
	gob.Register(ResStorePutFilesystem{})

	gob.Register(ReqGetChunks{})
	gob.Register(ResGetChunks{})

	gob.Register(ReqGetObjects{})
	gob.Register(ResGetObjects{})

	gob.Register(ReqGetMetadata{})
	gob.Register(ResGetMetadata{})

	gob.Register(ReqGetIndex{})
	gob.Register(ResGetIndex{})

	gob.Register(ReqGetFilesystem{})
	gob.Register(ResGetFilesystem{})

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

	gob.Register(ReqReferenceChunks{})
	gob.Register(ResReferenceChunks{})

	gob.Register(ReqReferenceObjects{})
	gob.Register(ResReferenceObjects{})

	gob.Register(ReqPutChunk{})
	gob.Register(ResPutChunk{})

	gob.Register(ReqPutObject{})
	gob.Register(ResPutObject{})

	gob.Register(ReqPutMetadata{})
	gob.Register(ResPutMetadata{})

	gob.Register(ReqPutIndex{})
	gob.Register(ResPutIndex{})

	gob.Register(ReqPutFilesystem{})
	gob.Register(ResPutFilesystem{})

	gob.Register(ReqCommit{})
	gob.Register(ResCommit{})

	gob.Register(ReqGetChunkRefCount{})
	gob.Register(ResGetChunkRefCount{})

	gob.Register(ReqGetObjectRefCount{})
	gob.Register(ResGetObjectRefCount{})

	gob.Register(ReqGetChunkSize{})
	gob.Register(ResGetChunkSize{})

	gob.Register(ReqGetObjectSize{})
	gob.Register(ResGetObjectSize{})
}
