package snapshot

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/metadata"
	"github.com/PlakarKorp/plakar/snapshot/statistics"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/google/uuid"
)

type Snapshot struct {
	repository *repository.Repository
	stateDelta *state.State

	filesystem *vfs.Filesystem

	SkipDirs []string

	Header *header.Header

	Metadata *metadata.Metadata

	statistics *statistics.Statistics

	packerChan     chan interface{}
	packerChanDone chan bool
}

type PackerMsg struct {
	Timestamp time.Time
	Type      packfile.Type
	Checksum  [32]byte
	Data      []byte
}

func packerJob(snap *Snapshot) {
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var packer *Packer

			for msg := range snap.packerChan {
				if packer == nil {
					packer = NewPacker()
				}

				if msg, ok := msg.(*PackerMsg); !ok {
					panic("received data with unexpected type")
				} else {
					logger.Trace("packer", "%x: PackerMsg(%d, %064x), dt=%s", snap.Header.GetIndexShortID(), msg.Type, msg.Checksum, time.Since(msg.Timestamp))
					packer.AddBlob(msg.Type, msg.Checksum, msg.Data)
				}

				if packer.Size() > uint32(snap.repository.Configuration().Packfile.MaxSize) {
					err := snap.PutPackfile(packer)
					if err != nil {
						panic(err)
					}
					packer = nil
				}
			}

			if packer != nil {
				err := snap.PutPackfile(packer)
				if err != nil {
					panic(err)
				}
				packer = nil
			}
		}()
	}
	wg.Wait()
	snap.packerChanDone <- true
	close(snap.packerChanDone)
}

func New(repo *repository.Repository, snapshotID [32]byte) (*Snapshot, error) {
	snap := &Snapshot{
		repository: repo,
		stateDelta: repo.NewStateDelta(),

		Header:   header.NewHeader(snapshotID),
		Metadata: metadata.New(),

		statistics: statistics.New(),

		packerChan:     make(chan interface{}, runtime.NumCPU()*2+1),
		packerChanDone: make(chan bool),
	}
	go packerJob(snap)

	logger.Trace("snapshot", "%x: New()", snap.Header.GetIndexShortID())
	return snap, nil
}

func Load(repo *repository.Repository, snapshotID [32]byte) (*Snapshot, error) {
	hdr, _, err := GetSnapshot(repo, snapshotID)
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{}
	snapshot.repository = repo
	snapshot.Header = hdr

	logger.Trace("snapshot", "%x: Load()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Fork(repo *repository.Repository, snapshotID [32]byte) (*Snapshot, error) {
	snap, err := Load(repo, snapshotID)
	if err != nil {
		return nil, err
	}
	snap.Header.CreationTime = time.Now()

	uuidBytes, err := uuid.Must(uuid.NewRandom()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	snap.stateDelta = state.New()
	snap.statistics = statistics.New()

	snap.Header.SnapshotID = repo.Checksum(uuidBytes[:])
	snap.packerChan = make(chan interface{}, runtime.NumCPU()*2+1)
	snap.packerChanDone = make(chan bool)
	go packerJob(snap)

	logger.Trace("snapshot", "%x: Fork(): %s", snap.Header.SnapshotID, snap.Header.GetIndexShortID())
	return snap, nil
}

func (snap *Snapshot) Event(evt events.Event) {
	snap.Repository().Context().Events().Send(evt)
}

func GetSnapshot(repo *repository.Repository, snapshotID [32]byte) (*header.Header, bool, error) {
	logger.Trace("snapshot", "repository.GetSnapshot(%x)", snapshotID)

	rd, _, err := repo.GetBlob(packfile.TYPE_SNAPSHOT, snapshotID)
	if err != nil {
		return nil, false, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, false, err
	}

	hdr, err := header.NewFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	return hdr, false, nil
}

func GetMetadata(repo *repository.Repository, checksum [32]byte) (*metadata.Metadata, [32]byte, error) {

	rd, _, err := repo.GetBlob(packfile.TYPE_DATA, checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, [32]byte{}, err
	}

	md, err := metadata.NewFromBytes(buffer)
	if err != nil {
		return nil, [32]byte{}, err
	}

	verifyChecksum := repo.Checksum(buffer)
	verifyChecksum32 := [32]byte{}
	copy(verifyChecksum32[:], verifyChecksum[:])

	return md, verifyChecksum32, nil
}

func (snapshot *Snapshot) Repository() *repository.Repository {
	return snapshot.repository
}

func (snap *Snapshot) PutPackfile(packer *Packer) error {

	repo := snap.repository

	serializedData, err := packer.Packfile.SerializeData()
	if err != nil {
		panic("could not serialize pack file data" + err.Error())
	}
	serializedIndex, err := packer.Packfile.SerializeIndex()
	if err != nil {
		panic("could not serialize pack file index" + err.Error())
	}
	serializedFooter, err := packer.Packfile.SerializeFooter()
	if err != nil {
		panic("could not serialize pack file footer" + err.Error())
	}

	encryptedIndex, err := repo.Encode(serializedIndex)
	if err != nil {
		return err
	}

	encryptedFooter, err := repo.Encode(serializedFooter)
	if err != nil {
		return err
	}

	encryptedFooterLength := uint8(len(encryptedFooter))

	versionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(versionBytes, packer.Packfile.Footer.Version)

	serializedPackfile := append(serializedData, encryptedIndex...)
	serializedPackfile = append(serializedPackfile, encryptedFooter...)
	serializedPackfile = append(serializedPackfile, versionBytes...)
	serializedPackfile = append(serializedPackfile, byte(encryptedFooterLength))

	checksum := snap.repository.Checksum(serializedPackfile)

	var checksum32 [32]byte
	copy(checksum32[:], checksum[:])

	atomic.AddUint64(&snap.statistics.PackfilesCount, 1)
	atomic.AddUint64(&snap.statistics.PackfilesSize, uint64(len(serializedPackfile)))

	logger.Trace("snapshot", "%x: PutPackfile(%x, ...)", snap.Header.GetIndexShortID(), checksum32)
	err = snap.repository.PutPackfile(checksum32, bytes.NewBuffer(serializedPackfile), uint64(len(serializedPackfile)))
	if err != nil {
		panic("could not write pack file")
	}

	atomic.AddUint64(&snap.statistics.PackfilesTransferCount, 1)
	atomic.AddUint64(&snap.statistics.PackfilesTransferSize, uint64(len(serializedPackfile)))

	for _, Type := range packer.Types() {
		for blobChecksum := range packer.Blobs[Type] {
			for idx, blob := range packer.Packfile.Index {
				if blob.Checksum == blobChecksum && blob.Type == Type {
					snap.Repository().SetPackfileForBlob(Type, checksum32,
						blobChecksum,
						packer.Packfile.Index[idx].Offset,
						packer.Packfile.Index[idx].Length)
					snap.stateDelta.SetPackfileForBlob(Type, checksum32,
						blobChecksum,
						packer.Packfile.Index[idx].Offset,
						packer.Packfile.Index[idx].Length)
					break
				}
			}
		}
	}

	return nil
}

func (snapshot *Snapshot) Commit() error {

	repo := snapshot.repository

	serializedHdr, err := snapshot.Header.Serialize()
	if err != nil {
		return err
	}

	if kp := snapshot.repository.Context().GetKeypair(); kp != nil {
		serializedHdrChecksum := snapshot.repository.Checksum(serializedHdr)
		signature := kp.Sign(serializedHdrChecksum[:])
		if err := snapshot.PutBlob(packfile.TYPE_SIGNATURE, snapshot.Header.SnapshotID, signature); err != nil {
			return err
		}
	}

	if err := snapshot.PutBlob(packfile.TYPE_SNAPSHOT, snapshot.Header.SnapshotID, serializedHdr); err != nil {
		return err
	}

	close(snapshot.packerChan)
	<-snapshot.packerChanDone

	serializedRepositoryIndex, err := snapshot.stateDelta.Serialize()
	if err != nil {
		logger.Warn("could not serialize repository index: %s", err)
		return err
	}
	indexChecksum := snapshot.repository.Checksum(serializedRepositoryIndex)
	indexChecksum32 := [32]byte{}
	copy(indexChecksum32[:], indexChecksum[:])
	_, err = repo.PutState(indexChecksum32, bytes.NewBuffer(serializedRepositoryIndex), int64(len(serializedRepositoryIndex)))
	if err != nil {
		return err
	}

	logger.Trace("snapshot", "%x: Commit()", snapshot.Header.GetIndexShortID())
	return nil
}

func (snapshot *Snapshot) LookupObject(checksum [32]byte) (*objects.Object, error) {
	buffer, err := snapshot.GetBlob(packfile.TYPE_OBJECT, checksum)
	if err != nil {
		return nil, err
	}
	return objects.NewObjectFromBytes(buffer)
}

func (snap *Snapshot) ListChunks() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		for filename := range fs.Files() {
			fsentry, err := fs.Stat(filename)
			if err != nil {
				break
			}
			for _, chunk := range fsentry.(*vfs.FileEntry).Object.Chunks {
				c <- chunk.Checksum
			}
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListObjects() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		for filename := range fs.Files() {
			fsentry, err := fs.Stat(filename)
			if err != nil {
				break
			}
			c <- fsentry.(*vfs.FileEntry).Object.Checksum
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListFiles() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		for checksum := range fs.FileChecksums() {
			c <- checksum
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListDirectories() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		c <- snap.Header.Root
		for checksum := range fs.DirectoryChecksums() {
			c <- checksum
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListDatas() (<-chan [32]byte, error) {
	c := make(chan [32]byte)

	go func() {
		c <- snap.Header.Metadata
		c <- snap.Header.Statistics
		close(c)
	}()

	return c, nil
}
