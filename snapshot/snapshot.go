package snapshot

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/statistics"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/google/uuid"
)

var (
	ErrNotFound = errors.New("snapshot not found")
)

type Snapshot struct {
	repository *repository.Repository
	stateDelta *state.State

	filesystem *vfs.Filesystem

	SkipDirs []string

	Header *header.Header

	statistics *statistics.Statistics

	packerChan     chan interface{}
	packerChanDone chan bool
}

type PackerMsg struct {
	Timestamp time.Time
	Type      packfile.Type
	Checksum  objects.Checksum
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
					snap.Logger().Trace("packer", "%x: PackerMsg(%d, %064x), dt=%s", snap.Header.GetIndexShortID(), msg.Type, msg.Checksum, time.Since(msg.Timestamp))
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

func New(repo *repository.Repository) (*Snapshot, error) {
	var identifier objects.Checksum

	n, err := rand.Read(identifier[:])
	if err != nil {
		return nil, err
	}
	if n != len(identifier) {
		return nil, io.ErrShortWrite
	}

	snap := &Snapshot{
		repository: repo,
		stateDelta: repo.NewStateDelta(),

		Header: header.NewHeader("default", identifier),

		statistics: statistics.New(),

		packerChan:     make(chan interface{}, runtime.NumCPU()*2+1),
		packerChanDone: make(chan bool),
	}

	if snap.Context().GetIdentity() != uuid.Nil {
		snap.Header.Identity.Identifier = snap.Context().GetIdentity()
		snap.Header.Identity.PublicKey = snap.Context().GetKeypair().PublicKey
	}

	snap.Header.SetContext("Hostname", snap.Context().GetHostname())
	snap.Header.SetContext("Username", snap.Context().GetUsername())
	snap.Header.SetContext("OperatingSystem", snap.Context().GetOperatingSystem())
	snap.Header.SetContext("MachineID", snap.Context().GetMachineID())
	snap.Header.SetContext("CommandLine", snap.Context().GetCommandLine())
	snap.Header.SetContext("ProcessID", fmt.Sprintf("%d", snap.Context().GetProcessID()))
	snap.Header.SetContext("Architecture", snap.Context().GetArchitecture())
	snap.Header.SetContext("NumCPU", fmt.Sprintf("%d", runtime.NumCPU()))
	snap.Header.SetContext("GOMAXPROCS", fmt.Sprintf("%d", runtime.GOMAXPROCS(0)))
	snap.Header.SetContext("Client", snap.Context().GetPlakarClient())

	go packerJob(snap)

	repo.Logger().Trace("snapshot", "%x: New()", snap.Header.GetIndexShortID())
	return snap, nil
}

func Load(repo *repository.Repository, Identifier objects.Checksum) (*Snapshot, error) {
	hdr, _, err := GetSnapshot(repo, Identifier)
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{}
	snapshot.repository = repo
	snapshot.Header = hdr

	repo.Logger().Trace("snapshot", "%x: Load()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Clone(repo *repository.Repository, Identifier objects.Checksum) (*Snapshot, error) {
	snap, err := Load(repo, Identifier)
	if err != nil {
		return nil, err
	}
	snap.Header.Timestamp = time.Now()

	uuidBytes, err := uuid.Must(uuid.NewRandom()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	snap.stateDelta = state.New()
	snap.statistics = statistics.New()

	snap.Header.Identifier = repo.Checksum(uuidBytes[:])
	snap.packerChan = make(chan interface{}, runtime.NumCPU()*2+1)
	snap.packerChanDone = make(chan bool)
	go packerJob(snap)

	repo.Logger().Trace("snapshot", "%x: Clone(): %s", snap.Header.Identifier, snap.Header.GetIndexShortID())
	return snap, nil
}

func Fork(repo *repository.Repository, Identifier objects.Checksum) (*Snapshot, error) {
	var identifier objects.Checksum

	n, err := rand.Read(identifier[:])
	if err != nil {
		return nil, err
	}
	if n != len(identifier) {
		return nil, io.ErrShortWrite
	}

	snap, err := Clone(repo, Identifier)
	if err != nil {
		return nil, err
	}

	snap.Header.Identifier = identifier

	repo.Logger().Trace("snapshot", "%x: Fork(): %s", snap.Header.Identifier, snap.Header.GetIndexShortID())
	return snap, nil
}

func (snap *Snapshot) Context() *context.Context {
	return snap.Repository().Context()
}

func (snap *Snapshot) Event(evt events.Event) {
	snap.Context().Events().Send(evt)
}

func GetSnapshot(repo *repository.Repository, Identifier objects.Checksum) (*header.Header, bool, error) {
	repo.Logger().Trace("snapshot", "repository.GetSnapshot(%x)", Identifier)

	rd, _, err := repo.GetBlob(packfile.TYPE_SNAPSHOT, Identifier)
	if err != nil {
		if errors.Is(err, repository.ErrBlobNotFound) {
			err = ErrNotFound
		}
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

func (snap *Snapshot) Repository() *repository.Repository {
	return snap.repository
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

	encryptedIndex, err := repo.EncodeBuffer(serializedIndex)
	if err != nil {
		return err
	}

	encryptedFooter, err := repo.EncodeBuffer(serializedFooter)
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

	var checksum32 objects.Checksum
	copy(checksum32[:], checksum[:])

	atomic.AddUint64(&snap.statistics.PackfilesCount, 1)
	atomic.AddUint64(&snap.statistics.PackfilesSize, uint64(len(serializedPackfile)))

	repo.Logger().Trace("snapshot", "%x: PutPackfile(%x, ...)", snap.Header.GetIndexShortID(), checksum32)
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

func (snap *Snapshot) Commit() error {

	repo := snap.repository

	serializedHdr, err := snap.Header.Serialize()
	if err != nil {
		return err
	}

	if kp := snap.Context().GetKeypair(); kp != nil {
		serializedHdrChecksum := snap.repository.Checksum(serializedHdr)
		signature := kp.Sign(serializedHdrChecksum[:])
		if err := snap.PutBlob(packfile.TYPE_SIGNATURE, snap.Header.Identifier, signature); err != nil {
			return err
		}
	}

	if err := snap.PutBlob(packfile.TYPE_SNAPSHOT, snap.Header.Identifier, serializedHdr); err != nil {
		return err
	}

	close(snap.packerChan)
	<-snap.packerChanDone

	serializedRepositoryIndex, err := snap.stateDelta.Serialize()
	if err != nil {
		snap.Logger().Warn("could not serialize repository index: %s", err)
		return err
	}
	indexChecksum := snap.repository.Checksum(serializedRepositoryIndex)
	indexChecksum32 := objects.Checksum{}
	copy(indexChecksum32[:], indexChecksum[:])
	_, err = repo.PutState(indexChecksum32, bytes.NewBuffer(serializedRepositoryIndex), int64(len(serializedRepositoryIndex)))
	if err != nil {
		return err
	}

	snap.Logger().Trace("snapshot", "%x: Commit()", snap.Header.GetIndexShortID())
	return nil
}

func (snap *Snapshot) LookupObject(checksum objects.Checksum) (*objects.Object, error) {
	buffer, err := snap.GetBlob(packfile.TYPE_OBJECT, checksum)
	if err != nil {
		return nil, err
	}
	return objects.NewObjectFromBytes(buffer)
}

func (snap *Snapshot) ListChunks() (<-chan objects.Checksum, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan objects.Checksum)
	go func() {
		for filename := range fs.Files() {
			fsentry, err := fs.GetEntry(filename)
			if err != nil {
				break
			}
			if fsentry.Object == nil {
				continue
			}
			for _, chunk := range fsentry.Object.Chunks {
				c <- chunk.Checksum
			}
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListObjects() (<-chan objects.Checksum, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan objects.Checksum)
	go func() {
		for filename := range fs.Files() {
			fsentry, err := fs.GetEntry(filename)
			if err != nil {
				break
			}
			if fsentry.Object == nil {
				continue
			}
			c <- fsentry.Object.Checksum
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListDatas() (<-chan objects.Checksum, error) {
	c := make(chan objects.Checksum)

	go func() {
		c <- snap.Header.Metadata
		c <- snap.Header.Statistics
		close(c)
	}()

	return c, nil
}

func (snap *Snapshot) Logger() *logging.Logger {
	return snap.Context().GetLogger()
}
