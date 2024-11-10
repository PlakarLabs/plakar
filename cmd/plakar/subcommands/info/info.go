/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package info

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/dustin/go-humanize"
)

func init() {
	subcommands.Register("info", cmd_info)
}

func cmd_info(ctx *context.Context, repo *repository.Repository, args []string) int {
	if len(args) == 0 {
		return info_repository(repo)
	}

	flags := flag.NewFlagSet("info", flag.ExitOnError)
	flags.Parse(args)

	// Determine which concept to show information for based on flags.Args()[0]
	switch flags.Arg(0) {
	case "snapshot":
		if len(flags.Args()) < 2 {
			logger.Error("usage: %s snapshot snapshotID", flags.Name())
			return 1
		}
		if err := info_snapshot(repo, flags.Args()[1]); err != nil {
			logger.Error("error: %s", err)
			return 1
		}

	case "state":
		if err := info_state(repo, flags.Args()[1:]); err != nil {
			logger.Error("error: %s", err)
			return 1
		}

	case "packfile":
		if err := info_packfile(repo, flags.Args()[1:]); err != nil {
			logger.Error("error: %s", err)
			return 1
		}

	case "object":
		if len(flags.Args()) < 2 {
			logger.Error("usage: %s object objectID", flags.Name())
			return 1
		}
		if err := info_object(repo, flags.Args()[1]); err != nil {
			logger.Error("error: %s", err)
			return 1
		}

	case "vfs":
		if len(flags.Args()) < 2 {
			logger.Error("usage: %s vfs snapshotPathname", flags.Name())
			return 1
		}
		if err := info_vfs(repo, flags.Args()[1]); err != nil {
			logger.Error("error: %s", err)
			return 1
		}

	default:
		fmt.Println("Invalid parameter. usage: info [repository|snapshot|object|chunk|state|packfile|vfs]")
		return 1
	}

	return 0
}

func info_repository(repo *repository.Repository) int {
	metadatas, err := utils.GetHeaders(repo, nil)
	if err != nil {
		logger.Warn("%s", err)
		return 1
	}

	fmt.Println("RepositoryID:", repo.Configuration().RepositoryID)
	fmt.Printf("CreationTime: %s\n", repo.Configuration().CreationTime)
	fmt.Println("Version:", repo.Configuration().Version)

	if repo.Configuration().Encryption != "" {
		fmt.Println("Encryption:", repo.Configuration().Encryption)
		fmt.Println("EncryptionKey:", repo.Configuration().EncryptionKey)
	} else {
		fmt.Println("Encryption:", "no")
	}

	if repo.Configuration().Compression != "" {
		fmt.Println("Compression:", repo.Configuration().Compression)
	} else {
		fmt.Println("Compression:", "no")
	}

	fmt.Println("Hashing:", repo.Configuration().Hashing)

	fmt.Println("Chunking:", repo.Configuration().Chunking)
	fmt.Printf("ChunkingMin: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().ChunkingMin)), repo.Configuration().ChunkingMin)
	fmt.Printf("ChunkingNormal: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().ChunkingNormal)), repo.Configuration().ChunkingNormal)
	fmt.Printf("ChunkingMax: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().ChunkingMax)), repo.Configuration().ChunkingMax)

	fmt.Println("Snapshots:", len(metadatas))
	totalSize := uint64(0)
	totalIndexSize := uint64(0)
	totalMetadataSize := uint64(0)
	for _, metadata := range metadatas {
		totalSize += metadata.ScanProcessedSize
	}
	fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)
	fmt.Printf("Index Size: %s (%d bytes)\n", humanize.Bytes(totalIndexSize), totalIndexSize)
	fmt.Printf("Metadata Size: %s (%d bytes)\n", humanize.Bytes(totalMetadataSize), totalMetadataSize)

	return 0
}

func info_snapshot(repo *repository.Repository, snapshotID string) error {

	snap, err := utils.OpenSnapshotByPrefix(repo, snapshotID)
	if err != nil {
		return err
	}

	header := snap.Header

	indexID := header.GetIndexID()
	fmt.Printf("IndexID: %s\n", hex.EncodeToString(indexID[:]))
	fmt.Printf("CreationTime: %s\n", header.CreationTime)
	fmt.Printf("CreationDuration: %s\n", header.CreationDuration)

	fmt.Printf("Root: %x\n", header.Root)
	fmt.Printf("Metadata: %x\n", header.Metadata)
	fmt.Printf("Statistics: %x\n", header.Statistics)

	fmt.Printf("Version: %s\n", repo.Configuration().Version)
	fmt.Printf("Hostname: %s\n", header.Hostname)
	fmt.Printf("Username: %s\n", header.Username)
	fmt.Printf("CommandLine: %s\n", header.CommandLine)
	fmt.Printf("OperatingSystem: %s\n", header.OperatingSystem)
	fmt.Printf("Architecture: %s\n", header.Architecture)
	fmt.Printf("NumCPU: %d\n", header.NumCPU)

	fmt.Printf("MachineID: %s\n", header.MachineID)
	fmt.Printf("PublicKey: %s\n", header.PublicKey)
	fmt.Printf("Tags: %s\n", strings.Join(header.Tags, ", "))
	fmt.Printf("Directories: %d\n", header.DirectoriesCount)
	fmt.Printf("Files: %d\n", header.FilesCount)

	fmt.Printf("Snapshot.Size: %s (%d bytes)\n", humanize.Bytes(header.ScanProcessedSize), header.ScanProcessedSize)
	return nil
}

func info_state(repo *repository.Repository, args []string) error {
	if len(args) == 0 {
		states, err := repo.GetStates()
		if err != nil {
			log.Fatal(err)
		}

		for _, state := range states {
			fmt.Printf("%x\n", state)
		}
	} else {
		for _, arg := range args {
			// convert arg to [32]byte
			if len(arg) != 64 {
				log.Fatalf("invalid packfile hash: %s", arg)
			}

			b, err := hex.DecodeString(arg)
			if err != nil {
				log.Fatalf("invalid packfile hash: %s", arg)
			}

			// Convert the byte slice to a [32]byte
			var byteArray [32]byte
			copy(byteArray[:], b)

			rawState, _, err := repo.GetState(byteArray)
			if err != nil {
				log.Fatal(err)
			}

			st, err := state.NewFromBytes(rawState)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Version: %d.%d.%d\n", st.Metadata.Version/100, (st.Metadata.Version/10)%10, st.Metadata.Version%10)
			fmt.Printf("Creation: %s\n", st.Metadata.CreationTime)
			if len(st.Metadata.Extends) > 0 {
				fmt.Printf("Extends:\n")
				for _, stateID := range st.Metadata.Extends {
					fmt.Printf("  %x\n", stateID)
				}
			}

			for snapshotID, subpart := range st.Snapshots {
				fmt.Printf("snapshot %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[snapshotID],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for chunk, subpart := range st.Chunks {
				fmt.Printf("chunk %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[chunk],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for object, subpart := range st.Objects {
				fmt.Printf("object %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[object],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for file, subpart := range st.Files {
				fmt.Printf("file %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[file],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for directory, subpart := range st.Directories {
				fmt.Printf("directory %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[directory],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for data, subpart := range st.Datas {
				fmt.Printf("data %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[data],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}
		}
	}
	return nil
}

func info_packfile(repo *repository.Repository, args []string) error {
	if len(args) == 0 {
		packfiles, err := repo.GetPackfiles()
		if err != nil {
			log.Fatal(err)
		}

		for _, packfile := range packfiles {
			fmt.Printf("%x\n", packfile)
		}
	} else {
		for _, arg := range args {
			// convert arg to [32]byte
			if len(arg) != 64 {
				log.Fatalf("invalid packfile hash: %s", arg)
			}

			b, err := hex.DecodeString(arg)
			if err != nil {
				log.Fatalf("invalid packfile hash: %s", arg)
			}

			// Convert the byte slice to a [32]byte
			var byteArray [32]byte
			copy(byteArray[:], b)

			rd, _, err := repo.GetPackfile(byteArray)
			if err != nil {
				log.Fatal(err)
			}

			rawPackfile, err := io.ReadAll(rd)
			if err != nil {
				log.Fatal(err)
			}

			versionBytes := rawPackfile[len(rawPackfile)-5 : len(rawPackfile)-5+4]
			version := binary.LittleEndian.Uint32(versionBytes)

			//			version := rawPackfile[len(rawPackfile)-2]
			footerOffset := rawPackfile[len(rawPackfile)-1]
			rawPackfile = rawPackfile[:len(rawPackfile)-5]

			_ = version

			footerbuf := rawPackfile[len(rawPackfile)-int(footerOffset):]
			rawPackfile = rawPackfile[:len(rawPackfile)-int(footerOffset)]

			footerbuf, err = repo.Decode(footerbuf)
			if err != nil {
				log.Fatal(err)
			}
			footer, err := packfile.NewFooterFromBytes(footerbuf)
			if err != nil {
				log.Fatal(err)
			}

			indexbuf := rawPackfile[int(footer.IndexOffset):]
			rawPackfile = rawPackfile[:int(footer.IndexOffset)]

			indexbuf, err = repo.Decode(indexbuf)
			if err != nil {
				log.Fatal(err)
			}

			hasher := sha256.New()
			hasher.Write(indexbuf)

			if !bytes.Equal(hasher.Sum(nil), footer.IndexChecksum[:]) {
				log.Fatal("index checksum mismatch")
			}

			rawPackfile = append(rawPackfile, indexbuf...)
			rawPackfile = append(rawPackfile, footerbuf...)

			p, err := packfile.NewFromBytes(rawPackfile)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Version: %d.%d.%d\n", p.Footer.Version/100, p.Footer.Version%100/10, p.Footer.Version%10)
			fmt.Printf("Timestamp: %s\n", time.Unix(0, p.Footer.Timestamp))
			fmt.Printf("Index checksum: %x\n", p.Footer.IndexChecksum)
			fmt.Println()

			for i, entry := range p.Index {
				fmt.Printf("blob[%d]: %x %d %d %s\n", i, entry.Checksum, entry.Offset, entry.Length, entry.TypeName())
			}
		}
	}
	return nil
}

func info_object(repo *repository.Repository, objectID string) error {
	if len(objectID) != 64 {
		log.Fatalf("invalid object hash: %s", objectID)
	}

	b, err := hex.DecodeString(objectID)
	if err != nil {
		log.Fatalf("invalid object hash: %s", objectID)
	}

	// Convert the byte slice to a [32]byte
	var byteArray [32]byte
	copy(byteArray[:], b)

	rd, _, err := repo.GetObject(byteArray)
	if err != nil {
		log.Fatal(err)
	}

	blob, err := io.ReadAll(rd)
	if err != nil {
		log.Fatal(err)
	}

	object, err := objects.NewObjectFromBytes(blob)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("object: %x\n", object.Checksum)
	fmt.Println("  type:", object.ContentType)
	if len(object.Tags) > 0 {
		fmt.Println("  tags:", strings.Join(object.Tags, ","))
	}

	fmt.Println("  chunks:")
	for _, chunk := range object.Chunks {
		fmt.Printf("    checksum: %x\n", chunk.Checksum)
	}
	return nil
}

func info_vfs(repo *repository.Repository, snapshotPath string) error {
	// TODO
	snapshotPrefix, pathname := utils.ParseSnapshotID(snapshotPath)
	snap1, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix)
	if err != nil {
		return err
	}
	fs, err := snap1.Filesystem()
	if err != nil {
		return err
	}

	fsinfo, err := fs.Stat(filepath.Clean(pathname))
	if err != nil {
		return err
	}

	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		fmt.Printf("[DirEntry]\n")
		fmt.Printf("Version: %d\n", dirEntry.Version)
		fmt.Printf("ParentPath: %s\n", dirEntry.ParentPath)
		fmt.Printf("Name: %s\n", dirEntry.FileInfo().Name())
		fmt.Printf("Type: %d\n", dirEntry.Type)
		fmt.Printf("Size: %d\n", dirEntry.FileInfo().Size())
		fmt.Printf("Permissions: %s\n", dirEntry.FileInfo().Mode())
		fmt.Printf("ModTime: %s\n", dirEntry.FileInfo().ModTime())
		fmt.Printf("DeviceID: %d\n", dirEntry.DeviceID)
		fmt.Printf("InodeID: %d\n", dirEntry.InodeID)
		fmt.Printf("UserID: %d\n", dirEntry.UserID)
		fmt.Printf("GroupID: %d\n", dirEntry.GroupID)
		fmt.Printf("NumLinks: %d\n", dirEntry.NumLinks)
		fmt.Printf("ExtendedAttributes: %s\n", dirEntry.ExtendedAttributes)
		fmt.Printf("CustomMetadata: %s\n", dirEntry.CustomMetadata)
		fmt.Printf("Tags: %s\n", dirEntry.Tags)
		for offset, child := range dirEntry.Children {
			fmt.Printf("Child[%d].Checksum: %x\n", offset, child.Checksum)
			fmt.Printf("Child[%d].FileInfo.Name(): %s\n", offset, child.FileInfo.Name())
			fmt.Printf("Child[%d].FileInfo.Size(): %d\n", offset, child.FileInfo.Size())
			fmt.Printf("Child[%d].FileInfo.Mode(): %s\n", offset, child.FileInfo.Mode())
			fmt.Printf("Child[%d].FileInfo.Dev(): %d\n", offset, child.FileInfo.Dev())
			fmt.Printf("Child[%d].FileInfo.Ino(): %d\n", offset, child.FileInfo.Ino())
			fmt.Printf("Child[%d].FileInfo.Uid(): %d\n", offset, child.FileInfo.Uid())
			fmt.Printf("Child[%d].FileInfo.Gid(): %d\n", offset, child.FileInfo.Gid())
			fmt.Printf("Child[%d].FileInfo.Nlink(): %d\n", offset, child.FileInfo.Nlink())
		}

	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile {
		fmt.Printf("[FileEntry]\n")
		fmt.Printf("Version: %d\n", fileEntry.Version)
		fmt.Printf("ParentPath: %s\n", fileEntry.ParentPath)
		fmt.Printf("Name: %s\n", fileEntry.FileInfo().Name())
		fmt.Printf("Type: %d\n", fileEntry.Type)
		fmt.Printf("Size: %d\n", fileEntry.FileInfo().Size())
		fmt.Printf("Permissions: %s\n", fileEntry.FileInfo().Mode())
		fmt.Printf("ModTime: %s\n", fileEntry.FileInfo().ModTime())
		fmt.Printf("DeviceID: %d\n", fileEntry.DeviceID)
		fmt.Printf("InodeID: %d\n", fileEntry.InodeID)
		fmt.Printf("UserID: %d\n", fileEntry.UserID)
		fmt.Printf("GroupID: %d\n", fileEntry.GroupID)
		fmt.Printf("NumLinks: %d\n", fileEntry.NumLinks)
		fmt.Printf("ExtendedAttributes: %s\n", fileEntry.ExtendedAttributes)
		fmt.Printf("FileAttributes: %v\n", fileEntry.FileAttributes)
		if fileEntry.SymlinkTarget != "" {
			fmt.Printf("SymlinkTarget: %s\n", fileEntry.SymlinkTarget)
		}
		fmt.Printf("CustomMetadata: %s\n", fileEntry.CustomMetadata)
		fmt.Printf("Tags: %s\n", fileEntry.Tags)
		fmt.Printf("Checksum: %x\n", fileEntry.Checksum)
		for offset, chunk := range fileEntry.Chunks {
			fmt.Printf("Chunk[%d].Checksum: %x\n", offset, chunk.Checksum)
			fmt.Printf("Chunk[%d].Length: %d\n", offset, chunk.Length)
		}
	}
	return nil
}
