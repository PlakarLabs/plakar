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

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/index"
	"github.com/PlakarLabs/plakar/metadata"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/google/uuid"
)

func parseSnapshotID(id string) (string, string) {
	tmp := strings.Split(id, ":")
	prefix := id
	pattern := ""
	if len(tmp) != 0 {
		prefix = tmp[0]
		pattern = strings.Join(tmp[1:], ":")
		if !strings.HasPrefix(pattern, "/") {
			pattern = "/" + pattern
		}
	}
	return prefix, pattern
}

func findSnapshotByPrefix(snapshots []uuid.UUID, prefix string) []uuid.UUID {
	ret := make([]uuid.UUID, 0)
	for _, snapshot := range snapshots {
		if strings.HasPrefix(snapshot.String(), prefix) {
			ret = append(ret, snapshot)
		}
	}
	return ret
}

func getSnapshotsList(repository *storage.Repository) ([]uuid.UUID, error) {
	snapshots, err := snapshot.List(repository)
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}

func getMetadatas(repository *storage.Repository, prefixes []string) ([]*metadata.Metadata, error) {
	snapshotsList, err := getSnapshotsList(repository)
	if err != nil {
		return nil, err
	}

	result := make([]*metadata.Metadata, 0)

	// no prefixes, this is a full fetch
	if prefixes == nil {
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, snapshotUuid := range snapshotsList {
			wg.Add(1)
			go func(snapshotUuid uuid.UUID) {
				defer wg.Done()
				metadata, _, err := snapshot.GetMetadata(repository, snapshotUuid)
				if err != nil {
					fmt.Println(err)
					return
				}
				mu.Lock()
				result = append(result, metadata)
				mu.Unlock()
			}(snapshotUuid)
		}
		wg.Wait()
		sort.Slice(result, func(i, j int) bool {
			return result[i].CreationTime.Before(result[j].CreationTime)
		})
		return result, nil
	}

	tags := make(map[string]uuid.UUID)
	tagsTimestamp := make(map[string]time.Time)

	for _, snapshotUuid := range snapshotsList {
		metadata, _, err := snapshot.GetMetadata(repository, snapshotUuid)
		if err != nil {
			return nil, err
		}
		for _, tag := range metadata.Tags {
			if recordTime, exists := tagsTimestamp[tag]; !exists {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			} else if recordTime.Before(metadata.CreationTime) {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			}
		}
	}

	// prefixes, preprocess snapshots to only fetch necessary ones
	for _, prefix := range prefixes {
		parsedUuidPrefix, _ := parseSnapshotID(prefix)

		matches := 0
		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) {
				matches++
			}
		}
		if matches == 0 {
			if _, exists := tags[parsedUuidPrefix]; !exists {
				log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), parsedUuidPrefix)
			}
		} else if matches > 1 {
			log.Fatalf("%s: snapshot ID is ambiguous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, matches)
		}

		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) || snapshotUuid == tags[parsedUuidPrefix] {
				metadata, _, err := snapshot.GetMetadata(repository, snapshotUuid)
				if err != nil {
					return nil, err
				}
				result = append(result, metadata)
			}
		}
	}
	return result, nil
}

func getIndexes(repository *storage.Repository, prefixes []string) ([]*index.Index, error) {
	snapshotsList, err := getSnapshotsList(repository)
	if err != nil {
		return nil, err
	}

	result := make([]*index.Index, 0)

	// no prefixes, this is a full fetch
	if prefixes == nil {
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, snapshotUuid := range snapshotsList {
			wg.Add(1)
			go func(snapshotUuid uuid.UUID) {
				defer wg.Done()
				index, _, err := snapshot.GetIndex(repository, snapshotUuid)
				if err != nil {
					fmt.Println(err)
					return
				}
				mu.Lock()
				result = append(result, index)
				mu.Unlock()
			}(snapshotUuid)
		}
		wg.Wait()
		return result, nil
	}

	tags := make(map[string]uuid.UUID)
	tagsTimestamp := make(map[string]time.Time)

	for _, snapshotUuid := range snapshotsList {
		metadata, _, err := snapshot.GetMetadata(repository, snapshotUuid)
		if err != nil {
			return nil, err
		}
		for _, tag := range metadata.Tags {
			if recordTime, exists := tagsTimestamp[tag]; !exists {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			} else if recordTime.Before(metadata.CreationTime) {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			}
		}
	}

	// prefixes, preprocess snapshots to only fetch necessary ones
	for _, prefix := range prefixes {
		parsedUuidPrefix, _ := parseSnapshotID(prefix)

		matches := 0
		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) {
				matches++
			}
		}
		if matches == 0 {
			if _, exists := tags[parsedUuidPrefix]; !exists {
				log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), parsedUuidPrefix)
			}
		} else if matches > 1 {
			log.Fatalf("%s: snapshot ID is ambiguous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, matches)
		}

		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) || snapshotUuid == tags[parsedUuidPrefix] {
				index, _, err := snapshot.GetIndex(repository, snapshotUuid)
				if err != nil {
					return nil, err
				}
				result = append(result, index)
			}
		}
	}
	return result, nil
}

func getFilesystems(repository *storage.Repository, prefixes []string) ([]*vfs.Filesystem, error) {
	snapshotsList, err := getSnapshotsList(repository)
	if err != nil {
		return nil, err
	}

	result := make([]*vfs.Filesystem, 0)

	// no prefixes, this is a full fetch
	if prefixes == nil {
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, snapshotUuid := range snapshotsList {
			wg.Add(1)
			go func(snapshotUuid uuid.UUID) {
				defer wg.Done()
				filesystem, _, err := snapshot.GetFilesystem(repository, snapshotUuid)
				if err != nil {
					fmt.Println(err)
					return
				}
				mu.Lock()
				result = append(result, filesystem)
				mu.Unlock()
			}(snapshotUuid)
		}
		wg.Wait()
		return result, nil
	}

	tags := make(map[string]uuid.UUID)
	tagsTimestamp := make(map[string]time.Time)

	for _, snapshotUuid := range snapshotsList {
		metadata, _, err := snapshot.GetMetadata(repository, snapshotUuid)
		if err != nil {
			return nil, err
		}
		for _, tag := range metadata.Tags {
			if recordTime, exists := tagsTimestamp[tag]; !exists {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			} else if recordTime.Before(metadata.CreationTime) {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			}
		}
	}

	// prefixes, preprocess snapshots to only fetch necessary ones
	for _, prefix := range prefixes {
		parsedUuidPrefix, _ := parseSnapshotID(prefix)

		matches := 0
		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) {
				matches++
			}
		}
		if matches == 0 {
			if _, exists := tags[parsedUuidPrefix]; !exists {
				log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), parsedUuidPrefix)
			}
		} else if matches > 1 {
			log.Fatalf("%s: snapshot ID is ambiguous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, matches)
		}

		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) || snapshotUuid == tags[parsedUuidPrefix] {
				filesystem, _, err := snapshot.GetFilesystem(repository, snapshotUuid)
				if err != nil {
					return nil, err
				}
				result = append(result, filesystem)
			}
		}
	}
	return result, nil
}

func getSnapshots(repository *storage.Repository, prefixes []string) ([]*snapshot.Snapshot, error) {
	snapshotsList, err := getSnapshotsList(repository)
	if err != nil {
		return nil, err
	}

	result := make([]*snapshot.Snapshot, 0)

	// no prefixes, this is a full fetch
	if prefixes == nil {
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, snapshotUuid := range snapshotsList {
			wg.Add(1)
			go func(snapshotUuid uuid.UUID) {
				defer wg.Done()
				snapshotInstance, err := snapshot.Load(repository, snapshotUuid)
				if err != nil {
					return
				}
				mu.Lock()
				result = append(result, snapshotInstance)
				mu.Unlock()
			}(snapshotUuid)
		}
		wg.Wait()
		return sortSnapshotsByDate(result), nil
	}

	tags := make(map[string]uuid.UUID)
	tagsTimestamp := make(map[string]time.Time)

	for _, snapshotUuid := range snapshotsList {
		metadata, _, err := snapshot.GetMetadata(repository, snapshotUuid)
		if err != nil {
			return nil, err
		}
		for _, tag := range metadata.Tags {
			if recordTime, exists := tagsTimestamp[tag]; !exists {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			} else if recordTime.Before(metadata.CreationTime) {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = metadata.CreationTime
			}
		}
	}

	// prefixes, preprocess snapshots to only fetch necessary ones
	for _, prefix := range prefixes {
		parsedUuidPrefix, _ := parseSnapshotID(prefix)

		matches := 0
		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) {
				matches++
			}
		}
		if matches == 0 {
			if _, exists := tags[parsedUuidPrefix]; !exists {
				log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), parsedUuidPrefix)
			}
		} else if matches > 1 {
			log.Fatalf("%s: snapshot ID is ambiguous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, matches)
		}

		for _, snapshotUuid := range snapshotsList {
			if strings.HasPrefix(snapshotUuid.String(), parsedUuidPrefix) || snapshotUuid == tags[parsedUuidPrefix] {
				snapshotInstance, err := snapshot.Load(repository, snapshotUuid)
				if err != nil {
					return nil, err
				}
				result = append(result, snapshotInstance)
			}
		}
	}
	return result, nil
}

func sortSnapshotsByDate(snapshots []*snapshot.Snapshot) []*snapshot.Snapshot {
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Metadata.CreationTime.Before(snapshots[j].Metadata.CreationTime)
	})
	return snapshots
}

/*
func checkSnapshotsArgs(snapshots []string) {
	for i := 0; i < len(snapshots); i++ {
		prefix, _ := parseSnapshotID(snapshots[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
	}
}
*/

func arrayContains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func indexArrayContains(a []uuid.UUID, x uuid.UUID) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func checksumArrayContains(a [][32]byte, x [32]byte) bool {
	for _, n := range a {
		if bytes.Equal(x[:], n[:]) {
			return true
		}
	}
	return false
}
