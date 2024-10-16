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
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/snapshot/header"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
	"github.com/PlakarLabs/plakar/storage/state"
	"github.com/google/uuid"
	"golang.org/x/mod/semver"
	"golang.org/x/tools/blog/atom"
)

func parseSnapshotID(id string) (string, string) {
	tmp := strings.Split(id, ":")
	prefix := id
	pattern := ""
	if len(tmp) != 0 {
		prefix = tmp[0]
		pattern = strings.Join(tmp[1:], ":")
		if runtime.GOOS != "windows" {
			if !strings.HasPrefix(pattern, "/") {
				pattern = "/" + pattern
			}
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

func getSnapshotsList(repo *repository.Repository) ([]uuid.UUID, error) {
	snapshots, err := snapshot.List(repo)
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}

func getHeaders(repo *repository.Repository, prefixes []string) ([]*header.Header, error) {
	snapshotsList, err := getSnapshotsList(repo)
	if err != nil {
		return nil, err
	}

	result := make([]*header.Header, 0)

	// no prefixes, this is a full fetch
	if prefixes == nil {
		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, snapshotUuid := range snapshotsList {
			wg.Add(1)
			go func(snapshotUuid uuid.UUID) {
				defer wg.Done()
				hdr, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
				if err != nil {
					fmt.Println(err)
					return
				}
				mu.Lock()
				result = append(result, hdr)
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
		hdr, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
		if err != nil {
			return nil, err
		}
		for _, tag := range hdr.Tags {
			if recordTime, exists := tagsTimestamp[tag]; !exists {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = hdr.CreationTime
			} else if recordTime.Before(hdr.CreationTime) {
				tags[tag] = snapshotUuid
				tagsTimestamp[tag] = hdr.CreationTime
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
				metadata, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
				if err != nil {
					return nil, err
				}
				result = append(result, metadata)
			}
		}
	}
	return result, nil
}

func getFilesystems(repo *repository.Repository, prefixes []string) ([]*vfs.Filesystem, error) {
	snapshotsList, err := getSnapshotsList(repo)
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

				md, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
				if err != nil {
					fmt.Println(err)
					return
				}

				var filesystemChecksum32 [32]byte
				copy(filesystemChecksum32[:], md.VFS.Checksum[:])

				filesystem, _, err := snapshot.GetFilesystem(repo, filesystemChecksum32)
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
		metadata, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
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
				md, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
				if err != nil {
					return nil, err
				}

				var filesystemChecksum32 [32]byte
				copy(filesystemChecksum32[:], md.VFS.Checksum[:])

				filesystem, _, err := snapshot.GetFilesystem(repo, filesystemChecksum32)
				if err != nil {
					return nil, err
				}
				result = append(result, filesystem)
			}
		}
	}
	return result, nil
}

func getSnapshots(repo *repository.Repository, prefixes []string) ([]*snapshot.Snapshot, error) {
	snapshotsList, err := getSnapshotsList(repo)
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
				snapshotInstance, err := snapshot.Load(repo, snapshotUuid)
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
		metadata, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
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
				snapshotInstance, err := snapshot.Load(repo, snapshotUuid)
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
		return snapshots[i].Header.CreationTime.Before(snapshots[j].Header.CreationTime)
	})
	return snapshots
}

func indexArrayContains(a []uuid.UUID, x uuid.UUID) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func loadRepositoryState(repo *repository.Repository) (*state.State, error) {
	indexes, err := repo.Store().GetStates()
	if err != nil {
		return nil, err
	}

	// XXX - we can clear the cache of any key prefixed by an index ID that's not in indexes
	// do that later

	repositoryIndex := state.New()
	wg := sync.WaitGroup{}
	for _, _indexID := range indexes {
		wg.Add(1)
		go func(indexID [32]byte) {
			defer wg.Done()
			idx, err := snapshot.GetState(repo, indexID)
			if err == nil {
				repositoryIndex.Merge(indexID, idx)
			}
		}(_indexID)
	}
	wg.Wait()
	repositoryIndex.ResetDirty()
	return repositoryIndex, nil
}

func HumanToDuration(human string) (time.Duration, error) {
	// support either one of the following:
	// - time.Duration string
	// - human readable string (e.g. 1h, 1d, 1w, 1m, 1y)
	// - human readable string with time.Duration suffix (e.g. 1h30m, 1d12h, 1w3d, 1m2w, 1y1m)

	// first we check if it's a time.Duration string
	duration, err := time.ParseDuration(human)
	if err == nil {
		return duration, nil
	}

	// TODO-handle iteratively constructed human readable strings

	return 0, fmt.Errorf("invalid duration: %s", human)
}

type ReleaseUpdateSummary struct {
	FoundCount int
	Latest     string

	SecurityFix    bool
	ReliabilityFix bool
}

func checkUpdate() (ReleaseUpdateSummary, error) {
	req, err := http.NewRequest("GET", "https://plakar.io/api/releases.atom", nil)
	if err != nil {
		return ReleaseUpdateSummary{}, err
	}

	req.Header.Set("User-Agent", fmt.Sprintf("plakar/%s (%s/%s)", VERSION, runtime.GOOS, runtime.GOARCH))

	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return ReleaseUpdateSummary{}, err
	}
	defer res.Body.Close()

	var feed []atom.Feed
	err = xml.NewDecoder(res.Body).Decode(&feed)
	if err != nil {
		return ReleaseUpdateSummary{}, err
	}

	found := false
	foundCount := 0
	var latestEntry *atom.Entry
	sawSecurityFix := false
	sawReliabilityFix := false
	for _, entry := range feed[0].Entry {
		if !semver.IsValid(entry.Title) {
			continue
		}
		if semver.Compare(VERSION, entry.Title) < 0 {
			found = true
			foundCount++
			if latestEntry == nil {
				latestEntry = entry
			} else {
				if semver.Compare(latestEntry.Title, entry.Title) < 0 {
					latestEntry = entry
				}
			}
			if latestEntry.Content != nil {
				if strings.Contains(*&latestEntry.Content.Body, "SECURITY") {
					sawSecurityFix = true
				}
				if strings.Contains(*&latestEntry.Content.Body, "RELIABILITY") {
					sawReliabilityFix = true
				}
			}
		}
	}
	if !found {
		return ReleaseUpdateSummary{FoundCount: 0, Latest: VERSION}, nil
	} else {
		return ReleaseUpdateSummary{FoundCount: foundCount, Latest: latestEntry.Title, SecurityFix: sawSecurityFix, ReliabilityFix: sawReliabilityFix}, nil
	}
}
