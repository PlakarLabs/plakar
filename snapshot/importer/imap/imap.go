/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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

package imap

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/snapshot/importer"
	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
)

type IMAPImporter struct {
	importer.ImporterBackend

	location string
	client   *client.Client
	clientMu sync.Mutex
}

func init() {
	importer.Register("imap", NewIMAPImporter)
}

func connect(location *url.URL) (*client.Client, error) {
	port := "993"
	if location.Port() != "" {
		port = location.Port()
	}
	return client.DialTLS(location.Host+":"+port, nil)
}

func NewIMAPImporter(location string) (importer.ImporterBackend, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	conn, err := connect(parsed)
	if err != nil {
		return nil, err
	}

	username := parsed.User.Username()
	password, _ := parsed.User.Password()

	err = conn.Login(username, password)
	if err != nil {
		return nil, err
	}

	return &IMAPImporter{
		client: conn,
	}, nil
}

func (p *IMAPImporter) Scan() (<-chan importer.ScanResult, error) {
	c := make(chan importer.ScanResult)

	go func() {
		directories := make(map[string]objects.FileInfo)
		files := make(map[string]objects.FileInfo)
		ino := uint64(0)
		fi := objects.NewFileInfo(
			"/",
			0,
			0700|fs.ModeDir,
			time.Now(),
			0,
			ino,
			0,
			0,
			1,
		)
		directories["/"] = fi
		ino++

		mailboxes := make(chan *imap.MailboxInfo, 10)
		done := make(chan error, 1)
		go func() {
			done <- p.client.List("", "*", mailboxes)
		}()

		for m := range mailboxes {
			atoms := strings.Split(m.Name, "/")
			for i := 0; i < len(atoms)-1; i++ {
				dir := strings.Join(atoms[0:i+1], "/")
				if _, exists := directories[dir]; !exists {
					fi := objects.NewFileInfo(
						atoms[i],
						0,
						0700|fs.ModeDir,
						time.Now(),
						0,
						ino,
						0,
						0,
						1,
					)
					directories["/"+dir] = fi
					ino++
				}
			}
			stat := objects.NewFileInfo(
				atoms[len(atoms)-1],
				0,
				0700|fs.ModeDir,
				time.Now(),
				0,
				ino,
				0,
				0,
				1,
			)
			ino++
			directories["/"+m.Name] = stat

			mbox, err := p.client.Select(m.Name, false)
			if err != nil {
				c <- importer.ScanError{Pathname: m.Name, Err: err}
				return
			}

			from := uint32(1)
			to := mbox.Messages
			if mbox.Messages == 0 {
				continue
			}

			seqset := new(imap.SeqSet)
			seqset.AddRange(from, to)

			messages := make(chan *imap.Message, 10)
			done = make(chan error, 1)
			go func() {
				done <- p.client.Fetch(seqset, []imap.FetchItem{imap.FetchRFC822Size, imap.FetchUid, imap.FetchEnvelope}, messages)
			}()

			for msg := range messages {
				stat := objects.NewFileInfo(
					fmt.Sprint(msg.Uid),
					int64(msg.Size),
					0700,
					msg.Envelope.Date,
					0,
					ino,
					0,
					0,
					1,
				)
				ino++
				files["/"+m.Name+"/"+fmt.Sprint(msg.Uid)] = stat
			}

			if err := <-done; err != nil {
				log.Fatal(err)
			}
		}

		directoryNames := make([]string, 0)
		for name := range directories {
			directoryNames = append(directoryNames, name)
		}

		fileNames := make([]string, 0)
		for name := range files {
			fileNames = append(fileNames, name)
		}

		sort.Slice(directoryNames, func(i, j int) bool {
			return len(directoryNames[i]) < len(directoryNames[j])
		})
		sort.Slice(fileNames, func(i, j int) bool {
			return len(fileNames[i]) < len(fileNames[j])
		})

		for _, directory := range directoryNames {
			c <- importer.ScanRecord{Pathname: directory, Stat: directories[directory]}
		}
		for _, filename := range fileNames {
			c <- importer.ScanRecord{Pathname: filename, Stat: files[filename]}
		}
		close(c)
	}()
	return c, nil
}

func (p *IMAPImporter) NewReader(pathname string) (io.ReadCloser, error) {
	atoms := strings.Split(pathname, "/")
	mbox := strings.Join(atoms[1:len(atoms)-1], "/")
	uid, err := strconv.ParseUint(atoms[len(atoms)-1], 10, 32)
	if err != nil {
		return nil, err
	}

	p.clientMu.Lock()
	defer p.clientMu.Unlock()

	seqset := imap.SeqSet{}
	seqset.AddNum(uint32(uid))

	messages := make(chan *imap.Message, 1)
	_, err = p.client.Select(mbox, true)
	if err != nil {
		return nil, err
	}
	err = p.client.UidFetch(&seqset, []imap.FetchItem{imap.FetchItem("BODY.PEEK[]")}, messages)
	if err != nil {
		return nil, err
	}

	msg := <-messages
	if len(msg.Body) == 0 {
		return nil, fmt.Errorf("failed to open body")
	}

	for _, literal := range msg.Body {
		return io.NopCloser(literal), nil
	}
	return nil, fmt.Errorf("failed to open body")
}

func (p *IMAPImporter) Close() error {
	return nil
}
