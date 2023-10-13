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

	"github.com/PlakarLabs/plakar/vfs"
	"github.com/PlakarLabs/plakar/vfs/importer"
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

func NewIMAPImporter() importer.ImporterBackend {
	return &IMAPImporter{}
}

func (p *IMAPImporter) connect(location *url.URL) error {
	port := "993"
	if location.Port() != "" {
		port = location.Port()
	}
	client, err := client.DialTLS(location.Host+":"+port, nil)
	if err != nil {
		return err
	}
	p.client = client
	return nil
}

func (p *IMAPImporter) Scan() (<-chan importer.ImporterRecord, <-chan error, error) {
	parsed, err := url.Parse(p.location)
	if err != nil {
		return nil, nil, err
	}

	err = p.connect(parsed)
	if err != nil {
		return nil, nil, err
	}

	username := parsed.User.Username()
	password, _ := parsed.User.Password()

	err = p.client.Login(username, password)
	if err != nil {
		return nil, nil, err
	}

	c := make(chan importer.ImporterRecord)
	cerr := make(chan error)

	go func() {
		directories := make(map[string]vfs.FileInfo)
		files := make(map[string]vfs.FileInfo)
		ino := uint64(0)
		fi := vfs.NewFileInfo(
			"/",
			0,
			0700|fs.ModeDir,
			time.Now(),
			0,
			ino,
			0,
			0,
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
					fi := vfs.NewFileInfo(
						atoms[i],
						0,
						0700|fs.ModeDir,
						time.Now(),
						0,
						ino,
						0,
						0,
					)
					directories["/"+dir] = fi
					ino++
				}
			}
			stat := vfs.NewFileInfo(
				atoms[len(atoms)-1],
				0,
				0700|fs.ModeDir,
				time.Now(),
				0,
				ino,
				0,
				0,
			)
			ino++
			directories["/"+m.Name] = stat

			mbox, err := p.client.Select(m.Name, false)
			if err != nil {
				log.Fatal(err)
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
				stat := vfs.NewFileInfo(
					fmt.Sprint(msg.Uid),
					int64(msg.Size),
					0700,
					msg.Envelope.Date,
					0,
					ino,
					0,
					0,
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
			c <- importer.ImporterRecord{Pathname: directory, Stat: directories[directory]}
		}
		for _, filename := range fileNames {
			c <- importer.ImporterRecord{Pathname: filename, Stat: files[filename]}
		}

		//fmt.Println(files)
		close(cerr)
		close(c)
	}()
	return c, cerr, nil
}

func (p *IMAPImporter) Open(pathname string) (io.ReadCloser, error) {
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

func (p *IMAPImporter) Begin(location string) error {
	p.location = location
	return nil
}

func (p *IMAPImporter) End() error {
	return nil
}
