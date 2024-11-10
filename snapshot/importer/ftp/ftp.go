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

package ftp

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/secsy/goftp"
)

type FTPImporter struct {
	importer.ImporterBackend
	host    string
	rootDir string
	client  *goftp.Client
}

func init() {
	importer.Register("ftp", NewFTPImporter)
}

func connectToFTP(host, username, password string) (*goftp.Client, error) {
	config := goftp.Config{
		User:     username,
		Password: password,
		Timeout:  10 * time.Second,
	}
	client, err := goftp.DialConfig(config, host)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func NewFTPImporter(location string) (importer.ImporterBackend, error) {

	//username := ""
	//password := ""

	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	//if parsed.User != nil {
	//	username = parsed.User.Username()
	//	if tmppass, passexists := parsed.User.Password(); passexists {
	//		password = tmppass
	//	}
	//}

	return &FTPImporter{
		host:    parsed.Host,
		rootDir: parsed.Path,
		//		client:  client,
	}, nil
}

func (p *FTPImporter) Scan() (<-chan importer.ScanResult, error) {
	client, err := connectToFTP(p.host, "", "")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	p.client = client
	c := make(chan importer.ScanResult)

	go func() {
		defer close(c)

		dirInfo := objects.NewFileInfo(
			"/", 0, 0700|os.ModeDir, time.Now(), 0, 0, 0, 0, 1,
		)
		c <- importer.ScanRecord{Pathname: "/", Stat: dirInfo}

		err := p.createParentNodes(p.rootDir, c)
		if err != nil {
			log.Printf("Error creating parent nodes: %v", err)
		}

		err = p.scanDirectory(p.rootDir, c)
		if err != nil {
			log.Printf("Error scanning FTP server: %v", err)
		}
	}()

	return c, nil
}

func (p *FTPImporter) createParentNodes(dirPath string, c chan importer.ScanResult) error {
	// Split the root directory into individual parts and create each directory node
	parts := strings.Split(dirPath, "/")

	// Reconstruct the path step-by-step, skipping empty parts
	currentPath := "/"
	for _, part := range parts {
		if part == "" {
			continue
		}
		currentPath = path.Join(currentPath, part)

		// Create a directory node for the current path
		dirInfo := objects.NewFileInfo(
			part,
			0,
			0700|os.ModeDir,
			time.Now(),
			0,
			0,
			0,
			0,
			1,
		)
		c <- importer.ScanRecord{Type: importer.RecordTypeDirectory, Pathname: currentPath, Stat: dirInfo}
	}

	return nil
}

func (p *FTPImporter) scanDirectory(dirPath string, c chan importer.ScanResult) error {
	entries, err := p.client.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		entryPath := filepath.Join(dirPath, entry.Name())
		fileInfo := objects.NewFileInfo(entry.Name(), entry.Size(), entry.Mode(), entry.ModTime(), 0, 0, 0, 0, 1)

		if entry.IsDir() {
			// Directory: Scan it recursively
			c <- importer.ScanRecord{Type: importer.RecordTypeDirectory, Pathname: entryPath, Stat: fileInfo}
			err := p.scanDirectory(entryPath, c)
			if err != nil {
				return err
			}
		} else {
			// File: Send file information
			c <- importer.ScanRecord{Type: importer.RecordTypeFile, Pathname: entryPath, Stat: fileInfo}
		}
	}
	return nil
}

func (p *FTPImporter) NewReader(pathname string) (io.ReadCloser, error) {
	tmpfile, err := os.CreateTemp("", "plakar-ftp-")
	if err != nil {
		return nil, err
	}

	err = p.client.Retrieve(pathname, tmpfile)
	if err != nil {
		return nil, err
	}
	tmpfile.Seek(0, 0)

	return tmpfile, nil
}

func (p *FTPImporter) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func (p *FTPImporter) Root() string {
	return p.rootDir
}

func (p *FTPImporter) Origin() string {
	return p.host
}

func (p *FTPImporter) Type() string {
	return "ftp"
}
