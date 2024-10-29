package compression

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

func DefaultAlgorithm() string {
	return "lz4"
}

func DeflateStream(name string, r io.Reader) (io.Reader, error) {
	m := map[string]func(io.Reader) (io.Reader, error){
		"gzip": DeflateGzipStream,
		"lz4":  DeflateLZ4Stream,
	}
	if fn, exists := m[name]; exists {
		return fn(r)
	}
	return nil, fmt.Errorf("unsupported compression method %q", name)
}

func DeflateGzipStream(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		gw := gzip.NewWriter(pw)
		defer gw.Close()
		_, err := io.Copy(gw, r)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}

func DeflateLZ4Stream(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		lw := lz4.NewWriter(pw)
		defer lw.Close()
		_, err := io.Copy(lw, r)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}

func InflateStream(name string, r io.Reader) (io.Reader, error) {
	m := map[string]func(io.Reader) (io.Reader, error){
		"gzip": InflateGzipStream,
		"lz4":  InflateLZ4Stream,
	}
	if fn, exists := m[name]; exists {
		return fn(r)
	}
	return nil, fmt.Errorf("unsupported compression method %q", name)
}

func InflateGzipStream(r io.Reader) (io.Reader, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		defer gz.Close()
		_, err := io.Copy(pw, gz)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}

func InflateLZ4Stream(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		lz := lz4.NewReader(r)
		_, err := io.Copy(pw, lz)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}
