package compression

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

func Deflate(buf []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(buf)
	w.Close()
	return b.Bytes()
}

func Inflate(buf []byte) ([]byte, error) {
	w, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer w.Close()

	data, err := ioutil.ReadAll(w)
	if err != nil {
		return nil, err
	}
	return data, nil
}
