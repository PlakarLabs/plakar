package local

import (
	"fmt"
	"io/ioutil"
	"os"
)

func Init(localdir string) {
	os.MkdirAll(localdir, 0700)
}

func GetEncryptedKeypair(localdir string) ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s/plakar.key", localdir))
}

func SetEncryptedKeypair(localdir string, buf []byte) error {
	return ioutil.WriteFile(fmt.Sprintf("%s/plakar.key", localdir), buf, 0600)
}
