package local

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func Init(localdir string) {
	os.MkdirAll(localdir, 0700)
	os.MkdirAll(fmt.Sprintf("%s/cache", localdir), 0700)
	os.MkdirAll(fmt.Sprintf("%s/keypairs", localdir), 0700)
	os.MkdirAll(fmt.Sprintf("%s/keys", localdir), 0700)
}

func GetEncryptedKeypair(localdir string, uuid string) ([]byte, error) {
	if uuid == "" {
		return ioutil.ReadFile(fmt.Sprintf("%s/keypairs/__default__", localdir))
	}
	return ioutil.ReadFile(fmt.Sprintf("%s/keypairs/%s", localdir, uuid))
}

func SetEncryptedKeypair(localdir string, uuid string, buf []byte) error {
	return ioutil.WriteFile(fmt.Sprintf("%s/keypairs/%s", localdir, uuid), buf, 0600)
}

func GetKeys(localdir string) ([]string, error) {
	files, err := ioutil.ReadDir(fmt.Sprintf("%s/keypairs", localdir))
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0)
	for _, file := range files {
		if file.Name() != "__default__" {
			keys = append(keys, file.Name())
		}
	}
	return keys, nil
}

func SetDefaultKeypairID(localdir string, uuid string) {
	err := os.Symlink(fmt.Sprintf("%s/keypairs/%s", localdir, uuid), fmt.Sprintf("%s/keypairs/__default__.tmp", localdir))
	if err == nil {
		os.Remove(fmt.Sprintf("%s/keypairs/__default__", localdir))
		os.Rename(fmt.Sprintf("%s/keypairs/__default__.tmp", localdir), fmt.Sprintf("%s/keypairs/__default__", localdir))
	}
}

func GetDefaultKeypairID(localdir string) (string, error) {
	originFile, err := os.Readlink(fmt.Sprintf("%s/keypairs/__default__", localdir))
	if err != nil {
		return "", err
	}
	atoms := strings.Split(originFile, "/")
	return atoms[len(atoms)-1], nil
}

func SetEncryptedSecret(localdir string, uuid string, buf []byte) error {
	return ioutil.WriteFile(fmt.Sprintf("%s/keys/%s", localdir, uuid), buf, 0600)
}

func GetEncryptedSecret(localdir string, uuid string) ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s/keys/%s", localdir, uuid))
}
