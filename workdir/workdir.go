package workdir

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Workdir struct {
	Directory string
}

func Create(localdir string) (*Workdir, error) {
	if _, err := os.Stat(localdir); !os.IsNotExist(err) {
		return nil, fmt.Errorf("directory exists")
	}
	err := os.MkdirAll(localdir, 0700)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(fmt.Sprintf("%s/%s", localdir, "secrets"), 0700)
	if err != nil {
		return nil, err
	}

	return New(localdir)
}

func New(localdir string) (*Workdir, error) {
	if _, err := os.Stat(localdir); os.IsNotExist(err) {
		return nil, err
	}
	return &Workdir{
		Directory: localdir,
	}, nil
}

func (wd *Workdir) SaveEncryptedKeypair(buf []byte) error {
	return ioutil.WriteFile(fmt.Sprintf("%s/keypair", wd.Directory), buf, 0600)
}

func (wd *Workdir) GetEncryptedKeypair() ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s/keypair", wd.Directory))
}

func (wd *Workdir) SaveEncryptedSecret(secretID string, buf []byte) error {
	return ioutil.WriteFile(fmt.Sprintf("%s/secrets/%s", wd.Directory, secretID), buf, 0600)
}

func (wd *Workdir) GetEncryptedSecret(secretID string) ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s/secrets/%s", wd.Directory, secretID))
}
