package keypair

import "crypto/ed25519"

type KeyPair struct {
	PrivateKey ed25519.PrivateKey
	PublicKey  ed25519.PublicKey
}

func Generate() (*KeyPair, error) {
	if publicKey, privateKey, err := ed25519.GenerateKey(nil); err != nil {
		return nil, err
	} else {
		return &KeyPair{
			PrivateKey: privateKey,
			PublicKey:  publicKey,
		}, nil
	}
}

func FromPrivateKey(privateKey ed25519.PrivateKey) *KeyPair {
	return &KeyPair{
		PrivateKey: privateKey,
		PublicKey:  privateKey.Public().(ed25519.PublicKey),
	}
}

func FromPublicKey(publicKey ed25519.PublicKey) *KeyPair {
	return &KeyPair{
		PrivateKey: nil,
		PublicKey:  publicKey,
	}
}

func (kp *KeyPair) Sign(data []byte) []byte {
	return ed25519.Sign(kp.PrivateKey, data)
}

func (kp *KeyPair) Verify(data []byte, signature []byte) bool {
	return ed25519.Verify(kp.PublicKey, data, signature)
}