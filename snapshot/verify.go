package snapshot

import (
	"crypto/ed25519"

	"github.com/google/uuid"
)

func (snap *Snapshot) Verify() (bool, error) {
	if snap.Header.Identity.Identifier == uuid.Nil {
		return false, nil
	}

	signature, err := snap.GetSignature(snap.Header.SnapshotID)
	if err != nil {
		return false, err
	}

	serializedHdr, err := snap.Header.Serialize()
	if err != nil {
		return false, err
	}
	serializedHdrChecksum := snap.repository.Checksum(serializedHdr)

	return ed25519.Verify(snap.Header.Identity.PublicKey, serializedHdrChecksum[:], signature), nil
}
