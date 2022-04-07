package snapshot

import (
	"encoding/json"
)

func indexFromBytes(data []byte) (*Index, error) {
	var index Index
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, err
	}
	index.Filesystem.Reindex()
	return &index, nil
}

func indexToBytes(index *Index) ([]byte, error) {
	serialized, err := json.Marshal(index)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}

func metadataFromBytes(data []byte) (*Metadata, error) {
	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func metadataToBytes(metadata *Metadata) ([]byte, error) {
	serialized, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}
