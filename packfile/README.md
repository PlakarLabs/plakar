# Packfile Format Design and Implementation

## Overview

The `packfile` package provides a mechanism to store, organize, and retrieve chunks of binary data in a serialized format. This design document outlines the structure of the `packfile`, its components, and how data is serialized and deserialized. The format is optimized for efficient storage and quick lookup of data segments, ensuring data integrity using checksums.

## Packfile Structure

A `PackFile` consists of several components:
1. **Data Section**: Raw binary data stored sequentially.
2. **Index**: Metadata about chunks stored in the data section.
3. **Footer**: Metadata about the entire packfile, including the version, timestamp, and a checksum to verify integrity.

### Components

#### `PackFile`

The `PackFile` structure represents the entire data file:

- `Data []byte`: Contains the raw binary data.
- `Index []Blob`: A list of `Blob` structures that describe the segments stored in the data.
- `Footer PackFileFooter`: Metadata and integrity information for the packfile.

#### `Blob`

The `Blob` structure describes a single chunk of data within the `PackFile`:

- `Type uint8`: Type of the blob. The code defines the following types:
  - `TYPE_CHUNK` (1): A chunk of data.
  - `TYPE_OBJECT` (2): Represents an object.
  - `TYPE_CONTAINER` (3): Represents a container of blobs.
  - `TYPE_BINARY` (4): Represents a binary file or data type.
- `Checksum [32]byte`: SHA-256 checksum to verify the integrity of the blob.
- `Offset uint32`: Offset within the `Data` section where this blob starts.
- `Length uint32`: Length of the blob's data in bytes.

#### `PackFileFooter`

The `PackFileFooter` structure provides metadata about the packfile and integrity information:

- `Version uint32`: Version of the packfile format (default is 100).
- `Timestamp int64`: Timestamp of when the packfile was created.
- `Count uint32`: Number of blobs stored in the packfile.
- `IndexOffset uint32`: Offset where the index starts in the data section.
- `IndexChecksum [32]byte`: SHA-256 checksum of the index to verify integrity.

## Packfile Format Layout

A serialized `PackFile` is stored in the following format:

|----------- Data Section -----------|------------ Index ------------|--------- Footer --------|
| Raw Data | Raw Data | ... | Raw Data | Index Entry | Index Entry | ... | Version | Timestamp | Count | IndexOffset | IndexChecksum |


1. **Data Section**: Contains raw binary data chunks.
2. **Index Section**: Each entry in the index corresponds to a `Blob` structure, providing metadata for each chunk stored in the data section.
3. **Footer Section**: Contains metadata such as the version, timestamp, the count of blobs, and a checksum for integrity verification.

## Serialization and Deserialization

### Serialization (`Serialize` Method)

The `Serialize` method converts a `PackFile` into a binary format that can be saved to a file or transmitted over a network. It performs the following steps:

1. Serializes the `Data` section.
2. Serializes each entry in the `Index`.
3. Calculates the checksum for the index and updates the `Footer`'s `IndexChecksum`.
4. Serializes the `Footer` and appends it at the end of the file.

### Deserialization (`NewFromBytes` Method)

The `NewFromBytes` method reads a serialized `PackFile` and reconstructs it into a `PackFile` object. It performs the following steps:

1. Reads the `Footer` to determine the position of the `Index` and the length of the `Data`.
2. Reads the `Data` section up to the `IndexOffset`.
3. Reads and reconstructs each `Blob` in the `Index`.
4. Verifies the integrity of the `Index` using the `IndexChecksum` stored in the `Footer`.

## Usage Examples

### Creating a New Packfile

```go
p := packfile.New()

// Add sample blobs to the PackFile
checksum := sha256.Sum256([]byte("sample data"))
p.AddBlob(packfile.TYPE_CHUNK, checksum, []byte("sample data"))

// Serialize the PackFile
serialized, err := p.Serialize()
if err != nil {
    log.Fatalf("Failed to serialize PackFile: %v", err)
}

// Deserialize from serialized bytes
p2, err := packfile.NewFromBytes(serialized)
if err != nil {
    log.Fatalf("Failed to deserialize PackFile: %v", err)
}

// Retrieve a blob by its checksum
blobData, exists := p2.GetBlob(checksum)
if !exists {
    log.Println("Blob not found")
} else {
    log.Printf("Retrieved Blob Data: %s", blobData)
}
```

### Deserializing a Packfile

```go
// Deserialize from serialized bytes
p2, err := packfile.NewFromBytes(serialized)
if err != nil {
    log.Fatalf("Failed to deserialize PackFile: %v", err)
}

// Retrieve a blob by its checksum
blobData, exists := p2.GetBlob(checksum)
if !exists {
    log.Println("Blob not found")
} else {
    log.Printf("Retrieved Blob Data: %s", blobData)
}
```

## Integrity Checks
The PackFile format uses SHA-256 checksums to ensure data integrity. Each Blob contains a Checksum field, and the Index itself is protected by a checksum (IndexChecksum) stored in the Footer. During deserialization, these checksums are verified to ensure the data has not been tampered with or corrupted.

## Index Integrity Check
When deserializing, the checksum of the index (IndexChecksum) is computed and compared against the value stored in the Footer. If they do not match, the deserialization will fail, indicating possible data corruption.

## Conclusion
The packfile format provides a compact and efficient way to store and organize binary data with robust integrity checks. Its structure allows for easy serialization and deserialization, making it suitable for use in backup solutions, object storage, and other data management systems.