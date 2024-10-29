# Encryption Streaming Library Design Document

## Overview
This library provides a secure and efficient way to encrypt and decrypt data streams using AES-GCM (Galois/Counter Mode). Each data stream is encrypted with a unique, randomly generated subkey, and integrity is ensured through AES-GCM’s authenticated encryption. The library supports large data streams or continuous data through chunked processing, reducing memory requirements.

## Goals
1. **Confidentiality and Integrity**: Ensure that data is both encrypted and authenticated to detect tampering.
2. **Streaming Support**: Encrypt and decrypt data as it streams, without loading entire data into memory.
3. **Secure Key Management**: Derive unique subkeys per data stream, removing the risk of nonce reuse.
4. **Error Handling**: Handle common error cases, such as incorrect passphrases and decryption with invalid keys, in a secure manner.

## Components

### 1. Key Derivation
- **Function**: `BuildSecretFromPassphrase(passphrase []byte) string`
- **Purpose**: Generate a base secret derived from a user-provided passphrase using PBKDF2.
- **Process**:
  - Generate a random salt of 16 bytes.
  - Use PBKDF2 with SHA-256 and 4096 iterations to derive a key from the passphrase and salt.
  - Hash the derived key and salt, then encode them in base64 for storage.

- **Function**: `DeriveSecret(passphrase []byte, secret string) ([]byte, error)`
- **Purpose**: Derive a decryption key from a passphrase using the stored base secret.
- **Process**:
  - Decode the base64-encoded secret.
  - Recompute the derived key with the passphrase and salt.
  - Compare with the stored hash to verify the passphrase.

### 2. Stream Encryption
- **Function**: `EncryptStream(key []byte, r io.Reader) (io.Reader, error)`
- **Purpose**: Encrypts data from an input `io.Reader` and returns an encrypted `io.Reader` for streaming.
- **Process**:
  1. **Subkey Generation**: Generate a 32-byte random subkey for data encryption.
  2. **Subkey Encryption**:
     - Encrypt the subkey with AES-GCM using the main key.
     - Generate a unique nonce per subkey and prepend the encrypted subkey and nonce to the encrypted data.
  3. **Data Encryption**:
     - Encrypt the data in chunks using AES-GCM with the subkey, handling each chunk individually to allow for streaming.
  4. **Output**: Write the encrypted subkey, nonce, and encrypted data chunks to the output `io.Reader`.

### 3. Stream Decryption
- **Function**: `DecryptStream(key []byte, r io.Reader) (io.Reader, error)`
- **Purpose**: Decrypts data from an input `io.Reader` that was encrypted with `EncryptStream`.
- **Process**:
  1. **Subkey Decryption**:
     - Read the nonce and encrypted subkey from the beginning of the stream.
     - Decrypt the subkey with the main key using AES-GCM.
  2. **Data Decryption**:
     - Initialize AES-GCM with the decrypted subkey.
     - Read and decrypt data chunks sequentially from the input stream, writing them to the output `io.Reader`.
  3. **Error Handling**:
     - If decryption fails, return an error immediately to prevent further processing of corrupted or tampered data.

### 4. Chunked Processing
- Data is processed in chunks to minimize memory usage and facilitate streaming. Each chunk is encrypted or decrypted independently.

## Data Flow

### Encryption
1. User calls `EncryptStream` with a main key and an `io.Reader` containing the data.
2. A unique subkey and nonce are generated for the data stream.
3. The subkey is encrypted with AES-GCM, and both the encrypted subkey and its nonce are written to the stream.
4. The data is encrypted in chunks and written to the output stream.

### Decryption
1. User calls `DecryptStream` with a main key and an `io.Reader` containing the encrypted data.
2. The encrypted subkey and nonce are read and decrypted using AES-GCM with the main key.
3. The decrypted subkey and data nonce are used to initialize AES-GCM for the data stream.
4. Data chunks are decrypted and written to the output stream in the original form.

## Security Considerations

1. **Subkey Management**: Subkeys are generated per data stream to ensure unique key-nonce pairs for AES-GCM, preventing the risk of nonce reuse.
2. **Data Integrity**: AES-GCM provides integrity, so any modification to the encrypted data will fail decryption.
3. **Key Derivation**: A strong KDF (PBKDF2 with SHA-256) and salt prevent dictionary attacks on passphrases.
4. **Error Handling**: Decryption failures, such as using an incorrect key, will result in errors, stopping data output and ensuring corrupted data is not processed.

## Testing

1. **Encryption/Decryption**:
   - Verify that encrypting and decrypting a stream returns the original data.
2. **Incorrect Key**:
   - Test that decryption fails with an incorrect key.
3. **Passphrase Derivation**:
   - Verify that only the correct passphrase derives the valid decryption key.
4. **Chunk Processing**:
   - Ensure the library handles data in chunks, maintaining memory efficiency with large streams.

## Future Enhancements

- **Multiple Cipher Support**: Expand to support additional encryption modes, such as AES-CTR with HMAC for use cases where only confidentiality is required.
- **Configurable Chunk Size**: Allow dynamic adjustment of chunk size based on user requirements for further optimization in memory management.
