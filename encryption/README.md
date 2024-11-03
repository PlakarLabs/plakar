# Encryption Streaming Library Design Document

## Overview
This library provides a secure, efficient mechanism for encrypting and decrypting data streams using AES-GCM with session-specific subkeys. Each session generates a unique subkey and encrypts it with the main key to ensure both confidentiality and integrity. The library is optimized for large data streams and can handle continuous data processing through chunked encryption and decryption.

## Goals
1. **Confidentiality and Integrity**: Ensure data is encrypted and authenticated to detect any tampering.
2. **Efficient Streaming**: Encrypt and decrypt data in a streaming manner, without loading the entire dataset into memory.
3. **Secure Key Management**: Utilize a session-specific subkey for each encryption session to prevent nonce reuse and enable secure handling of the main key.
4. **Robust Error Handling**: Safely manage errors to avoid leaking sensitive information and ensure reliability in production.

## Components

### 1. Key Derivation with `scrypt`
- **Function**: `BuildSecretFromPassphrase(passphrase []byte) (string, error)`
- **Purpose**: Derives a base secret from a user-provided passphrase using the memory-hard `scrypt` function to resist brute-force attacks.
- **Process**:
  - Generates a 16-byte random salt.
  - Uses `scrypt` with high CPU and memory costs (`N=32768, r=8, p=1`) to derive a 32-byte key.
  - Returns a base64-encoded secret, which includes the salt and derived key, for secure storage.

- **Function**: `DeriveSecret(passphrase []byte, secret string) ([]byte, error)`
- **Purpose**: Derives a decryption key from the passphrase and the stored secret.
- **Process**:
  - Decodes the base64-encoded secret to extract the salt and expected key.
  - Recomputes the derived key using `scrypt` and compares it with the stored key for passphrase validation.

### 2. Stream Encryption
- **Function**: `EncryptStream(key []byte, r io.Reader) (io.Reader, error)`
- **Purpose**: Encrypts an input stream using AES-GCM with a unique session-specific subkey.
- **Process**:
  1. **Generate Session-Specific Subkey**:
     - A random 32-byte subkey is generated for the encryption session.
  2. **Encrypt Subkey with Main Key**:
     - The main key is used to set up an AES-GCM instance.
     - A random nonce is generated, and the subkey is encrypted with AES-GCM to ensure its confidentiality and integrity.
  3. **Encrypt Data with Subkey**:
     - A new AES-GCM instance is set up with the session-specific subkey.
     - A separate nonce is generated for data encryption.
  4. **Output**:
     - The encrypted subkey, its nonce, and the data nonce are written to the output stream, followed by the encrypted data.
     - Data is processed in chunks to support large streams and avoid excessive memory usage.

### 3. Stream Decryption
- **Function**: `DecryptStream(key []byte, r io.Reader) (io.Reader, error)`
- **Purpose**: Decrypts an input stream that was encrypted with `EncryptStream`.
- **Process**:
  1. **Decrypt the Session-Specific Subkey**:
     - The nonce and encrypted subkey are read from the stream.
     - The main key is used with AES-GCM to decrypt and verify the subkey.
  2. **Decrypt Data with Subkey**:
     - The decrypted subkey is used to set up an AES-GCM instance for data decryption.
     - A separate data nonce is read from the stream and used for decrypting data in chunks.
  3. **Error Handling**:
     - Decryption errors result in immediate termination, preventing tampered data from being processed.

### 4. Chunked Processing
- Data is processed in chunks (1KB by default) to minimize memory use and enable efficient handling of large or continuous data streams.

## Data Flow

### Encryption
1. User calls `EncryptStream` with the main key and an `io.Reader` for the data.
2. A unique session-specific subkey and two nonces (one for subkey encryption, one for data encryption) are generated.
3. The subkey is encrypted with the main key and written to the output along with the nonces.
4. Data is encrypted in chunks with the session-specific subkey and written to the output stream.

### Decryption
1. User calls `DecryptStream` with the main key and an `io.Reader` for the encrypted data.
2. The encrypted subkey and its nonce are read and decrypted to obtain the session-specific subkey.
3. A separate nonce is read for data decryption, and data is decrypted in chunks using the session-specific subkey.
4. Decrypted data is streamed to the output.

## Security Considerations

1. **Subkey Management**: A session-specific subkey is generated per encryption session to prevent key/nonce reuse and ensure confidentiality.
2. **Data Integrity**: AES-GCM guarantees data integrity, so any tampering with the encrypted data or subkey will result in decryption failure.
3. **Key Derivation with `scrypt`**: The memory-hard `scrypt` function provides resistance to brute-force attacks, making it suitable for deriving keys from potentially low-entropy passphrases.
4. **Error Handling**: Error handling is carefully implemented, ensuring sensitive information is not exposed in case of failures.

## Testing

1. **Encryption/Decryption Verification**:
   - Encrypt a stream and then decrypt it to ensure the decrypted output matches the original data.
2. **Incorrect Key/Passphrase Handling**:
   - Attempt to decrypt with an incorrect key to ensure decryption fails.
3. **Passphrase Derivation Validation**:
   - Verify that only the correct passphrase derives a matching key.
4. **Chunked Data Processing**:
   - Confirm that large data streams are encrypted and decrypted correctly in chunks, without excessive memory usage.

## Future Enhancements

- **Dynamic Chunk Size Configuration**: Allow users to specify the chunk size for improved control over memory and performance.
- **Parallelizaiton**: Allow users to specify a concurrency value for parallel chunk decryption

---

This design achieves robust encryption and decryption with minimal memory footprint and ensures both data confidentiality and integrity in streaming contexts. It’s well-suited for high-security applications and efficient handling of large or continuous data transfers.
