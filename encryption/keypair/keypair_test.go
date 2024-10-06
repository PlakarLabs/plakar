package keypair

import (
	"bytes"
	"crypto/ed25519"
	"testing"
)

// TestGenerate checks if key pair generation works correctly
func TestGenerate(t *testing.T) {
	kp, err := Generate()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	// Check if private and public keys are not nil
	if kp.PrivateKey == nil || kp.PublicKey == nil {
		t.Fatal("Generated key pair has nil keys")
	}

	// Check if the generated public key matches the private key's public key
	if !bytes.Equal(kp.PublicKey, kp.PrivateKey.Public().(ed25519.PublicKey)) {
		t.Fatal("Generated public key does not match the private key's public key")
	}
}

// TestFromBytes checks if a key pair can be correctly deserialized from bytes
func TestFromBytes(t *testing.T) {
	// Create a key pair and serialize it to bytes
	kp, err := Generate()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	data, err := kp.ToBytes()
	if err != nil {
		t.Fatalf("Failed to serialize key pair: %v", err)
	}

	// Deserialize from bytes
	deserializedKP, err := FromBytes(data)
	if err != nil {
		t.Fatalf("Failed to deserialize key pair: %v", err)
	}

	// Check if the deserialized public and private keys match the original
	if !bytes.Equal(deserializedKP.PublicKey, kp.PublicKey) {
		t.Fatal("Deserialized public key does not match original")
	}
	if !bytes.Equal(deserializedKP.PrivateKey, kp.PrivateKey) {
		t.Fatal("Deserialized private key does not match original")
	}
}

// TestToBytes checks if a key pair can be correctly serialized to bytes
func TestToBytes(t *testing.T) {
	kp, err := Generate()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	data, err := kp.ToBytes()
	if err != nil {
		t.Fatalf("Failed to serialize key pair: %v", err)
	}

	// Ensure that the serialized data is not empty
	if len(data) == 0 {
		t.Fatal("Serialized data is empty")
	}
}

// TestFromPrivateKey checks if a key pair can be created from an existing private key
func TestFromPrivateKey(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	kp := FromPrivateKey(privateKey)
	if kp.PrivateKey == nil || kp.PublicKey == nil {
		t.Fatal("Key pair from private key has nil keys")
	}

	// Check if the public key matches the private key's public key
	if !bytes.Equal(kp.PublicKey, privateKey.Public().(ed25519.PublicKey)) {
		t.Fatal("Public key does not match the private key's public key")
	}
}

// TestFromPublicKey checks if a key pair can be created from an existing public key
func TestFromPublicKey(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate public key: %v", err)
	}

	kp := FromPublicKey(publicKey)
	if kp.PrivateKey != nil {
		t.Fatal("Key pair from public key should have nil private key")
	}

	if !bytes.Equal(kp.PublicKey, publicKey) {
		t.Fatal("Public key in key pair does not match the input public key")
	}
}

// TestSignAndVerify checks if signing and verification using a key pair works correctly
func TestSignAndVerify(t *testing.T) {
	// Generate a key pair for testing
	kp, err := Generate()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	data := []byte("This is a test message.")
	signature := kp.Sign(data)

	// Verify the signature using the same key pair
	if !kp.Verify(data, signature) {
		t.Fatal("Failed to verify signature with the same key pair")
	}

	// Create a different key pair to test signature verification failure
	otherKp, err := Generate()
	if err != nil {
		t.Fatalf("Failed to generate a different key pair: %v", err)
	}

	if otherKp.Verify(data, signature) {
		t.Fatal("Signature verified with a different key pair")
	}
}

// TestSignWithNilPrivateKey checks that signing fails when private key is nil
func TestSignWithNilPrivateKey(t *testing.T) {
	kp := FromPublicKey(make(ed25519.PublicKey, ed25519.PublicKeySize))
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic when signing with nil private key")
		}
	}()
	_ = kp.Sign([]byte("Some data"))
}
