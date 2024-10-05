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

// TestSignAndVerify checks if signing and verification work correctly
func TestSignAndVerify(t *testing.T) {
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
