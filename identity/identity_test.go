package identity

import (
	"bytes"
	"testing"

	"github.com/PlakarLabs/plakar/encryption/keypair"
	"github.com/google/uuid"
)

// TestNew checks if a new identity can be created with a valid address and key pair
func TestNew(t *testing.T) {
	// Generate a keypair for testing
	kp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate keypair: %v", err)
	}

	// Test with a valid email address
	validAddress := "test@example.com"
	id, err := New(validAddress, *kp)
	if err != nil {
		t.Fatalf("Failed to create identity with valid address: %v", err)
	}

	// Check if the address and key pair were set correctly
	if id.Address != validAddress {
		t.Errorf("Expected address %s, got %s", validAddress, id.Address)
	}
	if id.Keypair.PrivateKey == nil || id.Keypair.PublicKey == nil {
		t.Error("Identity keypair contains nil keys")
	}

	// Test with an invalid email address
	invalidAddress := "invalid-email"
	if _, err := New(invalidAddress, *kp); err == nil {
		t.Error("Expected error when creating identity with invalid email address")
	}
}

// TestFromBytes checks if an identity can be correctly deserialized from bytes
func TestFromBytes(t *testing.T) {
	// Create a sample identity for testing
	kp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate keypair: %v", err)
	}
	id, err := New("test@example.com", *kp)
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	// Serialize the identity to bytes
	data := id.ToBytes()
	if data == nil {
		t.Fatal("Failed to serialize identity to bytes")
	}

	// Deserialize the identity from bytes
	deserializedID, err := FromBytes(data)
	if err != nil {
		t.Fatalf("Failed to deserialize identity from bytes: %v", err)
	}

	// Compare fields to ensure correct deserialization
	if deserializedID.Address != id.Address {
		t.Errorf("Expected address %s, got %s", id.Address, deserializedID.Address)
	}
	if deserializedID.Identifier != id.Identifier {
		t.Errorf("Expected identifier %v, got %v", id.Identifier, deserializedID.Identifier)
	}
	if !bytes.Equal(deserializedID.Keypair.PublicKey, id.Keypair.PublicKey) {
		t.Error("Public keys do not match after deserialization")
	}
	if !bytes.Equal(deserializedID.Keypair.PrivateKey, id.Keypair.PrivateKey) {
		t.Error("Private keys do not match after deserialization")
	}
}

// TestToBytes checks if an identity can be correctly serialized to bytes
func TestToBytes(t *testing.T) {
	// Generate a keypair and create an identity for testing
	kp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate keypair: %v", err)
	}
	id, err := New("test@example.com", *kp)
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	// Serialize the identity to bytes
	data := id.ToBytes()
	if data == nil {
		t.Fatal("Failed to serialize identity to bytes")
	}

	// Ensure serialized data is not empty
	if len(data) == 0 {
		t.Error("Serialized data is empty")
	}
}

// TestSignAndVerify checks if signing and verification using an identity works correctly
func TestSignAndVerify(t *testing.T) {
	// Generate a keypair and identity for testing
	kp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate keypair: %v", err)
	}
	id, err := New("test@example.com", *kp)
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	data := []byte("This is a test message.")
	signature := id.Sign(data)

	// Verify the signature using the same identity
	if !id.Verify(data, signature) {
		t.Error("Failed to verify signature with the same identity")
	}

	// Create another identity to test signature verification failure
	otherKp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate a different keypair: %v", err)
	}
	otherId, err := New("other@example.com", *otherKp)
	if err != nil {
		t.Fatalf("Failed to create other identity: %v", err)
	}

	if otherId.Verify(data, signature) {
		t.Error("Signature verified with a different identity")
	}
}

// TestGetters checks if all getter methods return the correct values
func TestGetters(t *testing.T) {
	kp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate keypair: %v", err)
	}
	address := "getter@example.com"
	id, err := New(address, *kp)
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	if id.GetAddress() != address {
		t.Errorf("Expected address %s, got %s", address, id.GetAddress())
	}
	if id.GetIdentifier() == uuid.Nil {
		t.Error("Expected non-nil UUID identifier")
	}
	if id.GetPublicKey() == nil {
		t.Error("Expected non-nil public key")
	}
	if id.GetPrivateKey() == nil {
		t.Error("Expected non-nil private key")
	}
	if id.GetKeypair().PublicKey == nil || id.GetKeypair().PrivateKey == nil {
		t.Error("Keypair getter returned nil keys")
	}
}
