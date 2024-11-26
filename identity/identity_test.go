package identity

import (
	"testing"

	"github.com/PlakarKorp/plakar/encryption/keypair"
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
	if id.KeyPair.PrivateKey == nil || id.KeyPair.PublicKey == nil {
		t.Error("Identity keypair contains nil keys")
	}

	// Test with an invalid email address
	invalidAddress := "invalid-email"
	if _, err := New(invalidAddress, *kp); err == nil {
		t.Error("Expected error when creating identity with invalid email address")
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
