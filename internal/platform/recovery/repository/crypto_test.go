package repository

import (
	"bytes"
	"testing"
)

func TestCredentialEnvelopeRoundTripAndTamper(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	plain := []byte(`{"access_key":"example","secret_key":"not-printed"}`)
	ciphertext, nonce, algorithm, err := EncryptCredentials(key, plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, []byte("not-printed")) || len(nonce) != 12 || algorithm != CredentialAlgorithmAES256GCM {
		t.Fatal("credentials were not encrypted")
	}
	if CredentialAlgorithmName(algorithm) != CredentialAlgorithmAES256GCMName {
		t.Fatal("credential algorithm name is not stable")
	}
	if parsed, err := CredentialAlgorithmID(CredentialAlgorithmAES256GCMName); err != nil || parsed != algorithm {
		t.Fatal("credential algorithm ID is not reversible", parsed, err)
	}
	decoded, err := DecryptCredentials(key, ciphertext, nonce, algorithm)
	if err != nil || !bytes.Equal(decoded, plain) {
		t.Fatalf("round trip = %q, %v", decoded, err)
	}
	ciphertext[0] ^= 1
	if _, err := DecryptCredentials(key, ciphertext, nonce, algorithm); err == nil {
		t.Fatal("tampered credentials decrypted")
	}
}
