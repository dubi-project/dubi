package repository

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

const (
	CredentialAlgorithmAES256GCM     uint32 = 1
	CredentialAlgorithmAES256GCMName        = "aes-256-gcm"
	credentialKeyBytes                      = 32
)

func CredentialAlgorithmName(value uint32) string {
	if value == CredentialAlgorithmAES256GCM {
		return CredentialAlgorithmAES256GCMName
	}
	return ""
}

func CredentialAlgorithmID(value string) (uint32, error) {
	if value != CredentialAlgorithmAES256GCMName {
		return 0, errors.New("unsupported Repository credential algorithm")
	}
	return CredentialAlgorithmAES256GCM, nil
}

// EncryptCredentials encrypts an opaque credential document. The caller
// supplies the recovery platform's cluster key; this package deliberately does
// not decide where that key is stored or how it is distributed to Nodes.
func EncryptCredentials(key, plaintext []byte) (ciphertext, nonce []byte, algorithm uint32, err error) {
	if len(key) != credentialKeyBytes {
		return nil, nil, 0, errors.New("Repository credential key must be 32 bytes")
	}
	if len(plaintext) == 0 || len(plaintext) > 48<<10 {
		return nil, nil, 0, errors.New("Repository credential document is empty or too large")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, 0, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, 0, err
	}
	nonce = make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, 0, err
	}
	return aead.Seal(nil, nonce, plaintext, nil), nonce, CredentialAlgorithmAES256GCM, nil
}

func DecryptCredentials(key, ciphertext, nonce []byte, algorithm uint32) ([]byte, error) {
	if len(key) != credentialKeyBytes || algorithm != CredentialAlgorithmAES256GCM {
		return nil, errors.New("unsupported Repository credential envelope")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(nonce) != aead.NonceSize() {
		return nil, errors.New("invalid Repository credential nonce")
	}
	if len(ciphertext) == 0 || len(ciphertext) > 64<<10 {
		return nil, errors.New("invalid Repository credential ciphertext")
	}
	return aead.Open(nil, nonce, ciphertext, nil)
}
