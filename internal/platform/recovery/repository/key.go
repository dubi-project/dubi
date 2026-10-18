package repository

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	RepositoryKeyFileName = "DUBI_REPOSITORY_KEY"
	repositoryKeyBytes    = 32
)

// EnsureRepositoryKey creates the Store-local key once and returns it. The
// key is deliberately outside Root state and Repository JSON; only encrypted
// credential envelopes enter the replicated cluster metadata.
func EnsureRepositoryKey(store string) ([]byte, error) {
	if err := validateStorePath(store); err != nil {
		return nil, err
	}
	path := filepath.Join(store, RepositoryKeyFileName)
	key := make([]byte, repositoryKeyBytes)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return LoadRepositoryKey(store)
		}
		return nil, fmt.Errorf("create Repository key: %w", err)
	}
	if _, err := file.Write(key); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	if err := syncRepositoryDirectory(store); err != nil {
		return nil, err
	}
	return key, nil
}

// LoadRepositoryKey reads the existing Store-local key. It never generates a
// replacement silently: losing the key is an explicit credential recovery
// operation, not a reason to decrypt old Repository records with a new key.
func LoadRepositoryKey(store string) ([]byte, error) {
	if err := validateStorePath(store); err != nil {
		return nil, err
	}
	path := filepath.Join(store, RepositoryKeyFileName)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() != repositoryKeyBytes {
		return nil, errors.New("invalid Repository key file")
	}
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(key) != repositoryKeyBytes {
		return nil, errors.New("invalid Repository key length")
	}
	return key, nil
}

// InstallRepositoryKey installs a key received over an authenticated internal
// channel. An existing different key is rejected so a Node cannot silently
// switch Repository credentials because of a stale or misrouted transfer.
func InstallRepositoryKey(store string, key []byte) error {
	if err := validateStorePath(store); err != nil {
		return err
	}
	if len(key) != repositoryKeyBytes {
		return errors.New("invalid Repository key length")
	}
	path := filepath.Join(store, RepositoryKeyFileName)
	if existing, err := LoadRepositoryKey(store); err == nil {
		if !bytes.Equal(existing, key) {
			return errors.New("Repository key conflicts with the existing Store key")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(store, ".repository-key-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(key); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			installed, readErr := LoadRepositoryKey(store)
			if readErr != nil || !bytes.Equal(installed, key) {
				return errors.New("Repository key conflicts with the existing Store key")
			}
			return nil
		}
		return err
	}
	return syncRepositoryDirectory(store)
}

func validateStorePath(store string) error {
	if store == "" || !filepath.IsAbs(store) || filepath.Clean(store) != store {
		return errors.New("Repository key requires a clean absolute Store path")
	}
	info, err := os.Stat(store)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("Repository key Store path is not a directory")
	}
	return nil
}

func syncRepositoryDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
